//go:build n1ql

//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package glue

import (
	"os"
	"path/filepath"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/file"
	"github.com/couchbase/query/functions"
	"github.com/couchbase/query/parser/n1ql"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/planner"
	"github.com/couchbase/query/rewrite"
	"github.com/couchbase/query/semantics"
	"github.com/couchbase/query/settings"
	"github.com/couchbase/query/util"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/records"
)

// The query server normally calls functions.FunctionsInit() at startup to
// allocate the user-defined-function cache. n1k1 runs server-less and drops the
// functions/constructor subsystem to stay pure-Go, so that init never happens
// and functions.cache stays nil. The parser still routes an unknown function
// (e.g. SELECT array_vg(...)) through the UDF-resolution fallback, which reads
// functions.cache and would nil-panic. Initialize the cache here (pure-Go: just
// a util.GenCache) with storage-backed UDF loading disabled, so an unknown
// function resolves to a clean "Invalid function ..." parse error instead.
func init() {
	functions.FunctionsInit(1, func() bool { return false })
}

// ParseStatement parses and checks semantics on a N1QL statement.
func ParseStatement(stmt, namespace string, ent bool) (algebra.Statement, error) {
	queryContext := "" // TODO.

	s, err := n1ql.ParseStatement2(stmt, namespace, queryContext)
	if err != nil {
		return nil, err
	}

	txn := false // TODO.

	_, err = s.Accept(semantics.GetSemChecker(s.Type(), txn))
	if err != nil {
		return nil, err
	}

	// Standard pre-plan rewrite (REWRITE_PHASE1) -- cbq's sanitizer/server always run
	// this between semantics and planning. It resolves named WINDOW clause references
	// (`... OVER w ... WINDOW w AS (...)`) into their partition/order/frame, so the
	// frame applies instead of defaulting to the whole partition.
	if _, err = s.Accept(rewrite.NewRewrite(rewrite.REWRITE_PHASE1)); err != nil {
		return nil, err
	}

	return s, nil
}

// ------------------------------------------------------------------

// Store represents a datastore/systemstore configuration for
// processing N1QL statements.
type Store struct {
	Datastore       datastore.Datastore
	Systemstore     datastore.Systemstore
	IndexApiVersion int
	FeatureControls uint64
}

// ------------------------------------------------------------------

// InitParser initializes the global n1ql parser with store info.
func (g *Store) InitParser() error {
	ns, err := g.Datastore.NamespaceNames()
	if err != nil {
		return err
	}

	nsm := make(map[string]interface{}, len(ns))
	for i, _ := range ns {
		nsm[ns[i]] = true
	}

	n1ql.SetNamespaces(nsm)

	datastore.SetDatastore(g.Datastore)

	return nil
}

// ------------------------------------------------------------------

// PlanStatement returns a plan.Operator tree for a statement.
func (g *Store) PlanStatement(s algebra.Statement, namespace string,
	namedArgs map[string]value.Value, positionalArgs value.Values) (
	plan.Operator, error) {
	qp, err := g.PlanStatementQP(s, namespace, namedArgs, positionalArgs)
	if err != nil {
		return nil, err
	}
	return qp.PlanOp(), nil
}

// PlanStatementQP is PlanStatement but returns the whole *plan.QueryPlan, so
// callers can reach qp.Subqueries() (the pre-planned sub-SELECT operators the
// planner builds for expression subqueries) -- which qp.PlanOp() alone drops.
func (g *Store) PlanStatementQP(s algebra.Statement, namespace string,
	namedArgs map[string]value.Value, positionalArgs value.Values) (
	*plan.QueryPlan, error) {
	// PREPARE has no home in n1k1 (no prepared-statement store to save into), and
	// planner.Build nil-derefs on a *algebra.Prepare -- its prepare path assumes
	// infrastructure the CE build lacks. Reject it cleanly here so it surfaces as a
	// graceful "unsupported", not a recovered "panic:" (an engine-bug signal).
	if _, ok := s.(*algebra.Prepare); ok {
		return nil, &ErrUnsupported{Reason: "PREPARE not supported (no prepared-statement store)"}
	}
	// UDF statements (CREATE/DROP/EXECUTE FUNCTION) route through the functions
	// registry subsystem, which n1k1's CE build doesn't initialize -- planner.Build
	// nil-derefs in CreateFunction.Privileges (functions.go). Reject cleanly (same
	// rationale as PREPARE) rather than crash.
	switch s.(type) {
	case *algebra.CreateFunction, *algebra.DropFunction, *algebra.ExecuteFunction:
		return nil, &ErrUnsupported{Reason: "user-defined functions not supported (no functions registry)"}
	}

	var subquery bool
	var stream bool

	var pc planner.PrepareContext

	planner.NewPrepareContext(&pc,
		"requestId-0",    // requestId
		"queryContext-0", // queryContext
		namedArgs,
		positionalArgs,
		g.IndexApiVersion,
		g.FeatureControls,
		true,  // useFts -- let the planner use bleve FTS indexes for SEARCH() (idx_fts.go)
		false, // useCBO
		nil,   // optimizer
		nil,   // deltaKeyspaces
		nil,   // dsContext
		false, // isPrepare
		settings.PS_MODE_OFF,
		settings.PS_ERROR_FLEXIBLE,
		datastore.UNBOUNDED, // scanConsistency
	)

	// planner.Build returns a *plan.QueryPlan (+ a 4th duration map).
	//
	// forceSQBuild=true: pre-plan expression subqueries into qp.Subqueries() (the
	// planner otherwise does this only for EXPLAIN/ADVISE). n1k1's subquery
	// evaluator prefers these in-context sub-plans over re-planning each subquery
	// standalone -- standalone loses the outer keyspace scope, which degenerates a
	// correlated index span (e.g. `META(d).id = t.to`) to a null bound that
	// silently returns no rows. See glue/subquery.go compile().
	qp, _, err, _ := planner.Build(s, g.Datastore, g.Systemstore,
		namespace, subquery, stream, true /* forceSQBuild */, &pc)
	if err != nil {
		return nil, err
	}

	return qp, nil
}

// ------------------------------------------------------------------

// FileStore returns a store instance based on a file datastore.
//
// Systemstore is intentionally nil: the query/datastore/system package pulls in
// query/server -> indexing (cgo) via query/aus, which we drop to keep n1k1
// pure-Go. Queries that don't reference the system: namespace don't need it.
func FileStore(path string) (*Store, error) {
	return FileStoreBound(path, nil)
}

// FileStoreBound is FileStore with a per-bundle late-binding manifest installed: a
// logical keyspace name a detector corpus references (`FROM <logical>`) resolves to
// the manifest's glob pattern at bind time, so the SAME corpus runs against a new,
// differently-named bundle by re-binding to its root (DESIGN-prepare.md late binding;
// see binding.go). A nil/empty manifest is exactly FileStore (the wrapper is a no-op).
func FileStoreBound(path string, b Binding) (*Store, error) {
	// Single-file arg (DESIGN-data.md scenario B2): `n1k1 events.jsonl`. The fork's
	// file datastore ReadDir's its root, so it can't be a file -- build it against
	// the file's parent dir and remember the file to wrap below.
	dsPath, flatFile := path, ""
	if fi, err := os.Stat(path); err == nil && fi.Mode().IsRegular() &&
		records.IsRecordFile(path) {
		dsPath, flatFile = filepath.Dir(path), path
	}

	// Resolve a symlinked data-root to its real path. cbcollect bundles are commonly
	// symlinked (`support-bundle-ex01 -> cbcollect_info_...`), and filepath.Walk (used
	// by records glob/flat-root scans) won't descend a symlinked ROOT -- so an
	// unresolved symlink root silently yields zero rows. Resolving here makes every
	// downstream path (file datastore URL, flat-root RecordsDir, glob base) real.
	if resolved, rerr := filepath.EvalSymlinks(dsPath); rerr == nil {
		dsPath = resolved
	}

	ds, err := file.NewDatastore(dsPath)
	if err != nil {
		return nil, err
	}

	// Remember this datastore's root so a recipe describe of a file under it can find
	// its .n1k1 sidecar to memoize the ExtractSpec/SortedSourceMeta (extract_cache.go).
	registerDataRoot(dsPath)

	// Innermost: recognize inline glob keyspace names (DESIGN-data.md Mode 2b,
	// `FROM `./data/**/*.json``). Sits below the flat/secondary-index wrappers so
	// their type identities survive; they delegate unknown names down to it. Bare
	// (root-relative) globs anchor at dsPath (the data-root dir). See glob.go.
	ds = maybeGlob(dsPath, ds)

	// Late binding: layer the manifest just above the glob wrapper (still innermost
	// of the flat/si wrappers, so their type identities survive). A logical name in
	// the manifest resolves to its glob pattern; every other name delegates straight
	// down to the glob/real chain. No-op when b is empty. See binding.go.
	ds = maybeBind(dsPath, b, ds)

	if flatFile != "" {
		// Single file: fake a synthetic default:<stem> keyspace reading just it.
		ds = maybeFlatFile(flatFile, ds)
	} else {
		// Flat discovery: fake synthetic default keyspaces for loose top-level
		// record files -- union-by-basename for a pure flat root, or one keyspace
		// per file for a grab-bag dir with subdirs (e.g. ~/Desktop). No-op for the
		// normal <ns>/<keyspace> layout with no loose root files. See flat.go.
		if flat := maybeFlat(path, ds); flat != ds {
			ds = flat // flat layout (secondary indexes not wired here in v1)
		} else {
			// Normal <ns>/<keyspace> layout: advertise any secondary indexes
			// declared in .n1k1/catalog.json so selective queries plan an
			// IndexScan instead of a full primary scan. See idx_si.go.
			wrapped, werr := maybeSecondaryIndexes(path, ds)
			if werr != nil {
				return nil, werr
			}
			ds = wrapped
		}
	}

	return &Store{
		Datastore:       ds,
		Systemstore:     nil,
		IndexApiVersion: datastore.INDEX_API_MAX,
		FeatureControls: util.DEF_N1QL_FEAT_CTRL,
	}, nil
}
