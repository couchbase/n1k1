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
		false, // useFts
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
	qp, _, err, _ := planner.Build(s, g.Datastore, g.Systemstore,
		namespace, subquery, stream, false /* forceSQBuild */, &pc)
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
	// Single-file arg (DESIGN-data.md scenario B2): `n1k1 events.jsonl`. The fork's
	// file datastore ReadDir's its root, so it can't be a file -- build it against
	// the file's parent dir and remember the file to wrap below.
	dsPath, flatFile := path, ""
	if fi, err := os.Stat(path); err == nil && fi.Mode().IsRegular() &&
		records.IsRecordFile(path) {
		dsPath, flatFile = filepath.Dir(path), path
	}

	ds, err := file.NewDatastore(dsPath)
	if err != nil {
		return nil, err
	}

	if flatFile != "" {
		// Single file: fake a synthetic default:<stem> keyspace reading just it.
		ds = maybeFlatFile(flatFile, ds)
	} else {
		// Flat root (data files directly under path, no ns/keyspace subdirs): fake a
		// synthetic default:<basename> keyspace so `FROM <basename>` plans. No-op for
		// the normal <ns>/<keyspace> layout. See flatroot.go.
		ds = maybeFlatRoot(path, ds)
	}

	return &Store{
		Datastore:       ds,
		Systemstore:     nil,
		IndexApiVersion: datastore.INDEX_API_MAX,
		FeatureControls: util.DEF_N1QL_FEAT_CTRL,
	}, nil
}
