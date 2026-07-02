//go:build n1ql

//  Copyright (c) 2026 Couchbase, Inc.
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

// Phase 2 FTS: an embedded-bleve full-text index, advertised to the cbq planner so
// a `SELECT ... WHERE SEARCH(keyspace, "query")` runs locally against bleve --
// no cbft cluster, no n1fty. See DESIGN-indexing.md "Phase 2".
//
// Like the GSI work this is entirely n1k1-side with ZERO fork edits: the planner
// gathers FTS indexes from keyspace.Indexers() (an indexer whose Name() is
// datastore.FTS), so we advertise a bleve-backed datastore.FTSIndex by wrapping
// the keyspace (siKeyspace also grows an ftsIndexer). Definitions live in
// .n1k1/catalog.json with "kind":"fts"; the bleve index is built into
// .n1k1/<ns>/<ks>/idx/<name>__fts__<defhash>/bleve/ from a full keyspace scan,
// rebuilt when the source signature changes (same static-data model as gsi).
//
// Shipped: explicit SEARCH() (Sargable + Search); SEARCH_SCORE()/SEARCH_META()
// (the hit score/meta surfaced via value.ATT_SMETA -- see DatastoreScanFTS);
// declared field mappings (a def with "keys" indexes exactly those fields; empty
// keys stays dynamic -- see ftsMapping); and the implicit-predicate "flex" path
// (SargableFlex translates a plain WHERE predicate to a bleve query). Not yet:
// per-field analyzers/types (declared fields are all mapped as text).

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	bleve "github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/records"
)

const ftsSigFile = "sig" // <instDir>/sig -- source signature for freshness

// ---------------------------------------------------------------- FTS indexer

// ftsIndexer is a read-only datastore.Indexer of type FTS advertising the
// keyspace's bleve-backed full-text indexes.
type ftsIndexer struct {
	ks      *siKeyspace
	indexes []*ftsIndex
}

func (ix *ftsIndexer) BucketId() string          { return "" }
func (ix *ftsIndexer) ScopeId() string           { return "" }
func (ix *ftsIndexer) KeyspaceId() string        { return ix.ks.Id() }
func (ix *ftsIndexer) Name() datastore.IndexType { return datastore.FTS }

func (ix *ftsIndexer) IndexIds() ([]string, errors.Error) {
	rv := make([]string, len(ix.indexes))
	for i, fi := range ix.indexes {
		rv[i] = fi.Id()
	}
	return rv, nil
}

func (ix *ftsIndexer) IndexNames() ([]string, errors.Error) {
	rv := make([]string, len(ix.indexes))
	for i, fi := range ix.indexes {
		rv[i] = fi.Name()
	}
	return rv, nil
}

func (ix *ftsIndexer) IndexById(id string) (datastore.Index, errors.Error) {
	return ix.IndexByName(id)
}

func (ix *ftsIndexer) IndexByName(name string) (datastore.Index, errors.Error) {
	for _, fi := range ix.indexes {
		if fi.Name() == name {
			return fi, nil
		}
	}
	return nil, errors.NewError(nil, "fts: no index "+name)
}

func (ix *ftsIndexer) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) { return nil, nil }

func (ix *ftsIndexer) Indexes() ([]datastore.Index, errors.Error) {
	rv := make([]datastore.Index, len(ix.indexes))
	for i, fi := range ix.indexes {
		rv[i] = fi
	}
	return rv, nil
}

func (ix *ftsIndexer) CreatePrimaryIndex(requestId, name string, with value.Value) (
	datastore.PrimaryIndex, errors.Error) {
	return nil, errors.NewError(nil, "fts: CreatePrimaryIndex not supported")
}

func (ix *ftsIndexer) CreateIndex(requestId, name string, seekKey, rangeKey expression.Expressions,
	where expression.Expression, with value.Value) (datastore.Index, errors.Error) {
	return nil, errors.NewError(nil, "fts: CREATE INDEX not supported (declare in .n1k1/catalog.json)")
}

func (ix *ftsIndexer) BuildIndexes(requestId string, names ...string) errors.Error       { return nil }
func (ix *ftsIndexer) Refresh() errors.Error                                             { return nil }
func (ix *ftsIndexer) MetadataVersion() uint64                                           { return 0 }
func (ix *ftsIndexer) SetLogLevel(level logging.Level)                                   {}
func (ix *ftsIndexer) SetConnectionSecurityConfig(c *datastore.ConnectionSecurityConfig) {}

// ------------------------------------------------------------------ FTS index

// ftsIndex is a bleve-backed datastore.FTSIndex.
type ftsIndex struct {
	ks  *siKeyspace
	def *indexDef
	idx bleve.Index
}

var _ datastore.FTSIndex = (*ftsIndex)(nil)

func (fi *ftsIndex) KeyspaceId() string               { return fi.ks.Id() }
func (fi *ftsIndex) Id() string                       { return fi.def.Name }
func (fi *ftsIndex) Name() string                     { return fi.def.Name }
func (fi *ftsIndex) Type() datastore.IndexType        { return datastore.FTS }
func (fi *ftsIndex) Indexer() datastore.Indexer       { return fi.ks.ftsIndexerL() }
func (fi *ftsIndex) SeekKey() expression.Expressions  { return nil }
func (fi *ftsIndex) RangeKey() expression.Expressions { return nil }
func (fi *ftsIndex) Condition() expression.Expression { return nil }
func (fi *ftsIndex) IsPrimary() bool                  { return false }

// Scan satisfies the embedded datastore.Index; an FTS index is queried via
// Search() (plan.IndexFtsSearch), never a range Scan, so this is a no-op.
func (fi *ftsIndex) Scan(requestId string, span *datastore.Span, distinct bool,
	limit int64, cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	conn.Sender().Close()
}

func (fi *ftsIndex) State() (datastore.IndexState, string, errors.Error) {
	return datastore.ONLINE, "", nil
}
func (fi *ftsIndex) Statistics(requestId string, span *datastore.Span) (
	datastore.Statistics, errors.Error) {
	return nil, nil
}
func (fi *ftsIndex) Drop(requestId string) errors.Error {
	return errors.NewError(nil, "fts: DROP INDEX not supported (edit .n1k1/catalog.json)")
}

// Sargable qualifies this index for a SEARCH(field, query) predicate. The bleve
// dynamic mapping indexes every field, so any field/query qualifies. exact=true so
// the planner drops the residual SEARCH() filter -- essential, because n1k1 has no
// way to re-evaluate SEARCH() outside the index (it would return false and drop
// every row).
func (fi *ftsIndex) Sargable(field string, query, options expression.Expression,
	mappings interface{}) (nkeys int, size int64, exact, knn bool, omappings interface{}, err errors.Error) {
	return 1, 0, true, false, mappings, nil
}

// Pageable: v1 doesn't push order/offset/limit into bleve (n1k1 applies LIMIT
// downstream), so decline.
func (fi *ftsIndex) Pageable(order []string, offset, limit int64,
	query, options expression.Expression) bool {
	return false
}

// SargableFlex is the implicit-predicate -> FTS "flex" path: it lets a plain
// WHERE predicate (no explicit SEARCH()) be served by this bleve index. It
// translates the sargable part of req.Pred into a bleve query DSL; the planner
// wraps that as a synthetic SEARCH(keyspace, <query>) and plans a
// plan.IndexFtsSearch (which DatastoreScanFTS already runs). Returning nil (no
// translatable predicate) just declines the flex path.
//
// We deliberately never set FTS_FLEXINDEX_EXACT, so the planner keeps the original
// predicate in the residual Filter -- which n1k1 re-evaluates against the fetched
// doc. That makes correctness independent of translation precision: the bleve query
// need only be a superset (candidate filter), and the residual Filter narrows it
// exactly. (Contrast the explicit SEARCH() path, whose residual SEARCH() n1k1
// can't re-evaluate, so it must be exact + stripped.)
func (fi *ftsIndex) SargableFlex(requestId string, req *datastore.FTSFlexRequest) (
	*datastore.FTSFlexResponse, errors.Error) {
	if req == nil || req.Pred == nil {
		return nil, nil
	}
	q, sarged, ok := fi.flexTranslate(req.Pred, req.Keyspace)
	if !ok || len(sarged) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(q)
	if err != nil {
		return nil, nil
	}
	// Options name the index, matching n1fty. Also, the planner builds
	// search.NewSearch(ident, query, options) and only parses options when
	// SearchOptions != "" -- an empty string leaves a nil options operand that its
	// later Equivalent() comparison dereferences (panics), so always return one.
	opts, err := json.Marshal(map[string]interface{}{"index": fi.def.Name})
	if err != nil {
		return nil, nil
	}
	return &datastore.FTSFlexResponse{
		SearchQuery:    string(raw), // a JSON object literal -- also valid N1QL, so the planner can parse it
		SearchOptions:  string(opts),
		StaticSargKeys: sarged,
		RespFlags:      0, // never EXACT: the residual Filter re-checks the predicate
		NumIndexedKeys: uint32(len(sarged)),
	}, nil
}

// flexTranslate converts a predicate subtree into a bleve query DSL fragment
// (map[string]interface{}, JSON-marshalable) plus the field->expression paths it
// sarged. ok=false means "not translatable" -- the caller decides how to handle it:
// an AND may drop an untranslatable conjunct (the result stays a superset, which the
// residual Filter narrows), but an OR must bail entirely if any disjunct is
// untranslatable (dropping a disjunct would wrongly narrow the result). Only Eq / LT
// / LE (>, >= are parsed as LT/LE with swapped operands) over indexed fields are
// handled; everything else declines.
func (fi *ftsIndex) flexTranslate(e expression.Expression, alias string) (
	q map[string]interface{}, sarged map[string]expression.Expression, ok bool) {
	switch t := e.(type) {
	case *expression.And:
		conj := make([]interface{}, 0, len(t.Operands()))
		sarged = map[string]expression.Expression{}
		for _, c := range t.Operands() {
			cq, cs, cok := fi.flexTranslate(c, alias)
			if !cok {
				continue // drop untranslatable conjunct -> superset (residual re-checks)
			}
			conj = append(conj, cq)
			for k, v := range cs {
				sarged[k] = v
			}
		}
		if len(conj) == 0 {
			return nil, nil, false
		}
		if len(conj) == 1 {
			return conj[0].(map[string]interface{}), sarged, true
		}
		return map[string]interface{}{"conjuncts": conj}, sarged, true
	case *expression.Or:
		disj := make([]interface{}, 0, len(t.Operands()))
		sarged = map[string]expression.Expression{}
		for _, c := range t.Operands() {
			cq, cs, cok := fi.flexTranslate(c, alias)
			if !cok {
				return nil, nil, false // OR must translate every branch or none
			}
			disj = append(disj, cq)
			for k, v := range cs {
				sarged[k] = v
			}
		}
		if len(disj) == 0 {
			return nil, nil, false
		}
		return map[string]interface{}{"disjuncts": disj}, sarged, true
	case *expression.Eq:
		return fi.flexCompare(t.Operands()[0], t.Operands()[1], "eq", alias)
	case *expression.LT:
		return fi.flexCompare(t.Operands()[0], t.Operands()[1], "lt", alias)
	case *expression.LE:
		return fi.flexCompare(t.Operands()[0], t.Operands()[1], "le", alias)
	}
	return nil, nil, false
}

// flexCompare translates one comparison (field OP const, in either operand order)
// into a bleve clause. The field must be indexed by this index (fieldIndexed).
func (fi *ftsIndex) flexCompare(a, b expression.Expression, op, alias string) (
	map[string]interface{}, map[string]expression.Expression, bool) {
	// Identify the field side and the constant side (comparisons are field OP
	// const or const OP field).
	fp, fok := fieldPath(a)
	fieldExpr, constExpr := a, b
	fieldFirst := true
	if !fok || b.Value() == nil {
		fp, fok = fieldPath(b)
		fieldExpr, constExpr = b, a
		fieldFirst = false
	}
	if !fok || constExpr.Value() == nil {
		return nil, nil, false
	}
	// Predicate field refs are keyspace-qualified (e.g. `d.age` -> ["d","age"]);
	// bleve indexed the doc fields unqualified, so drop the leading alias to get the
	// bleve field path ("age", or "addr.city" for a nested key).
	if len(fp) > 1 && fp[0] == alias {
		fp = fp[1:]
	}
	field := strings.Join(fp, ".")
	if !fi.fieldIndexed(field) {
		return nil, nil, false
	}
	clause := bleveClause(field, constExpr.Value(), op, fieldFirst)
	if clause == nil {
		return nil, nil, false
	}
	return clause, map[string]expression.Expression{field: fieldExpr}, true
}

// bleveClause builds a single bleve DSL clause for `field OP const`. Equality on a
// string becomes a match query (analyzer-consistent); equality on a number a
// point range; <,<=,>,>= become numeric range bounds (fieldFirst distinguishes
// `field < c` from `c < field`). Non-numeric ranges decline (nil) -- the residual
// Filter still enforces them.
func bleveClause(field string, cv value.Value, op string, fieldFirst bool) map[string]interface{} {
	switch op {
	case "eq":
		switch a := cv.Actual().(type) {
		case string:
			return map[string]interface{}{"field": field, "match": a}
		case float64:
			return map[string]interface{}{"field": field, "min": a, "max": a,
				"inclusive_min": true, "inclusive_max": true}
		case bool:
			return map[string]interface{}{"field": field, "bool": a}
		}
		return nil
	case "lt", "le":
		n, isNum := cv.Actual().(float64)
		if !isNum {
			return nil
		}
		incl := op == "le"
		// `field < c` bounds the max; `c < field` (fieldFirst=false) bounds the min.
		if fieldFirst {
			return map[string]interface{}{"field": field, "max": n, "inclusive_max": incl}
		}
		return map[string]interface{}{"field": field, "min": n, "inclusive_min": incl}
	}
	return nil
}

// fieldIndexed reports whether a field path is indexed by this index: a dynamic
// mapping (no declared keys) indexes every field; a declared mapping indexes only
// its listed field-path keys. For fts, def.Keys are the field-path strings
// themselves (e.g. "title", "addr.city") -- the same form as the bleve field path
// -- so compare directly (an fts def parses no key expressions; see si_catalog).
func (fi *ftsIndex) fieldIndexed(field string) bool {
	if len(fi.def.Keys) == 0 {
		return true
	}
	for _, k := range fi.def.Keys {
		if k == field {
			return true
		}
	}
	return false
}

// Search runs the SEARCH() query against the bleve index and sends one IndexEntry
// per hit (PrimaryKey = docID, MetaData = score). The drain in DatastoreScanFTS
// reads PrimaryKey (and score) exactly like the GSI scan path.
func (fi *ftsIndex) Search(requestId string, si *datastore.FTSSearchInfo,
	cons datastore.ScanConsistency, vector timestamp.Vector, conn *datastore.IndexConnection) {
	defer conn.Sender().Close()

	q, err := bleveQuery(si)
	if err != nil {
		conn.Error(errors.NewError(err, "fts search query"))
		return
	}
	req := bleve.NewSearchRequest(q)
	// bleve defaults Size to 10; we want all matches (n1k1 applies any LIMIT). Cap
	// at the index's doc count so a broad query still returns everything.
	if n, e := fi.idx.DocCount(); e == nil && n > 0 {
		req.Size = int(n)
	} else {
		req.Size = 10000
	}
	res, serr := fi.idx.Search(req)
	if serr != nil {
		conn.Error(errors.NewError(serr, "fts search"))
		return
	}
	for _, hit := range res.Hits {
		if !conn.Sender().SendEntry(&datastore.IndexEntry{
			PrimaryKey: hit.ID,
			MetaData:   value.NewValue(hit.Score),
		}) {
			break
		}
	}
}

// bleveQuery builds a bleve query from the SEARCH() info: a plain string becomes a
// match query on the named field (or a query-string query over all fields when no
// field is given); a JSON object is parsed as a raw bleve query.
func bleveQuery(si *datastore.FTSSearchInfo) (query.Query, error) {
	field := ""
	if si.Field != nil {
		if s, ok := si.Field.Actual().(string); ok {
			// The field arrives as a N1QL identifier path (e.g. `title`, `a`.`b`);
			// strip the backticks to get bleve's field path (a.b).
			field = strings.ReplaceAll(s, "`", "")
		}
	}
	if si.Query == nil {
		return nil, fmt.Errorf("empty SEARCH query")
	}
	switch qv := si.Query.Actual().(type) {
	case string:
		if field != "" {
			mq := bleve.NewMatchQuery(qv)
			mq.SetField(field)
			return mq, nil
		}
		return bleve.NewQueryStringQuery(qv), nil
	default:
		// An object: a bleve query DSL (e.g. from the flex path's translated
		// predicate, or an explicit SEARCH(ks, {...})). Parse it as bleve's query
		// DSL rather than treating the JSON text as a query string.
		raw, e := json.Marshal(qv)
		if e != nil {
			return nil, e
		}
		return query.ParseQuery(raw)
	}
}

// ftsMapping builds the bleve index mapping for a def. With no declared keys it
// returns the default dynamic mapping (index every field). With declared field
// keys it returns a non-dynamic mapping that indexes exactly those fields (nested
// dotted paths like "addr.city" become sub-document mappings) as text -- so the
// index is scoped to the declared fields and a SEARCH() on any other field matches
// nothing, honoring the catalog definition. (Per-field analyzers/types are a
// future item; v1 maps every declared field as text.)
func ftsMapping(def *indexDef) mapping.IndexMapping {
	im := bleve.NewIndexMapping()
	if len(def.Keys) == 0 {
		return im // dynamic: index every field
	}
	im.DefaultMapping.Dynamic = false
	for _, key := range def.Keys {
		parts := strings.Split(key, ".")
		dm := im.DefaultMapping
		for _, p := range parts[:len(parts)-1] {
			sub := dm.Properties[p]
			if sub == nil {
				sub = bleve.NewDocumentMapping()
				sub.Dynamic = false
				dm.AddSubDocumentMapping(p, sub)
			}
			dm = sub
		}
		dm.AddFieldMappingsAt(parts[len(parts)-1], bleve.NewTextFieldMapping())
	}
	return im
}

// ------------------------------------------------------------------- build

// openFTSIndexes caches opened bleve indexes by directory across the process
// (bleve, like bbolt, takes an exclusive lock on its dir), matching openIndexes.
var openFTSIndexes = struct {
	sync.Mutex
	m map[string]*ftsSlot
}{m: map[string]*ftsSlot{}}

type ftsSlot struct {
	once sync.Once
	mu   sync.Mutex
	fi   *ftsIndex
	err  error
}

func ftsSlotFor(dir string) *ftsSlot {
	openFTSIndexes.Lock()
	defer openFTSIndexes.Unlock()
	s := openFTSIndexes.m[dir]
	if s == nil {
		s = &ftsSlot{}
		openFTSIndexes.m[dir] = s
	}
	return s
}

// openFTSIndex opens (building/rebuilding as needed) the bleve index backing def
// on ks. Cached per bleve dir; a stale source signature triggers a rebuild.
func openFTSIndex(ks *siKeyspace, def *indexDef, onDoc func(int), force bool) (*ftsIndex, error) {
	ns := ks.Namespace().Name()
	instDir := filepath.Join(ks.sds.root, sidecarDir, ns, ks.Name(), "idx",
		fmt.Sprintf("%s__fts__%s", fsSafe(def.Name), def.defHash()))
	bleveDir := filepath.Join(instDir, "bleve")
	srcDir := filepath.Join(ks.sds.root, ns, ks.Name())

	slot := ftsSlotFor(bleveDir)

	slot.once.Do(func() {
		if e := os.MkdirAll(instDir, 0o755); e != nil {
			slot.err = e
			return
		}
		sig, e := sourceSignature(srcDir)
		if e != nil {
			slot.err = e
			return
		}
		idx, e := openOrBuildBleve(bleveDir, instDir, ks, def, srcDir, sig, onDoc)
		if e != nil {
			slot.err = e
			return
		}
		slot.fi = &ftsIndex{ks: ks, def: def, idx: idx}
	})
	if slot.err != nil {
		return nil, slot.err
	}

	// Per-call freshness recheck (serialized per index).
	slot.mu.Lock()
	defer slot.mu.Unlock()
	sig, err := sourceSignature(srcDir)
	if err != nil {
		return nil, err
	}
	if force || readFTSSig(instDir) != sig {
		slot.fi.idx.Close()
		idx, e := buildBleve(bleveDir, instDir, ks, def, srcDir, sig, onDoc)
		if e != nil {
			return nil, e
		}
		slot.fi.idx = idx
	}
	slot.fi.ks = ks
	return slot.fi, nil
}

// openOrBuildBleve opens the bleve dir if it exists and is fresh, else builds it.
func openOrBuildBleve(bleveDir, instDir string, ks *siKeyspace, def *indexDef,
	srcDir, sig string, onDoc func(int)) (bleve.Index, error) {
	if _, err := os.Stat(bleveDir); err == nil && readFTSSig(instDir) == sig {
		if idx, e := bleve.Open(bleveDir); e == nil {
			return idx, nil
		}
		// Fall through to rebuild on a corrupt/unopenable dir.
	}
	return buildBleve(bleveDir, instDir, ks, def, srcDir, sig, onDoc)
}

// buildBleve (re)creates the bleve index from a full keyspace scan.
func buildBleve(bleveDir, instDir string, ks *siKeyspace, def *indexDef,
	srcDir, sig string, onDoc func(int)) (bleve.Index, error) {
	if err := os.RemoveAll(bleveDir); err != nil {
		return nil, err
	}
	idx, err := bleve.New(bleveDir, ftsMapping(def))
	if err != nil {
		return nil, fmt.Errorf("fts build, bleve.New %q: %w", bleveDir, err)
	}

	opts := ScanWalkOptions
	opts.PathPrefix = ""
	src, err := records.Walk(srcDir, opts)
	if err != nil {
		idx.Close()
		return nil, fmt.Errorf("fts build, walk %q: %w", srcDir, err)
	}
	defer src.Close()

	batch := idx.NewBatch()
	var rec records.Record
	scanned := 0
	for {
		ok, err := src.Next(&rec)
		if err != nil {
			idx.Close()
			return nil, fmt.Errorf("fts build, next: %w", err)
		}
		if !ok {
			break
		}
		var doc interface{}
		if err := json.Unmarshal(rec.Doc, &doc); err != nil {
			continue // skip undecodable docs
		}
		if err := batch.Index(string(rec.ID), doc); err != nil {
			idx.Close()
			return nil, fmt.Errorf("fts build, index %q: %w", rec.ID, err)
		}
		scanned++
		if scanned%512 == 0 {
			if err := idx.Batch(batch); err != nil {
				idx.Close()
				return nil, err
			}
			batch.Reset()
			if onDoc != nil {
				onDoc(scanned)
			}
		}
	}
	if err := idx.Batch(batch); err != nil {
		idx.Close()
		return nil, err
	}
	if onDoc != nil {
		onDoc(scanned)
	}
	if err := writeFTSSig(instDir, sig); err != nil {
		idx.Close()
		return nil, err
	}
	return idx, nil
}

func readFTSSig(instDir string) string {
	b, _ := os.ReadFile(filepath.Join(instDir, ftsSigFile))
	return string(b)
}

func writeFTSSig(instDir, sig string) error {
	return os.WriteFile(filepath.Join(instDir, ftsSigFile), []byte(sig), 0o644)
}

// fillFTSInfo populates an IndexInfo for a built fts index (for .index list/show).
func (fi *ftsIndex) fillInfo(info *IndexInfo) {
	info.Built = true
	if n, err := fi.idx.DocCount(); err == nil {
		info.Entries = int(n)
	}
	if p := fi.idx.Name(); p != "" {
		info.Path = p
	}
}
