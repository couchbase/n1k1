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
	"bytes"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"
)

func DatastoreScanKeys(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	context := vars.Temps[0].(*GlueContext)

	scan := vars.Temps[o.Params[0].(int)].(*plan.KeyScan)

	// A correlated `USE KEYS <expr>` (e.g. the outer term of subqexp[2], whose
	// keys are themselves a correlated subquery) needs the outer row to resolve
	// its key expression; scanParent supplies corrParent (else the WITH scope),
	// nil at top level.
	parent := context.scanParent()

	keys, err := scan.Keys().Evaluate(parent, context)
	if err != nil {
		context.Error(errors.NewEvaluationError(err, "KEYS"))
		yieldErr(err)
		return
	}

	var valId base.Val
	var vals base.Vals

	var yieldKey func(interface{})

	yieldKey = func(k interface{}) {
		if s, ok := k.(string); ok {
			valId = strconv.AppendQuote(valId[:0], s)
			vals = append(vals[:0], valId)

			yieldVals(vals)

			return
		} else if v, ok := k.(value.Value); ok {
			yieldKey(v.Actual())

			return
		}

		context.Warning(errors.NewWarning(
			fmt.Sprintf("Document key must be string: %v", k)))
	}

	act := keys.Actual()

	if acts, ok := act.([]interface{}); ok {
		for _, key := range acts {
			yieldKey(key)
		}
	} else {
		yieldKey(act)
	}

	yieldErr(nil)
}

// -------------------------------------------------------------------

// ScanWalkOptions controls how DatastoreScanRecords discovers/decodes files
// (formats, recursion, compression). It defaults to the flexible AllModes; the
// CLI's -scan flag overrides it via records.ParseModes to lock scanning
// down (e.g. don't recurse into unwanted subdirs). Process-global to match the
// engine.ExecOpEx registration style -- fine for the single-process CLI; a
// per-store field is the cleaner future form (see DESIGN-data.md).
var ScanWalkOptions = records.AllModes()

// recordsScanPlan is the subset of the plan scan ops the n1k1-native records
// scan needs -- just the target keyspace. plan.PrimaryScan/PrimaryScan3 and
// plan.CountScan all satisfy it. A LIMIT (present on PrimaryScan*, absent on
// CountScan) is read via the optional limiter interface below.
type recordsScanPlan interface {
	Keyspace() datastore.Keyspace
}

// limiter is the optional LIMIT accessor (PrimaryScan/PrimaryScan3 have it).
type limiter interface {
	Limit() expression.Expression
}

// DatastoreScanRecords reads a file keyspace's directory n1k1-native via the
// records package (union of files, recurse subdirs, decode JSONL /
// multi-doc / single-doc JSON, transparent gzip) and yields whole documents
// directly -- the `.` label = the doc's JSON bytes and `^id` = its key. This
// replaces cbq's scan-keys + fetch-docs round-trip for the file datastore (see
// DESIGN-data.md "Where this code lives" A2): the bytes flow straight to
// base.Val = []byte with no cbq value.AnnotatedValue boxing, and multi-record
// files (which have no natural per-record key for the scan/fetch split) work.
func DatastoreScanRecords(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	context := vars.Temps[0].(*GlueContext)

	scan, ok := vars.Temps[o.Params[0].(int)].(recordsScanPlan)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreScanRecords: unexpected plan %T",
			vars.Temps[o.Params[0].(int)]))
		return
	}

	keyspace := scan.Keyspace()

	limit := int64(math.MaxInt64)
	if lim, ok := scan.(limiter); ok {
		limit = EvalExprInt64(context, lim.Limit(), nil, math.MaxInt64)
	}

	// Per-scan: set the _meta.path prefix to this keyspace's dir-relative
	// location, so metadata paths read like "default/orders/order-1001.json".
	opts := ScanWalkOptions
	opts.PathPrefix = metaPathPrefix(keyspace)

	// Resolve the keyspace to a records.Source (a single flat file if it advertises
	// RecordsFile, else its directory walked + unioned). openKeyspaceRecords is the
	// one place this resolution lives, shared with the .index suggest advisor so the
	// scan and the sampler can never disagree about where a keyspace's data is.
	src, err := openKeyspaceRecords(keyspace, opts)
	if err != nil {
		yieldErr(fmt.Errorf("DatastoreScanRecords, open %q: %v", keyspace.Name(), err))
		return
	}
	defer src.Close()

	var vals base.Vals
	var idBuf []byte
	var rec records.Record

	var n int64
	for n < limit {
		ok, err := src.Next(&rec)
		if err != nil {
			yieldErr(fmt.Errorf("DatastoreScanRecords, next: %v", err))
			return
		}
		if !ok {
			break
		}

		// `^id` must be canonical JSON (a quoted string) so Convert reads it as a
		// string, matching the fetch path. rec.Doc / rec.ID are borrowed until the
		// next Next -- fine, the engine consumes each yield synchronously.
		idBuf = strconv.AppendQuote(idBuf[:0], string(rec.ID))

		vals = append(vals[:0], base.Val(rec.Doc)) // Label ".alias".
		vals = append(vals, base.Val(idBuf))       // Label "^id".

		yieldVals(vals)
		n++
	}

	yieldErr(nil)
}

// metaPathPrefix is the keyspace's location relative to the datastore dir, used
// to prefix records' _meta.path. For a flat root the files sit directly under the
// datastore dir, so there's no prefix.
func metaPathPrefix(keyspace datastore.Keyspace) string {
	if _, ok := keyspace.(interface{ RecordsDir() string }); ok {
		return "" // flat root
	}
	if ns := keyspace.Namespace(); ns != nil {
		return ns.Name() + "/" + keyspace.Name()
	}
	return keyspace.Name()
}

// keyspaceDir resolves a file-datastore keyspace to its on-disk directory,
// <root>/<namespace>/<keyspace>, from the datastore's file:// URL. n1k1 owns
// scan/fetch execution, so it reads the directory itself rather than routing
// through cbq's ScanEntries/Fetch.
func keyspaceDir(keyspace datastore.Keyspace) (string, error) {
	// A synthetic flat-root keyspace knows its own directory (the root itself),
	// which isn't <root>/<ns>/<keyspace>. See flat.go.
	if rd, ok := keyspace.(interface{ RecordsDir() string }); ok {
		return rd.RecordsDir(), nil
	}
	ns := keyspace.Namespace()
	if ns == nil || ns.Datastore() == nil {
		return "", fmt.Errorf("keyspaceDir: keyspace %q has no datastore", keyspace.Name())
	}
	url := ns.Datastore().URL()
	root := strings.TrimPrefix(url, "file://")
	if root == url {
		return "", fmt.Errorf("keyspaceDir: non-file datastore URL %q", url)
	}
	return filepath.Join(root, ns.Name(), keyspace.Name()), nil
}

// openKeyspaceRecords opens a records.Source for a keyspace: a single flat file
// when it advertises RecordsFile (DESIGN-data.md scenario B2), otherwise its
// directory (flat-root or <root>/<ns>/<keyspace>) walked and unioned. This is the
// single resolver for "where does a keyspace's data live", used by both the scan
// op (DatastoreScanRecords) and the .index suggest sampler (sampleKeyspace) so
// they can never diverge -- the class of bug that once made .schema, which sampled
// the filesystem on its own, report 0 docs for these layouts.
func openKeyspaceRecords(ks datastore.Keyspace, opts records.WalkOptions) (records.Source, error) {
	if rf, ok := ks.(interface{ RecordsFile() string }); ok && rf.RecordsFile() != "" {
		return records.File(rf.RecordsFile(), opts)
	}
	dir, err := keyspaceDir(ks)
	if err != nil {
		return nil, err
	}
	return records.Walk(dir, opts)
}

// -------------------------------------------------------------------

func DatastoreScanPrimary(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	DatastoreScan(o, vars, yieldVals, yieldErr,
		func(context *GlueContext, conn *datastore.IndexConnection) {
			// File datastore: no scan vector, default consistency.
			if scan, ok := vars.Temps[o.Params[0].(int)].(*plan.PrimaryScan); ok {
				limit := EvalExprInt64(context, scan.Limit(), nil, math.MaxInt64)

				go scan.Index().ScanEntries(glueRequestId, limit,
					datastore.UNBOUNDED, nil, conn)
			} else if scan, ok := vars.Temps[o.Params[0].(int)].(*plan.PrimaryScan3); ok {
				offset := EvalExprInt64(context, scan.Offset(), nil, int64(0))
				limit := EvalExprInt64(context, scan.Limit(), nil, math.MaxInt64)

				var indexProjection *datastore.IndexProjection
				var indexOrder datastore.IndexKeyOrders
				var indexGroupAggs *datastore.IndexGroupAggregates

				// TODO: Handle advanced PrimaryScan3 params.

				go scan.Index().ScanEntries3(glueRequestId,
					indexProjection, offset, limit, indexGroupAggs, indexOrder,
					datastore.UNBOUNDED, nil, conn)
			}
		})
}

// -------------------------------------------------------------------

func DatastoreScanIndex(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	DatastoreScan(o, vars, yieldVals, yieldErr,
		func(context *GlueContext, conn *datastore.IndexConnection) {
			scan := vars.Temps[o.Params[0].(int)].(*plan.IndexScan)

			if si, isSI := scan.Index().(*secondaryIndex); isSI {
				scanSISpans(context, conn, scan, si, false)
				return
			}

			// A non-n1k1 index -- e.g. the file datastore's #primary used for a
			// covering IndexScan. Its interface Scan closes the sender itself,
			// so keep the original goroutine-per-span shape (single span in
			// practice) and don't add our own close (that would double-close and
			// truncate the drain to zero rows).
			limit := EvalExprInt64(context, scan.Limit(), nil, math.MaxInt64)

			// A correlated index span (e.g. `META(d).id = t.to` in subqexp[6,7])
			// evaluates its Low/High against the outer row; scanParent supplies it
			// (corrParent, else WITH scope), nil at top level and for a nested-loop
			// join outer (a separate TODO).
			outerValue := context.scanParent()

			for _, span := range scan.Spans() {
				go func(span *plan.Span) {
					dspan, empty, err := EvalSpan(context, span, outerValue)
					if err != nil || empty {
						if err != nil {
							context.Error(errors.NewEvaluationError(err, "span"))
						}
						conn.Sender().Close()
						return
					}
					scan.Index().Scan(glueRequestId, dspan, scan.Distinct(),
						limit, datastore.UNBOUNDED, nil, conn)
				}(span)
			}
		})
}

// scanSISpans launches the n1k1 secondary index scan for all of an IndexScan's
// spans in ONE goroutine sharing a single sender, closing it exactly once at the
// end. (A predicate can produce several spans -- an IN list, a same-field OR, a
// DistinctScan. A goroutine-per-span, each Close-ing the shared sender, would let
// the first to finish truncate the drain and drop the others' entries.) scanSpan
// doesn't close; docIDs are deduped across spans so overlapping ranges never
// double-emit. projectKeys threads through to decode key values for a covering
// scan (DatastoreScanIndexCovering).
func scanSISpans(context *GlueContext, conn *datastore.IndexConnection,
	scan *plan.IndexScan, si *secondaryIndex, projectKeys bool) {
	limit := EvalExprInt64(context, scan.Limit(), nil, math.MaxInt64)

	// A correlated index span evaluates its Low/High against the outer row;
	// scanParent supplies it (corrParent, else WITH scope). nil at top level and
	// for a nested-loop join outer (a separate TODO).
	outerValue := context.scanParent()

	go func() {
		defer conn.Sender().Close()
		// Dedup docIDs across the whole scan: multi-span predicates (IN / OR /
		// DistinctScan) can legitimately revisit a docID, and it's also cheap
		// insurance against a stale/rebuilt index emitting a key twice (v1 freshness
		// is a coarse mtime signature). A selective index scan's result set is
		// small, so the map cost is negligible.
		seen := map[string]bool{}
		for _, span := range scan.Spans() {
			dspan, empty, err := EvalSpan(context, span, outerValue)
			if err != nil {
				context.Error(errors.NewEvaluationError(err, "span"))
				continue
			}
			if empty {
				continue
			}
			si.scanSpan(dspan, limit, seen, projectKeys, conn)
		}
	}()
}

// DatastoreScanIndexCovering answers a covering IndexScan over an n1k1 secondary
// index straight from the index -- no fetch. It runs the scan with projectKeys so
// each entry carries its decoded key values, then reconstructs the projected
// document from the index def's field paths and emits it under the `.alias` label
// (plus `^id`) in the exact shape a fetch would -- so the peeled cover field
// accesses (stripCovers, expr.go) and META().id resolve against it identically.
// conv.go only routes here when coverableIndexScan is satisfied (all keys are
// plain field refs, no filter-covers). See DESIGN-indexing.md "true covering".
func DatastoreScanIndexCovering(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	scan := vars.Temps[o.Params[0].(int)].(*plan.IndexScan)
	si, ok := scan.Index().(*secondaryIndex)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreScanIndexCovering: index %T is not an n1k1 secondary index",
			scan.Index()))
		return
	}
	paths := si.def.keyPaths

	var docBuf bytes.Buffer
	var idBuf base.Val
	row := make(base.Vals, 2)

	buildRow := func(context *GlueContext, entry *datastore.IndexEntry) (base.Vals, error) {
		doc, err := reconstructCoverDoc(paths, entry.EntryKey, &docBuf)
		if err != nil {
			return nil, err
		}
		idBuf = strconv.AppendQuote(idBuf[:0], entry.PrimaryKey)
		row[0] = doc             // Label ".alias".
		row[1] = base.Val(idBuf) // Label "^id".
		return row, nil
	}

	datastoreScanDrain(o, vars, yieldVals, yieldErr,
		func(context *GlueContext, conn *datastore.IndexConnection) {
			scanSISpans(context, conn, scan, si, true)
		}, buildRow)
}

// reconstructCoverDoc rebuilds the projected document of a covering scan from the
// decoded index-key values and the index def's field paths (e.g. paths
// [["region"],["address","city"]] + keys [v0,v1] -> {"region":v0,"address":{"city":v1}}).
// A MISSING/absent key value leaves the field out (matching a doc that lacked it).
// The buffer is reused across rows (the drain consumes each yield synchronously).
func reconstructCoverDoc(paths [][]string, keys value.Values,
	buf *bytes.Buffer) (base.Val, error) {
	doc := value.NewValue(map[string]interface{}{})
	for i, p := range paths {
		if i >= len(keys) || len(p) == 0 {
			continue
		}
		kv := keys[i]
		if kv == nil || kv.Type() == value.MISSING {
			continue
		}
		cur := doc
		for j := 0; j < len(p)-1; j++ {
			nxt, ok := cur.Field(p[j])
			if !ok || nxt.Type() != value.OBJECT {
				m := value.NewValue(map[string]interface{}{})
				if err := cur.SetField(p[j], m); err != nil {
					return nil, err
				}
				nxt = m
			}
			cur = nxt
		}
		if err := cur.SetField(p[len(p)-1], kv); err != nil {
			return nil, err
		}
	}
	buf.Reset()
	if err := doc.WriteJSON(nil, buf, "", "", true); err != nil {
		return nil, err
	}
	return base.Val(buf.Bytes()), nil
}

// -------------------------------------------------------------------

// DatastoreScanFTS drives an FTS scan (plan.IndexFtsSearch): it runs the
// bleve-backed index's Search, then fetches the matching docs itself and emits one
// row per hit as `.alias` (the doc) + `^id` + `^smeta`. The `^smeta` carries
// {outname: {score, id}}, which ConvertVals attaches under value.ATT_SMETA on the
// alias value so SEARCH_SCORE()/SEARCH_META() resolve (they read that attachment;
// see expr.go). It fetches here rather than leaving it to a following plan.Fetch
// because the hit score is only available at the scan and would be lost across a
// separate fetch op -- so VisitIndexFtsSearch emits no synth fetch and VisitFetch
// passes through after this op.
func DatastoreScanFTS(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	context := vars.Temps[0].(*GlueContext)
	scan := vars.Temps[o.Params[0].(int)].(*plan.IndexFtsSearch)
	fts, ok := scan.Index().(datastore.FTSIndex)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreScanFTS: index %T is not an FTSIndex", scan.Index()))
		return
	}
	outName := scan.SearchInfo().OutName()

	// Evaluate the SEARCH() info (field/query/options/offset/limit) on this
	// goroutine; only the bleve search itself runs concurrently with the drain.
	var parent value.Value
	si := scan.SearchInfo()
	field, _, _ := EvalExpr(context, si.FieldName(), parent)
	query, _, qerr := EvalExpr(context, si.Query(), parent)
	if qerr != nil {
		yieldErr(fmt.Errorf("DatastoreScanFTS, SEARCH query: %v", qerr))
		return
	}
	options, _, _ := EvalExpr(context, si.Options(), parent)
	info := &datastore.FTSSearchInfo{
		Field:   field,
		Query:   query,
		Options: options,
		Order:   si.Order(),
		Offset:  EvalExprInt64(context, si.Offset(), parent, 0),
		Limit:   EvalExprInt64(context, si.Limit(), parent, math.MaxInt64),
	}

	// Run the search and collect (docID, score) hits.
	conn := datastore.NewIndexConnection(context)
	defer conn.Dispose()
	defer conn.SendStop()

	go fts.Search(glueRequestId, info, datastore.UNBOUNDED, nil, conn)

	type ftsHit struct {
		id    string
		score value.Value
	}
	var hits []ftsHit
	sender := conn.Sender()
	for {
		entry, ok := sender.GetEntry()
		if !ok || entry == nil {
			break
		}
		hits = append(hits, ftsHit{id: entry.PrimaryKey, score: entry.MetaData})
	}

	// Fetch the hit documents from the keyspace (one batch; a CLI result set is small).
	keyspace := scan.Keyspace()
	keys := make([]string, 0, len(hits))
	for _, h := range hits {
		keys = append(keys, h.id)
	}
	fetchMap := make(map[string]value.AnnotatedValue, len(keys))
	if len(keys) > 0 {
		errs := keyspace.Fetch(keys, fetchMap, datastore.NULL_QUERY_CONTEXT, nil, nil, false)
		for _, e := range errs {
			yieldErr(fmt.Errorf("DatastoreScanFTS, fetch: %v", e))
		}
	}

	var docBuf, smBuf bytes.Buffer
	var idBuf base.Val
	row := make(base.Vals, 3)

	for _, h := range hits {
		v, ok := fetchMap[h.id]
		if !ok || v == nil {
			continue
		}
		docBuf.Reset()
		if err := v.WriteJSON(nil, &docBuf, "", "", true); err != nil {
			yieldErr(fmt.Errorf("DatastoreScanFTS, encode doc %q: %v", h.id, err))
			return
		}
		smBuf.Reset()
		if err := writeSmetaJSON(&smBuf, outName, h.score, h.id); err != nil {
			yieldErr(fmt.Errorf("DatastoreScanFTS, encode smeta %q: %v", h.id, err))
			return
		}
		idBuf = strconv.AppendQuote(idBuf[:0], h.id)

		row[0] = base.Val(docBuf.Bytes()) // Label ".alias".
		row[1] = base.Val(idBuf)          // Label "^id".
		row[2] = base.Val(smBuf.Bytes())  // Label "^smeta".
		yieldVals(row)
	}

	yieldErr(nil)
}

// writeSmetaJSON writes the search-meta attachment for one hit -- {outname:
// {score, id}} -- which ConvertVals binds under value.ATT_SMETA on the alias value.
// SEARCH_META(alias) reads outname's object off that attachment and SEARCH_SCORE
// reads its `.score` (see the search package's SearchMeta/SearchScore).
func writeSmetaJSON(buf *bytes.Buffer, outName string, score value.Value, id string) error {
	meta := map[string]interface{}{"id": id}
	if score != nil {
		meta["score"] = score
	}
	sm := value.NewValue(map[string]interface{}{outName: meta})
	return sm.WriteJSON(nil, buf, "", "", true)
}

// -------------------------------------------------------------------

func DatastoreScan(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr,
	cb func(*GlueContext, *datastore.IndexConnection)) {
	datastoreScanDrain(o, vars, yieldVals, yieldErr, cb, nil)
}

// datastoreScanDrain runs an index-connection scan (cb launches it) and drains
// its sender. With buildRow == nil it yields one val per entry -- the ^id (the
// default scan+fetch path, where a following Fetch materializes the doc). With a
// buildRow it yields whatever that builds per entry -- used by
// DatastoreScanIndexCovering to emit a reconstructed `.alias` doc + `^id` with no
// fetch. (The cbq cover-slot mechanism -- IndexEntry.EntryKey/FilterCovers ->
// AnnotatedValue.SetCover -- is not used: n1k1 has no cover slots on base.Val, so
// covering is answered by reconstructing the doc and letting stripCovers peel the
// covers back to plain field accesses; see conv.go/expr.go.)
func datastoreScanDrain(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr,
	cb func(*GlueContext, *datastore.IndexConnection),
	buildRow func(*GlueContext, *datastore.IndexEntry) (base.Vals, error)) {
	context := vars.Temps[0].(*GlueContext)

	conn := datastore.NewIndexConnection(context)

	defer conn.Dispose()
	defer conn.SendStop()

	cb(context, conn)

	sender := conn.Sender()

	var valId base.Val
	var vals base.Vals

	for {
		entry, ok := sender.GetEntry()
		if !ok || entry == nil {
			break
		}

		if buildRow != nil {
			row, err := buildRow(context, entry)
			if err != nil {
				yieldErr(err)
				return
			}
			yieldVals(row)
			continue
		}

		valId = strconv.AppendQuote(valId[:0], entry.PrimaryKey)
		vals = append(vals[:0], valId)

		yieldVals(vals)
	}

	yieldErr(nil)
}

// -------------------------------------------------------------------

func EvalSpan(context *GlueContext, ps *plan.Span, parent value.Value) (
	dspan *datastore.Span, empty bool, err error) {
	dspan = &datastore.Span{}

	dspan.Seek, empty, err = EvalExprs(context, ps.Seek, nil)
	if err != nil || empty {
		return nil, empty, err
	}

	dspan.Range.Low, empty, err = EvalExprs(context, ps.Range.Low, parent)
	if err != nil || empty {
		return nil, empty, err
	}

	dspan.Range.High, empty, err = EvalExprs(context, ps.Range.High, parent)
	if err != nil || empty {
		return nil, empty, err
	}

	dspan.Range.Inclusion = ps.Range.Inclusion

	return dspan, false, nil
}

// -------------------------------------------------------------------

func EvalExprs(context *GlueContext, cx expression.Expressions,
	parent value.Value) (cv value.Values, empty bool, err error) {
	if len(cx) > 0 {
		cv = make(value.Values, len(cx))

		for i, expr := range cx {
			cv[i], empty, err = EvalExpr(context, expr, parent)
			if err != nil || empty {
				return nil, empty, err
			}
		}
	}

	return cv, false, nil
}

// -------------------------------------------------------------------

func EvalExpr(context *GlueContext, expr expression.Expression,
	parent value.Value) (v value.Value, empty bool, err error) {
	if expr != nil {
		v, err = expr.Evaluate(parent, context)
		if err != nil {
			return nil, false, fmt.Errorf("EvalExpr, err: %v", err)
		}

		if v != nil && (v.Type() == value.NULL || v.Type() == value.MISSING) &&
			expr.Value() == nil {
			return nil, true, nil
		}
	}

	return v, false, nil
}

// -------------------------------------------------------------------

func EvalExprInt64(context *GlueContext, expr expression.Expression,
	parent value.Value, defval int64) (val int64) {
	if expr != nil {
		val, err := expr.Evaluate(parent, context)
		if err == nil && val.Type() == value.NUMBER {
			return val.(value.NumberValue).Int64()
		}
	}

	return defval
}
