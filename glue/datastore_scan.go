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
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/recordsource"

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

	var parent value.Value // TODO: handle parent?

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
// CLI's --modes flag overrides it via recordsource.ParseModes to lock scanning
// down (e.g. don't recurse into unwanted subdirs). Process-global to match the
// engine.ExecOpEx registration style -- fine for the single-process CLI; a
// per-store field is the cleaner future form (see DESIGN-data.md).
var ScanWalkOptions = recordsource.AllModes()

// recordsScanPlan is the subset of plan.PrimaryScan / plan.PrimaryScan3 that the
// n1k1-native records scan needs: the target keyspace and an optional LIMIT.
type recordsScanPlan interface {
	Keyspace() datastore.Keyspace
	Limit() expression.Expression
}

// DatastoreScanRecords reads a file keyspace's directory n1k1-native via the
// recordsource package (union of files, recurse subdirs, decode JSONL /
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

	dir, err := keyspaceDir(keyspace)
	if err != nil {
		yieldErr(err)
		return
	}

	limit := EvalExprInt64(context, scan.Limit(), nil, math.MaxInt64)

	src, err := recordsource.Walk(dir, ScanWalkOptions)
	if err != nil {
		yieldErr(fmt.Errorf("DatastoreScanRecords, walk %q: %v", dir, err))
		return
	}
	defer src.Close()

	var vals base.Vals
	var idBuf []byte
	var rec recordsource.Record

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

// keyspaceDir resolves a file-datastore keyspace to its on-disk directory,
// <root>/<namespace>/<keyspace>, from the datastore's file:// URL. n1k1 owns
// scan/fetch execution, so it reads the directory itself rather than routing
// through cbq's ScanEntries/Fetch.
func keyspaceDir(keyspace datastore.Keyspace) (string, error) {
	// A synthetic flat-root keyspace knows its own directory (the root itself),
	// which isn't <root>/<ns>/<keyspace>. See flatroot.go.
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

			/* covers := scan.Covers() // TODO: Do we care about covers?
			if len(covers) > 0 {
				panic("covers unimplemented / TODO")
			} */

			limit := EvalExprInt64(context, scan.Limit(), nil, math.MaxInt64)

			// TODO: for nested-loop join we need to pass in values from
			// left-hand-side (outer) of the join for span evaluation?
			// outerValue := parent
			// if !scan.Term().IsUnderNL() {
			//     outerValue = nil
			// }

			var outerValue value.Value

			for _, span := range scan.Spans() {
				go func(span *plan.Span) {
					// TODO: defer context.Recover(nil) // Recover from any panic?

					dspan, empty, err := EvalSpan(context, span, outerValue)
					if err != nil || empty {
						if err != nil {
							context.Error(errors.NewEvaluationError(err, "span"))
						}

						conn.Sender().Close()

						return
					}

					scan.Index().Scan(glueRequestId, dspan, scan.Distinct(), limit,
						datastore.UNBOUNDED, nil, conn)
				}(span)
			}
		})
}

// -------------------------------------------------------------------

func DatastoreScan(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr,
	cb func(*GlueContext, *datastore.IndexConnection)) {
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

		valId = strconv.AppendQuote(valId[:0], entry.PrimaryKey)
		vals = append(vals[:0], valId)

		yieldVals(vals)

		// TODO: Handle NL case.
		// scopeValue := parent
		// if scan.Term().IsUnderNL() {
		//     scopeValue = nil
		// }

		// av := this.newEmptyDocumentWithKey(entry.PrimaryKey, scopeValue, context)

		// TODO: The COVER() expression which accesses the SetCover()
		// data appears in a GROUP BY & aggregate expr rewrite.
		// Need to put this into the vals as meta-ish entries?
		/*
			covers := scan.Covers()
			if len(covers) > 0 {
				for c, v := range scan.FilterCovers() {
					av.SetCover(c.Text(), v)
				}

				// Matches planner.builder.buildCoveringScan()
				for i, ek := range entry.EntryKey {
					av.SetCover(covers[i].Text(), ek)
				}

				// Matches planner.builder.buildCoveringScan()
				av.SetCover(covers[len(covers)-1].Text(),
					value.NewValue(entry.PrimaryKey))

				av.SetField(this.plan.Term().Alias(), av) // TODO?
			}

			av.SetBit(this.bit) // TODO: Needed for intersect scan.

			ok = this.sendItem(av)
		*/
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
