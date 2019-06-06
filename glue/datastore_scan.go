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
	"strconv"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/execution"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"
)

func DatastoreScanKeys(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	context := vars.Temps[0].(*execution.Context)

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

func DatastoreScanPrimary(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	DatastoreScan(o, vars, yieldVals, yieldErr,
		func(context *execution.Context, conn *datastore.IndexConnection) {
			nks := vars.Temps[o.Params[0].(int)].(Termer).Term()
			vec := context.ScanVectorSource().ScanVector(nks.Namespace(), nks.Keyspace())

			if scan, ok := vars.Temps[o.Params[0].(int)].(*plan.PrimaryScan); ok {
				limit := EvalExprInt64(context, scan.Limit(), nil, math.MaxInt64)

				go scan.Index().ScanEntries(context.RequestId(), limit,
					context.ScanConsistency(), vec, conn)
			} else if scan, ok := vars.Temps[o.Params[0].(int)].(*plan.PrimaryScan3); ok {
				offset := EvalExprInt64(context, scan.Offset(), nil, int64(0))
				limit := EvalExprInt64(context, scan.Limit(), nil, math.MaxInt64)

				var indexProjection *datastore.IndexProjection
				var indexOrder datastore.IndexKeyOrders
				var indexGroupAggs *datastore.IndexGroupAggregates

				// TODO: Handle advanced PrimaryScan3 params.

				go scan.Index().ScanEntries3(context.RequestId(),
					indexProjection, offset, limit, indexGroupAggs, indexOrder,
					context.ScanConsistency(), vec, conn)
			}
		})
}

// -------------------------------------------------------------------

func DatastoreScanIndex(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	DatastoreScan(o, vars, yieldVals, yieldErr,
		func(context *execution.Context, conn *datastore.IndexConnection) {
			scan := vars.Temps[o.Params[0].(int)].(*plan.IndexScan)

			/* covers := scan.Covers() // TODO: Do we care about covers?
			if len(covers) > 0 {
				panic("covers unimplemented / TODO")
			} */

			nks := scan.Term()
			vec := context.ScanVectorSource().ScanVector(nks.Namespace(), nks.Keyspace())

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

					scan.Index().Scan(context.RequestId(), dspan, scan.Distinct(), limit,
						context.ScanConsistency(), vec, conn)
				}(span)
			}
		})
}

// -------------------------------------------------------------------

func DatastoreScan(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr,
	cb func(*execution.Context, *datastore.IndexConnection)) {
	context := vars.Temps[0].(*execution.Context)

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

func EvalSpan(context *execution.Context, ps *plan.Span, parent value.Value) (
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

func EvalExprs(context *execution.Context, cx expression.Expressions,
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

func EvalExpr(context *execution.Context, expr expression.Expression,
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

func EvalExprInt64(context *execution.Context, expr expression.Expression,
	parent value.Value, defval int64) (val int64) {
	if expr != nil {
		val, err := expr.Evaluate(parent, context)
		if err == nil && val.Type() == value.NUMBER {
			return val.(value.NumberValue).Int64()
		}
	}

	return defval
}
