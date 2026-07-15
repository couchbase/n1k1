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

// Predicate pushdown into a records scan (DESIGN-data.md §7, DESIGN-col.md). When a WHERE
// filter sits directly above a datastore-scan-records, VisitFilter attaches a NEUTRAL
// records.ScanPredicate to the scan op; DatastoreScanRecords hands it to a records.RowFilterer
// source (e.g. Iceberg, which prunes whole Parquet files by manifest column stats). It is a
// pure optimization: the filter op is ALWAYS kept, so the engine still applies the real WHERE
// -- a partial/absent/loose pushdown is correct. Only a flat AND/OR of `field <op> const`
// (numeric / string / bool) is extracted; anything else simply isn't pushed.

import (
	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// DisableRowFilterPushdown turns off predicate pushdown (parity with
// DisableColumnProjection) -- used by tests to compare pushed vs unpushed results.
var DisableRowFilterPushdown bool

// scanRowFilterSpec extracts a pushable predicate from a filter condition and encodes it as
// a scan-op param (["row-filter", [mode, [[field, op, const], ...]]]) with JSON-friendly
// leaf types (float64/string/bool), or ok=false to push nothing.
func scanRowFilterSpec(cond expression.Expression) (interface{}, bool) {
	pred, ok := extractScanPredicate(cond)
	if !ok {
		return nil, false
	}
	rawCls := make([]interface{}, len(pred.Clauses))
	for i, cl := range pred.Clauses {
		rawCls[i] = []interface{}{cl.Field, cl.Op, cl.Const}
	}
	return []interface{}{"row-filter", []interface{}{pred.Mode, rawCls}}, true
}

// scanRowFilter decodes the "row-filter" param VisitFilter attached to a scan op, or
// ok=false if none / malformed.
func scanRowFilter(o *base.Op) (records.ScanPredicate, bool) {
	for _, p := range o.Params {
		pp, ok := p.([]interface{})
		if !ok || len(pp) != 2 {
			continue
		}
		if k, _ := pp[0].(string); k != "row-filter" {
			continue
		}
		spec, ok := pp[1].([]interface{})
		if !ok || len(spec) != 2 {
			return records.ScanPredicate{}, false
		}
		mode, _ := spec[0].(string)
		rawCls, ok := spec[1].([]interface{})
		if !ok {
			return records.ScanPredicate{}, false
		}
		cls := make([]records.ScanClause, 0, len(rawCls))
		for _, rc := range rawCls {
			c, ok := rc.([]interface{})
			if !ok || len(c) != 3 {
				return records.ScanPredicate{}, false
			}
			field, _ := c[0].(string)
			op, _ := c[1].(string)
			cls = append(cls, records.ScanClause{Field: field, Op: op, Const: c[2]})
		}
		return records.ScanPredicate{Mode: mode, Clauses: cls}, true
	}
	return records.ScanPredicate{}, false
}

// extractScanPredicate reduces a filter condition to a flat AND/OR of `field <op> const`
// comparisons (a lone comparison is mode "and", one clause). A nested/mixed boolean or a
// non-pushable leaf -> ok=false (all-or-nothing, mirroring the columnar path): correctness
// beats a partial push, and the engine's filter covers whatever isn't pushed.
func extractScanPredicate(cond expression.Expression) (records.ScanPredicate, bool) {
	switch cond.(type) {
	case *expression.And:
		cls, ok := flattenScanClauses(cond, "and")
		if !ok || len(cls) == 0 {
			return records.ScanPredicate{}, false
		}
		return records.ScanPredicate{Mode: "and", Clauses: cls}, true
	case *expression.Or:
		cls, ok := flattenScanClauses(cond, "or")
		if !ok || len(cls) < 2 {
			return records.ScanPredicate{}, false
		}
		return records.ScanPredicate{Mode: "or", Clauses: cls}, true
	default:
		cl, ok := pushableComparison(cond)
		if !ok {
			return records.ScanPredicate{}, false
		}
		return records.ScanPredicate{Mode: "and", Clauses: []records.ScanClause{cl}}, true
	}
}

// flattenScanClauses collects comparison clauses from a tree of same-mode boolean nodes
// (cbq nests `a AND b AND c` as And(And(a,b),c)). A different-mode node or a non-pushable
// leaf -> ok=false.
func flattenScanClauses(e expression.Expression, mode string) ([]records.ScanClause, bool) {
	var ops expression.Expressions
	switch n := e.(type) {
	case *expression.And:
		if mode != "and" {
			return nil, false
		}
		ops = n.Operands()
	case *expression.Or:
		if mode != "or" {
			return nil, false
		}
		ops = n.Operands()
	default:
		cl, ok := pushableComparison(e)
		if !ok {
			return nil, false
		}
		return []records.ScanClause{cl}, true
	}
	var out []records.ScanClause
	for _, o := range ops {
		sub, ok := flattenScanClauses(o, mode)
		if !ok {
			return nil, false
		}
		out = append(out, sub...)
	}
	return out, true
}

// pushableComparison reduces one comparison to (field, op, const), orienting so the field is
// on the left (a `c < field` becomes `field > c`). cbq normalizes `>`/`>=` to LT/LE with
// swapped operands, so only Eq/LT/LE appear; the orientation below recovers gt/ge. One side
// must be a bare keyspace field, the other a numeric/string/bool constant.
func pushableComparison(cond expression.Expression) (records.ScanClause, bool) {
	var a, b expression.Expression
	var op string
	switch e := cond.(type) {
	case *expression.Eq:
		a, b, op = e.First(), e.Second(), "eq"
	case *expression.LT:
		a, b, op = e.First(), e.Second(), "lt"
	case *expression.LE:
		a, b, op = e.First(), e.Second(), "le"
	default:
		return records.ScanClause{}, false
	}
	if field, ok := bareFieldOfExpr(a); ok {
		if k, ok := constOfExpr(b); ok {
			return records.ScanClause{Field: field, Op: op, Const: k}, true
		}
	}
	if field, ok := bareFieldOfExpr(b); ok {
		if k, ok := constOfExpr(a); ok {
			return records.ScanClause{Field: field, Op: flipOp(op), Const: k}, true
		}
	}
	return records.ScanClause{}, false
}

// flipOp reverses a comparison for when the field is the RIGHT operand (`c < field`).
func flipOp(op string) string {
	switch op {
	case "lt":
		return "gt"
	case "le":
		return "ge"
	case "gt":
		return "lt"
	case "ge":
		return "le"
	}
	return op // eq / ne are symmetric.
}

// constOfExpr returns a scalar constant (float64 / string / bool) from a constant
// expression, or ok=false for anything else (null, array, object, non-constant).
func constOfExpr(e expression.Expression) (interface{}, bool) {
	c, ok := e.(*expression.Constant)
	if !ok {
		return nil, false
	}
	v := c.Value()
	if v == nil {
		return nil, false
	}
	switch v.Type() {
	case value.NUMBER:
		if f, ok := v.Actual().(float64); ok {
			return f, true
		}
	case value.STRING:
		if s, ok := v.Actual().(string); ok {
			return s, true
		}
	case value.BOOLEAN:
		if b, ok := v.Actual().(bool); ok {
			return b, true
		}
	}
	return nil, false
}
