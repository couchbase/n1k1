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
		rawCls[i] = []interface{}{cl.Field, cl.Op, cl.Const, cl.Consts}
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
			if !ok || len(c) != 4 {
				return records.ScanPredicate{}, false
			}
			field, _ := c[0].(string)
			op, _ := c[1].(string)
			consts, _ := c[3].([]interface{})
			cls = append(cls, records.ScanClause{Field: field, Op: op, Const: c[2], Consts: consts})
		}
		return records.ScanPredicate{Mode: mode, Clauses: cls}, true
	}
	return records.ScanPredicate{}, false
}

// extractScanPredicate reduces a filter condition to a flat AND/OR of pushable clauses
// (comparison / IN / IS [NOT] NULL, plus their negations). A lone clause is mode "and" with
// one clause. Under an AND an unpushable conjunct is dropped (widening the pruning filter is
// safe); an OR must be fully pushable (dropping a branch narrows it, which could prune
// matching rows) -- so a not-fully-pushable OR yields ok=false and nothing is pushed.
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
		cl, ok := pushableClause(cond)
		if !ok {
			return records.ScanPredicate{}, false
		}
		return records.ScanPredicate{Mode: "and", Clauses: []records.ScanClause{cl}}, true
	}
}

// flattenScanClauses collects clauses from a tree of same-mode boolean nodes (cbq nests
// `a AND b AND c` as And(And(a,b),c)). AND: recurse into nested ANDs, DROP any operand that
// isn't pushable (a nested OR, a UDF, ...) -- ok as long as ≥1 clause survives. OR: every
// operand must be a pushable clause or a nested OR (all-or-nothing); a nested AND or an
// unpushable leaf -> ok=false.
func flattenScanClauses(e expression.Expression, mode string) ([]records.ScanClause, bool) {
	switch n := e.(type) {
	case *expression.And:
		if mode != "and" {
			return nil, false // a nested AND under an OR can't be pushed as a flat OR.
		}
		var out []records.ScanClause
		for _, o := range n.Operands() {
			if sub, ok := flattenScanClauses(o, "and"); ok {
				out = append(out, sub...) // else DROP this conjunct (widen -- safe).
			}
		}
		return out, len(out) > 0
	case *expression.Or:
		if mode != "or" {
			return nil, false // signals the caller to DROP this OR conjunct under an AND.
		}
		var out []records.ScanClause
		for _, o := range n.Operands() {
			sub, ok := flattenScanClauses(o, "or")
			if !ok {
				return nil, false // OR is all-or-nothing.
			}
			out = append(out, sub...)
		}
		return out, true
	default:
		cl, ok := pushableClause(e)
		if !ok {
			return nil, false
		}
		return []records.ScanClause{cl}, true
	}
}

// pushableClause reduces one leaf expression to a ScanClause: a comparison, an IN list, an
// IS [NOT] NULL, or a NOT of any of those (cbq expresses `!=` as NOT(=) and `NOT IN` as
// NOT(IN)). ok=false for anything else.
func pushableClause(e expression.Expression) (records.ScanClause, bool) {
	switch n := e.(type) {
	case *expression.Eq:
		return orientComparison(n.First(), n.Second(), "eq")
	case *expression.LT:
		return orientComparison(n.First(), n.Second(), "lt")
	case *expression.LE:
		return orientComparison(n.First(), n.Second(), "le")
	case *expression.In:
		field, ok := bareFieldOfExpr(n.First())
		if !ok {
			return records.ScanClause{}, false
		}
		consts, ok := constListOfExpr(n.Second())
		if !ok {
			return records.ScanClause{}, false
		}
		return records.ScanClause{Field: field, Op: "in", Consts: consts}, true
	case *expression.IsNull:
		if field, ok := bareFieldOfExpr(n.Operand()); ok {
			return records.ScanClause{Field: field, Op: "isnull"}, true
		}
	case *expression.IsNotNull:
		if field, ok := bareFieldOfExpr(n.Operand()); ok {
			return records.ScanClause{Field: field, Op: "notnull"}, true
		}
	case *expression.Not:
		if cl, ok := pushableClause(n.Operand()); ok {
			if neg, ok := negateOp(cl.Op); ok {
				cl.Op = neg
				return cl, true
			}
		}
	}
	return records.ScanClause{}, false
}

// orientComparison builds a comparison clause with the field on the left, recovering gt/ge
// when the field is the RIGHT operand (cbq normalizes `>`/`>=` to swapped LT/LE). One side
// must be a bare keyspace field, the other a scalar constant.
func orientComparison(a, b expression.Expression, op string) (records.ScanClause, bool) {
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

// negateOp returns the op of NOT(clause). Ordering flips account for SQL three-valued logic
// only as a pruning HINT (the engine applies the real predicate): for every row the real
// NOT(x<c) keeps, x>=c holds, so pruning by x>=c is sound.
func negateOp(op string) (string, bool) {
	switch op {
	case "eq":
		return "ne", true
	case "ne":
		return "eq", true
	case "lt":
		return "ge", true
	case "le":
		return "gt", true
	case "gt":
		return "le", true
	case "ge":
		return "lt", true
	case "in":
		return "notin", true
	case "notin":
		return "in", true
	case "isnull":
		return "notnull", true
	case "notnull":
		return "isnull", true
	}
	return "", false
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

// constListOfExpr returns the scalar elements of a constant array expression (the RHS of an
// `IN [...]`), or ok=false if it isn't a constant array of scalars. Works whether cbq folded
// the list to a constant value or left it as an all-constant ArrayConstruct (Value() covers
// both). A non-scalar element (nested array/object/null) makes the whole list unpushable.
func constListOfExpr(e expression.Expression) ([]interface{}, bool) {
	v := e.Value()
	if v == nil || v.Type() != value.ARRAY {
		return nil, false
	}
	arr, ok := v.Actual().([]interface{})
	if !ok || len(arr) == 0 {
		return nil, false
	}
	out := make([]interface{}, 0, len(arr))
	for _, el := range arr {
		switch el.(type) {
		case float64, string, bool:
			out = append(out, el)
		default:
			return nil, false
		}
	}
	return out, true
}
