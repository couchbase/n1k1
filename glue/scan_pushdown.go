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

// scanRowFilterSpec extracts a pushable predicate TREE from a filter condition and encodes
// it as a scan-op param (["row-filter", <node>]), or ok=false to push nothing.
func scanRowFilterSpec(cond expression.Expression) (interface{}, bool) {
	pred, ok := buildScanExpr(cond, false)
	if !ok {
		return nil, false
	}
	return []interface{}{"row-filter", encodeScanPred(pred)}, true
}

// encodeScanPred serializes a predicate tree to JSON-friendly interface{} values. A leaf is
// ["", field, op, const, consts]; a boolean node is ["and"|"or", [child, ...]].
func encodeScanPred(p records.ScanPredicate) interface{} {
	if p.Bool == "" {
		return []interface{}{"", p.Clause.Field, p.Clause.Op, p.Clause.Const, p.Clause.Consts}
	}
	kids := make([]interface{}, len(p.Children))
	for i, c := range p.Children {
		kids[i] = encodeScanPred(c)
	}
	return []interface{}{p.Bool, kids}
}

// scanRowFilter decodes the "row-filter" param VisitFilter attached to a scan op.
func scanRowFilter(o *base.Op) (records.ScanPredicate, bool) {
	for _, p := range o.Params {
		pp, ok := p.([]interface{})
		if !ok || len(pp) != 2 {
			continue
		}
		if k, _ := pp[0].(string); k != "row-filter" {
			continue
		}
		return decodeScanPred(pp[1])
	}
	return records.ScanPredicate{}, false
}

// decodeScanPred is the inverse of encodeScanPred.
func decodeScanPred(v interface{}) (records.ScanPredicate, bool) {
	a, ok := v.([]interface{})
	if !ok || len(a) < 2 {
		return records.ScanPredicate{}, false
	}
	b, _ := a[0].(string)
	if b == "" {
		if len(a) != 5 {
			return records.ScanPredicate{}, false
		}
		field, _ := a[1].(string)
		op, _ := a[2].(string)
		consts, _ := a[4].([]interface{})
		return records.ScanPredicate{Clause: records.ScanClause{
			Field: field, Op: op, Const: a[3], Consts: consts}}, true
	}
	kidsRaw, ok := a[1].([]interface{})
	if !ok {
		return records.ScanPredicate{}, false
	}
	kids := make([]records.ScanPredicate, 0, len(kidsRaw))
	for _, kr := range kidsRaw {
		k, ok := decodeScanPred(kr)
		if !ok {
			return records.ScanPredicate{}, false
		}
		kids = append(kids, k)
	}
	return records.ScanPredicate{Bool: b, Children: kids}, true
}

// buildScanExpr converts a filter condition to a pushable predicate tree in negation-normal
// form, tracking whether we're under an odd number of NOTs (negate). De Morgan flips the
// operator under negation (AND<->OR) and negates each leaf's op, so the result is a pure
// monotone AND/OR of leaves. An effective AND drops unpushable children (widen -- safe, needs
// >=1); an effective OR is all-or-nothing. This handles genuinely nested boolean, e.g.
// `(a AND b) OR c` or `NOT((a OR b) AND c)`.
func buildScanExpr(e expression.Expression, negate bool) (records.ScanPredicate, bool) {
	switch n := e.(type) {
	case *expression.And:
		return buildBoolNode(n.Operands(), negate, true)
	case *expression.Or:
		return buildBoolNode(n.Operands(), negate, false)
	case *expression.Not:
		return buildScanExpr(n.Operand(), !negate)
	default:
		cl, ok := pushableLeaf(e)
		if !ok {
			return records.ScanPredicate{}, false
		}
		if negate {
			neg, ok := negateOp(cl.Op)
			if !ok {
				return records.ScanPredicate{}, false
			}
			cl.Op = neg
		}
		return records.ScanPredicate{Clause: cl}, true
	}
}

// buildBoolNode builds an AND/OR node from operands. isAnd is the SOURCE operator; under
// negation it flips (De Morgan). The effective AND drops unconvertible children; the
// effective OR requires all. A single surviving child collapses to itself.
func buildBoolNode(ops expression.Expressions, negate, isAnd bool) (records.ScanPredicate, bool) {
	effAnd := isAnd != negate // XOR: a NOT flips AND<->OR.
	kids := make([]records.ScanPredicate, 0, len(ops))
	for _, o := range ops {
		child, ok := buildScanExpr(o, negate)
		if !ok {
			if !effAnd {
				return records.ScanPredicate{}, false // OR: all-or-nothing.
			}
			continue // AND: drop the unconvertible child (widen -- safe).
		}
		kids = append(kids, child)
	}
	if len(kids) == 0 {
		return records.ScanPredicate{}, false
	}
	if len(kids) == 1 {
		return kids[0], true
	}
	bool := "or"
	if effAnd {
		bool = "and"
	}
	return records.ScanPredicate{Bool: bool, Children: kids}, true
}

// pushableLeaf reduces one leaf expression to a ScanClause: a comparison, an IN list, or an
// IS [NOT] NULL. NOT is handled one level up (buildScanExpr), so it isn't a case here.
func pushableLeaf(e expression.Expression) (records.ScanClause, bool) {
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
