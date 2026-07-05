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

// Vectorized aggregation, DESIGN-col.md Step 5.1: `SELECT SUM(x) FROM data` over a
// Parquet keyspace runs a fused scan->agg that reads the borrowed Arrow column
// buffer directly (no transpose), reusing base.AggSum's accumulator via the
// sum_v_* catalog keys. A post-conv rewrite pass (vectorizeColumnarAggs) rewrites a
// qualifying `group` op in place into a `columnar-agg` op; anything that doesn't
// qualify keeps the ordinary row path. Correctness bound: results must match the
// row lane (TestParquetSumVectorizedDifferential), and the pass is conservative --
// it only fires when it can prove a numeric column of a records scan.
//
// Step 5.4c/5.4d fuse a WHERE into that path: `SELECT SUM(x) FROM data WHERE y > c`
// matches group->filter->scan; extractColPredicate reduces the cbq condition to a
// flat AND/OR of numeric field-vs-constant comparisons; per batch each clause's
// filter kernel (base.Filter*) makes a dense bitmap, the clauses AND/OR-combine
// (base.And/OrBitmap) into one selection, and the masked reducers fold over it. Any
// predicate we can't reduce to that form (nested boolean, field-vs-field, non-numeric)
// stays on the row path. cbq normalizes >/>= to LT/LE with swapped operands (match
// only LT/LE/Eq) and nests `a AND b AND c` as And(And(a,b),c) (flattened recursively).
//
// Nulls (DESIGN-col.md "Beyond null_count==0"): a column's null_count no longer
// forces the row path. Each batch carries its Arrow validity bitmap (same LSB-first
// layout as the selection), and the executor folds through it -- SUM/AVG-sum over
// selection∧validity (skip nulls), COUNT/AVG-count over the selection alone (n1k1
// COUNT(x) counts every row, like COUNT(*)).

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"
)

// DisableVectorizedAgg forces the row path (no columnar-agg / metadata-agg
// rewrite). The differential tests flip it to prove vectorized == scalar.
var DisableVectorizedAgg bool

// VectorizedAggApplied / MetadataAggApplied count how many of each fused op
// actually executed (test observability).
var (
	VectorizedAggApplied int64
	MetadataAggApplied   int64
)

// vectorizeColumnarAggs walks the finished op tree and rewrites each qualifying
// ungrouped-SUM-over-a-Parquet-column `group` op into a fused `columnar-agg` op.
// temps is Conv.Temps (so the scan's keyspace can be resolved to peek its schema).
func vectorizeColumnarAggs(op *base.Op, temps []interface{}) {
	if op == nil || DisableVectorizedAgg {
		return
	}
	maybeVectorizeGroup(op, temps)
	for _, c := range op.Children {
		vectorizeColumnarAggs(c, temps)
	}
}

func maybeVectorizeGroup(op *base.Op, temps []interface{}) {
	if op.Kind != "group" || len(op.Params) != 3 {
		return
	}
	// Ungrouped only (no GROUP BY keys) -- so the group yields exactly one row.
	if groups, ok := op.Params[0].([]interface{}); !ok || len(groups) != 0 {
		return
	}
	// Child is either the records scan directly, or a single-comparison WHERE
	// filter over it (5.4c). A filter we can't reduce to (field, op, const) keeps
	// the row path -- we must NOT silently drop it.
	if len(op.Children) != 1 {
		return
	}
	scan := op.Children[0]
	var pred *colPredicate
	if scan.Kind == "filter" {
		if len(scan.Children) != 1 || scan.Children[0].Kind != "datastore-scan-records" {
			return
		}
		p, ok := extractColPredicate(scan)
		if !ok {
			return
		}
		pred = &p
		scan = scan.Children[0]
	}
	if scan.Kind != "datastore-scan-records" || len(scan.Params) == 0 {
		return
	}
	scanTemp, ok := scan.Params[0].(int)
	if !ok {
		return
	}

	aggExprs, ok := op.Params[1].([]interface{})
	if !ok {
		return
	}
	aggCalcs, ok := op.Params[2].([]interface{})
	if !ok || len(aggCalcs) != len(aggExprs) || len(aggCalcs) == 0 {
		return
	}

	// The keyspace's columns (types + null_count), peeked from the footer.
	cols := scanKeyspaceColumns(temps, scanTemp)
	if cols == nil {
		return
	}
	colByName := map[string]records.ColumnMeta{}
	for _, c := range cols {
		colByName[c.Name] = c
	}

	// Every predicate clause's column must be numeric; nulls are fine (the executor
	// ANDs the batch validity into each clause, so a null clause row isn't selected).
	// An INT64 column requires an integer constant so the int64 kernel is exact.
	// Otherwise keep the row path.
	if pred != nil {
		for k := range pred.clauses {
			cl := &pred.clauses[k]
			cm, ok := colByName[cl.field]
			if !ok {
				return
			}
			switch cm.Type {
			case "DOUBLE":
				cl.colType = "DOUBLE"
			case "INT64":
				if cl.c != math.Trunc(cl.c) || math.IsInf(cl.c, 0) {
					return // non-integer constant vs int column -- row path
				}
				cl.colType = "INT64"
			default:
				return
			}
		}
	}

	// Rewrite in place: the group op BECOMES a fused op. Its Labels (the
	// "^aggregates|..." ones) are kept verbatim so the downstream project reads
	// results identically; the scan child is absorbed (the op opens the source
	// itself via scanTemp). metadata-agg (zero scan) only applies unfiltered;
	// otherwise columnar-agg carries the optional predicate. Fall through to the
	// row path if neither applies.
	if pred == nil {
		if specs, ok := metadataAggSpecs(aggExprs, aggCalcs, colByName); ok {
			op.Kind = "metadata-agg"
			op.Params = []interface{}{scanTemp, specs}
			op.Children = nil
			return
		}
	}
	if specs, ok := columnarAggSpecs(aggExprs, aggCalcs, colByName); ok {
		op.Kind = "columnar-agg"
		op.Params = []interface{}{scanTemp, specs, predSpec(pred)}
		op.Children = nil
		return
	}
}

// colClause is one vectorizable comparison `field <op> c`.
type colClause struct {
	field   string
	op      base.CmpOp
	c       float64
	colType string // "DOUBLE" | "INT64", filled once the column is resolved
}

// colPredicate is a flat conjunction or disjunction of comparison clauses (a single
// comparison is mode "and" with one clause). Nested/mixed boolean structure isn't
// handled -- such predicates keep the row path.
type colPredicate struct {
	mode    string // "and" | "or"
	clauses []colClause
}

// predSpec flattens a predicate to plain interface{} for op.Params, or nil.
func predSpec(p *colPredicate) interface{} {
	if p == nil {
		return nil
	}
	cls := make([]interface{}, len(p.clauses))
	for i, cl := range p.clauses {
		cls[i] = []interface{}{cl.field, int(cl.op), cl.c, cl.colType}
	}
	return []interface{}{p.mode, cls}
}

// extractColPredicate reduces a `filter` op's cbq condition to a flat AND/OR of
// numeric field-vs-constant comparisons. A top-level And/Or contributes its
// operands as clauses (each must itself be a bare comparison); anything else (a
// lone comparison aside) -- nested boolean, field-to-field, non-numeric -> false.
func extractColPredicate(filter *base.Op) (colPredicate, bool) {
	if len(filter.Params) != 2 {
		return colPredicate{}, false
	}
	cond, ok := filter.Params[1].(expression.Expression)
	if !ok {
		return colPredicate{}, false
	}
	var mode string
	switch cond.(type) {
	case *expression.And:
		mode = "and"
	case *expression.Or:
		mode = "or"
	default:
		cl, ok := extractComparison(cond)
		if !ok {
			return colPredicate{}, false
		}
		return colPredicate{mode: "and", clauses: []colClause{cl}}, true
	}
	clauses, ok := flattenClauses(cond, mode)
	if !ok || len(clauses) < 2 {
		return colPredicate{}, false
	}
	return colPredicate{mode: mode, clauses: clauses}, true
}

// flattenClauses collects comparison clauses from a tree of same-mode boolean
// nodes (cbq nests `a AND b AND c` as And(And(a,b),c)). A different-mode node or a
// non-comparison leaf -> ok=false (that predicate keeps the row path).
func flattenClauses(e expression.Expression, mode string) ([]colClause, bool) {
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
		cl, ok := extractComparison(e)
		if !ok {
			return nil, false
		}
		return []colClause{cl}, true
	}
	var out []colClause
	for _, o := range ops {
		sub, ok := flattenClauses(o, mode)
		if !ok {
			return nil, false
		}
		out = append(out, sub...)
	}
	return out, true
}

// extractComparison reduces a single cbq comparison to (field, op, const). cbq
// rewrites >/>= to LT/LE with swapped operands, so we handle LT/LE/Eq and read
// operand order to get the effective op.
func extractComparison(cond expression.Expression) (colClause, bool) {
	var a, b expression.Expression
	var lt, le bool // else Eq
	switch e := cond.(type) {
	case *expression.LT:
		a, b, lt = e.First(), e.Second(), true
	case *expression.LE:
		a, b, le = e.First(), e.Second(), true
	case *expression.Eq:
		a, b = e.First(), e.Second()
	default:
		return colClause{}, false
	}

	// Orient: exactly one side a bare field, the other a numeric constant.
	field, fieldFirst, c, ok := orientComparison(a, b)
	if !ok {
		return colClause{}, false
	}
	var op base.CmpOp
	switch {
	case lt && fieldFirst:
		op = base.CmpLT // field < c
	case lt && !fieldFirst:
		op = base.CmpGT // c < field  ==>  field > c
	case le && fieldFirst:
		op = base.CmpLE
	case le && !fieldFirst:
		op = base.CmpGE
	default:
		op = base.CmpEQ
	}
	return colClause{field: field, op: op, c: c}, true
}

// orientComparison returns (field, fieldIsFirst, const) when one operand is a bare
// keyspace field and the other a numeric constant.
func orientComparison(a, b expression.Expression) (string, bool, float64, bool) {
	if field, ok := bareFieldOfExpr(a); ok {
		if c, ok := numericConst(b); ok {
			return field, true, c, true
		}
	}
	if field, ok := bareFieldOfExpr(b); ok {
		if c, ok := numericConst(a); ok {
			return field, false, c, true
		}
	}
	return "", false, 0, false
}

// numericConst returns the float64 value of a numeric *expression.Constant.
func numericConst(e expression.Expression) (float64, bool) {
	c, ok := e.(*expression.Constant)
	if !ok {
		return 0, false
	}
	v := c.Value()
	if v == nil || v.Type() != value.NUMBER {
		return 0, false
	}
	f, ok := v.Actual().(float64)
	return f, ok
}

// metadataAggSpecs returns per-agg specs when EVERY aggregate can be answered from
// the footer stats alone -- COUNT(*), COUNT(x), MIN(x), MAX(x) -- so the query
// needs zero data-page reads. ok=false if any aggregate needs a scan (SUM/AVG) or a
// stat it lacks. Each spec: [kind, field, type], kind in count-star/count/min/max.
func metadataAggSpecs(aggExprs, aggCalcs []interface{}, colByName map[string]records.ColumnMeta) ([]interface{}, bool) {
	specs := make([]interface{}, 0, len(aggCalcs))
	for i := range aggCalcs {
		calc, ok := aggCalcs[i].([]interface{})
		if !ok || len(calc) != 1 {
			return nil, false
		}
		switch name, _ := calc[0].(string); name {
		case "count":
			if isStarOperand(aggExprs[i]) {
				specs = append(specs, []interface{}{"count-star", "", ""})
				continue
			}
			field, ok := bareAggField(aggExprs[i])
			if !ok {
				return nil, false
			}
			cm, ok := colByName[field]
			if !ok || cm.Count < 0 || cm.NullCount < 0 {
				return nil, false
			}
			specs = append(specs, []interface{}{"count", field, ""})
		case "min", "max":
			field, ok := bareAggField(aggExprs[i])
			if !ok {
				return nil, false
			}
			cm, ok := colByName[field]
			if !ok || (cm.Type != "INT64" && cm.Type != "DOUBLE") {
				return nil, false
			}
			if (name == "min" && cm.Min == nil) || (name == "max" && cm.Max == nil) {
				return nil, false
			}
			specs = append(specs, []interface{}{name, field, cm.Type})
		default:
			return nil, false // sum/avg/... need a scan
		}
	}
	return specs, true
}

// columnarAggSpecs returns per-agg specs when EVERY aggregate is a vectorizable
// SUM/AVG/COUNT whose operand is a bare numeric column OR a binary +/-/* of
// numeric column/constant terms (Step 5.5). Nulls are fine: the executor folds over
// each batch's validity bitmap (SUM/AVG skip nulls, COUNT counts every row), so
// null_count>0 no longer forces the row path. Each spec is [key, srcSpec] where
// srcSpec is a field name (bare) or ["arith", op, leftTerm, rightTerm]. ok=false else.
func columnarAggSpecs(aggExprs, aggCalcs []interface{}, colByName map[string]records.ColumnMeta) ([]interface{}, bool) {
	specs := make([]interface{}, 0, len(aggCalcs))
	for i := range aggCalcs {
		calc, ok := aggCalcs[i].([]interface{})
		if !ok || len(calc) != 1 {
			return nil, false
		}
		aggName, _ := calc[0].(string)
		srcSpec, resultType, ok := parseAggOperandSpec(aggExprs[i], colByName)
		if !ok {
			return nil, false
		}
		key, ok := vecAggCatalogKey(aggName, resultType)
		if !ok {
			return nil, false
		}
		specs = append(specs, []interface{}{key, srcSpec})
	}
	return specs, true
}

// parseAggOperandSpec resolves an aggregate's operand to (srcSpec, resultType).
// A bare numeric column keeps its Parquet type (so SUM(int) stays int64); a binary
// +/-/* of numeric column/constant terms materializes as float64 (matching the row
// engine's JSON-number arithmetic). At least one term must be a column.
func parseAggOperandSpec(aggExpr interface{}, colByName map[string]records.ColumnMeta) (interface{}, string, bool) {
	e, ok := aggExpr.([]interface{})
	if !ok || len(e) != 2 {
		return nil, "", false
	}
	if k, _ := e[0].(string); k != "exprTree" {
		return nil, "", false
	}
	expr, ok := e[1].(expression.Expression)
	if !ok {
		return nil, "", false
	}
	// Bare numeric column: SUM(x), keeps its physical type.
	if field, ok := bareFieldOfExpr(expr); ok {
		cm, ok := colByName[field]
		if !ok || (cm.Type != "DOUBLE" && cm.Type != "INT64") {
			return nil, "", false
		}
		return field, cm.Type, true
	}
	// Binary +/-/* of numeric column/const terms: SUM(price * qty), materialized f64.
	op, a, b, ok := binaryArith(expr)
	if !ok {
		return nil, "", false
	}
	lTerm, lok := arithTermSpec(a, colByName)
	rTerm, rok := arithTermSpec(b, colByName)
	if !lok || !rok || (isConstTerm(lTerm) && isConstTerm(rTerm)) {
		return nil, "", false
	}
	return []interface{}{"arith", string(op), lTerm, rTerm}, "DOUBLE", true
}

// binaryArith matches a two-operand +, -, or * (cbq: Add/Mult are commutative with
// Operands(), Sub is binary). Div/Neg/>2-operand/nested aren't handled.
func binaryArith(expr expression.Expression) (op byte, a, b expression.Expression, ok bool) {
	switch e := expr.(type) {
	case *expression.Add:
		if ops := e.Operands(); len(ops) == 2 {
			return '+', ops[0], ops[1], true
		}
	case *expression.Mult:
		if ops := e.Operands(); len(ops) == 2 {
			return '*', ops[0], ops[1], true
		}
	case *expression.Sub:
		return '-', e.First(), e.Second(), true
	}
	return 0, nil, nil, false
}

// arithTermSpec resolves one arithmetic operand to ["col", field, type] (numeric
// column) or ["const", val] (numeric constant).
func arithTermSpec(e expression.Expression, colByName map[string]records.ColumnMeta) ([]interface{}, bool) {
	if field, ok := bareFieldOfExpr(e); ok {
		cm, ok := colByName[field]
		if !ok || (cm.Type != "DOUBLE" && cm.Type != "INT64") {
			return nil, false
		}
		return []interface{}{"col", field, cm.Type}, true
	}
	if c, ok := numericConst(e); ok {
		return []interface{}{"const", c}, true
	}
	return nil, false
}

func isConstTerm(term []interface{}) bool {
	return len(term) > 0 && term[0].(string) == "const"
}

// isStarOperand reports whether an aggregate operand is COUNT(*)'s ["json","true"].
func isStarOperand(aggExpr interface{}) bool {
	e, ok := aggExpr.([]interface{})
	if !ok || len(e) < 1 {
		return false
	}
	k, _ := e[0].(string)
	return k == "json"
}

// bareAggField returns the field name if aggExpr is ["exprTree", <alias.field>]
// with a plain top-level field of a keyspace identifier (SUM(x), not SUM(x+y),
// SUM(*), or SUM(a.b)).
func bareAggField(aggExpr interface{}) (string, bool) {
	e, ok := aggExpr.([]interface{})
	if !ok || len(e) != 2 {
		return "", false
	}
	if k, _ := e[0].(string); k != "exprTree" {
		return "", false
	}
	expr, ok := e[1].(expression.Expression)
	if !ok {
		return "", false
	}
	return bareFieldOfExpr(expr)
}

// bareFieldOfExpr returns the field name if expr is a plain top-level field of a
// keyspace identifier (`x` / `alias.x`, not `a.b`, `x+y`, or a dynamic step).
func bareFieldOfExpr(expr expression.Expression) (string, bool) {
	f, ok := expr.(*expression.Field)
	if !ok {
		return "", false
	}
	if _, ok := f.First().(*expression.Identifier); !ok {
		return "", false // not a bare keyspace.field
	}
	fn, ok := f.Second().(*expression.FieldName)
	if !ok {
		return "", false // dynamic field step
	}
	return fn.Alias(), true
}

// vecAggCatalogKey maps (aggregate name, Parquet physical type) to the vectorized
// aggregate's catalog key, or ok=false when not vectorizable. Only fixed-width
// 8-byte numeric columns (DOUBLE/INT64) are supported; SUM/AVG are typed, COUNT is
// type-agnostic (element count) but still requires a supported column.
func vecAggCatalogKey(aggName, colType string) (string, bool) {
	var suffix string
	switch colType {
	case "DOUBLE":
		suffix = "float64"
	case "INT64":
		suffix = "int64"
	default:
		return "", false
	}
	switch aggName {
	case "sum":
		return "sum_v_" + suffix, true
	case "avg":
		return "avg_v_" + suffix, true
	case "count":
		return "count_v", true
	}
	return "", false
}

// scanKeyspaceColumns resolves the records scan's keyspace and peeks its column
// schema (footer only) via ColumnsSource; nil if unavailable.
func scanKeyspaceColumns(temps []interface{}, scanTemp int) []records.ColumnMeta {
	if scanTemp < 0 || scanTemp >= len(temps) {
		return nil
	}
	scan, ok := temps[scanTemp].(recordsScanPlan)
	if !ok {
		return nil
	}
	ks := scan.Keyspace()
	if ks == nil {
		return nil
	}
	opts := ScanWalkOptions
	opts.PathPrefix = metaPathPrefix(ks)
	src, err := openKeyspaceRecords(ks, opts, nil)
	if err != nil {
		return nil
	}
	defer src.Close()
	cs, ok := src.(records.ColumnsSource)
	if !ok {
		return nil
	}
	return cs.Columns()
}

// aggOperandRT is the runtime source for one aggregate: a bare projected column, or
// a binary arithmetic of two column/constant terms materialized as float64.
type aggOperandRT struct {
	arith          bool
	col            int // bare: projected column index
	op             byte
	lCol, rCol     int     // arith term column index, or -1 for a constant
	lConst, rConst float64 // used when the corresponding col index is -1
	lIsInt, rIsInt bool    // widen an int64 term to float64 before arithmetic
}

// parseAggOperandRT decodes a spec's srcSpec (a field name or an ["arith", ...]
// form) into runtime form, projecting each referenced column via addCol.
func parseAggOperandRT(src interface{}, addCol func(string) int) aggOperandRT {
	if field, ok := src.(string); ok {
		return aggOperandRT{col: addCol(field)}
	}
	a := src.([]interface{}) // ["arith", op, leftTerm, rightTerm]
	lCol, lConst, lIsInt := parseTermRT(a[2], addCol)
	rCol, rConst, rIsInt := parseTermRT(a[3], addCol)
	return aggOperandRT{
		arith: true, op: a[1].(string)[0],
		lCol: lCol, lConst: lConst, lIsInt: lIsInt,
		rCol: rCol, rConst: rConst, rIsInt: rIsInt,
	}
}

func parseTermRT(t interface{}, addCol func(string) int) (col int, c float64, isInt bool) {
	term := t.([]interface{})
	if term[0].(string) == "col" {
		return addCol(term[1].(string)), 0, term[2].(string) == "INT64"
	}
	return -1, term[1].(float64), false
}

// DatastoreColumnarAgg executes a fused columnar aggregation: open the keyspace,
// project the aggregated columns, and fold each Arrow column batch directly into
// the vectorized accumulators (no transpose), emitting one result row.
func DatastoreColumnarAgg(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	atomic.AddInt64(&VectorizedAggApplied, 1)

	context := vars.Temps[0].(*GlueContext)

	scanTemp := o.Params[0].(int)
	scan, ok := vars.Temps[scanTemp].(recordsScanPlan)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreColumnarAgg: unexpected plan %T", vars.Temps[scanTemp]))
		return
	}
	keyspace := scan.Keyspace()

	opts := ScanWalkOptions
	opts.PathPrefix = metaPathPrefix(keyspace)
	src, err := openKeyspaceRecords(keyspace, opts, context)
	if err != nil {
		yieldErr(fmt.Errorf("DatastoreColumnarAgg, open %q: %v", keyspace.Name(), err))
		return
	}
	defer src.Close()

	specs := o.Params[1].([]interface{})
	aggs := make([]*base.Agg, len(specs))
	keys := make([]string, len(specs)) // catalog key per agg, for masked dispatch
	// Project the DISTINCT referenced fields (several aggs/operands may share one,
	// e.g. SUM(x),COUNT(x),AVG(x) or SUM(x*y),SUM(x)); operands[i] maps agg i to its
	// bare column or arithmetic terms in each batch.
	pos := map[string]int{}
	var proj []string
	addCol := func(field string) int {
		p, seen := pos[field]
		if !seen {
			p = len(proj)
			pos[field] = p
			proj = append(proj, field)
		}
		return p
	}
	operands := make([]aggOperandRT, len(specs))
	for i, sp := range specs {
		s := sp.([]interface{})
		keys[i] = s[0].(string)
		aggs[i] = base.Aggs[base.AggCatalog[keys[i]]]
		operands[i] = parseAggOperandRT(s[1], addCol)
	}

	// Optional WHERE predicate (5.4c/5.4d): project each clause's column and, per
	// batch, evaluate the clauses into one selection bitmap (AND/OR-combined) that
	// gates the masked reducers.
	var pred *colPredicate
	var clauseCol []int // projected column index per clause
	if len(o.Params) > 2 && o.Params[2] != nil {
		ps := o.Params[2].([]interface{})
		rawCls := ps[1].([]interface{})
		clauses := make([]colClause, len(rawCls))
		clauseCol = make([]int, len(rawCls))
		for i, rc := range rawCls {
			c := rc.([]interface{})
			clauses[i] = colClause{
				field:   c[0].(string),
				op:      base.CmpOp(c[1].(int)),
				c:       c[2].(float64),
				colType: c[3].(string),
			}
			clauseCol[i] = addCol(clauses[i].field)
		}
		pred = &colPredicate{mode: ps[0].(string), clauses: clauses}
	}

	cp, ok := src.(records.ColumnsProjector)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreColumnarAgg: %q source not projectable", keyspace.Name()))
		return
	}
	if err := cp.ProjectColumns(proj); err != nil {
		yieldErr(err)
		return
	}
	cbs, ok := src.(records.ColumnBatchSource)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreColumnarAgg: %q source not a ColumnBatchSource", keyspace.Name()))
		return
	}

	accs := make([][]byte, len(aggs))
	for i := range aggs {
		accs[i] = aggs[i].Init(vars, nil)
	}

	// Scratch buffers reused across batches: predMask accumulates the combined WHERE
	// selection, clauseMask holds one clause's mask while combining, combined holds
	// sel AND an agg source's validity; arithDst holds a materialized arithmetic
	// column, lWiden/rWiden hold int64->float64 widened operands, and arithVal holds
	// a combined operand validity.
	var predMask, clauseMask, combined []byte
	var arithDst, lWiden, rWiden, arithVal []byte

	// f64View returns a float64 view of column ci: the borrowed bytes when it's
	// already DOUBLE, else the int64 column widened into scratch.
	f64View := func(ci int, isInt bool, scratch *[]byte, cols [][]byte, rows int) []byte {
		if !isInt {
			return cols[ci]
		}
		*scratch = resizeBytes(*scratch, rows*8)
		base.LoadFloat64FromInt64(*scratch, cols[ci], rows)
		return *scratch
	}

	// aggSource returns the column bytes an aggregate folds over (a borrowed column
	// or a materialized arithmetic result) and that source's validity (nil = all
	// valid). Arithmetic materializes into arithDst as float64; its validity is the
	// AND of the term columns' validities (a constant term is always valid).
	aggSource := func(op aggOperandRT, cols, valids [][]byte, rows int) (colBytes, av []byte) {
		if !op.arith {
			return cols[op.col], valids[op.col]
		}
		arithDst = resizeBytes(arithDst, rows*8)
		switch {
		case op.lCol >= 0 && op.rCol >= 0:
			base.ArithFloat64(arithDst,
				f64View(op.lCol, op.lIsInt, &lWiden, cols, rows),
				f64View(op.rCol, op.rIsInt, &rWiden, cols, rows), rows, op.op)
		case op.lCol >= 0: // right is the constant
			base.ScaleFloat64(arithDst, f64View(op.lCol, op.lIsInt, &lWiden, cols, rows),
				op.rConst, op.op, true, rows)
		default: // left is the constant
			base.ScaleFloat64(arithDst, f64View(op.rCol, op.rIsInt, &rWiden, cols, rows),
				op.lConst, op.op, false, rows)
		}
		lv, rv := colValidity(op.lCol, valids), colValidity(op.rCol, valids)
		switch {
		case lv == nil:
			av = rv
		case rv == nil:
			av = lv
		default:
			arithVal = resizeBytes(arithVal, (rows+7)/8)
			copy(arithVal, lv)
			base.AndBitmap(arithVal, rv)
			av = arithVal
		}
		return arithDst, av
	}

	// evalClause writes clause cl's selected-and-valid rows into dst (compare ->
	// bitmap, then AND the clause column's validity so a null clause row is 0 --
	// which is the right identity for both AND- and OR-combining under N1QL's
	// three-valued logic: NULL never makes WHERE true).
	evalClause := func(dst []byte, cl colClause, ci int, cols, valids [][]byte, rows int) {
		switch cl.colType {
		case "DOUBLE":
			base.FilterFloat64(dst, cols[ci], rows, cl.op, cl.c)
		case "INT64":
			base.FilterInt64(dst, cols[ci], rows, cl.op, int64(cl.c))
		}
		if v := valids[ci]; v != nil {
			base.AndBitmap(dst, v)
		}
	}

	for {
		cols, valids, rows, ok, err := cbs.NextColumns()
		if err != nil {
			yieldErr(fmt.Errorf("DatastoreColumnarAgg, next: %v", err))
			return
		}
		if !ok {
			break
		}
		if len(cols) != len(proj) {
			yieldErr(fmt.Errorf("DatastoreColumnarAgg: got %d columns, want %d", len(cols), len(proj)))
			return
		}

		// Batch selection mask (WHERE): evaluate clause 0, then AND/OR each further
		// clause's mask into it. sel stays nil (== all rows) when there's no WHERE.
		var sel []byte
		if pred != nil {
			predMask = resize(predMask, rows)
			evalClause(predMask, pred.clauses[0], clauseCol[0], cols, valids, rows)
			for k := 1; k < len(pred.clauses); k++ {
				clauseMask = resize(clauseMask, rows)
				evalClause(clauseMask, pred.clauses[k], clauseCol[k], cols, valids, rows)
				if pred.mode == "or" {
					base.OrBitmap(predMask, clauseMask)
				} else {
					base.AndBitmap(predMask, clauseMask)
				}
			}
			sel = predMask
		}

		for i := range aggs {
			colBytes, av := aggSource(operands[i], cols, valids, rows)
			if sel == nil && av == nil {
				// No filter, no nulls: the unmasked fast path (all rows).
				accs[i], _, _ = aggs[i].Update(vars, base.Val(colBytes), nil, accs[i], nil)
				continue
			}
			// sum mask = selection ∧ this source's validity (nil-aware): SUM/AVG-sum
			// skip nulls; COUNT/AVG-count use sel alone (see applyMaskedAgg).
			var sum []byte
			switch {
			case sel == nil:
				sum = av // nulls only
			case av == nil:
				sum = sel // filter only
			default:
				combined = resize(combined, rows)
				copy(combined, sel)
				base.AndBitmap(combined, av)
				sum = combined
			}
			applyMaskedAgg(keys[i], accs[i], colBytes, sel, sum, rows)
		}
	}

	// One result row: each aggregate's formatted value, positionally under the
	// group op's kept "^aggregates|..." labels. Fresh buffer per result so the
	// values don't alias.
	var out base.Vals
	for i := range aggs {
		v, _, _ := aggs[i].Result(vars, accs[i], nil)
		out = append(out, v)
	}
	yieldVals(out)

	yieldErr(nil)
}

// resizeBytes returns buf reused (or freshly grown) to hold exactly nBytes.
func resizeBytes(buf []byte, nBytes int) []byte {
	if cap(buf) < nBytes {
		return make([]byte, nBytes)
	}
	return buf[:nBytes]
}

// colValidity returns column col's validity, or nil for a constant term (col < 0).
func colValidity(col int, valids [][]byte) []byte {
	if col < 0 {
		return nil
	}
	return valids[col]
}

// resize returns buf reused (or freshly grown) to hold a dense bitmap for n rows.
func resize(buf []byte, n int) []byte {
	nb := (n + 7) / 8
	if cap(buf) < nb {
		return make([]byte, nb)
	}
	return buf[:nb]
}

// applyMaskedAgg folds a column into acc, dispatched by the same catalog key
// columnarAggSpecs assigned -- so the masked path reuses the exact accumulators
// (and thus Result formatting) as the unmasked lane. sel is the row SELECTION
// (predicate; nil = all rows); sum is sel∧validity (nil = all lanes). SUM folds
// over sum (skips nulls); COUNT counts sel (n1k1 COUNT(x) counts every selected
// row, null or not); AVG divides sum-over-sum by count-over-sel.
func applyMaskedAgg(key string, acc, col, sel, sum []byte, n int) {
	switch key {
	case "sum_v_float64":
		base.SumMaskedFloat64(acc, col, sum, n)
	case "sum_v_int64":
		base.SumMaskedInt64(acc, col, sum, n)
	case "count_v":
		base.CountMasked(acc, sel, n)
	case "avg_v_float64":
		base.AvgMaskedFloat64(acc, col, sel, sum, n)
	case "avg_v_int64":
		base.AvgMaskedInt64(acc, col, sel, sum, n)
	}
}

// DatastoreMetadataAgg answers COUNT/MIN/MAX from the keyspace's footer statistics
// alone -- no data pages read. It reads the aggregate ColumnsSource stats (summed
// counts, min-of-mins, max-of-maxs across parts) and emits one result row.
func DatastoreMetadataAgg(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	atomic.AddInt64(&MetadataAggApplied, 1)

	context := vars.Temps[0].(*GlueContext)
	scanTemp := o.Params[0].(int)
	scan, ok := vars.Temps[scanTemp].(recordsScanPlan)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreMetadataAgg: unexpected plan %T", vars.Temps[scanTemp]))
		return
	}
	keyspace := scan.Keyspace()

	opts := ScanWalkOptions
	opts.PathPrefix = metaPathPrefix(keyspace)
	src, err := openKeyspaceRecords(keyspace, opts, context)
	if err != nil {
		yieldErr(fmt.Errorf("DatastoreMetadataAgg, open %q: %v", keyspace.Name(), err))
		return
	}
	defer src.Close()

	cs, ok := src.(records.ColumnsSource)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreMetadataAgg: %q source exposes no schema", keyspace.Name()))
		return
	}
	cols := cs.Columns()
	colByName := map[string]records.ColumnMeta{}
	for _, c := range cols {
		colByName[c.Name] = c
	}
	var rowCount int64
	if len(cols) > 0 {
		rowCount = cols[0].Count // any flat column's value count == the row count
	}

	specs := o.Params[1].([]interface{})
	out := make(base.Vals, 0, len(specs))
	for _, sp := range specs {
		s := sp.([]interface{})
		kind, field, typ := s[0].(string), s[1].(string), s[2].(string)
		var v base.Val
		switch kind {
		case "count-star":
			v = base.Val(strconv.AppendInt(nil, rowCount, 10))
		case "count":
			// n1k1 COUNT(x) counts every row (null/missing included), like COUNT(*),
			// so it's the row count -- NOT Count-NullCount.
			_ = field
			v = base.Val(strconv.AppendInt(nil, rowCount, 10))
		case "min":
			v = formatStat(typ, colByName[field].Min)
		case "max":
			v = formatStat(typ, colByName[field].Max)
		}
		out = append(out, v)
	}
	yieldVals(out)

	yieldErr(nil)
}

// formatStat renders an encoded (8-byte LE) numeric stat as JSON, matching how the
// transpose/row path would render the same value (AppendInt / 'g' float), so a
// metadata MIN/MAX is byte-identical to the scalar one.
func formatStat(typ string, enc []byte) base.Val {
	if len(enc) != 8 {
		return base.ValMissing
	}
	switch typ {
	case "INT64":
		return base.Val(strconv.AppendInt(nil, int64(binary.LittleEndian.Uint64(enc)), 10))
	case "DOUBLE":
		f := math.Float64frombits(binary.LittleEndian.Uint64(enc))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return base.ValNull
		}
		return base.Val(strconv.AppendFloat(nil, f, 'g', -1, 64))
	}
	return base.ValMissing
}
