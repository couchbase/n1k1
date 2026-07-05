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
// it only fires when it can prove a numeric, non-null column of a records scan.
//
// Step 5.4c fuses a single-comparison WHERE into that path: `SELECT SUM(x) FROM data
// WHERE y > c` matches group->filter->scan, extracts (field, op, const) from the
// filter's cbq predicate, and per batch evaluates the filter kernel (base.Filter*)
// into a dense selection bitmap, then folds via the masked reducers (base.SumMasked*).
// The predicate column must be numeric + non-null (FilterFloat64/Int64 don't consult
// validity); any predicate we can't prove numeric-single-comparison stays on the row
// path. cbq normalizes >/>= to LT/LE with swapped operands, so we match only LT/LE/Eq.

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

	// A predicate's column must be numeric + non-null (the selection kernels don't
	// consult validity), and an INT64 column requires an integer constant so the
	// int64 kernel is exact. Otherwise keep the row path.
	if pred != nil {
		cm, ok := colByName[pred.field]
		if !ok || cm.NullCount != 0 {
			return
		}
		switch cm.Type {
		case "DOUBLE":
			pred.colType = "DOUBLE"
		case "INT64":
			if pred.c != math.Trunc(pred.c) || math.IsInf(pred.c, 0) {
				return // non-integer constant vs int column -- row path
			}
			pred.colType = "INT64"
		default:
			return
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

// colPredicate is a single vectorizable comparison `field <op> c`.
type colPredicate struct {
	field   string
	op      base.CmpOp
	c       float64
	colType string // "DOUBLE" | "INT64", filled once the column is resolved
}

// predSpec flattens a predicate to plain interface{} for op.Params, or nil.
func predSpec(p *colPredicate) interface{} {
	if p == nil {
		return nil
	}
	return []interface{}{p.field, int(p.op), p.c, p.colType}
}

// extractColPredicate reduces a `filter` op's cbq condition to a single numeric
// comparison of a bare keyspace field against a constant. cbq rewrites >/>= to
// LT/LE with swapped operands, so we handle LT/LE/Eq and read operand order to get
// the effective op. Anything else (AND/OR, field-to-field, non-numeric) -> false.
func extractColPredicate(filter *base.Op) (colPredicate, bool) {
	if len(filter.Params) != 2 {
		return colPredicate{}, false
	}
	cond, ok := filter.Params[1].(expression.Expression)
	if !ok {
		return colPredicate{}, false
	}
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
		return colPredicate{}, false
	}

	// Orient: exactly one side a bare field, the other a numeric constant.
	field, fieldFirst, c, ok := orientComparison(a, b)
	if !ok {
		return colPredicate{}, false
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
	return colPredicate{field: field, op: op, c: c}, true
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
// SUM/AVG/COUNT of a bare, numeric, non-null column (scan kernels). ok=false else.
func columnarAggSpecs(aggExprs, aggCalcs []interface{}, colByName map[string]records.ColumnMeta) ([]interface{}, bool) {
	specs := make([]interface{}, 0, len(aggCalcs))
	for i := range aggCalcs {
		calc, ok := aggCalcs[i].([]interface{})
		if !ok || len(calc) != 1 {
			return nil, false
		}
		aggName, _ := calc[0].(string)
		field, ok := bareAggField(aggExprs[i])
		if !ok {
			return nil, false
		}
		cm, ok := colByName[field]
		if !ok || cm.NullCount != 0 {
			return nil, false
		}
		key, ok := vecAggCatalogKey(aggName, cm.Type)
		if !ok {
			return nil, false
		}
		specs = append(specs, []interface{}{key, field})
	}
	return specs, true
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
	// Project the DISTINCT agg fields (several aggs may share one, e.g.
	// SUM(x),COUNT(x),AVG(x)); aggCol[i] maps agg i to its column in each batch.
	pos := map[string]int{}
	var proj []string
	aggCol := make([]int, len(specs))
	addCol := func(field string) int {
		p, seen := pos[field]
		if !seen {
			p = len(proj)
			pos[field] = p
			proj = append(proj, field)
		}
		return p
	}
	for i, sp := range specs {
		s := sp.([]interface{})
		keys[i] = s[0].(string)
		aggs[i] = base.Aggs[base.AggCatalog[keys[i]]]
		aggCol[i] = addCol(s[1].(string))
	}

	// Optional WHERE predicate (5.4c): also project its column and evaluate it into
	// a per-batch selection bitmap that gates the masked reducers.
	var pred *colPredicate
	predCol := -1
	if len(o.Params) > 2 && o.Params[2] != nil {
		ps := o.Params[2].([]interface{})
		pred = &colPredicate{
			field:   ps[0].(string),
			op:      base.CmpOp(ps[1].(int)),
			c:       ps[2].(float64),
			colType: ps[3].(string),
		}
		predCol = addCol(pred.field)
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

	var mask []byte // reused across batches when a predicate is present
	for {
		cols, rows, ok, err := cbs.NextColumns()
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
		if pred == nil {
			for i := range aggs {
				accs[i], _, _ = aggs[i].Update(vars, base.Val(cols[aggCol[i]]), nil, accs[i], nil)
			}
			continue
		}
		// Filtered: predicate column -> selection bitmap -> masked reduce.
		if nb := (rows + 7) / 8; cap(mask) < nb {
			mask = make([]byte, nb)
		} else {
			mask = mask[:nb]
		}
		switch pred.colType {
		case "DOUBLE":
			base.FilterFloat64(mask, cols[predCol], rows, pred.op, pred.c)
		case "INT64":
			base.FilterInt64(mask, cols[predCol], rows, pred.op, int64(pred.c))
		}
		for i := range aggs {
			applyMaskedAgg(keys[i], accs[i], cols[aggCol[i]], mask, rows)
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

// applyMaskedAgg folds the set lanes of a column (per the selection bitmap) into
// acc, dispatched by the same catalog key columnarAggSpecs assigned -- so the
// filtered path reuses the exact accumulators (and thus Result formatting) as the
// unfiltered vectorized path. count_v ignores the column (counts set bits).
func applyMaskedAgg(key string, acc, col, mask []byte, n int) {
	switch key {
	case "sum_v_float64":
		base.SumMaskedFloat64(acc, col, mask, n)
	case "sum_v_int64":
		base.SumMaskedInt64(acc, col, mask, n)
	case "count_v":
		base.CountMasked(acc, mask, n)
	case "avg_v_float64":
		base.AvgMaskedFloat64(acc, col, mask, n)
	case "avg_v_int64":
		base.AvgMaskedInt64(acc, col, mask, n)
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
			cm := colByName[field]
			v = base.Val(strconv.AppendInt(nil, cm.Count-cm.NullCount, 10))
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
