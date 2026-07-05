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

import (
	"fmt"
	"sync/atomic"

	"github.com/couchbase/query/expression"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"
)

// DisableVectorizedAgg forces the row path (no columnar-agg rewrite). The
// differential test flips it to prove vectorized == scalar.
var DisableVectorizedAgg bool

// VectorizedAggApplied counts how many columnar-agg ops actually executed (test
// observability).
var VectorizedAggApplied int64

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
	// Child must be a records scan directly (no intervening filter/WHERE -- that
	// needs selection vectors, a later step).
	if len(op.Children) != 1 || op.Children[0].Kind != "datastore-scan-records" {
		return
	}
	scan := op.Children[0]
	if len(scan.Params) == 0 {
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

	// Every aggregate must be a supported vectorizable SUM of a bare, numeric,
	// non-null column -- else bail entirely (conservative; keep the row path).
	specs := make([]interface{}, 0, len(aggCalcs))
	for i := range aggCalcs {
		calc, ok := aggCalcs[i].([]interface{})
		if !ok || len(calc) != 1 {
			return
		}
		aggName, _ := calc[0].(string)
		field, ok := bareAggField(aggExprs[i])
		if !ok {
			return
		}
		cm, ok := colByName[field]
		if !ok || cm.NullCount != 0 {
			return
		}
		key, ok := vecAggCatalogKey(aggName, cm.Type)
		if !ok {
			return
		}
		specs = append(specs, []interface{}{key, field})
	}

	// Rewrite in place: the group op BECOMES the fused columnar-agg op. Its Labels
	// (the "^aggregates|..." ones) are kept verbatim, so the downstream project op
	// reads the results identically. The scan child is absorbed (the op opens the
	// source itself via scanTemp).
	op.Kind = "columnar-agg"
	op.Params = []interface{}{scanTemp, specs}
	op.Children = nil
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
	// Project the DISTINCT agg fields (several aggs may share one, e.g.
	// SUM(x),COUNT(x),AVG(x)); aggCol[i] maps agg i to its column in each batch.
	pos := map[string]int{}
	var proj []string
	aggCol := make([]int, len(specs))
	for i, sp := range specs {
		s := sp.([]interface{})
		aggs[i] = base.Aggs[base.AggCatalog[s[0].(string)]]
		field := s[1].(string)
		p, seen := pos[field]
		if !seen {
			p = len(proj)
			pos[field] = p
			proj = append(proj, field)
		}
		aggCol[i] = p
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

	for {
		cols, _, ok, err := cbs.NextColumns()
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
		for i := range aggs {
			accs[i], _, _ = aggs[i].Update(vars, base.Val(cols[aggCol[i]]), nil, accs[i], nil)
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
