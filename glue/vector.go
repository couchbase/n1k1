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

// Columnar VECTOR_DISTANCE (DESIGN-vectors.md), the "compute columnar, then transpose
// to rows" design: a top-K vector query
//
//   SELECT id, VECTOR_DISTANCE(vec, $q, "cosine") AS d FROM ks ORDER BY d ASC LIMIT k
//
// lowers to project(VECTOR_DISTANCE) -> scan-records. The ONLY step worth vectorizing
// is the distance MAP -- the sort is over one float per row (trivial). So
// VectorColumnarScan reads the vec column as borrowed float32 (records.VectorBatchSource,
// no JSON parse), computes each row's distance on the byte lane
// (base.VectorDistanceVFloat32), and TRANSPOSES each page into ordinary rows
// {scalars..., distance}. Those rows feed the EXISTING row-lane order-offset-limit +
// project machinery, so ORDER/LIMIT/OFFSET need no new columnar operator -- the risky
// top-K executor never has to exist.

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"
)

// VectorColumnarApplied counts how many times the fused columnar VECTOR_DISTANCE op
// executed (test observability, like AggColumnarApplied).
var VectorColumnarApplied int64

// VectorColumnarScan streams src's vec column, computes each row's distance to q (the
// static query vector, already parsed to float64), and calls yield once per row with a
// fresh base.Vals = [scalarFields[0], .., scalarFields[n-1], distance]. Distances are
// formatted to match cbq's boxed / n1k1's native float64 VECTOR_DISTANCE bit-for-bit
// (base.VectorDistanceVFloat32 promotes the float32 storage to float64); a NULL distance
// (zero-norm cosine, or a NULL/absent vec row) is base.ValNull. The yielded Vals own
// their bytes, so they outlive the borrowed batch. yield may return an error to stop.
func VectorColumnarScan(src records.VectorBatchSource, vecField string, scalarFields []string,
	q []float64, metric string, yield func(base.Vals) error) error {
	var dist []float64
	for {
		vb, ok, err := src.NextVectorBatch(vecField, scalarFields)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if cap(dist) < vb.Rows {
			dist = make([]float64, vb.Rows)
		}
		dist = dist[:vb.Rows]

		switch {
		case len(q) != vb.Dim:
			// Query vector length != stored dim (a length mismatch, or a NULL/missing
			// query vector left q empty) -> every distance is NULL, matching cbq.
			for r := 0; r < vb.Rows; r++ {
				dist[r] = math.NaN()
			}
		case vb.Regular:
			// No nulls, fixed stride: the tight contiguous kernel over the whole page.
			base.VectorDistanceVFloat32(dist, vb.Vec, q, vb.Rows, vb.Dim, metric)
		default:
			// Nulls present -> per-row via offsets; a zero-length (NULL) row is NaN.
			for r := 0; r < vb.Rows; r++ {
				rv := vb.RowVec(r)
				if len(rv) == 0 {
					dist[r] = math.NaN()
					continue
				}
				base.VectorDistanceVFloat32(dist[r:r+1], rv, q, 1, len(rv), metric)
			}
		}

		for r := 0; r < vb.Rows; r++ {
			row := make(base.Vals, len(scalarFields)+1)
			for c := range scalarFields {
				row[c] = scalarValAt(&vb, c, r)
			}
			if math.IsNaN(dist[r]) {
				row[len(scalarFields)] = base.ValNull
			} else {
				row[len(scalarFields)] = base.Val(strconv.AppendFloat(nil, dist[r], 'f', -1, 64))
			}
			if err := yield(row); err != nil {
				return err
			}
		}
	}
	return nil
}

// scalarValAt formats scalar column c's row-r value as a JSON Val (owning its bytes),
// honoring the validity bitmap (a null lane -> ValNull). INT64 renders as an integer,
// DOUBLE as cbq's 'g' float (formatStat), and UTF8 as a JSON string -- matching the row
// lane. So a string doc id is a first-class kept column on the columnar fast path.
func scalarValAt(vb *records.VectorBatch, c, r int) base.Val {
	if v := vb.ScalarValids[c]; v != nil && v[r>>3]&(1<<(uint(r)&7)) == 0 {
		return base.ValNull
	}
	if vb.ScalarTypes[c] == "UTF8" {
		b, err := json.Marshal(vb.ScalarStrings[c][r])
		if err != nil {
			return base.ValNull
		}
		return base.Val(b)
	}
	buf := vb.Scalars[c]
	return formatStat(vb.ScalarTypes[c], buf[r*8:r*8+8])
}

// DatastoreVectorColumnar executes the fused columnar VECTOR_DISTANCE op: it opens the
// keyspace's vec source, runs VectorColumnarScan, and emits one row per source row with
// values positioned under the op's kept Labels (the inner project's labels). Params:
// [scanTemp, vecField, metric, q ([]float64), labelSpecs]. Each labelSpec (parallel to
// Labels) is ["scalar", field] | ["dist"] | ["pass"] (a discarded source passthrough,
// emitted MISSING -- the rewrite only fires when the passthrough is provably unread).
func DatastoreVectorColumnar(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
	atomic.AddInt64(&VectorColumnarApplied, 1)

	context := vars.Temps[0].(*GlueContext)
	scanTemp := o.Params[0].(int)
	vecField := o.Params[1].(string)
	metric := o.Params[2].(string)
	rawSpecs := o.Params[4].([]interface{})

	// The query vector: a plan-time constant []float64, or a row-independent expression
	// (a WITH alias / $param / VECTORIZE_BATCH(...)) evaluated ONCE here -- same vector for
	// every row, so the columnar kernel runs just like the literal case. A query that
	// doesn't evaluate to a numeric array (NULL / missing / wrong shape) leaves q nil, and
	// the length-mismatch guard in VectorColumnarScan makes every distance NULL (matching
	// cbq's VECTOR_DISTANCE on a bad query vector).
	var q []float64
	switch qv := o.Params[3].(type) {
	case []float64:
		q = qv
	case expression.Expression:
		val, _, err := EvalExpr(context, qv, context.withScope)
		if err != nil {
			yieldErr(fmt.Errorf("DatastoreVectorColumnar, query vector: %v", err))
			return
		}
		q = valueToFloat64s(val)
	}

	// Build scalarFields (distinct, first-seen order) + a per-output-label fill plan.
	type fill struct {
		kind string // "scalar" | "dist" | "pass"
		k    int    // scalar column index (for kind=="scalar")
	}
	fills := make([]fill, len(rawSpecs))
	var scalarFields []string
	fieldIdx := map[string]int{}
	for i, rs := range rawSpecs {
		sp := rs.([]interface{})
		switch sp[0].(string) {
		case "scalar":
			f := sp[1].(string)
			k, ok := fieldIdx[f]
			if !ok {
				k = len(scalarFields)
				fieldIdx[f] = k
				scalarFields = append(scalarFields, f)
			}
			fills[i] = fill{"scalar", k}
		case "dist":
			fills[i] = fill{kind: "dist"}
		default:
			fills[i] = fill{kind: "pass"}
		}
	}

	scan, ok := vars.Temps[scanTemp].(keyspacer)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreVectorColumnar: unexpected plan %T", vars.Temps[scanTemp]))
		return
	}
	keyspace := scan.Keyspace()
	opts := ScanWalkOptions
	opts.PathPrefix = KeyspaceMetaPathPrefix(keyspace)
	src, err := KeyspaceRecordsOpen(keyspace, opts, context)
	if err != nil {
		yieldErr(fmt.Errorf("DatastoreVectorColumnar, open %q: %v", keyspace.Name(), err))
		return
	}
	defer src.Close()
	vbs, ok := src.(records.VectorBatchSource)
	if !ok {
		yieldErr(fmt.Errorf("DatastoreVectorColumnar: %q source is not a VectorBatchSource", keyspace.Name()))
		return
	}

	scanErr := VectorColumnarScan(vbs, vecField, scalarFields, q, metric, func(row base.Vals) error {
		// Fresh per row: the parent order-offset-limit buffers rows, so we must NOT
		// alias one output slice across rows.
		out := make(base.Vals, len(fills))
		for i, f := range fills {
			switch f.kind {
			case "scalar":
				out[i] = row[f.k]
			case "dist":
				out[i] = row[len(scalarFields)]
			default:
				out[i] = base.ValMissing // discarded passthrough
			}
		}
		yieldVals(out)
		return nil
	})
	if scanErr != nil {
		yieldErr(fmt.Errorf("DatastoreVectorColumnar, scan: %v", scanErr))
		return
	}
	yieldErr(nil)
}

// maybeVectorColumnarFuse rewrites the canonical columnar-VECTOR_DISTANCE top-K shape
//
//	project(strip) -> order-offset-limit -> project(aug: VECTOR_DISTANCE over a Parquet
//	                                        vec column + scalar fields) -> scan-records
//
// by fusing the inner (aug) project + scan into one "vector-distance-columnar" op that
// computes distances columnar and transposes to rows; the order-offset-limit + strip are
// left intact (the row lane does the actual top-K). Conservative: it fires only when it
// can prove (a) exactly one operand is VECTOR_DISTANCE over a list<float32> vec column
// with a constant metric and a query vector that is either a plan-time constant OR a
// row-independent expression (a WITH alias / $param / VECTORIZE_BATCH call) the fused op
// evaluates ONCE, (b) the other operands are bare numeric
// scalar columns or source passthroughs, and (c) every passthrough label is UNREFERENCED
// above (dropped by the strip, not used by the ORDER sort) -- so emitting it MISSING is
// invisible. Anything else keeps the row path.
func maybeVectorColumnarFuse(strip *base.Op, temps []interface{}) {
	if strip.Kind != "project" || len(strip.Children) != 1 {
		return
	}
	order := strip.Children[0]
	if order.Kind != "order-offset-limit" || len(order.Children) != 1 || len(order.Params) == 0 {
		return
	}
	aug := order.Children[0]
	if aug.Kind != "project" || len(aug.Children) != 1 || len(aug.Params) != len(aug.Labels) {
		return
	}
	scan := aug.Children[0]
	if scan.Kind != "datastore-scan-records" || len(scan.Params) == 0 {
		return
	}
	scanTemp, ok := scan.Params[0].(int)
	if !ok {
		return
	}

	// Open the keyspace footer (schema only) to confirm the vec + scalar columns.
	sc, ok := temps[scanTemp].(keyspacer)
	if !ok {
		return
	}
	ks := sc.Keyspace()
	if ks == nil {
		return
	}
	opts := ScanWalkOptions
	opts.PathPrefix = KeyspaceMetaPathPrefix(ks)
	src, err := KeyspaceRecordsOpen(ks, opts, nil)
	if err != nil {
		return
	}
	defer src.Close()
	vss, ok := src.(records.VectorSchemaSource)
	if !ok {
		return
	}

	// The source (keyspace) alias, so a query vector that references it is row-DEPENDENT
	// and can't be hoisted; a query vector referencing only WITH aliases / params / literals
	// is row-INDEPENDENT and is evaluated once at runtime.
	srcAlias, srcAliasOK := "", false
	if len(scan.Labels) > 0 {
		srcAlias, srcAliasOK = labelToAlias(string(scan.Labels[0]))
	}

	specs := make([]interface{}, len(aug.Labels))
	var vecField, metric string
	var qArg interface{} // []float64 (constant) or expression.Expression (evaluated once)
	haveDist := false
	passLabels := map[string]bool{}
	for i := range aug.Labels {
		p, ok := aug.Params[i].([]interface{})
		if !ok || len(p) < 1 {
			return
		}
		switch p[0].(string) {
		case "labelPath":
			specs[i] = []interface{}{"pass"}
			passLabels[string(aug.Labels[i])] = true
		case "exprTree":
			expr, ok := p[1].(expression.Expression)
			if !ok {
				return
			}
			if vf, qConst, qExpr, m, ok := vectorDistanceParts(expr); ok {
				if haveDist || !vss.VectorField(vf) {
					return
				}
				if qConst != nil {
					qArg = qConst
				} else if rowIndependent(qExpr, srcAlias, srcAliasOK) {
					// Hoistable: same vector for every row -> evaluate once at runtime.
					qArg = qExpr
				} else {
					return // a per-row query vector can't be hoisted -> row lane
				}
				vecField, metric, haveDist = vf, m, true
				specs[i] = []interface{}{"dist"}
			} else if f, ok := bareFieldOfExpr(expr); ok {
				if !vss.ScalarField(f) {
					return
				}
				specs[i] = []interface{}{"scalar", f}
			} else {
				return
			}
		default:
			return
		}
	}
	if !haveDist {
		return
	}

	// Safety: every passthrough label must be provably unread above -- dropped by the
	// strip AND not referenced by the ORDER sort terms. Else emitting MISSING for it
	// would change results, so keep the row path.
	if len(passLabels) > 0 {
		for _, kept := range strip.Labels {
			if passLabels[string(kept)] {
				return
			}
		}
		if orderRefsPassthrough(order, passLabels) {
			return
		}
	}

	aug.Kind = "vector-distance-columnar"
	aug.Params = []interface{}{scanTemp, vecField, metric, qArg, specs}
	aug.Children = nil
}

// rowIndependent reports whether a query-vector expression can be hoisted out of the
// per-row loop (evaluated once): it must not reference the scanned document. In the
// single-scan search shape the only keyspace identifier is srcAlias, so any other
// identifier (a WITH alias, a $param, a function over literals) is row-independent.
// A field ref like `v.other` re-introduces srcAlias -> not hoistable.
func rowIndependent(e expression.Expression, srcAlias string, srcAliasOK bool) bool {
	if !srcAliasOK {
		return false // couldn't identify the source alias -> be conservative
	}
	ids := map[string]bool{}
	collectIdentifiers(e, ids)
	return !ids[srcAlias]
}

// vectorDistanceParts extracts the parts of a VECTOR_DISTANCE(field, query, metric)
// expression. ok=false unless the 1st operand is a bare field ref and the 3rd a constant
// string metric. The query (2nd operand) comes back one of two ways: qConst is set when
// it's a plan-time constant numeric array (the literal fast path), else qExpr is the raw
// operand expression -- a $param, a WITH alias, or a VECTORIZE_BATCH(...) call. The caller
// decides whether qExpr is hoistable (row-independent -> evaluate once) or keeps the row
// path.
func vectorDistanceParts(e expression.Expression) (field string, qConst []float64, qExpr expression.Expression, metric string, ok bool) {
	fn, isFn := e.(expression.Function)
	if !isFn || fn.Name() != "vector_distance" {
		return "", nil, nil, "", false
	}
	ops := fn.Operands()
	if len(ops) != 3 {
		return "", nil, nil, "", false
	}
	field, ok = bareFieldOfExpr(ops[0])
	if !ok {
		return "", nil, nil, "", false
	}
	mv := ops[2].Value()
	if mv == nil {
		return "", nil, nil, "", false
	}
	ms, ok := stringOf(mv.Actual())
	if !ok {
		return "", nil, nil, "", false
	}
	metric = strings.ToLower(ms)

	// Query operand: a plan-time constant numeric array -> qConst; otherwise hand back the
	// raw expression for the caller to hoist (evaluate once at runtime) if row-independent.
	if av := ops[1].Value(); av != nil {
		if arr, ok := av.Actual().([]interface{}); ok && len(arr) > 0 {
			q := make([]float64, len(arr))
			allNum := true
			for i, x := range arr {
				if f, ok := floatOf(x); ok {
					q[i] = f
				} else {
					allNum = false
					break
				}
			}
			if allNum {
				return field, q, nil, metric, true
			}
		}
	}
	return field, nil, ops[1], metric, true
}

// orderRefsPassthrough reports whether any ORDER BY sort term references a passthrough
// label (directly as a labelPath, or via an identifier equal to a passthrough label's
// alias) -- in which case the fused op can't emit that label as MISSING.
func orderRefsPassthrough(order *base.Op, passLabels map[string]bool) bool {
	passAlias := map[string]bool{}
	for l := range passLabels {
		a, ok := labelToAlias(l)
		if !ok {
			return true // unparseable label -> be conservative
		}
		passAlias[a] = true
	}
	terms, ok := order.Params[0].([]interface{})
	if !ok {
		return true
	}
	for _, t := range terms {
		tp, ok := t.([]interface{})
		if !ok || len(tp) < 1 {
			return true
		}
		switch tp[0].(string) {
		case "labelPath":
			if l, ok := tp[1].(string); ok && passLabels[l] {
				return true
			}
		case "exprTree":
			expr, ok := tp[1].(expression.Expression)
			if !ok {
				return true
			}
			ids := map[string]bool{}
			collectIdentifiers(expr, ids)
			for a := range ids {
				if passAlias[a] {
					return true
				}
			}
		}
	}
	return false
}

func collectIdentifiers(e expression.Expression, out map[string]bool) {
	if id, ok := e.(*expression.Identifier); ok {
		out[id.Alias()] = true
	}
	for _, c := range e.Children() {
		collectIdentifiers(c, out)
	}
}

// labelToAlias parses a simple top-level label `.["name"]` to its alias.
func labelToAlias(l string) (string, bool) {
	if strings.HasPrefix(l, `.["`) && strings.HasSuffix(l, `"]`) && len(l) > 5 {
		return l[3 : len(l)-2], true
	}
	return "", false
}

// floatOf unwraps a constant array element (a raw float64 or a boxed value.Value) to a
// float64. cbq's ArrayConstruct.Value().Actual() yields []interface{} of value.Value.
func floatOf(x interface{}) (float64, bool) {
	switch v := x.(type) {
	case float64:
		return v, true
	case value.Value:
		f, ok := v.Actual().(float64)
		return f, ok
	}
	return 0, false
}

// valueToFloat64s unwraps a value.Value numeric array to []float64 (nil if it isn't a
// numeric array -- a NULL / MISSING / non-array / non-numeric query vector). Used to turn
// the once-evaluated query-vector expression into the kernel's []float64.
func valueToFloat64s(v value.Value) []float64 {
	if v == nil || v.Type() != value.ARRAY {
		return nil
	}
	arr, ok := v.Actual().([]interface{})
	if !ok || len(arr) == 0 {
		return nil
	}
	out := make([]float64, len(arr))
	for i := range arr {
		ev, _ := v.Index(i)
		f, ok := floatOf(ev)
		if !ok {
			return nil
		}
		out[i] = f
	}
	return out
}

func stringOf(x interface{}) (string, bool) {
	switch v := x.(type) {
	case string:
		return v, true
	case value.Value:
		s, ok := v.Actual().(string)
		return s, ok
	}
	return "", false
}
