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

		if vb.Regular {
			// No nulls, fixed stride: the tight contiguous kernel over the whole page.
			base.VectorDistanceVFloat32(dist, vb.Vec, q, vb.Rows, vb.Dim, metric)
		} else {
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
				row[c] = scalarValAt(vb.Scalars[c], vb.ScalarTypes[c], vb.ScalarValids[c], r)
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

// scalarValAt formats a scalar column's row-r value as a JSON Val (owning its bytes),
// honoring the validity bitmap (a null lane -> ValNull). Reuses formatStat (columnar.go)
// so an INT64 id renders as an integer and a DOUBLE as cbq's 'g' float, matching the
// row lane.
func scalarValAt(buf []byte, typ string, valid []byte, r int) base.Val {
	if valid != nil && buf != nil && valid[r>>3]&(1<<(uint(r)&7)) == 0 {
		return base.ValNull
	}
	return formatStat(typ, buf[r*8:r*8+8])
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
	q := o.Params[3].([]float64)
	rawSpecs := o.Params[4].([]interface{})

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
// with a STATIC query vector + constant metric, (b) the other operands are bare numeric
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

	specs := make([]interface{}, len(aug.Labels))
	var vecField, metric string
	var q []float64
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
			if vf, qv, m, ok := vectorDistanceParts(expr); ok {
				if haveDist || !vss.VectorField(vf) {
					return
				}
				vecField, q, metric, haveDist = vf, qv, m, true
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
	aug.Params = []interface{}{scanTemp, vecField, metric, q, specs}
	aug.Children = nil
}

// vectorDistanceParts extracts (vecField, static query vector, metric) from a
// VECTOR_DISTANCE(field, <const array>, <const string>) expression. ok=false unless the
// first operand is a bare field ref, the second a constant numeric array (a $param or
// WITH alias has a nil constant Value -> row path), and the third a constant string.
func vectorDistanceParts(e expression.Expression) (field string, q []float64, metric string, ok bool) {
	fn, isFn := e.(expression.Function)
	if !isFn || fn.Name() != "vector_distance" {
		return "", nil, "", false
	}
	ops := fn.Operands()
	if len(ops) != 3 {
		return "", nil, "", false
	}
	field, ok = bareFieldOfExpr(ops[0])
	if !ok {
		return "", nil, "", false
	}
	av := ops[1].Value()
	if av == nil {
		return "", nil, "", false
	}
	arr, ok := av.Actual().([]interface{})
	if !ok || len(arr) == 0 {
		return "", nil, "", false
	}
	q = make([]float64, len(arr))
	for i, x := range arr {
		f, ok := floatOf(x)
		if !ok {
			return "", nil, "", false
		}
		q[i] = f
	}
	mv := ops[2].Value()
	if mv == nil {
		return "", nil, "", false
	}
	ms, ok := stringOf(mv.Actual())
	if !ok {
		return "", nil, "", false
	}
	return field, q, strings.ToLower(ms), true
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
