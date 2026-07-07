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

package base

// Differential test for the vectorized SUM kernels (DESIGN-col.md Step 5.1): a
// sum_v_* over a packed column must produce BYTE-IDENTICAL output to the scalar
// AggSum folding the same values one at a time. Bit-exact is achievable (not
// epsilon) because both accumulate float64 in the same order and reuse AggSum's
// Result formatter -- the whole point of reusing the accumulator.

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"
)

// sumScalar folds vals through the scalar AggSum, feeding each as JSON text (how
// a number reaches SUM on the row path), and returns AggSum's formatted result.
func sumScalar(vals []float64, asInt bool) string {
	agg := AggSum.Init(nil, nil)
	for _, f := range vals {
		var js []byte
		if asInt {
			js = strconv.AppendInt(nil, int64(f), 10)
		} else {
			js = strconv.AppendFloat(nil, f, 'g', -1, 64)
		}
		agg, _, _ = AggSum.Update(nil, Val(js), nil, agg, nil)
	}
	v, _, _ := AggSum.Result(nil, agg, nil)
	return string(v)
}

// sumVec folds vals through a vectorized aggregate (looked up by catalog name) as
// a single packed little-endian column Val, and returns its formatted result.
func sumVec(t *testing.T, aggName string, vals []float64, asInt bool) string {
	agg, ok := AggCatalog[aggName]
	if !ok {
		t.Fatalf("AggCatalog has no %q", aggName)
	}
	a := Aggs[agg]

	col := make([]byte, len(vals)*8)
	for i, f := range vals {
		if asInt {
			binary.LittleEndian.PutUint64(col[i*8:], uint64(int64(f)))
		} else {
			binary.LittleEndian.PutUint64(col[i*8:], math.Float64bits(f))
		}
	}

	acc := a.Init(nil, nil)
	acc, _, _ = a.Update(nil, Val(col), nil, acc, nil)
	v, _, _ := a.Result(nil, acc, nil)
	return string(v)
}

func TestAggSumVectorizedMatchesScalar(t *testing.T) {
	floatCases := [][]float64{
		{},                                    // empty -> "0"
		{0.5},                                 // single
		{1, 2, 3, 4, 5},                       // clean ints as floats
		{-1.5, 2.25, 1000.125, 3.14159, -0.0}, // signs, fractions, -0
		{0.1, 0.2, 0.3},                       // classic fp non-exactness
		{1e18, 1, -1e18},                      // magnitude spread (order matters)
	}
	// a larger deterministic case to exercise many slots
	big := make([]float64, 4096)
	for i := range big {
		big[i] = float64(i%1000) + 0.5
	}
	floatCases = append(floatCases, big)

	for i, vals := range floatCases {
		want := sumScalar(vals, false)
		got := sumVec(t, "sum_v_float64", vals, false)
		if got != want {
			t.Errorf("float case %d: sum_v_float64=%q, scalar sum=%q (vals=%v)", i, got, want, vals)
		}
	}

	intCases := [][]float64{
		{},
		{42},
		{-3, 0, 7, 1000000},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}
	bigI := make([]float64, 2048)
	for i := range bigI {
		bigI[i] = float64(i - 1000)
	}
	intCases = append(intCases, bigI)

	for i, vals := range intCases {
		want := sumScalar(vals, true)
		got := sumVec(t, "sum_v_int64", vals, true)
		if got != want {
			t.Errorf("int case %d: sum_v_int64=%q, scalar sum=%q (vals=%v)", i, got, want, vals)
		}
	}
}

func countScalar(n int) string {
	agg := AggCount.Init(nil, nil)
	for i := 0; i < n; i++ {
		// count_v counts a null-free column (null_count==0), so drive the scalar
		// reference with a non-NULL value per element -- COUNT skips NULL/MISSING.
		agg, _, _ = AggCount.Update(nil, Val("1"), nil, agg, nil)
	}
	v, _, _ := AggCount.Result(nil, agg, nil)
	return string(v)
}

func avgScalar(vals []float64, asInt bool) string {
	agg := AggAvg.Init(nil, nil)
	for _, f := range vals {
		var js []byte
		if asInt {
			js = strconv.AppendInt(nil, int64(f), 10)
		} else {
			js = strconv.AppendFloat(nil, f, 'g', -1, 64)
		}
		agg, _, _ = AggAvg.Update(nil, Val(js), nil, agg, nil)
	}
	v, _, _ := AggAvg.Result(nil, agg, nil)
	return string(v)
}

func TestAggCountAvgVectorizedMatchesScalar(t *testing.T) {
	cases := [][]float64{{}, {0.5}, {1, 2, 3, 4, 5}, {0.1, 0.2, 0.3}, {-5, 10, 2.5}}
	big := make([]float64, 3000)
	for i := range big {
		big[i] = float64(i%700) + 0.25
	}
	cases = append(cases, big)

	for i, vals := range cases {
		if got, want := sumVec(t, "count_v", vals, false), countScalar(len(vals)); got != want {
			t.Errorf("count case %d: count_v=%q, scalar=%q", i, got, want)
		}
		if got, want := sumVec(t, "avg_v_float64", vals, false), avgScalar(vals, false); got != want {
			t.Errorf("avg float case %d: avg_v_float64=%q, scalar=%q (vals=%v)", i, got, want, vals)
		}
		if got, want := sumVec(t, "avg_v_int64", vals, true), avgScalar(vals, true); got != want {
			t.Errorf("avg int case %d: avg_v_int64=%q, scalar=%q (vals=%v)", i, got, want, vals)
		}
	}
}
