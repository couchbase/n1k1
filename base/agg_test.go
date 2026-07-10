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

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"
)

// TestArrayAggEmptyIsNull guards ARRAY_AGG's empty-group semantics: a group with
// no non-MISSING values aggregates to NULL, not [] (matches cbq -- aggregate over
// an empty set is NULL). Regression for the gsi array_functions cases where
// array_agg of an all-MISSING field fed array_append(NULL,...) = NULL. The fix
// lives in AggArrayAgg / AggArrayAggDistinct Result (agg.go); this is the fast
// unit-level guard for it.
func TestArrayAggEmptyIsNull(t *testing.T) {
	vc := &ValComparer{}

	for _, agg := range []*Agg{AggArrayAgg, AggArrayAggDistinct} {
		// Empty group (only a MISSING update, which is ignored) -> NULL.
		st := agg.Init(nil, nil)
		st, _, _ = agg.Update(nil, ValMissing, nil, st, vc)
		if v, _, _ := agg.Result(nil, st, nil); string(v) != "null" {
			t.Errorf("empty ARRAY_AGG = %q, want null", string(v))
		}

		// A real value still aggregates to a one-element array.
		st = agg.Init(nil, nil)
		st, _, _ = agg.Update(nil, Val(`"x"`), nil, st, vc)
		if v, _, _ := agg.Result(nil, st, nil); string(v) != `["x"]` {
			t.Errorf("ARRAY_AGG([\"x\"]) = %q, want [\"x\"]", string(v))
		}
	}
}

// aggResultOver runs the named aggregate over vals and returns its Result string.
func aggResultOver(name string, vals ...string) string {
	vc := &ValComparer{}
	agg := Aggs[AggCatalog[name]]
	st := agg.Init(nil, nil)
	for _, s := range vals {
		st, _, _ = agg.Update(nil, Val(s), nil, st, vc)
	}
	v, _, _ := agg.Result(nil, st, nil)
	return string(v)
}

// TestStatisticalAggs guards COUNTN / MEDIAN / STDDEV / VARIANCE. The expected
// floats mirror couchbase/query's two-pass algorithm exactly (values 10,10,11:
// mean=31/3, sum of squared deviations=2/3; var_samp=1/3, var_pop=2/9).
func TestStatisticalAggs(t *testing.T) {
	cases := []struct {
		name, want string
		vals       []string
	}{
		// COUNTN counts only numbers (the string is ignored); COUNT would be 4.
		{"countn", "3", []string{"10", "10", "11", `"x"`}},
		{"countn_distinct", "2", []string{"10", "10", "11"}},
		{"variance", "0.3333333333333333", []string{"10", "10", "11"}}, // sample
		{"var_samp", "0.3333333333333333", []string{"10", "10", "11"}},
		{"var_pop", "0.2222222222222222", []string{"10", "10", "11"}},
		{"stddev", "0.5773502691896257", []string{"10", "10", "11"}}, // sqrt(1/3)
		{"stddev_pop", "0.4714045207910317", []string{"10", "10", "11"}},
		{"median", "10", []string{"11", "10", "10"}},            // sorts first
		{"median", "1.5", []string{"2", "1"}},                   // even -> avg of two
		{"median_distinct", "10.5", []string{"10", "10", "11"}}, // distinct {10,11}
		// Population stat of a single value is 0; empty group is NULL.
		{"var_pop", "0", []string{"5"}},
		{"variance", "null", []string{"5"}},   // sample of one -> NULL
		{"variance", "null", []string{`"x"`}}, // no numbers -> NULL
	}
	for _, c := range cases {
		if got := aggResultOver(c.name, c.vals...); got != c.want {
			t.Errorf("%s(%v) = %q, want %q", c.name, c.vals, got, c.want)
		}
	}
}

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

// TestAggSumNullForNoNumericInput: N1QL SUM over zero numeric inputs is NULL, not 0
// (AggSum's NaN "no-input" sentinel). A sum of numbers that happens to be 0 still
// returns 0 -- the sentinel distinguishes "nothing summed" from "summed to zero".
func TestAggSumNullForNoNumericInput(t *testing.T) {
	sum := func(jsons ...string) string {
		agg := AggSum.Init(nil, nil)
		for _, js := range jsons {
			agg, _, _ = AggSum.Update(nil, Val(js), nil, agg, nil)
		}
		v, _, _ := AggSum.Result(nil, agg, nil)
		return string(v)
	}
	for _, c := range []struct {
		name, want string
		in         []string
	}{
		{"empty", "null", nil},
		{"all-strings", "null", []string{`"a"`, `"b"`}},
		{"all-null", "null", []string{"null", "null"}},
		{"mixed", "6", []string{`"a"`, "1", "null", "2", "3"}},
		{"sums-to-zero", "0", []string{"5", "-5"}},
	} {
		if got := sum(c.in...); got != c.want {
			t.Errorf("%s: SUM=%q want %q", c.name, got, c.want)
		}
	}
}

// TestAggAvgNullForEmpty: AVG over an empty group (count == 0) is NULL, not MISSING
// (Val(nil)) -- the empty-window-frame case. (NOTE: a group with rows but no NUMERIC
// values -- AVG(["a","b"]) -- still returns 0 here, not NULL; AggCount counts
// non-numeric inputs. That's a separate pre-existing gap, not exercised by the window
// corpus where the operand is always numeric.)
func TestAggAvgNullForEmpty(t *testing.T) {
	avg := func(jsons ...string) string {
		agg := AggAvg.Init(nil, nil)
		for _, js := range jsons {
			agg, _, _ = AggAvg.Update(nil, Val(js), nil, agg, nil)
		}
		v, _, _ := AggAvg.Result(nil, agg, nil)
		return string(v)
	}
	if got := avg(); got != "null" {
		t.Errorf("empty: AVG=%q want %q", got, "null")
	}
	if got := avg("1", "3"); got != "2" {
		t.Errorf("numeric: AVG=%q want %q", got, "2")
	}
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
