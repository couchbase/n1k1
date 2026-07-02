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

import "testing"

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
	cases := []struct{ name, want string; vals []string }{
		// COUNTN counts only numbers (the string is ignored); COUNT would be 4.
		{"countn", "3", []string{"10", "10", "11", `"x"`}},
		{"countn_distinct", "2", []string{"10", "10", "11"}},
		{"variance", "0.3333333333333333", []string{"10", "10", "11"}},   // sample
		{"var_samp", "0.3333333333333333", []string{"10", "10", "11"}},
		{"var_pop", "0.2222222222222222", []string{"10", "10", "11"}},
		{"stddev", "0.5773502691896257", []string{"10", "10", "11"}},     // sqrt(1/3)
		{"stddev_pop", "0.4714045207910317", []string{"10", "10", "11"}},
		{"median", "10", []string{"11", "10", "10"}},                     // sorts first
		{"median", "1.5", []string{"2", "1"}},                            // even -> avg of two
		{"median_distinct", "10.5", []string{"10", "10", "11"}},          // distinct {10,11}
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
