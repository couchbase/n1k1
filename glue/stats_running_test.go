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

import (
	"strings"
	"testing"
)

// TestRunningAggLabels checks that RunningAggLabels recovers, from a converted
// plan, each running aggregate's expression plus the SQL alias of a result column
// that is EXACTLY that aggregate -- and leaves the alias empty when the aggregate
// is nested in a larger projection term (so the running partial is no single
// column). See stats.go's running block.
func TestRunningAggLabels(t *testing.T) {
	root := writePlainBeers(t, 5)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	// nil / no-group plans yield no entries.
	if got := RunningAggLabels(nil); got != nil {
		t.Errorf("RunningAggLabels(nil) = %v, want nil", got)
	}

	// Bare aggregates: each result column IS the aggregate, so both get an alias.
	// The projection order need not match the group's internal aggregate order --
	// RunningAggLabels reports the group's order (aligned with RunningAggRow.Aggs).
	res, err := s.Run(`SELECT count(*) AS c, sum(b.i) AS s FROM beers b`)
	if err != nil {
		t.Fatal(err)
	}
	labels := RunningAggLabels(res.Plan)
	if len(labels) != 1 {
		t.Fatalf("want 1 group op, got %d: %+v", len(labels), labels)
	}
	byExpr := map[string]string{} // expr -> alias
	for _, l := range labels[0] {
		byExpr[l.Expr] = l.Alias
	}
	if len(labels[0]) != 2 {
		t.Fatalf("want 2 aggregates, got %d: %+v", len(labels[0]), labels[0])
	}
	// count(*) -> c
	if a, ok := byExpr["count(*)"]; !ok || a != "c" {
		t.Errorf("count(*) alias = %q (present=%v), want \"c\"; labels=%+v", a, ok, labels[0])
	}
	// the sum expression -> s (expr text is cbq's, e.g. "sum((`b`.`i`))")
	foundSum := false
	for expr, alias := range byExpr {
		if strings.HasPrefix(expr, "sum(") {
			foundSum = true
			if alias != "s" {
				t.Errorf("%s alias = %q, want \"s\"", expr, alias)
			}
		}
	}
	if !foundSum {
		t.Errorf("no sum aggregate found in %+v", labels[0])
	}

	// A wrapped aggregate: revenue = round(sum(b.i), 2). The aggregate (raw sum) is
	// nested, so no result column equals it -> its alias must be empty.
	res2, err := s.Run(`SELECT round(sum(b.i), 2) AS revenue FROM beers b`)
	if err != nil {
		t.Fatal(err)
	}
	labels2 := RunningAggLabels(res2.Plan)
	if len(labels2) != 1 || len(labels2[0]) != 1 {
		t.Fatalf("want one group op with one aggregate, got %+v", labels2)
	}
	if got := labels2[0][0]; got.Alias != "" {
		t.Errorf("wrapped aggregate alias = %q, want \"\" (nested in round(...)); expr=%q", got.Alias, got.Expr)
	} else if !strings.HasPrefix(got.Expr, "sum(") {
		t.Errorf("wrapped aggregate expr = %q, want a sum(...)", got.Expr)
	}
}
