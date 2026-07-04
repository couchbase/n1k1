//go:build n1ql

package main

import "testing"

func TestExprStatsLine(t *testing.T) {
	cases := []struct {
		native, boxed int
		boxedEvals    int64
		want          string
	}{
		{3, 0, 0, ""}, // fully native -> no line (absence implies native)
		{0, 1, 84, "expr: 1/1 exprs boxed · 84 boxed evals"},
		{2, 1, 7056, "expr: 1/3 exprs boxed · 7.1K boxed evals"},
		{2, 1, 0, "expr: 1/3 exprs boxed · 0 boxed evals"}, // boxed expr present but elided/no rows
		{0, 0, 0, ""},                    // nothing to report
		{0, 0, 5, "expr: 5 boxed evals"}, // boxed evals from ops not statically counted
	}
	for _, c := range cases {
		if got := exprStatsLine(c.native, c.boxed, c.boxedEvals); got != c.want {
			t.Errorf("exprStatsLine(%d,%d,%d) = %q, want %q", c.native, c.boxed, c.boxedEvals, got, c.want)
		}
	}
}
