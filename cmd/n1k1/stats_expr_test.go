//go:build n1ql

package main

import "testing"

func TestExprStatsLine(t *testing.T) {
	cases := []struct {
		native, boxed int
		boxedEvals    int64
		want          string
	}{
		{3, 0, 0, "expr: 3/3 exprs native · 0 boxed evals"},
		{0, 1, 84, "expr: 0/1 exprs native · 84 boxed evals"},
		{2, 1, 7056, "expr: 2/3 exprs native · 7.1K boxed evals"},
		{0, 0, 0, ""},                    // nothing to report (e.g. count(*) with no project/filter exprs)
		{0, 0, 5, "expr: 5 boxed evals"}, // runtime boxed evals but no static project/filter exprs
	}
	for _, c := range cases {
		if got := exprStatsLine(c.native, c.boxed, c.boxedEvals); got != c.want {
			t.Errorf("exprStatsLine(%d,%d,%d) = %q, want %q", c.native, c.boxed, c.boxedEvals, got, c.want)
		}
	}
}
