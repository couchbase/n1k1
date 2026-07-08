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

package main

import (
	"io"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// TestStatsLines checks the aligned-table formatting: a header row, a tree-
// indented op column, right-aligned numeric columns for stats shared by >=2 ops
// (RowsIn, RowsOut), and a trailing misc column for one-off stats (GroupsOut).
func TestStatsLines(t *testing.T) {
	s := &base.Stats{
		Counters: []int64{1, 1, 6, 5, 6}, // group RowsIn/GroupsOut, filter RowsIn/RowsOut, scan RowsOut
		Ops: []base.StatsOpInfo{
			{Id: "0", Kind: "group", Base: 0, Names: []string{"RowsIn", "GroupsOut"}},
			{Id: "0/0", Kind: "filter", Base: 2, Names: []string{"RowsIn", "RowsOut"}},
			{Id: "0/0/0", Kind: "datastore-scan-records", Base: 4, Names: []string{"RowsOut"}},
		},
	}

	got := strings.Join(statsLines(s), "\n")
	want := "op                          RowsIn  RowsOut  misc\n" +
		"group                            1           GroupsOut=1\n" +
		"  filter                         6        5\n" +
		"    datastore-scan-records                6"

	if got != want {
		t.Errorf("statsLines mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// TestStatsLinesTotals checks the "cur/total" progress form for a counter with a
// known estimate (Totals), including a re-run inner scan whose current pass (2) is
// below its self-observed peak (5) -- a bar that can sit mid-range / reset.
func TestStatsLinesTotals(t *testing.T) {
	s := &base.Stats{
		Counters: []int64{2}, // current inner pass
		Totals:   []int64{5}, // self-observed peak
		Ops: []base.StatsOpInfo{
			{Id: "0", Kind: "datastore-scan-index", Base: 0, Names: []string{"RowsOut"}},
		},
	}

	got := strings.Join(statsLines(s), "\n")
	if !strings.Contains(got, "2/5") {
		t.Errorf("expected a cur/total cell \"2/5\", got:\n%s", got)
	}
}

// TestStatsLinesEmpty: no counter-contributing ops -> no lines (the caller then
// prints nothing).
func TestStatsLinesEmpty(t *testing.T) {
	if got := statsLines(&base.Stats{}); len(got) != 0 {
		t.Errorf("statsLines(empty) = %v, want none", got)
	}
}

// TestStatsGlossary: the glossary is alphabetized, "name: desc", concatenated with
// a separator on one line when it fits, prefixed "glossary: ".
func TestStatsGlossary(t *testing.T) {
	base.StatAbout["RowsIn"] = "input rows"
	base.StatAbout["RowsOut"] = "emitted rows"

	got := statsGlossary([]string{"RowsOut", "RowsIn", "RowsOut"}, 200)
	if len(got) != 1 {
		t.Fatalf("expected 1 line, got %d: %v", len(got), got)
	}
	want := "glossary: RowsIn: input rows; RowsOut: emitted rows"
	if got[0] != want {
		t.Errorf("glossary:\n got: %q\nwant: %q", got[0], want)
	}
}

// TestStatsGlossaryWrap: too-narrow width wraps, continuations aligned under the
// first entry (indented by len("glossary: ")).
func TestStatsGlossaryWrap(t *testing.T) {
	base.StatAbout["RowsIn"] = "input rows"
	base.StatAbout["RowsOut"] = "emitted rows"

	got := statsGlossary([]string{"RowsIn", "RowsOut"}, 30)
	if len(got) != 2 {
		t.Fatalf("expected 2 wrapped lines, got %d: %v", len(got), got)
	}
	if !strings.HasPrefix(got[0], "glossary: ") {
		t.Errorf("line 0 missing prefix: %q", got[0])
	}
	if !strings.HasPrefix(got[1], strings.Repeat(" ", len("glossary: "))) {
		t.Errorf("continuation not aligned: %q", got[1])
	}
}

// TestHumanCount checks the K/M/G count abbreviation used in the runtime line.
func TestHumanCount(t *testing.T) {
	cases := map[uint64]string{0: "0", 999: "999", 1500: "1.5K", 214000: "214.0K", 8_000_000: "8.0M", 2_000_000_000: "2.0G"}
	for n, want := range cases {
		if got := humanCount(n); got != want {
			t.Errorf("humanCount(%d) = %q, want %q", n, got, want)
		}
	}
}

// TestBodyLinesRuntime checks that the live/footer body appends a process
// "runtime:" line after the per-op table.
func TestBodyLinesRuntime(t *testing.T) {
	s := &base.Stats{
		Counters: []int64{6},
		Ops:      []base.StatsOpInfo{{Id: "0", Kind: "scan", Base: 0, Names: []string{"RowsOut"}}},
	}
	sv := newStatsView(io.Discard, false, 0) // captures a runtime baseline
	lines := sv.bodyLines(s)
	if len(lines) != len(statsLines(s))+1 {
		t.Fatalf("bodyLines has %d lines, want table+1", len(lines))
	}
	if last := lines[len(lines)-1]; !strings.HasPrefix(last, "runtime: ") {
		t.Errorf("last body line = %q, want a runtime: line", last)
	}
}

// TestParseStatsMode checks the -stats/.stats value parsing, incl. aliases.
func TestParseStatsMode(t *testing.T) {
	ok := map[string]string{
		"": statsOff, "off": statsOff, "false": statsOff,
		"on": statsOn, "true": statsOn, "LIVE": statsOn,
		"final": statsFinal, "end": statsFinal, "summary": statsFinal, "Totals": statsFinal,
	}
	for in, want := range ok {
		if got, err := parseStatsMode(in); err != nil || got != want {
			t.Errorf("parseStatsMode(%q) = (%q,%v), want %q", in, got, err, want)
		}
	}
	if _, err := parseStatsMode("bogus"); err == nil {
		t.Errorf("parseStatsMode(bogus) should error")
	}
}

// TestDisplayDepths checks that indentation reflects nesting among *shown* ops:
// uncounted intermediate ops (gaps in the id path) must not inflate the indent,
// and siblings sit at the same depth.
func TestDisplayDepths(t *testing.T) {
	ops := []base.StatsOpInfo{
		{Id: "0/0", Kind: "group"},                        // 0
		{Id: "0/0/0/0", Kind: "joinNL-inner"},             // 1 (project/seq gap above)
		{Id: "0/0/0/0/0", Kind: "joinNL-inner"},           // 2
		{Id: "0/0/0/0/0/0", Kind: "datastore-scan-index"}, // 3
		{Id: "0/0/0/0/0/1", Kind: "datastore-scan-index"}, // 3 (sibling)
		{Id: "0/0/0/0/1", Kind: "datastore-scan-index"},   // 2 (outer join's other child)
	}
	got := displayDepths(ops)
	want := []int{0, 1, 2, 3, 3, 2}
	if len(got) != len(want) {
		t.Fatalf("displayDepths len=%d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("displayDepths%v = %v, want %v", ops, got, want)
			break
		}
	}
}

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

// TestRunningAggLines checks the in-flight running-aggregate footer block: the
// "running:" header, name=partial formatting, the vectorized-name strip
// (sum_v_float64 -> sum), a no-op for a Stats with no running aggregates, and the
// "... N more" cap. Rows are built via the exported RunningAggs.Next/AddAgg/
// FinishRow so the test needs no live query.
func TestRunningAggLines(t *testing.T) {
	if got := runningAggLines(nil); got != nil {
		t.Fatalf("nil Stats: got %q, want nil", got)
	}
	if got := runningAggLines(&base.Stats{}); got != nil {
		t.Fatalf("empty Stats: got %q, want nil", got)
	}

	// One ungrouped row (no key): header + one line; sum_v_float64 renders as "sum".
	s := &base.Stats{RunningAggs: make([]base.RunningAggs, 1)}
	r := s.RunningAggs[0].Next("0")
	r.AddAgg("count", base.Val("5"))
	r.AddAgg("sum_v_float64", base.Val("34.7"))
	r.AddAgg("avg", base.Val("6.94"))
	s.RunningAggs[0].FinishRow(r)

	got := strings.Join(runningAggLines(s), "\n")
	want := "running:\n  count=5 sum=34.7 avg=6.94"
	if got != want {
		t.Fatalf("ungrouped:\n got %q\nwant %q", got, want)
	}

	// More than runningAggDisplayMax rows collapse into a trailing "... N more".
	s2 := &base.Stats{RunningAggs: make([]base.RunningAggs, 1)}
	extra := 3
	for i := 0; i < runningAggDisplayMax+extra; i++ {
		rr := s2.RunningAggs[0].Next("0")
		rr.AddAgg("count", base.Val("1"))
		s2.RunningAggs[0].FinishRow(rr)
	}
	lines := runningAggLines(s2)
	if len(lines) != 1+runningAggDisplayMax+1 { // header + capped rows + "more"
		t.Fatalf("cap: got %d lines, want %d: %q", len(lines), 1+runningAggDisplayMax+1, lines)
	}
	if last := lines[len(lines)-1]; !strings.Contains(last, "3 more") {
		t.Fatalf("cap: last line = %q, want a '3 more' summary", last)
	}
}

// TestRunningAggLinesLabeled checks the labeled form (live footer + final block):
// one "alias (expr): value" line per aggregate, the alias omitted when empty, and
// numeric values aligned on their decimal point. Labels ride on
// Stats.RunningAggLabels (filled by glue.RunningAggLabels); here they're set
// directly so the test needs no plan.
func TestRunningAggLinesLabeled(t *testing.T) {
	s := &base.Stats{RunningAggs: make([]base.RunningAggs, 1)}
	r := s.RunningAggs[0].Next("0")
	r.AddAgg("count", base.Val("5"))            // -> aliased "c", integer
	r.AddAgg("sum_v_float64", base.Val("34.7")) // -> no alias (wrapped), fractional
	s.RunningAggs[0].FinishRow(r)
	s.RunningAggLabels = [][]base.RunningAggLabel{{
		{Alias: "c", Expr: "count(*)"},
		{Alias: "", Expr: "sum(x)"},
	}}

	lines := runningAggLines(s)
	if len(lines) != 3 || lines[0] != "running:" {
		t.Fatalf("got %d lines, want header + 2 aggs: %q", len(lines), lines)
	}
	cLine, sLine := lines[1], lines[2]

	// Aliased aggregate shows "alias (expr):"; unaliased shows the bare expr (no
	// parens wrapper, no alias).
	if !strings.Contains(cLine, "c (count(*)):") {
		t.Errorf("count line = %q, want it to contain \"c (count(*)):\"", cLine)
	}
	if !strings.Contains(sLine, "sum(x):") || strings.Contains(sLine, "(sum(x))") {
		t.Errorf("sum line = %q, want a bare \"sum(x):\" with no alias wrap", sLine)
	}

	// Decimal-point alignment: the integer "5" has no dot and is trimmed of trailing
	// pad, while "34.7"'s dot sits one past its units digit. The "5" must land in the
	// units column -- exactly left of where the sum's decimal point is -- so the two
	// numbers align on the (implicit) decimal point.
	dot := strings.IndexByte(sLine, '.')
	if dot < 1 {
		t.Fatalf("sum line has no decimal point: %q", sLine)
	}
	if strings.ContainsRune(cLine, '.') {
		t.Errorf("integer count line should have no decimal point: %q", cLine)
	}
	if dot-1 >= len(cLine) || cLine[dot-1] != '5' || sLine[dot-1] != '4' {
		t.Errorf("values not decimal-aligned: count=%q sum=%q (dot at %d)", cLine, sLine, dot)
	}
}
