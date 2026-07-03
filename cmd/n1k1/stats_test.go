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
