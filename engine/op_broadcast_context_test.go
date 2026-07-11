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

package engine

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// ctxRow is one input line for the broadcast-context tests: a partition p, an in-
// partition ordinal pos, and a severity (ERROR = a match). The stream is built sorted
// by (p, pos) -- the shared upstream sort the op relies on.
type ctxRow struct {
	p     string
	pos   int
	error bool
}

// grepExpected is the brute-force reference for grep -B<before>/-A<after> over one
// partition-ordered stream: a match at position m marks [m-before, m+after] (clamped to
// the partition), each row emitted once, in order. The op must reproduce this exactly.
func grepExpected(rows []ctxRow, before, after int) []int {
	emit := map[int]bool{}
	// partition boundaries: rows with the same p are contiguous (stream is sorted).
	for i := range rows {
		if !rows[i].error {
			continue
		}
		// walk back/forward within the same partition only.
		for j := i; j >= 0 && j >= i-before && rows[j].p == rows[i].p; j-- {
			emit[j] = true
		}
		for j := i; j < len(rows) && j <= i+after && rows[j].p == rows[i].p; j++ {
			emit[j] = true
		}
	}
	var out []int
	for i := range rows {
		if emit[i] {
			out = append(out, rows[i].pos)
		}
	}
	return out
}

// ctxJSONL renders rows as one JSON doc per line (the jsonsData scan source).
func ctxJSONL(rows []ctxRow) string {
	var sb strings.Builder
	for _, r := range rows {
		sev := "info"
		if r.error {
			sev = "ERROR"
		}
		fmt.Fprintf(&sb, `{"p":%q,"pos":%d,"sev":%q}`+"\n", r.p, r.pos, sev)
	}
	return sb.String()
}

// ctxExtractor builds one broadcast-context extractor param:
// {tag, before, after, pred(sev="ERROR"), [proj(pos)]}.
func ctxExtractor(tag string, before, after int) []interface{} {
	pred := []interface{}{"eq", lp(".", "sev"), []interface{}{"json", `"ERROR"`}}
	proj := lp(".", "pos")
	return []interface{}{tag, before, after, pred, []interface{}{proj}}
}

// runContextBroadcast runs the op over rows and returns findings grouped by tag, each a
// list of the emitted pos values (in emission order).
func runContextBroadcast(t *testing.T, rows []ctxRow, exts ...[]interface{}) map[string][]int {
	t.Helper()
	extParams := make([]interface{}, len(exts))
	for i, e := range exts {
		extParams[i] = e
	}
	op := &base.Op{
		Kind:   "broadcast-context",
		Labels: base.Labels{"tag", "pos"},
		Params: []interface{}{
			extParams,
			lp(".", "p"), // partition key
		},
		Children: []*base.Op{{
			Kind:   "scan",
			Labels: base.Labels{"."},
			Params: []interface{}{"jsonsData", ctxJSONL(rows)},
		}},
	}

	got := map[string][]int{}
	for _, row := range collectRows(t, op, broadcastVars()) {
		if len(row) != 2 {
			t.Fatalf("finding has %d slots, want 2: %v", len(row), row)
		}
		tag, err := strconv.Unquote(row[0])
		if err != nil {
			t.Fatalf("slot-0 tag %q not a JSON string: %v", row[0], err)
		}
		pos, err := strconv.Atoi(row[1])
		if err != nil {
			t.Fatalf("slot-1 pos %q not an int: %v", row[1], err)
		}
		got[tag] = append(got[tag], pos)
	}
	return got
}

// TestOpBroadcastContextGrep drives K extractors (grep -C1, -B2, -A2) over ONE shared
// sorted scan and checks each tag's emitted rows equal the brute-force grep reference --
// so the shared fan-out is byte-equivalent to running each detector alone. It spans two
// partitions to prove context never leaks across them.
func TestOpBroadcastContextGrep(t *testing.T) {
	var rows []ctxRow
	// f1: pos 0..7, ERROR at 2 and 6.
	for i := 0; i < 8; i++ {
		rows = append(rows, ctxRow{p: "f1", pos: i, error: i == 2 || i == 6})
	}
	// f2: pos 0..3, ERROR at 0 (near the partition start -- would leak into f1 without
	// partition isolation).
	for i := 0; i < 4; i++ {
		rows = append(rows, ctxRow{p: "f2", pos: i, error: i == 0})
	}

	cases := []struct {
		tag           string
		before, after int
	}{
		{"c1", 1, 1}, // grep -C1
		{"b2", 2, 0}, // grep -B2
		{"a2", 0, 2}, // grep -A2
		{"c0", 0, 0}, // just the match lines
	}

	var exts [][]interface{}
	for _, c := range cases {
		exts = append(exts, ctxExtractor(c.tag, c.before, c.after))
	}
	got := runContextBroadcast(t, rows, exts...)

	for _, c := range cases {
		want := grepExpected(rows, c.before, c.after)
		if !reflect.DeepEqual(got[c.tag], want) {
			t.Errorf("%s (-B%d/-A%d): got pos %v, want %v", c.tag, c.before, c.after, got[c.tag], want)
		}
	}

	// Explicit spot-checks of the intended windows (independent of the reference).
	// c1: f1 {1,2,3}∪{5,6,7}, f2 {0,1}. Note f2's pos are 0,1 (not leaked from f1).
	if want := []int{1, 2, 3, 5, 6, 7, 0, 1}; !reflect.DeepEqual(got["c1"], want) {
		t.Errorf("c1 grep -C1: got %v, want %v", got["c1"], want)
	}
	// c0: exactly the match lines (f1 pos 2,6; f2 pos 0).
	if want := []int{2, 6, 0}; !reflect.DeepEqual(got["c0"], want) {
		t.Errorf("c0 (match lines only): got %v, want %v", got["c0"], want)
	}
}

// TestOpBroadcastContextNoLeakAcrossPartitions isolates the partition-boundary reset: an
// ERROR at the END of f1 with -A2 must NOT pull f2's opening lines as "after" context.
func TestOpBroadcastContextNoLeakAcrossPartitions(t *testing.T) {
	rows := []ctxRow{
		{p: "f1", pos: 0, error: false},
		{p: "f1", pos: 1, error: true}, // ERROR at the last f1 line
		{p: "f2", pos: 0, error: false},
		{p: "f2", pos: 1, error: false},
	}
	got := runContextBroadcast(t, rows, ctxExtractor("a2", 0, 2))
	// -A2 from f1 pos1 would want pos 2,3 -- but the partition ends, so only pos1.
	if want := []int{1}; !reflect.DeepEqual(got["a2"], want) {
		t.Errorf("cross-partition -A2 leak: got %v, want %v", got["a2"], want)
	}
}

// TestOpBroadcastContextNoPartition: with no partition key (Params[1] absent) the whole
// stream is one partition -- context spans everything.
func TestOpBroadcastContextNoPartition(t *testing.T) {
	rows := []ctxRow{
		{p: "x", pos: 0, error: false},
		{p: "x", pos: 1, error: true},
		{p: "x", pos: 2, error: false},
	}
	op := &base.Op{
		Kind:     "broadcast-context",
		Labels:   base.Labels{"tag", "pos"},
		Params:   []interface{}{[]interface{}{ctxExtractor("c1", 1, 1)}}, // no Params[1]
		Children: []*base.Op{{Kind: "scan", Labels: base.Labels{"."}, Params: []interface{}{"jsonsData", ctxJSONL(rows)}}},
	}
	var got []int
	for _, row := range collectRows(t, op, broadcastVars()) {
		pos, _ := strconv.Atoi(row[1])
		got = append(got, pos)
	}
	if want := []int{0, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("no-partition -C1: got %v, want %v", got, want)
	}
}

// TestOpBroadcastContextAlwaysWake covers the always-wake path: an extractor whose
// predicate has no extractable literal (a numeric comparison, pos > 1) is evaluated on
// every row (not AC-gated) and still produces the correct grep -C0 (match-line-only)
// result -- proving the AC index and the always-wake fallback compose.
func TestOpBroadcastContextAlwaysWake(t *testing.T) {
	var rows []ctxRow
	for i := 0; i < 5; i++ {
		rows = append(rows, ctxRow{p: "f1", pos: i, error: false})
	}
	// pos > 1 -> no string literal -> PrefilterLiteral false -> always-wake.
	ext := []interface{}{"aw", 0, 0,
		[]interface{}{"gt", lp(".", "pos"), []interface{}{"json", "1"}},
		[]interface{}{lp(".", "pos")}}
	got := runContextBroadcast(t, rows, ext)
	if want := []int{2, 3, 4}; !reflect.DeepEqual(got["aw"], want) {
		t.Errorf("always-wake pos>1: got %v, want %v", got["aw"], want)
	}
}
