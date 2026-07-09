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

// jsonLit builds a ["json", "\"S\""] constant node for a string literal S.
func jsonLit(s string) []interface{} {
	return []interface{}{"json", strconv.Quote(s)}
}

// dataScan makes a scan op over the given one-JSON-doc-per-line data string.
func dataScan(data string) *base.Op {
	return &base.Op{Kind: "scan", Labels: base.Labels{"."}, Params: []interface{}{"jsonsData", data}}
}

// findingsByTag runs an op tree and groups the interleaved tagged findings by
// tag (slot 0, a JSON string), preserving per-tag row order. Slot 1+ is the
// projected evidence, compared verbatim.
func findingsByTag(t *testing.T, root *base.Op) map[string][][]string {
	t.Helper()
	got := map[string][][]string{}
	for _, row := range collectRows(t, root, broadcastVars()) {
		if len(row) < 1 {
			t.Fatalf("finding has no tag slot: %v", row)
		}
		tag, err := strconv.Unquote(row[0])
		if err != nil {
			t.Fatalf("slot-0 tag %q not a JSON string: %v", row[0], err)
		}
		got[tag] = append(got[tag], append([]string(nil), row[1:]...))
	}
	return got
}

// TestOpBroadcastIndexedEquivalence is THE invariant guard: the predicate-index
// op must produce findings BYTE-IDENTICAL (per tag, in row order) to a plain
// "broadcast" over the same detectors + scan. The corpus deliberately mixes:
//
//   - detectors with distinct RARE literals (contains / regexp plain literal),
//   - detectors SHARING a literal (an eq-const and a regexp both keyed "ERROR"),
//   - a detector with NO extractable prefilter (bare gt) -> always-wake,
//   - an OVER-WAKE detector (its literal appears but the full AND predicate then
//     fails) -> the full predicate must re-check and drop it,
//   - a regex-with-metacharacters detector -> always-wake (no literal extracted).
func TestOpBroadcastIndexedEquivalence(t *testing.T) {
	findings := base.Labels{"tag", "ev"}
	proj := []interface{}{lp(".", "line")}

	dets := []Detector{
		// Distinct rare literals.
		{Tag: "panic", Pred: []interface{}{"contains", lp(".", "line"), jsonLit("panic")}, Proj: proj},
		{Tag: "oom", Pred: []interface{}{"contains", lp(".", "line"), jsonLit("OutOfMemory")}, Proj: proj},
		// regexp with a PLAIN literal pattern -> literal "segfault".
		{Tag: "segfault", Pred: []interface{}{"regexp_contains", lp(".", "line"), jsonLit("segfault")}, Proj: proj},
		// Two detectors SHARING the literal "ERROR" (an eq const + a plain regexp).
		{Tag: "err_eq", Pred: []interface{}{"eq", lp(".", "level"), jsonLit("ERROR")}, Proj: proj},
		{Tag: "err_re", Pred: []interface{}{"regexp_contains", lp(".", "line"), jsonLit("ERROR")}, Proj: proj},
		// Always-wake: a bare gt has no extractable literal.
		{Tag: "hot", Pred: []interface{}{"gt", lp(".", "count"), jsonLit2(100)}, Proj: proj},
		// Over-wake: literal "disk" is present in some rows, but the AND's second
		// conjunct (count > 1000000) fails, so the full predicate drops the row.
		{Tag: "diskflood", Pred: []interface{}{"and",
			[]interface{}{"contains", lp(".", "line"), jsonLit("disk")},
			[]interface{}{"gt", lp(".", "count"), jsonLit2(1000000)},
		}, Proj: proj},
		// Regex WITH metachars -> not a plain literal -> always-wake.
		{Tag: "re_meta", Pred: []interface{}{"regexp_contains", lp(".", "line"), jsonLit("e.*r")}, Proj: proj},
	}

	data := makeLogData(200)

	plain := &base.Op{
		Kind:     "broadcast",
		Labels:   findings,
		Params:   []interface{}{detParamsOf(dets)},
		Children: []*base.Op{dataScan(data)},
	}
	indexed := BroadcastIndexed(dataScan(data), dets, findings)

	want := findingsByTag(t, plain)
	got := findingsByTag(t, indexed)

	if !reflect.DeepEqual(got, want) {
		// Report the first divergent tag for a readable failure.
		for _, d := range dets {
			if !reflect.DeepEqual(got[d.Tag], want[d.Tag]) {
				t.Fatalf("tag %q diverged:\n indexed=%v\n   plain=%v", d.Tag, got[d.Tag], want[d.Tag])
			}
		}
		t.Fatalf("indexed findings != plain broadcast findings\n indexed=%v\n plain=%v", got, want)
	}

	// Sanity: the corpus actually exercised each interesting case.
	if len(want["panic"]) == 0 || len(want["err_eq"]) == 0 || len(want["hot"]) == 0 {
		t.Fatalf("test corpus too weak: panic=%d err_eq=%d hot=%d",
			len(want["panic"]), len(want["err_eq"]), len(want["hot"]))
	}
	if len(want["diskflood"]) != 0 {
		t.Fatalf("over-wake detector 'diskflood' must yield 0 findings (full pred fails), got %d", len(want["diskflood"]))
	}
}

// TestOpBroadcastIndexedSparsity proves the whole point: with K detectors each
// keyed to a DISTINCT literal and rows that each contain exactly ONE such
// literal, the indexed op evaluates ~= (1 matched + always-wake) FULL predicates
// per row, NOT K. The PredEvals stat is the instrument.
func TestOpBroadcastIndexedSparsity(t *testing.T) {
	const k = 50
	const n = 300

	findings := base.Labels{"tag", "ev"}
	proj := []interface{}{lp(".", "line")}

	dets := make([]Detector, 0, k+1)
	for j := 0; j < k; j++ {
		dets = append(dets, Detector{
			Tag:  "tok" + strconv.Itoa(j),
			Pred: []interface{}{"contains", lp(".", "line"), jsonLit(tokLit(j))},
			Proj: proj,
		})
	}
	// One always-wake detector (bare gt, no extractable literal).
	dets = append(dets, Detector{
		Tag:  "alwaysA",
		Pred: []interface{}{"gt", lp(".", "n"), jsonLit2(-1)},
		Proj: proj,
	})
	const alwaysWake = 1

	// Rows: each contains exactly ONE distinct token (i % k).
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteString(fmt.Sprintf(`{"line":"msg %s tail","n":%d}`, tokLit(i%k), i))
		sb.WriteString("\n")
	}
	data := sb.String()

	indexed := BroadcastIndexed(dataScan(data), dets, findings)

	stats := base.LayoutStats(indexed)
	if stats == nil {
		t.Fatal("LayoutStats returned nil")
	}
	vars := broadcastVars()
	vars.Ctx.Stats = stats
	collectRows(t, indexed, vars)

	predEvals, ok := stats.Get("0:PredEvals")
	if !ok {
		t.Fatal("no PredEvals stat")
	}
	rowsIn, _ := stats.Get("0:RowsIn")

	// Each row wakes exactly its one matching token detector + the always-wake
	// detector => (1 + alwaysWake) full-predicate evaluations per row.
	wantEvals := int64(n * (1 + alwaysWake))
	if predEvals != wantEvals {
		t.Fatalf("indexed PredEvals=%d, want %d (=%d rows x (1 match + %d always-wake))",
			predEvals, wantEvals, n, alwaysWake)
	}
	if rowsIn != n {
		t.Fatalf("RowsIn=%d, want %d", rowsIn, n)
	}

	// The point: a plain broadcast would evaluate K per row.
	broadcastEvals := int64(n * (k + alwaysWake))
	if predEvals >= broadcastEvals {
		t.Fatalf("indexed PredEvals=%d not below broadcast's %d", predEvals, broadcastEvals)
	}
	// t.Logf("sparsity: indexed %d predicate evals vs broadcast %d (%.1fx fewer) over %d rows, K=%d",
	//	predEvals, broadcastEvals, float64(broadcastEvals)/float64(predEvals), n, k)
}

// -----------------------------------------------------

// jsonLit2 builds a ["json","<int>"] numeric constant node.
func jsonLit2(i int) []interface{} { return []interface{}{"json", strconv.Itoa(i)} }

// detParamsOf renders detectors into the broadcast Params[0] detector-spec shape.
func detParamsOf(dets []Detector) []interface{} {
	out := make([]interface{}, 0, len(dets))
	for _, d := range dets {
		out = append(out, []interface{}{d.Tag, d.Pred, d.Proj})
	}
	return out
}

// tokLit is a distinct, non-overlapping literal token (fixed width so no token
// is a substring of another).
func tokLit(i int) string { return fmt.Sprintf("TOK%04dEND", i) }

// makeLogData builds n varied one-JSON-per-line log docs cycling through
// templates that exercise the equivalence corpus's literals.
func makeLogData(n int) string {
	templates := []string{
		`{"line":"kernel panic detected","level":"WARN","count":3}`,
		`{"line":"OutOfMemory killed proc","level":"ERROR","count":7}`,
		`{"line":"request handled ERROR path","level":"INFO","count":2}`,
		`{"line":"disk usage high segfault","level":"INFO","count":50}`,
		`{"line":"steady state nominal","level":"INFO","count":250}`,
		`{"line":"error string appears here","level":"DEBUG","count":9}`,
		`{"line":"disk pressure noticed","level":"ERROR","count":11}`,
		`{"line":"nothing notable today","level":"INFO","count":1}`,
	}
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteString(templates[i%len(templates)])
		sb.WriteString("\n")
	}
	return sb.String()
}

// BenchmarkBroadcastIndexed contrasts the predicate-index op against a plain
// broadcast at LARGE K: K detectors each keyed to a distinct literal, over N rows
// where each row contains ~1 of those literals. The indexed op does one
// Aho-Corasick pass + ~O(1) predicate evals/row; the broadcast does O(K). The gap
// should widen with K.
func BenchmarkBroadcastIndexed(b *testing.B) {
	findings := base.Labels{"tag", "ev"}
	proj := []interface{}{lp(".", "line")}

	sink := func(base.Vals) {}
	noErr := func(err error) {
		if err != nil {
			b.Fatalf("yieldErr: %v", err)
		}
	}

	const n = 2000

	for _, k := range []int{64, 256, 1000} {
		dets := make([]Detector, 0, k)
		for j := 0; j < k; j++ {
			dets = append(dets, Detector{
				Tag:  "tok" + strconv.Itoa(j),
				Pred: []interface{}{"contains", lp(".", "line"), jsonLit(tokLit(j))},
				Proj: proj,
			})
		}

		var sb strings.Builder
		for i := 0; i < n; i++ {
			sb.WriteString(fmt.Sprintf(`{"line":"msg %s tail","n":%d}`, tokLit(i%k), i))
			sb.WriteString("\n")
		}
		data := sb.String()

		detParams := detParamsOf(dets)

		b.Run(fmt.Sprintf("indexed/K=%d", k), func(b *testing.B) {
			indexed := BroadcastIndexed(dataScan(data), dets, findings)
			vars := broadcastVars()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ExecOp(indexed, vars, sink, noErr, "", "")
			}
		})

		b.Run(fmt.Sprintf("broadcast/K=%d", k), func(b *testing.B) {
			bc := &base.Op{Kind: "broadcast", Labels: findings,
				Params: []interface{}{detParams}, Children: []*base.Op{dataScan(data)}}
			vars := broadcastVars()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ExecOp(bc, vars, sink, noErr, "", "")
			}
		})
	}
}
