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
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// scanOfAs builds a scan op over one-per-line JSON docs {"a":v} for the given
// values, so a test can give each source a DISTINCT, recognizable row set (used
// to make a mis-route produce a visibly wrong finding value).
func scanOfAs(vals ...int) *base.Op {
	var sb strings.Builder
	for _, v := range vals {
		sb.WriteString(`{"a":`)
		sb.WriteString(strconv.Itoa(v))
		sb.WriteString("}\n")
	}
	return &base.Op{
		Kind:   "scan",
		Labels: base.Labels{"."},
		Params: []interface{}{"jsonsData", sb.String()},
	}
}

// collectFindingsByTag runs a routed plan and returns, per tag, the SORTED
// evidence values (slot 1, a JSON number) of its findings. Sorting makes the
// comparison a multiset compare, robust to union-all's concurrent (order-
// nondeterministic across sources) yield.
func collectFindingsByTag(t *testing.T, routed *base.Op) map[string][]int {
	t.Helper()

	byTag := map[string][]int{}
	for _, row := range collectRows(t, routed, broadcastVars()) {
		if len(row) != 2 {
			t.Fatalf("finding has %d slots, want 2: %v", len(row), row)
		}
		tag, err := strconv.Unquote(row[0])
		if err != nil {
			t.Fatalf("slot-0 tag %q not a JSON string: %v", row[0], err)
		}
		v, err := strconv.Atoi(row[1])
		if err != nil {
			t.Fatalf("slot-1 evidence %q not an int: %v", row[1], err)
		}
		byTag[tag] = append(byTag[tag], v)
	}
	for _, vs := range byTag {
		sort.Ints(vs)
	}
	return byTag
}

// TestBroadcastRouteCorrectness proves a detector runs ONLY against its target
// source: 3 present sources with DISJOINT row sets, plus detectors targeting an
// ABSENT source. Data is chosen so a mis-route would be visible (a detector
// bound to source A, with a predicate that would ALSO match B's larger values,
// must never see B's rows). Absent-source detectors must come back as orphans,
// never in the findings.
func TestBroadcastRouteCorrectness(t *testing.T) {
	// Disjoint value ranges per source: A small, B large, C huge.
	srcVals := map[string][]int{
		"A": {1, 2, 3},
		"B": {100, 200, 300},
		"C": {1000},
	}

	sources := map[string]*base.Op{}
	for id, vs := range srcVals {
		sources[id] = scanOfAs(vs...)
	}

	// pred a>n, projecting a. If "aBig" (a>2, bound to A) ever saw B's rows
	// (100,200,300 -- all >2) the multiset compare below would blow up.
	gt := func(n int) []interface{} {
		return []interface{}{"gt", lp(".", "a"), []interface{}{"json", strconv.Itoa(n)}}
	}
	projA := []interface{}{lp(".", "a")}
	truth := []interface{}{"json", "true"}

	detectors := []Detector{
		{Tag: "aAll", TargetSource: "A", Pred: truth, Proj: projA},
		{Tag: "aBig", TargetSource: "A", Pred: gt(2), Proj: projA}, // -> {3}
		{Tag: "bAll", TargetSource: "B", Pred: truth, Proj: projA},
		{Tag: "bBig", TargetSource: "B", Pred: gt(150), Proj: projA}, // -> {200,300}
		{Tag: "cAll", TargetSource: "C", Pred: truth, Proj: projA},
		{Tag: "orphanX", TargetSource: "X", Pred: truth, Proj: projA}, // absent
		{Tag: "orphanY", TargetSource: "Y", Pred: gt(0), Proj: projA}, // absent
	}

	routed, orphans := BroadcastRoute(sources, detectors, base.Labels{"tag", "a"})
	if routed == nil {
		t.Fatal("routed plan is nil; want a plan for 3 contributing sources")
	}

	// Orphans: exactly the two absent-source detectors, in input order.
	var orphanTags []string
	for _, o := range orphans {
		orphanTags = append(orphanTags, o.Tag)
	}
	if !reflect.DeepEqual(orphanTags, []string{"orphanX", "orphanY"}) {
		t.Fatalf("orphans=%v, want [orphanX orphanY]", orphanTags)
	}

	got := collectFindingsByTag(t, routed)

	// Absent-source detectors contribute NOTHING to the findings.
	for _, tag := range []string{"orphanX", "orphanY"} {
		if len(got[tag]) != 0 {
			t.Fatalf("orphan detector %q leaked %d findings: %v", tag, len(got[tag]), got[tag])
		}
	}

	// Each present detector's findings == running it against ONLY its source.
	want := map[string][]int{
		"aAll": {1, 2, 3},
		"aBig": {3},
		"bAll": {100, 200, 300},
		"bBig": {200, 300},
		"cAll": {1000},
	}
	for tag, w := range want {
		if !reflect.DeepEqual(got[tag], w) {
			t.Fatalf("detector %q: findings %v, want %v (a mis-route would show foreign values)", tag, got[tag], w)
		}
	}

	// Cross-check against the oracle: each detector as its own scan->filter->
	// project over its DECLARED source's data only.
	for _, d := range detectors {
		vs, present := srcVals[d.TargetSource]
		if !present {
			continue // orphan; already checked absent from findings
		}
		oracle := &base.Op{
			Kind:   "project",
			Labels: base.Labels{"a"},
			Params: d.Proj,
			Children: []*base.Op{{
				Kind:     "filter",
				Labels:   base.Labels{"."},
				Params:   d.Pred,
				Children: []*base.Op{scanOfAs(vs...)},
			}},
		}
		var oracleVals []int
		for _, row := range collectRows(t, oracle, broadcastVars()) {
			v, _ := strconv.Atoi(row[0])
			oracleVals = append(oracleVals, v)
		}
		sort.Ints(oracleVals)
		if !reflect.DeepEqual(got[d.Tag], oracleVals) {
			t.Fatalf("detector %q: routed %v != oracle %v", d.Tag, got[d.Tag], oracleVals)
		}
	}
}

// TestBroadcastRouteEdgeCases covers: a source with zero detectors contributes
// nothing (and a lone contributing source needs no union-all wrapper); an empty
// corpus yields a nil plan and no orphans; an all-orphaned corpus yields a nil
// plan and returns every detector as an orphan.
func TestBroadcastRouteEdgeCases(t *testing.T) {
	labels := base.Labels{"tag", "a"}
	projA := []interface{}{lp(".", "a")}
	truth := []interface{}{"json", "true"}

	// (1) Source with zero detectors contributes nothing. "A" has a detector;
	// "D" has none -> only ONE contributing source -> the broadcast is returned
	// directly (no union-all), and its child is A's scan, never D's.
	aScan := scanOfAs(1, 2)
	dScan := scanOfAs(9, 9, 9)
	sources := map[string]*base.Op{"A": aScan, "D": dScan}

	routed, orphans := BroadcastRoute(sources,
		[]Detector{{Tag: "onlyA", TargetSource: "A", Pred: truth, Proj: projA}}, labels)
	if len(orphans) != 0 {
		t.Fatalf("unexpected orphans: %v", orphans)
	}
	if routed == nil || routed.Kind != "broadcast" {
		t.Fatalf("single contributing source: routed=%+v, want a lone broadcast", routed)
	}
	if len(routed.Children) != 1 || routed.Children[0] != aScan {
		t.Fatalf("broadcast child is not A's scan; D (zero detectors) must not contribute")
	}
	got := collectFindingsByTag(t, routed)
	if !reflect.DeepEqual(got["onlyA"], []int{1, 2}) {
		t.Fatalf("onlyA findings=%v, want [1 2] (D's 9s must not appear)", got["onlyA"])
	}

	// (2) Empty corpus: nil plan, no orphans.
	routed, orphans = BroadcastRoute(sources, nil, labels)
	if routed != nil {
		t.Fatalf("empty corpus: routed=%+v, want nil", routed)
	}
	if len(orphans) != 0 {
		t.Fatalf("empty corpus: orphans=%v, want none", orphans)
	}

	// (3) All detectors orphaned: nil plan, all returned as orphans (in order).
	allOrphan := []Detector{
		{Tag: "z0", TargetSource: "Z", Pred: truth, Proj: projA},
		{Tag: "z1", TargetSource: "W", Pred: truth, Proj: projA},
	}
	routed, orphans = BroadcastRoute(sources, allOrphan, labels)
	if routed != nil {
		t.Fatalf("all-orphaned: routed=%+v, want nil", routed)
	}
	var tags []string
	for _, o := range orphans {
		tags = append(tags, o.Tag)
	}
	if !reflect.DeepEqual(tags, []string{"z0", "z1"}) {
		t.Fatalf("all-orphaned: orphans=%v, want [z0 z1]", tags)
	}
}

// BenchmarkBroadcastRouting contrasts a ROUTED plan (each detector runs only on
// its target source -- one broadcast per source, K/M detectors each) with the
// UNROUTED all-to-all baseline (every source's broadcast holds ALL K detectors).
// With K detectors spread across M sources, routing does ~1/M the per-row
// predicate work: routed evaluates n*K predicates total (n rows/source x K/M
// detectors x M sources), unrouted evaluates M*n*K. Expect routed to be ~M-fold
// cheaper in time + findings-eval allocations.
func BenchmarkBroadcastRouting(b *testing.B) {
	const m = 4
	const k = 64
	const n = 500

	srcIDs := make([]string, m)
	srcData := make(map[string]string, m)
	for j := 0; j < m; j++ {
		srcIDs[j] = fmt.Sprintf("s%d", j)
		srcData[srcIDs[j]] = makeJsons(n)
	}
	newSources := func() map[string]*base.Op {
		s := make(map[string]*base.Op, m)
		for _, id := range srcIDs {
			s[id] = &base.Op{Kind: "scan", Labels: base.Labels{"."},
				Params: []interface{}{"jsonsData", srcData[id]}}
		}
		return s
	}

	// K detectors, round-robin bound across the M sources.
	detectors := make([]Detector, k)
	for i := 0; i < k; i++ {
		detectors[i] = Detector{
			Tag:          "d" + strconv.Itoa(i),
			TargetSource: srcIDs[i%m],
			Pred:         []interface{}{"gt", lp(".", "a"), []interface{}{"json", strconv.Itoa(i)}},
			Proj:         []interface{}{lp(".", "a")},
		}
	}

	labels := base.Labels{"tag", "a"}
	sink := func(base.Vals) {}
	noErr := func(err error) {
		if err != nil {
			b.Fatalf("yieldErr: %v", err)
		}
	}

	b.Run("routed", func(b *testing.B) {
		routed, orphans := BroadcastRoute(newSources(), detectors, labels)
		if len(orphans) != 0 {
			b.Fatalf("unexpected orphans: %d", len(orphans))
		}
		vars := broadcastVars()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ExecOp(routed, vars, sink, noErr, "", "")
		}
	})

	b.Run("unrouted", func(b *testing.B) {
		// All-to-all: replicate the whole K-detector corpus onto every source.
		unrouted := make([]Detector, 0, k*m)
		for _, id := range srcIDs {
			for i := 0; i < k; i++ {
				d := detectors[i]
				d.TargetSource = id
				unrouted = append(unrouted, d)
			}
		}
		routed, _ := BroadcastRoute(newSources(), unrouted, labels)
		vars := broadcastVars()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ExecOp(routed, vars, sink, noErr, "", "")
		}
	})
}
