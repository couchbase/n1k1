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
	"encoding/json"
	"sort"
	"strconv"

	"github.com/couchbase/n1k1/base"
)

// BroadcastCSE is the corpus common-subexpression-elimination plan builder --
// the next PREPARE++ multi-query-optimization lever after source routing
// (DESIGN-prepare.md, "Corpus CSE": detectors share sub-predicates like
// level="ERROR" or line LIKE '%panic%', and "a global common-subexpression pass
// over the corpus computes each shared term once per row, not once per
// detector").
//
// In a flat "broadcast" (op_broadcast.go) every detector's predicate/projection
// ExprFuncs run INDEPENDENTLY per row, so a sub-expression shared by K detectors
// -- e.g. regexp_contains(line,"panic") -- is re-evaluated K times per row.
// BroadcastCSE removes that redundancy WITHOUT touching the broadcast op: it is
// PURE plan composition (mirroring BroadcastRoute), reusing only the existing
// "broadcast" + "project" ops (so no new engine op, no intermed / codegen
// change).
//
// The transform:
//
//   - A "precompute" project is inserted BELOW the broadcast. It passes the whole
//     row through (label ".") AND appends one synthetic column per shared
//     sub-expression (labels "^cse0", "^cse1", ...), so each shared term is
//     evaluated ONCE per row (in the project's reused output buffer).
//   - Every detector's Pred / Proj is rewritten so each occurrence of a shared
//     sub-expression becomes a cheap slot read ["labelPath","^cseN"] instead of
//     recomputing the whole term. Bare field reads (["labelPath",".","a"]) are
//     unchanged -- they still navigate the passed-through "." slot.
//
// So the per-row cost drops from "K evaluations of the shared term" to "1
// evaluation + K slot reads". The win grows with K and with the shared term's
// own cost (see BenchmarkBroadcastCSE).
//
// A sub-expression is a CSE candidate iff it (a) occurs >= 2 times across the
// corpus AND (b) is NON-TRIVIAL -- its head is an operation, not a leaf accessor
// ("json" / "labelPath" / "labelUint64"): caching a bare field read or constant
// just adds a slot indirection for no gain. Candidates are assigned "^cseN"
// labels in a deterministic (canonical-key sorted) order, so the emitted plan is
// stable regardless of map iteration order.
//
// If NO candidate is shared, BroadcastCSE returns a plain broadcast over scan
// (no precompute project) -- zero overhead when nothing is shared.
//
// Composition: BroadcastCSE applies WITHIN one source's detector group. It
// composes with BroadcastRoute (which splits the corpus by TargetSource): a
// caller runs BroadcastRoute to route, then wraps each per-source group with
// BroadcastCSE over that source's scan. This builder does the CSE for one such
// group; wiring the two together is the caller's (corpus-compiler's) job.
//
//   - scan: the shared-scan child *base.Op (its Labels are typically ["."]).
//   - detectors: the group's corpus (TargetSource is ignored here; the caller
//     has already routed).
//   - findingsLabels: the uniform findings schema of the returned broadcast.
func BroadcastCSE(scan *base.Op, detectors []Detector,
	findingsLabels base.Labels) *base.Op {
	// (1) Count every sub-expression-tree occurring in every detector's Pred and
	// Proj, keyed by a deterministic canonical serialization.
	counts := map[string]int{}
	trees := map[string][]interface{}{} // canonical key -> a representative tree
	for i := range detectors {
		cseCount(detectors[i].Pred, counts, trees)
		cseCount(detectors[i].Proj, counts, trees)
	}

	// (2) Select candidates: occurs >= 2 AND non-trivial (head is an operation,
	// not a leaf accessor). Order by canonical key so "^cseN" assignment is
	// deterministic.
	keys := make([]string, 0, len(counts))
	for k, n := range counts {
		if n >= 2 && !cseTrivial(trees[k]) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	// No sharing worth hoisting: a plain broadcast over scan, no precompute.
	if len(keys) == 0 {
		return cseBroadcast(scan, scan.Labels, detectors, findingsLabels)
	}

	// Assign a synthetic "^cseN" label to each candidate.
	cseLabels := make(map[string]string, len(keys))
	for i, k := range keys {
		cseLabels[k] = "^cse" + strconv.Itoa(i)
	}

	// (3) Build the precompute project: pass the whole row through under "." and
	// append one column per candidate (its ORIGINAL sub-expression tree). Its
	// output labels are the child labels the rewritten broadcast resolves against.
	projLabels := base.Labels{"."}
	projExprs := []interface{}{[]interface{}{"labelPath", "."}} // whole-row passthrough
	for i, k := range keys {
		projLabels = append(projLabels, "^cse"+strconv.Itoa(i))
		projExprs = append(projExprs, trees[k])
	}

	precompute := &base.Op{
		Kind:     "project",
		Labels:   projLabels,
		Params:   projExprs,
		Children: []*base.Op{scan},
	}

	// (4) Rewrite each detector so shared candidate subtrees read their "^cseN"
	// slot; other subtrees (incl. bare field reads) are structurally unchanged.
	rewritten := make([]Detector, len(detectors))
	for i := range detectors {
		d := detectors[i]
		d.Pred, _ = cseRewrite(d.Pred, cseLabels).([]interface{})
		d.Proj, _ = cseRewrite(d.Proj, cseLabels).([]interface{})
		rewritten[i] = d
	}

	// (5) Broadcast over the precompute project, resolving against its labels.
	return cseBroadcast(precompute, projLabels, rewritten, findingsLabels)
}

// cseBroadcast assembles a "broadcast" op over child with the given detectors.
// childLabels is documentary here (BroadcastExec reads child.Labels itself), but
// keeps the call site parallel to how the rewritten detectors resolve.
func cseBroadcast(child *base.Op, childLabels base.Labels,
	detectors []Detector, findingsLabels base.Labels) *base.Op {
	_ = childLabels

	detParams := make([]interface{}, 0, len(detectors))
	for _, d := range detectors {
		// The existing broadcast detector spec: []interface{}{tag, pred, proj}.
		detParams = append(detParams, []interface{}{d.Tag, d.Pred, d.Proj})
	}

	return &base.Op{
		Kind:     "broadcast",
		Labels:   append(base.Labels(nil), findingsLabels...),
		Params:   []interface{}{detParams},
		Children: []*base.Op{child},
	}
}

// cseLeafHeads are the leaf accessor heads: a constant ("json") or a bare label
// read ("labelPath" / "labelUint64"). A subtree headed by one of these is
// TRIVIAL and never a CSE candidate -- hoisting it would only add a slot
// indirection over an already-cheap read.
var cseLeafHeads = map[string]bool{
	"json":        true,
	"labelPath":   true,
	"labelUint64": true,
}

// cseIsExprTree reports whether node is an expr-tree node -- a []interface{}
// whose head (element 0) is a string operation/leaf name. The projection LIST
// (whose elements are themselves expr-trees) is NOT an expr-tree by this test,
// so it is traversed element-wise rather than treated as one subtree.
func cseIsExprTree(node interface{}) ([]interface{}, bool) {
	t, ok := node.([]interface{})
	if !ok || len(t) == 0 {
		return nil, false
	}
	if _, ok := t[0].(string); !ok {
		return nil, false
	}
	return t, true
}

// cseTrivial reports whether an expr-tree is a leaf accessor (see cseLeafHeads).
func cseTrivial(t []interface{}) bool {
	if len(t) == 0 {
		return true
	}
	head, _ := t[0].(string)
	return cseLeafHeads[head]
}

// cseCanon is the canonical identity of an expr-tree: a deterministic JSON
// serialization. These trees are plain strings/numbers/nested []interface{} (no
// maps), so json.Marshal is stable and two structurally-equal trees share a key.
func cseCanon(t []interface{}) string {
	b, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	return string(b)
}

// cseCount walks node, tallying every expr-tree subtree (including nested ones)
// by canonical key into counts, and recording one representative tree per key.
// It descends into ALL []interface{} elements, so both a Pred (itself a tree)
// and a Proj (a list of trees) are fully covered.
func cseCount(node interface{}, counts map[string]int, trees map[string][]interface{}) {
	t, ok := node.([]interface{})
	if !ok {
		return
	}
	if _, isExpr := cseIsExprTree(node); isExpr {
		key := cseCanon(t)
		counts[key]++
		if _, seen := trees[key]; !seen {
			trees[key] = t
		}
	}
	for _, e := range t {
		cseCount(e, counts, trees)
	}
}

// cseRewrite returns node with every candidate subtree replaced by a cheap
// ["labelPath","^cseN"] slot read. It checks a node BEFORE descending, so an
// outer candidate is replaced whole and its interior is never rewritten again
// (outermost-first) -- avoiding double-cover of nested candidates. A shared
// child left un-hoisted inside a hoisted parent is still correct: the parent's
// "^cseN" column just recomputes that child once, in the precompute project.
func cseRewrite(node interface{}, cseLabels map[string]string) interface{} {
	t, ok := node.([]interface{})
	if !ok {
		return node // leaf string / number: unchanged
	}
	if _, isExpr := cseIsExprTree(node); isExpr {
		if lbl, ok := cseLabels[cseCanon(t)]; ok {
			return []interface{}{"labelPath", lbl}
		}
	}
	out := make([]interface{}, len(t))
	for i, e := range t {
		out[i] = cseRewrite(e, cseLabels)
	}
	return out
}
