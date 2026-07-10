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
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/base"
)

// OpBroadcastIndexed is the "predicate index" scale lever of the PREPARE++
// multi-query optimization (DESIGN-prepare.md, "Predicate index (the scale
// trick)"): a filter-index node feeding a SPARSE fan-out.
//
// A plain "broadcast" (op_broadcast.go) evaluates ALL K detector predicates on
// EVERY row -- O(K x rows). This op instead indexes each detector by a cheap
// REQUIRED literal (a substring the detector's full predicate cannot be true
// without). Per row it runs ONE Aho-Corasick pass over the raw row bytes to find
// which required literals are present, then wakes ONLY the detectors keyed to a
// present literal (plus a small "always-wake" set of detectors from which no
// required literal could be safely extracted). With thousands of detectors keyed
// to distinct literals and rows each mentioning a handful, this turns O(K x
// rows) into ~O(hits x rows).
//
// THE INVARIANT (soundness): a prefilter literal is extracted ONLY when it is a
// NECESSARY condition of the full predicate -- absent literal => predicate false
// => safe to skip. Over-waking (a literal present but the full predicate then
// fails) is harmless: the full predicate re-checks and drops the row, so output
// is IDENTICAL to a plain broadcast (TestOpBroadcastIndexedEquivalence proves
// byte-identical findings). Under-waking would be a correctness bug and is
// avoided by falling back to always-wake whenever no necessary literal is
// provable (see PrefilterLiteral).
//
// The Aho-Corasick pass runs over the whole raw row bytes (base.Val slots), not
// per-field: if a detector requires contains(field,"panic") and the row is true,
// then "panic" is in the serialized row, so presence-in-row is necessary. A
// literal that happens to appear in a different field merely over-wakes (safe).
//
// Interpreter-oriented like OpBroadcast / OpMergeScan: this delegates via a
// single "// !lz" line to BroadcastIndexedExec, whose body is free of any "lz"
// tokens so the gen-compiler copies it VERBATIM into the intermed package (it
// compiles cleanly there but is never dispatched -- no SQL plan emits this op
// yet). Compiled-path fusion is future work.
func OpBroadcastIndexed(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	BroadcastIndexedExec(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz
}

// BroadcastIndexedExec holds the actual (non-lazy) sparse fan-out logic. Its
// params are named WITHOUT the "lz" prefix so every line is copied verbatim by
// the gen-compiler (mirroring BroadcastExec / MergeScanExec).
//
// Params layout is IDENTICAL to OpBroadcast (Params[0] = []detector, each a
// []interface{}{tag, predExpr, projExprs}); the build helper BroadcastIndexed
// assembles it. The child scan's Labels drive predicate/projection resolution;
// o.Labels are the stable findings schema ([<tag>, <evidence...>]).
func BroadcastIndexedExec(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	childLabels := base.Labels(nil)
	if len(o.Children) > 0 {
		childLabels = o.Children[0].Labels
	}

	// Setup (once): build each detector's predicate/projection funcs AND extract
	// its required prefilter literal (or mark it always-wake). Garbage here is
	// fine; the hot loop below is not.
	specs := broadcastDetectorSpecs(o.Params)

	detectors := make([]broadcastDetector, 0, len(specs))

	// litIDs maps a distinct required literal string to its Aho-Corasick pattern
	// id; litDetectors[id] lists the detectors keyed to that literal. alwaysWake
	// lists detectors with NO safely-extractable literal (evaluated every row).
	litIDs := map[string]int{}
	var literals []string
	var litDetectors [][]int
	var alwaysWake []int

	for i, spec := range specs {
		predFunc := MakeExprFunc(vars, childLabels, spec.pred, pathNext, "pred"+strconv.Itoa(i))

		projFunc := MakeProjectFunc(vars, childLabels, spec.proj, pathNext, "proj"+strconv.Itoa(i))

		di := len(detectors)
		detectors = append(detectors, broadcastDetector{
			tagVal:   base.Val(strconv.Quote(spec.tag)),
			predFunc: predFunc,
			projFunc: projFunc,
			woken:    spec.woken,
		})

		lit, ok := PrefilterLiteral(spec.pred)
		if !ok {
			alwaysWake = append(alwaysWake, di)
			continue
		}

		id, seen := litIDs[lit]
		if !seen {
			id = len(literals)
			litIDs[lit] = id
			literals = append(literals, lit)
			litDetectors = append(litDetectors, nil)
		}
		litDetectors[id] = append(litDetectors[id], di)
	}

	// Build the Aho-Corasick automaton over the distinct required literals + a
	// reused, zero-garbage match set. All allocation is here in setup.
	ac := base.BuildAhoCorasick(literals)
	matched := ac.NewMatchSet()

	// Stats: RowsIn (shared rows fanned), FindingsOut (tagged findings), and
	// PredEvals -- the sparsity signal: how many FULL detector predicates were
	// evaluated (woken + always-wake), vs a plain broadcast's K-per-row. Live,
	// interpreter-only (genCompiler:hide).
	stats := StatsFromVars(vars)                                       // <== genCompiler:hide
	statsBase := o.StatsBase                                           // <== genCompiler:hide
	StatsCounterZero(stats, statsBase+StatBroadcastIndexedRowsIn)      // <== genCompiler:hide
	StatsCounterZero(stats, statsBase+StatBroadcastIndexedFindingsOut) // <== genCompiler:hide
	StatsCounterZero(stats, statsBase+StatBroadcastIndexedPredEvals)   // <== genCompiler:hide

	// Per row: (1) one AC pass over the row bytes -> the present literal ids; (2)
	// evaluate ONLY the detectors those literals wake, plus the always-wake set;
	// (3) on a truthy full predicate, project into the detector's REUSED buffer
	// (tag first) and yield -- exactly as OpBroadcast does. Because each detector
	// is keyed to at most ONE literal, the woken lists are disjoint (no dedup
	// needed), and always-wake detectors have no literal (disjoint from woken).
	childYield := func(vals base.Vals) {
		StatsCounterBump(stats, statsBase+StatBroadcastIndexedRowsIn) // stats: live // <== genCompiler:hide

		// One Aho-Corasick pass across all row byte-slots (streaming, zero alloc).
		matched.Reset()
		acState := 0
		for _, v := range vals {
			acState = ac.Advance(acState, v, matched)
		}

		// Woken detectors: those keyed to a present literal.
		for _, litID := range matched.IDs() {
			for _, di := range litDetectors[litID] {
				d := &detectors[di]

				StatsCounterBump(stats, statsBase+StatBroadcastIndexedPredEvals) // stats: live // <== genCompiler:hide
				bcBumpWoken(d)                                                   // stats: live // <== genCompiler:hide

				predVal := d.predFunc(vals, yieldErr)
				if base.ValTruthy(predVal) {
					out := d.outReuse[:0]
					out = append(out, d.tagVal)
					out = d.projFunc(vals, out, yieldErr)
					d.outReuse = out

					StatsCounterBump(stats, statsBase+StatBroadcastIndexedFindingsOut) // stats: live // <== genCompiler:hide

					yieldVals(out)
				}
			}
		}

		// Always-wake detectors: no required literal was provable, so they are
		// evaluated on every row (the safe fallback that preserves correctness).
		for _, di := range alwaysWake {
			d := &detectors[di]

			StatsCounterBump(stats, statsBase+StatBroadcastIndexedPredEvals) // stats: live // <== genCompiler:hide
			bcBumpWoken(d)                                                   // stats: live // <== genCompiler:hide

			predVal := d.predFunc(vals, yieldErr)
			if base.ValTruthy(predVal) {
				out := d.outReuse[:0]
				out = append(out, d.tagVal)
				out = d.projFunc(vals, out, yieldErr)
				d.outReuse = out

				StatsCounterBump(stats, statsBase+StatBroadcastIndexedFindingsOut) // stats: live // <== genCompiler:hide

				yieldVals(out)
			}
		}
	}

	ExecOp(o.Children[0], vars, childYield, yieldErr, pathNext, "0")
}

// regexMetaChars are the regexp metacharacters. A "regexp_contains" / "regexp_like"
// pattern containing ANY of these is NOT a plain literal, so no required
// substring can be extracted from it (see PrefilterLiteral).
const regexMetaChars = `.*+?()[]{}|^$\`

// PrefilterLiteral returns a REQUIRED plain-substring literal of pred -- one that
// MUST appear in the row bytes whenever pred is true -- or ("", false) if none is
// provable (=> the detector must always-wake). Exported so the glue-level corpus
// lint ("detect lint") can report, per detector, whether its predicate is
// index-pruned by a necessary literal or is always-wake. This is the correctness heart of
// the predicate index: it extracts ONLY when the literal is a necessary
// condition (never under-wake). Supported necessary forms:
//
//   - ["contains", <field>, ["json","\"LIT\""]]  -- LIT is a required substring.
//   - ["eq", <field>, ["json","\"CONST\""]] (either operand order) -- the field
//     equals the string CONST, so CONST's bytes appear in the row.
//   - ["regexp_contains"|"regexp_like", <field>, ["json","\"PAT\""]] -- ONLY if
//     PAT is a plain literal (no regex metacharacters); then PAT is required.
//   - ["and", C1, C2, ...] -- the detector requires ALL conjuncts, so requiring
//     the FIRST extractable conjunct's literal is necessary.
//
// Anything else (or, comparisons, non-string constants, unrecognized) yields no
// literal -> always-wake. Conservative by design: when in doubt, always-wake.
func PrefilterLiteral(pred []interface{}) (string, bool) {
	if len(pred) == 0 {
		return "", false
	}
	head, _ := pred[0].(string)

	switch head {
	case "contains":
		// contains(field, "LIT"): the NEEDLE (2nd operand) is the required
		// substring. contains("LIT", field) gives no required row substring, so
		// only the 2nd-operand-literal form qualifies. The SEARCHED operand must be
		// a raw field (labelPath): a transformed operand -- contains(UPPER(field),
		// "LOAD") -- does NOT put "LOAD" in the row bytes (they hold "load"), so
		// extracting it would UNDER-wake (wrongly prune). Then it stays always-wake.
		if len(pred) >= 3 && isFieldOperand(pred[1]) {
			if lit, ok := jsonStringLiteral(pred[2]); ok {
				return lit, true
			}
		}
		return "", false

	case "eq":
		// Equality is symmetric: a string constant equated to a raw FIELD is a
		// required substring of the row (the field serializes to exactly that
		// string). The non-literal side must be a field, for the same reason as
		// contains -- eq(LOWER(field), "x") does not put "x" in the row bytes.
		if len(pred) >= 3 {
			if lit, ok := jsonStringLiteral(pred[2]); ok && isFieldOperand(pred[1]) {
				return lit, true
			}
			if lit, ok := jsonStringLiteral(pred[1]); ok && isFieldOperand(pred[2]) {
				return lit, true
			}
		}
		return "", false

	case "regexp_contains", "regexp_like":
		// A regex whose pattern is a PLAIN literal (no metachars) matches iff that
		// literal is a substring of the field -> required substring. A pattern with
		// metachars is not a plain substring, so no literal is extractable. As with
		// contains, the searched operand must be a raw field.
		if len(pred) >= 3 && isFieldOperand(pred[1]) {
			if pat, ok := jsonStringLiteral(pred[2]); ok &&
				pat != "" && !strings.ContainsAny(pat, regexMetaChars) {
				return pat, true
			}
		}
		return "", false

	case "and":
		// Every conjunct is required, so any one extractable conjunct's literal is
		// necessary. Take the FIRST extractable one (rarest-literal selection is
		// future work).
		for _, c := range pred[1:] {
			ct, ok := c.([]interface{})
			if !ok {
				continue
			}
			if lit, ok := PrefilterLiteral(ct); ok {
				return lit, true
			}
		}
		return "", false
	}

	return "", false
}

// isFieldOperand reports whether node is a raw field reference -- a ["labelPath",
// ...] navigation into the row. Only such an operand serializes verbatim into the
// row bytes, which is what makes an extracted contains/eq/regexp literal a sound
// NECESSARY substring. A transformed operand (UPPER/LOWER/SUBSTR/... over a field,
// or any other computed expression) does not, so it must not yield a literal.
func isFieldOperand(node interface{}) bool {
	t, ok := node.([]interface{})
	if !ok || len(t) == 0 {
		return false
	}
	h, _ := t[0].(string)
	return h == "labelPath"
}

// jsonStringLiteral returns the decoded string of a ["json","\"...\""] node whose
// constant is a JSON STRING, or ("", false) otherwise (a number, object, array,
// or a non-json node). The decoded string is what actually appears in the row
// bytes for an equality / contains match.
func jsonStringLiteral(node interface{}) (string, bool) {
	t, ok := node.([]interface{})
	if !ok || len(t) < 2 {
		return "", false
	}
	if h, _ := t[0].(string); h != "json" {
		return "", false
	}
	raw, ok := t[1].(string)
	if !ok || len(raw) == 0 || raw[0] != '"' {
		return "", false // not a JSON string literal
	}
	s, err := strconv.Unquote(raw)
	if err != nil {
		return "", false
	}
	return s, true
}

// BroadcastIndexed is the build helper: it assembles an OpBroadcastIndexed op
// from a scan + a detector corpus (mirroring BroadcastRoute / BroadcastCSE). It
// carries the SAME detector-spec Params shape as a plain "broadcast", so the two
// are drop-in interchangeable -- the prefilter extraction + Aho-Corasick index
// are built at run time in BroadcastIndexedExec's setup, keeping this a pure
// plan-shape helper.
//
//   - scan: the shared-scan child *base.Op (its Labels drive resolution).
//   - detectors: the corpus (TargetSource is ignored here; the caller routes).
//   - findingsLabels: the uniform findings schema ([tag, evidence...]).
func BroadcastIndexed(scan *base.Op, detectors []Detector,
	findingsLabels base.Labels) *base.Op {
	detParams := make([]interface{}, 0, len(detectors))
	for _, d := range detectors {
		// The existing broadcast detector spec: []interface{}{tag, pred, proj}.
		detParams = append(detParams, []interface{}{d.Tag, d.Pred, d.Proj})
	}

	return &base.Op{
		Kind:     "broadcast-indexed",
		Labels:   append(base.Labels(nil), findingsLabels...),
		Params:   []interface{}{detParams},
		Children: []*base.Op{scan},
	}
}
