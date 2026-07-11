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
	"bytes"
	"strconv"

	"github.com/couchbase/n1k1/base"
)

// OpBroadcastContext is the SHARED SORTED-STREAM fan-out (DESIGN-mqo-sorted.md step 3,
// the "shared context operator" -- the first stateful consumer type of the shared
// sorted substrate).
//
// It fans ONE already-sorted, partition-grouped input stream to K "context extractors",
// so K grep -A/-B/-C style detectors share a single scan AND a single sort (the dominant
// cost on GB logs) instead of each re-scanning + re-sorting the keyspace. Each extractor
// emits, per matching row, that row plus surrounding CONTEXT lines -- the evidence-
// gathering idiom that a windowed match-flag expresses in SQL++
// (MAX(CASE WHEN <pred> ...) OVER (PARTITION BY p ORDER BY o ROWS ...) then WHERE flag).
//
// Shape:
//
//   - Exactly ONE child (o.Children[0]): the input stream, REQUIRED to arrive sorted by
//     (partition, order) -- e.g. an order-offset-limit over a scan, or an already-ordered
//     source. This op does no sorting; it relies on the shared upstream sort.
//   - o.Params[0] []interface{} -- the K extractors (see contextExtractorSpec).
//   - o.Params[1] -- OPTIONAL partition-key expr-tree. Rows are grouped into partitions
//     by this key's value (bytes-equal to the previous row's); a new partition resets
//     every extractor's context window, so context never spans partitions (rotated logs:
//     one file's lines never bleed into another's). Absent/nil => one partition.
//
// Per extractor per matching row m, the emitted rows are m's `beforeMatch` predecessors,
// m itself, and its `afterMatch` successors within the same partition (grep -B/-A/-C),
// each emitted AT MOST ONCE across overlapping match windows and tagged with the
// extractor's id in output slot 0 -- so the interleaved findings demultiplex by tag,
// exactly like OpBroadcast.
//
// Interpreter-oriented like OpBroadcast / OpMergeJoin: OpBroadcastContext delegates via a
// single "// !lz" line to BroadcastContextExec, whose body carries no "lz" tokens so the
// gen-compiler copies it VERBATIM (it compiles in the intermed package but is not
// dispatched there -- the glue recognition that would emit a "broadcast-context" op is a
// later slice, exactly as OpBroadcast's emission was).
func OpBroadcastContext(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	BroadcastContextExec(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz
}

// contextExtractorSpec is the parsed description of one context extractor:
//
//	det[0] tag         string        -- extractor id (output slot 0)
//	det[1] beforeMatch int           -- context lines to emit BEFORE a match (grep -B)
//	det[2] afterMatch  int           -- context lines to emit AFTER a match  (grep -A)
//	det[3] predExpr    []interface{} -- the match predicate expr-tree
//	det[4] projExprs   []interface{} -- the evidence projection (list of expr-trees)
type contextExtractorSpec struct {
	tag         string
	beforeMatch int
	afterMatch  int
	pred        []interface{}
	proj        []interface{}
}

// contextExtractor is one extractor's setup-built machinery plus its streaming state.
// The state implements grep -B/-A over a stream that arrives in (partition, order):
//
//   - buf holds the most recent NOT-yet-emitted rows (deep-copied, since the child reuses
//     its row buffers), capped at beforeMatch -- the backward-context candidates. On a
//     match they are flushed (they are the match's preceding context); a row emitted as
//     forward context is never buffered, so no row is ever emitted twice.
//   - afterLeft counts remaining forward-context rows owed by the most recent match.
//
// Both reset at a partition boundary, so context is partition-local.
type contextExtractor struct {
	spec     contextExtractorSpec
	tagVal   base.Val
	predFunc base.ExprFunc
	projFunc base.ProjectFunc

	buf       []base.Vals // recent non-emitted rows (deep copies), len <= beforeMatch
	afterLeft int
	outReuse  base.Vals // reused projection output buffer
}

// reset clears an extractor's context window at a partition boundary.
func (e *contextExtractor) reset() {
	e.buf = e.buf[:0]
	e.afterLeft = 0
}

// BroadcastContextExec holds the actual (non-lazy) shared-context fan-out logic. Params
// are named WITHOUT the "lz" prefix so the gen-compiler copies every line verbatim (same
// pattern as BroadcastExec / MergeJoinExec).
func BroadcastContextExec(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	childLabels := base.Labels(nil)
	if len(o.Children) > 0 {
		childLabels = o.Children[0].Labels
	}

	specs := contextExtractorSpecs(o.Params)

	extractors := make([]*contextExtractor, 0, len(specs))
	for i, spec := range specs {
		extractors = append(extractors, &contextExtractor{
			spec:     spec,
			tagVal:   base.Val(strconv.Quote(spec.tag)),
			predFunc: MakeExprFunc(vars, childLabels, spec.pred, pathNext, "cpred"+strconv.Itoa(i)),
			projFunc: MakeProjectFunc(vars, childLabels, spec.proj, pathNext, "cproj"+strconv.Itoa(i)),
		})
	}

	// Optional partition-key func: a new partition (its value differs from the prior
	// row's) resets every extractor's window, so context never spans partitions.
	var partFunc base.ExprFunc
	if len(o.Params) > 1 && o.Params[1] != nil {
		partSpec, _ := o.Params[1].([]interface{})
		if len(partSpec) > 0 {
			partFunc = MakeExprFunc(vars, childLabels, partSpec, pathNext, "cpart")
		}
	}

	var prevPart base.Val
	havePart := false

	// emit projects one row for an extractor (tag first) into its reused buffer and
	// yields the tagged finding.
	emit := func(e *contextExtractor, row base.Vals) {
		out := e.outReuse[:0]
		out = append(out, e.tagVal)
		out = e.projFunc(row, out, yieldErr)
		e.outReuse = out
		yieldVals(out)
	}

	childYield := func(vals base.Vals) {
		// Partition boundary detection: reset every extractor's window when the key value
		// changes (or on a boundary where one side lacks a key -- treated as a change).
		if partFunc != nil {
			pv := partFunc(vals, yieldErr)
			if !havePart || !bytes.Equal(pv, prevPart) {
				for _, e := range extractors {
					e.reset()
				}
				prevPart = append(prevPart[:0], pv...)
				havePart = true
			}
		}

		for _, e := range extractors {
			match := base.ValTruthy(e.predFunc(vals, yieldErr))

			switch {
			case match:
				// Flush the backward-context buffer (the match's preceding non-emitted
				// rows), then the match row; owe `afterMatch` forward rows.
				for _, b := range e.buf {
					emit(e, b)
				}
				e.buf = e.buf[:0]
				emit(e, vals)
				e.afterLeft = e.spec.afterMatch

			case e.afterLeft > 0:
				// A forward-context row of a recent match. Emitted (not buffered -- so a
				// later match's backward window won't re-emit it).
				emit(e, vals)
				e.afterLeft--

			default:
				// A non-context row: a backward-context candidate for a FUTURE match. Keep
				// only the last `beforeMatch` such rows (deep-copied -- the child reuses
				// its buffers). beforeMatch == 0 buffers nothing (grep -A0 / -B0).
				if e.spec.beforeMatch > 0 {
					e.buf = append(e.buf, mergeCopyVals(vals))
					if len(e.buf) > e.spec.beforeMatch {
						e.buf = e.buf[1:]
					}
				}
			}
		}
	}

	ExecOp(o.Children[0], vars, childYield, yieldErr, pathNext, "0")
}

// contextExtractorSpecs parses o.Params[0] into per-extractor specs. A malformed entry
// (glue builds these) is skipped rather than half-decoded.
func contextExtractorSpecs(params []interface{}) []contextExtractorSpec {
	if len(params) == 0 || params[0] == nil {
		return nil
	}
	raw, ok := params[0].([]interface{})
	if !ok {
		return nil
	}
	specs := make([]contextExtractorSpec, 0, len(raw))
	for _, r := range raw {
		det, ok := r.([]interface{})
		if !ok || len(det) < 5 {
			continue
		}
		tag, _ := det[0].(string)
		proj, _ := det[4].([]interface{})
		pred, _ := det[3].([]interface{})
		specs = append(specs, contextExtractorSpec{
			tag:         tag,
			beforeMatch: contextInt(det[1]),
			afterMatch:  contextInt(det[2]),
			pred:        pred,
			proj:        proj,
		})
	}
	return specs
}

// contextInt reads a spec count (before/after) tolerating int / int64 / float64.
func contextInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}
