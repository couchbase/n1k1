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

	"github.com/couchbase/n1k1/base"
)

// OpMergeJoin is the sorted merge JOIN op of DESIGN-merging.md §2. Its two inputs
// are ALREADY ordered by an int64 sort key sitting in a labeled register of each
// row (produced by a merge-scan, or any sorted source) -- so the join
// co-advances two cursors in ONE linear pass, O(N+M), instead of re-driving the
// inner branch per outer row the way OpJoinNestedLoop does (O(N*M)).
//
//	o.Children[0] = the LEFT / probe input  (streamed, ascending by its key).
//	o.Children[1] = the RIGHT / build input (ascending by its key).
//
// Three modes, all reading only SCALAR codegen-friendly choices from o.Params:
//
//   - EQUI merge-join (asof "off") -- classic sort-merge equijoin on key
//     equality: for each left key, emit the cross-product with the right rows of
//     the same key. Inner drops unmatched left rows; left-outer NULL-extends them.
//
//   - ASOF nearest-preceding (asof "asof") -- for each left row, the single right
//     row with the GREATEST key <= the left key (DuckDB/kdb+ "backward" ASOF).
//     One `held` right row is carried forward (per equality-partition when
//     partition-key indices are supplied); the right cursor never rewinds. This is
//     the linear-pass replacement for the O(N*M) correlated argmax subquery.
//
//   - SOFT ASOF (asof "soft") -- ASOF plus a max look-back tolerance Δt: the held
//     row matches only if left.key - held.key <= Δt, else the row is treated as
//     unmatched (NULL-extended for left-outer, dropped for inner) --
//     within-tolerance-or-null. One subtraction-and-compare at emit time.
//
// Interpreter-oriented like OpMergeScan: OpMergeJoin delegates via a single
// "// !lz" line to MergeJoinExec, whose body carries no "lz" tokens so the
// gen-compiler copies it VERBATIM (it compiles in the generated package but is
// not dispatched there, since the recognition that emits "merge-join" is opt-in
// glue -- see glue/optimize_temporal.go rewriteASOF).
func OpMergeJoin(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	MergeJoinExec(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz
}

// MergeJoinExec holds the actual (non-lazy) merge-join logic. Params, all scalar
// and optional past index 1:
//
//	Params[0] int           -- leftKeyIdx: index of the int64 sort key in a left row.
//	Params[1] int           -- rightKeyIdx: index of the int64 sort key in a right row.
//	Params[2] string        -- joinType: "inner" (default) | "left" (left-outer).
//	Params[3] string        -- asof: "off" (default, equi) | "asof" | "soft".
//	Params[4] int64         -- toleranceNanos: max look-back Δt for asof "soft".
//	Params[5] []interface{} -- left partition-key indices (equality partitions; ASOF).
//	Params[6] []interface{} -- right partition-key indices (parallel to Params[5]).
//
// The right (build) input is materialized once into an ordered, forward-only
// cursor -- the same first-slice stand-in OpMergeScan uses for its cursors -- and
// the left input is then STREAMED, so ASOF holds exactly one right row (or one
// per active partition) and never rewinds: O(N+M), streaming, zero-alloc in the
// steady state (a single reused join-row buffer, zero-alloc int64 key parse, no
// string([]byte) on the hot path). The partitioned ASOF path additionally keeps a
// small map of partition -> held right index.
func MergeJoinExec(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	leftKeyIdx := mergeJoinInt(o.Params, 0, 0)
	rightKeyIdx := mergeJoinInt(o.Params, 1, 0)

	joinType := mergeJoinStr(o.Params, 2, "inner")
	asof := mergeJoinStr(o.Params, 3, "off")

	var tolerance int64
	if len(o.Params) > 4 {
		switch n := o.Params[4].(type) {
		case int64:
			tolerance = n
		case int:
			tolerance = int64(n)
		case float64:
			tolerance = int64(n)
		}
	}

	leftParts := mergeInt64Slice(o.Params, 5)
	rightParts := mergeInt64Slice(o.Params, 6)

	leftOuter := joinType == "left"

	lenLabelsA := len(o.Children[0].Labels)
	lenLabelsB := len(o.Children[1].Labels)

	// Materialize the right/build input into an ordered, forward-only cursor. This
	// stands in for a resumable actor-per-cursor right stream (DESIGN-merging.md
	// §2 re-entrancy); the left side stays a true stream below.
	build, execErr := mergeJoinBuildRight(o, vars, pathNext, rightKeyIdx, rightParts)
	if execErr != nil {
		yieldErr(execErr)
		return
	}

	// One reused join-row buffer (left cols ++ right cols) -- no per-row alloc.
	valsJoin := make(base.Vals, 0, lenLabelsA+lenLabelsB)

	// emit joins the current left row to a right row (or to NULLs when rightRow is
	// nil, the outer/no-match/expired case) into the reused buffer and yields it.
	emit := func(leftVals base.Vals, rightRow base.Vals) {
		valsJoin = valsJoin[:0]
		valsJoin = append(valsJoin, leftVals...)
		if rightRow != nil {
			valsJoin = append(valsJoin, rightRow...)
		} else {
			for i := 0; i < lenLabelsB; i++ {
				valsJoin = append(valsJoin, base.ValMissing)
			}
		}
		yieldVals(valsJoin)
	}

	// Per-left-row cursor / held state (see the two step functions below).
	state := &mergeJoinState{
		build:      build,
		asof:       asof,
		leftOuter:  leftOuter,
		tolerance:  tolerance,
		leftKeyIdx: leftKeyIdx,
		leftParts:  leftParts,
		heldOne:    -1,
	}
	if asof != "off" && len(leftParts) > 0 {
		state.held = map[string]int{}
	}

	var lastKey int64
	emitted := false

	leftYield := func(leftVals base.Vals) {
		if execErr != nil {
			return
		}

		k, ok := mergeParseKey(leftVals, leftKeyIdx)
		if !ok {
			execErr = mergeJoinKeyErr("left", leftKeyIdx)
			return
		}

		// Output the left side ascending -- validate the promised order (a lying
		// left source would silently corrupt the co-advance).
		if emitted && k < lastKey {
			execErr = mergeJoinOrderErr("left", k, lastKey)
			return
		}
		lastKey = k
		emitted = true

		if asof == "off" {
			mergeJoinStepEqui(state, leftVals, k, leftOuter, emit)
		} else {
			mergeJoinStepAsof(state, leftVals, k, emit)
		}
	}

	leftErr := func(err error) {
		if err != nil && execErr == nil {
			execErr = err
		}
	}

	ExecOp(o.Children[0], vars, leftYield, leftErr, pathNext, "MJL")

	yieldErr(execErr)
}

// mergeJoinState carries the forward-only right cursor and the per-left-row match
// state across left rows. rpos is the right absorb pointer (monotonic, since both
// sides are ascending); heldOne is the single held right index for unpartitioned
// ASOF; held is the partition -> held right index map for partitioned ASOF.
type mergeJoinState struct {
	build *mergeJoinSide

	asof      string
	leftOuter bool
	tolerance int64

	leftKeyIdx int
	leftParts  []int64

	rpos    int            // right absorb / group pointer (never rewinds).
	heldOne int            // unpartitioned ASOF: index of the held right row, or -1.
	held    map[string]int // partitioned ASOF: partition key -> held right index.

	partBuf []byte // reused scratch for building a left partition key.
}

// mergeJoinStepEqui runs one left row through the equi (sort-merge) join: skip
// right rows whose key is below the left key (monotonic across left rows), then
// emit the cross-product with the equal-key right group. Inner drops an unmatched
// left row; left-outer NULL-extends it.
func mergeJoinStepEqui(s *mergeJoinState, leftVals base.Vals, k int64,
	leftOuter bool, emit func(base.Vals, base.Vals)) {
	b := s.build

	// Advance past right rows strictly below the left key. Never rewinds: the next
	// left key is >= this one, so a skipped right row can never be needed again.
	for s.rpos < len(b.keys) && b.keys[s.rpos] < k {
		s.rpos++
	}

	matched := false
	for j := s.rpos; j < len(b.keys) && b.keys[j] == k; j++ {
		emit(leftVals, b.rows[j])
		matched = true
	}

	if !matched && leftOuter {
		emit(leftVals, nil)
	}
}

// mergeJoinStepAsof runs one left row through nearest-preceding (soft) ASOF:
// absorb every right row whose key <= the left key into the held state (one held
// row overall, or one per partition), then emit the held row -- soft-gated by the
// tolerance -- or NULLs / nothing for the no-match case.
func mergeJoinStepAsof(s *mergeJoinState, leftVals base.Vals, k int64,
	emit func(base.Vals, base.Vals)) {
	b := s.build

	if s.held == nil {
		// Unpartitioned: a single held index, advanced to the greatest key <= k.
		for s.rpos < len(b.keys) && b.keys[s.rpos] <= k {
			s.heldOne = s.rpos
			s.rpos++
		}
		if s.heldOne >= 0 && mergeJoinWithinTolerance(s, k, b.keys[s.heldOne]) {
			emit(leftVals, b.rows[s.heldOne])
			return
		}
	} else {
		// Partitioned: absorb each passed right row as its partition's held row.
		for s.rpos < len(b.keys) && b.keys[s.rpos] <= k {
			s.held[b.part[s.rpos]] = s.rpos
			s.rpos++
		}
		pk := mergeJoinLeftPartKey(s, leftVals)
		if hj, ok := s.held[pk]; ok && mergeJoinWithinTolerance(s, k, b.keys[hj]) {
			emit(leftVals, b.rows[hj])
			return
		}
	}

	// No nearest-preceding match (or it expired the tolerance).
	if s.leftOuter {
		emit(leftVals, nil)
	}
}

// mergeJoinWithinTolerance reports whether the held row satisfies soft ASOF's max
// look-back: leftKey - heldKey <= tolerance. Plain ASOF ("asof") has no bound.
func mergeJoinWithinTolerance(s *mergeJoinState, leftKey, heldKey int64) bool {
	if s.asof != "soft" {
		return true
	}
	return leftKey-heldKey <= s.tolerance
}

// mergeJoinLeftPartKey builds the canonical partition key for a left row from its
// partition-key indices, into a reused scratch buffer. Reads the raw JSON bytes of
// each partition field separated by a byte that cannot appear between two JSON
// tokens; the returned string is used only as a map key.
func mergeJoinLeftPartKey(s *mergeJoinState, leftVals base.Vals) string {
	s.partBuf = mergeJoinPartKey(s.partBuf[:0], leftVals, s.leftParts)
	return string(s.partBuf)
}

// -----------------------------------------------------

// mergeJoinSide is the right/build input drained into memory: parallel rows/keys
// slices (ascending by key) plus, when ASOF is partitioned, a canonical partition
// key per row. Same materialize-stand-in as MergeCursor (DESIGN-merging.md §2
// spill / re-entrancy is a later slice).
type mergeJoinSide struct {
	rows []base.Vals
	keys []int64
	part []string
}

// mergeJoinBuildRight drains o.Children[1] into an ordered mergeJoinSide, deep-
// copying each row so it survives the child scan reusing its buffers, and
// validating that the right input really is ascending by its key (a lying build
// source would corrupt the co-advance). When partitioned, it precomputes each
// right row's canonical partition key once.
func mergeJoinBuildRight(o *base.Op, vars *base.Vars, pathNext string,
	rightKeyIdx int, rightParts []int64) (*mergeJoinSide, error) {
	side := &mergeJoinSide{}

	var buildErr error
	var lastKey int64
	seen := false
	var partBuf []byte

	rightYield := func(vals base.Vals) {
		if buildErr != nil {
			return
		}

		k, ok := mergeParseKey(vals, rightKeyIdx)
		if !ok {
			buildErr = mergeJoinKeyErr("right", rightKeyIdx)
			return
		}
		if seen && k < lastKey {
			buildErr = mergeJoinOrderErr("right", k, lastKey)
			return
		}
		lastKey = k
		seen = true

		side.rows = append(side.rows, mergeCopyVals(vals))
		side.keys = append(side.keys, k)

		if len(rightParts) > 0 {
			partBuf = mergeJoinPartKey(partBuf[:0], vals, rightParts)
			side.part = append(side.part, string(partBuf))
		}
	}

	rightErr := func(err error) {
		if err != nil && buildErr == nil {
			buildErr = err
		}
	}

	ExecOp(o.Children[1], vars, rightYield, rightErr, pathNext, "MJR")

	return side, buildErr
}

// mergeJoinPartKey appends the raw JSON bytes of vals[idxs...] into dst separated
// by 0x1f (unit separator -- never appears inside a JSON scalar/string token), so
// distinct partition tuples map to distinct keys. dst is reused across calls.
func mergeJoinPartKey(dst []byte, vals base.Vals, idxs []int64) []byte {
	for n, idx := range idxs {
		if n > 0 {
			dst = append(dst, 0x1f)
		}
		i := int(idx)
		if i >= 0 && i < len(vals) {
			dst = append(dst, vals[i]...)
		}
	}
	return dst
}

// mergeJoinInt reads o.Params[i] as an int (tolerating int / int64 / float64),
// falling back to def when absent or a non-number.
func mergeJoinInt(params []interface{}, i, def int) int {
	if i >= len(params) {
		return def
	}
	switch n := params[i].(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return def
}

// mergeJoinStr reads o.Params[i] as a non-empty string, else def.
func mergeJoinStr(params []interface{}, i int, def string) string {
	if i >= len(params) {
		return def
	}
	if s, ok := params[i].(string); ok && s != "" {
		return s
	}
	return def
}

// mergeJoinKeyErr is the missing / non-int64 sort-key error for a merge-join side.
func mergeJoinKeyErr(side string, keyIdx int) error {
	return fmt.Errorf("merge-join: %s row has a missing or non-int64 sort key"+
		" at index %d", side, keyIdx)
}

// mergeJoinOrderErr is the monotonicity tripwire: a side yielded a key below its
// predecessor, so it violated the ascending order the merge-join relies on.
func mergeJoinOrderErr(side string, key, lastKey int64) error {
	return fmt.Errorf("merge-join: %s side yielded out-of-order key %d < %d"+
		" (source violated its declared sort order)", side, key, lastKey)
}
