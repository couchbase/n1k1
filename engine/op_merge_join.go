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

// MergeJoinBuildSpillBytes caps how many bytes of right/build ROW PAYLOADS the merge-join
// keeps resident before spilling them to a heap (disk-backed, bounded RAM). The key index
// (keys[]/part[]) always stays resident -- it's ~25x smaller than the rows for a typical
// log line -- and ASOF/equi read rows only at/near the cursor, so a spilled build decodes
// just the active row(s) on access instead of pinning the whole (possibly multi-GB) right
// keyspace in memory. 0 disables spilling (always resident). Package var so a caller can
// tune it before a run. NOTE: this bounds the ROW payload RAM, not the O(N) key index --
// fully streaming the build (evicting keys past the ASOF band) is the deferred
// resumable-cursor work the whole merge family shares (see DESIGN-merging.md §2).
var MergeJoinBuildSpillBytes int64 = 64 << 20 // 64 MiB of resident row payloads.

// MergeJoinStreamASOF turns on the two-stream ASOF co-advance: instead of materializing
// the whole build (right) side, the right runs in its own goroutine pushing rows into a
// bounded channel while the left streams, so the join holds only the ASOF BAND resident
// (one held row -- or one per partition -- plus a one-row lookahead), not the keyspace.
// This is the memory fix that the build spill alone could not deliver (the build spill
// bounds a materialization; this avoids it). false => the materialized build path. The
// following-partitioned case still materializes (it needs a per-partition lookahead index).
var MergeJoinStreamASOF = true

// The right build streams to the consumer in BATCHES, not row-by-row: a per-row channel
// handoff spends most of its CPU in goroutine park/unpark (pthread_cond_signal) because
// the consumer keeps pace and the channel ping-pongs near-empty. Batching amortizes the
// wakeup over mergeStreamBatchRows rows (measured: ~55% of streaming CPU was cond_signal;
// batching removes it). mergeJoinStreamChanCap bounds the channel in BATCHES (backpressure);
// total buffered rows = cap x batch, still bounded.
const (
	mergeStreamBatchRows   = 128 // rows per channel batch.
	mergeJoinStreamChanCap = 8   // batches buffered in the channel.
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
//   - ASOF nearest-preceding (asof "asof", direction "preceding" -- the default) --
//     for each left row, the single right row with the GREATEST key <= the left key
//     (DuckDB/kdb+ "backward" ASOF). One `held` right row is carried forward (per
//     equality-partition when partition-key indices are supplied); the right cursor
//     never rewinds. The linear-pass replacement for the O(N*M) correlated argmax.
//
//   - ASOF nearest-FOLLOWING (asof "asof"/"soft", direction "following") -- the
//     mirror: for each left row, the SMALLEST-key right row with key >= the left key
//     ("forward" ASOF). A forward cursor advances to the first key >= k but does NOT
//     consume it (one right row can follow several left rows). Partitioned following
//     keeps a per-partition ascending index list + cursor. Serves "the nearest
//     <content-matching> row AFTER each left row" (e.g. XYZ then ABC soon after).
//
//   - SOFT ASOF (asof "soft") -- ASOF plus a max tolerance Δt: preceding matches
//     only if left.key - held.key <= Δt (look-BACK); following matches only if
//     held.key - left.key <= Δt (look-AHEAD -- "within Δt after"). Else the row is
//     treated as unmatched (NULL-extended for left-outer, dropped for inner).
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
//	Params[7] string        -- direction: "preceding" (default) | "following" (ASOF only).
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

	direction := mergeJoinStr(o.Params, 7, "preceding")
	following := asof != "off" && direction == "following"

	leftOuter := joinType == "left"

	lenLabelsA := len(o.Children[0].Labels)
	lenLabelsB := len(o.Children[1].Labels)

	// Two-stream ASOF: co-advance both sides, holding only the ASOF band -- no build
	// materialization. The one case that stays materialized is PLAIN (unbounded)
	// following-partitioned: a partition's follow can be arbitrarily far ahead, so a
	// streamed per-partition lookahead has no bound. SOFT following-partitioned IS
	// streamable -- the tolerance caps the lookahead to [k, k+Δt] (see mergeJoinStreamAsof).
	if asof != "off" && MergeJoinStreamASOF &&
		!(following && len(leftParts) > 0 && asof != "soft") {
		mergeJoinStreamAsof(o, vars, yieldVals, yieldErr, pathNext,
			leftKeyIdx, rightKeyIdx, leftParts, following, asof, tolerance,
			leftOuter, lenLabelsA, lenLabelsB)
		return
	}

	// Materialize the right/build input into an ordered, forward-only cursor. This
	// stands in for a resumable actor-per-cursor right stream (DESIGN-merging.md
	// §2 re-entrancy); the left side stays a true stream below.
	build, execErr := mergeJoinBuildRight(o, vars, pathNext, rightKeyIdx, rightParts)
	if execErr != nil {
		yieldErr(execErr)
		return
	}
	if build.closeSpill != nil {
		defer build.closeSpill()
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

	// Per-left-row cursor / held state (see the step functions below).
	state := &mergeJoinState{
		build:      build,
		asof:       asof,
		following:  following,
		leftOuter:  leftOuter,
		tolerance:  tolerance,
		leftKeyIdx: leftKeyIdx,
		leftParts:  leftParts,
		heldOne:    -1,
	}
	if asof != "off" && len(leftParts) > 0 {
		state.held = map[string]int{}
		if following {
			// Following-partitioned needs each partition's ascending index list so the
			// per-partition forward cursor (held[pk]) can advance to the first key >= k.
			// The build is already ascending, so appending in order keeps each list sorted.
			state.partIdx = map[string][]int{}
			for j := range build.keys {
				pk := build.part[j]
				state.partIdx[pk] = append(state.partIdx[pk], j)
			}
		}
	}

	var lastKey int64
	emitted := false

	leftYield := func(leftVals base.Vals) {
		if execErr != nil {
			return
		}

		k, ok := mergeParseKey(leftVals, leftKeyIdx)
		if !ok {
			vars.Ctx.MergeStats.AddNoKeySkipped(1) // keyless probe row (banner / multiline) -- can't correlate, skip.
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
		} else if following {
			mergeJoinStepAsofFollowing(state, leftVals, k, emit)
		} else {
			mergeJoinStepAsof(state, leftVals, k, emit)
		}

		if state.spillErr != nil && execErr == nil {
			execErr = state.spillErr // a spill-heap read failed mid-join.
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
	following bool // nearest-following (forward) instead of nearest-preceding (backward).
	leftOuter bool
	tolerance int64

	leftKeyIdx int
	leftParts  []int64

	rpos    int            // right absorb / group / forward pointer (never rewinds).
	heldOne int            // unpartitioned preceding ASOF: index of the held right row, or -1.
	held    map[string]int // partitioned ASOF: partition key -> held right index (preceding)
	// or the per-partition cursor into partIdx[pk] (following).
	partIdx map[string][]int // following-partitioned: partition key -> ascending build indices.

	partBuf  []byte // reused scratch for building a left partition key.
	spillErr error  // a build spill-heap read error, surfaced to the left pass.
}

// row fetches a build row (resident or decoded from the spill heap). On a spill read error
// it latches s.spillErr (the left pass checks it and aborts) and returns ok=false so the
// caller skips the emit.
func (s *mergeJoinState) row(j int) (base.Vals, bool) {
	r, err := s.build.rowAt(j)
	if err != nil {
		s.spillErr = err
		return nil, false
	}
	return r, true
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
		r, ok := s.row(j)
		if !ok {
			return
		}
		emit(leftVals, r)
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
			if r, ok := s.row(s.heldOne); ok {
				emit(leftVals, r)
			}
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
			if r, ok := s.row(hj); ok {
				emit(leftVals, r)
			}
			return
		}
	}

	// No nearest-preceding match (or it expired the tolerance).
	if s.leftOuter {
		emit(leftVals, nil)
	}
}

// mergeJoinStepAsofFollowing runs one left row through nearest-FOLLOWING ASOF: the
// smallest-key right row with key >= the left key. The cursor advances to that row but
// does NOT consume it (the same right row can follow several ascending left rows). Since
// the nearest following is the smallest qualifying key, a soft look-ahead only has to
// gate that one candidate. Unpartitioned uses the single forward pointer rpos;
// partitioned advances a per-partition cursor over partIdx[pk] (both monotonic because
// left keys are non-decreasing).
func mergeJoinStepAsofFollowing(s *mergeJoinState, leftVals base.Vals, k int64,
	emit func(base.Vals, base.Vals)) {
	b := s.build

	if s.held == nil {
		// Unpartitioned: advance past right rows strictly below k; the first key >= k is
		// the nearest following (do not step past it -- it may follow later left rows too).
		for s.rpos < len(b.keys) && b.keys[s.rpos] < k {
			s.rpos++
		}
		if s.rpos < len(b.keys) && mergeJoinWithinToleranceFwd(s, k, b.keys[s.rpos]) {
			if r, ok := s.row(s.rpos); ok {
				emit(leftVals, r)
			}
			return
		}
	} else {
		// Partitioned: advance this partition's cursor over its ascending index list to
		// the first index whose key >= k.
		pk := mergeJoinLeftPartKey(s, leftVals)
		idxs := s.partIdx[pk]
		c := s.held[pk]
		for c < len(idxs) && b.keys[idxs[c]] < k {
			c++
		}
		s.held[pk] = c
		if c < len(idxs) && mergeJoinWithinToleranceFwd(s, k, b.keys[idxs[c]]) {
			if r, ok := s.row(idxs[c]); ok {
				emit(leftVals, r)
			}
			return
		}
	}

	// No following match (or it exceeded the look-ahead tolerance).
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

// mergeJoinWithinToleranceFwd is the following (look-AHEAD) analog: the nearest following
// row matches only if held.key - left.key <= Δt. Plain following ("asof") has no bound.
func mergeJoinWithinToleranceFwd(s *mergeJoinState, leftKey, heldKey int64) bool {
	if s.asof != "soft" {
		return true
	}
	return heldKey-leftKey <= s.tolerance
}

// mergeJoinStreamRow is one build row carried over the right-stream channel: its int64
// key, a deep copy of its Vals (the scan reuses its buffers, so we must copy before
// handing the row to another goroutine), and -- when partitioned -- its canonical
// partition key.
type mergeJoinStreamRow struct {
	key  int64
	vals base.Vals
	part string
}

// mergeJoinStreamAsof runs an ASOF join by CO-ADVANCING both sorted inputs, so neither
// side is materialized: the build (right) is driven by a goroutine (on a cloned Ctx, so
// it never races the left over the shared Ctx) that pushes deep-copied rows into a bounded
// channel; the left streams in this goroutine and, per left row, pulls the right forward
// just far enough to position the ASOF match. Resident state is the band only -- one held
// row (nearest-preceding) or one per partition, plus a single lookahead row (or, for soft
// following-partitioned, per-partition queues bounded to the tolerance window) -- not the
// keyspace. Handles preceding (un/partitioned), following (unpartitioned), and SOFT
// following-partitioned; only PLAIN (unbounded) following-partitioned is routed to the
// materialized path by the caller.
func mergeJoinStreamAsof(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, pathNext string, leftKeyIdx, rightKeyIdx int,
	leftParts []int64, following bool, asof string, tolerance int64, leftOuter bool,
	lenLabelsA, lenLabelsB int) {
	if vars.Ctx != nil {
		vars.Ctx.MergeStats.RecordStreamJoin()
	}

	// --- right (build) producer: a base.StageCursor (goroutine + per-actor Vars clone +
	// deep-copy + batching + cancel/join, all shared). Its actor runs the build scan,
	// dropping keyless rows and validating ascending order at the source. ---
	bc := base.NewStageCursor(vars, mergeStreamBatchRows, mergeJoinStreamChanCap,
		func(rVars *base.Vars, yieldVals base.YieldVals, yieldErr base.YieldErr) {
			var lastKey int64
			seen := false
			ry := func(vals base.Vals) {
				k, ok := mergeParseKey(vals, rightKeyIdx)
				if !ok {
					mergeStats(rVars).AddNoKeySkipped(1)
					return
				}
				if seen && k < lastKey {
					yieldErr(mergeJoinOrderErr("right", k, lastKey))
					return
				}
				lastKey, seen = k, true
				yieldVals(vals)
			}
			ExecOp(o.Children[1], rVars, ry, yieldErr, pathNext, "MJR")
		})

	// --- right cursor with a one-row lookahead. Each pulled batch is parsed once into a
	// fresh []mergeJoinStreamRow (key + partition key); peek returns a pointer into it,
	// stable across further pulls (the slice stays alive while held/queues reference it),
	// so consume advances within/across batches. ---
	var parsed []mergeJoinStreamRow
	bi := 0
	chClosed := false
	var partBuf []byte
	peek := func() *mergeJoinStreamRow {
		for bi >= len(parsed) {
			if chClosed {
				return nil
			}
			b, ok := bc.NextBatch()
			if !ok {
				chClosed = true
				return nil
			}
			p := make([]mergeJoinStreamRow, len(b))
			for i, vals := range b {
				k, _ := mergeParseKey(vals, rightKeyIdx)
				p[i] = mergeJoinStreamRow{key: k, vals: vals}
				if len(leftParts) > 0 {
					partBuf = mergeJoinPartKey(partBuf[:0], vals, mergeRightParts(o))
					p[i].part = string(partBuf)
				}
			}
			parsed, bi = p, 0
		}
		return &parsed[bi]
	}
	consume := func() { bi++ }

	// held: nearest-preceding row per partition ("" when unpartitioned). Bounded by the
	// number of distinct partitions, not by rows.
	held := map[string]*mergeJoinStreamRow{}
	partitioned := len(leftParts) > 0

	// pendByPart: SOFT following-partitioned only -- a per-partition FIFO of ascending
	// right rows within the current tolerance window, so a partition's follow can be found
	// even when other partitions' rows sit between it and the left row. Bounded to rows in
	// [left key, left key + tolerance] (the advance stops past k+Δt, and every queue is
	// pruned of keys < k as the monotone left advances).
	pendByPart := map[string][]*mergeJoinStreamRow{}

	valsJoin := make(base.Vals, 0, lenLabelsA+lenLabelsB)
	emit := func(leftVals base.Vals, right *mergeJoinStreamRow) {
		valsJoin = valsJoin[:0]
		valsJoin = append(valsJoin, leftVals...)
		if right != nil {
			valsJoin = append(valsJoin, right.vals...)
		} else {
			for i := 0; i < lenLabelsB; i++ {
				valsJoin = append(valsJoin, base.ValMissing)
			}
		}
		yieldVals(valsJoin)
	}

	var execErr error
	var partScratch []byte
	var lastLeftKey int64
	leftEmitted := false

	leftYield := func(leftVals base.Vals) {
		if execErr != nil {
			return
		}
		k, ok := mergeParseKey(leftVals, leftKeyIdx)
		if !ok {
			vars.Ctx.MergeStats.AddNoKeySkipped(1) // keyless probe row -- can't correlate.
			return
		}
		if leftEmitted && k < lastLeftKey {
			execErr = mergeJoinOrderErr("left", k, lastLeftKey)
			return
		}
		lastLeftKey, leftEmitted = k, true

		pk := ""
		if partitioned {
			partScratch = mergeJoinPartKey(partScratch[:0], leftVals, leftParts)
			pk = string(partScratch)
		}

		if following && partitioned {
			// SOFT following-partitioned: enqueue right rows within the tolerance window
			// [k, k+tolerance] into their partitions' queues, then the front of pk's queue
			// (after dropping keys < k) is the nearest follow within Δt.
			limit := k + tolerance
			for {
				r := peek()
				if r == nil || r.key > limit {
					break
				}
				pendByPart[r.part] = append(pendByPart[r.part], r)
				consume()
			}
			for p, q := range pendByPart { // prune keys now behind the (monotone) left.
				i := 0
				for i < len(q) && q[i].key < k {
					i++
				}
				if i > 0 {
					pendByPart[p] = q[i:]
				}
			}
			if q := pendByPart[pk]; len(q) > 0 { // front is in [k, k+tolerance] by construction.
				emit(leftVals, q[0])
			} else if leftOuter {
				emit(leftVals, nil)
			}
			return
		}

		if following {
			// Unpartitioned: advance past right rows strictly below k; the first >= k is the
			// nearest following (kept as `pending` -- one right row can follow several lefts).
			for {
				r := peek()
				if r == nil || r.key >= k {
					break
				}
				consume()
			}
			if r := peek(); r != nil && (asof != "soft" || r.key-k <= tolerance) {
				emit(leftVals, r)
			} else if leftOuter {
				emit(leftVals, nil)
			}
			return
		}

		// Preceding: absorb every right row with key <= k as its partition's held row;
		// the first key > k stays as the lookahead.
		for {
			r := peek()
			if r == nil || r.key > k {
				break
			}
			held[r.part] = r
			consume()
		}
		if h := held[pk]; h != nil && (asof != "soft" || k-h.key <= tolerance) {
			emit(leftVals, h)
		} else if leftOuter {
			emit(leftVals, nil)
		}
	}

	leftErr := func(err error) {
		if err != nil && execErr == nil {
			execErr = err
		}
	}

	ExecOp(o.Children[0], vars, leftYield, leftErr, pathNext, "MJL")

	// Left drained: Stop the producer (it drops any unconsumed tail) and Wait for it to
	// exit so a build-side error surfaces and no goroutine leaks.
	bc.Stop()
	if e := bc.Wait(); e != nil && execErr == nil {
		execErr = e
	}
	yieldErr(execErr)
}

// mergeRightParts returns the right partition-key indices from the op params (Params[6]),
// parallel to the left's -- used by the streaming producer to key each build row.
func mergeRightParts(o *base.Op) []int64 {
	return mergeInt64Slice(o.Params, 6)
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
	rows []base.Vals // resident row payloads; nil once spilled.
	keys []int64
	part []string

	// When the resident rows exceed MergeJoinBuildSpillBytes the payloads move to a spill
	// heap: getRow decodes row j from it on access, closeSpill frees it. keys[]/part[] stay
	// resident as the index. Closures (not a *store.Heap field) so this verbatim-copied op
	// never NAMES rhmap/store -- the gen-compiler strips that import from intermed.
	getRow     func(int) (base.Vals, error)
	closeSpill func()
}

// rowAt returns build row j, resident or decoded from the spill heap.
func (b *mergeJoinSide) rowAt(j int) (base.Vals, error) {
	if b.getRow != nil {
		return b.getRow(j)
	}
	return b.rows[j], nil
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

	// Spill state, all func-typed so no *store.Heap is NAMED in this verbatim-copied op
	// (the gen-compiler strips rhmap/store from intermed). spillPush is nil until the
	// resident row payloads cross the budget, at which point startSpill flushes them to a
	// heap and every later row goes straight to it; getRow/closeSpill drive rowAt.
	var accum, buildBytes int64
	var spillPush func(base.Vals) error
	startSpill := func() error {
		if vars.Ctx == nil || vars.Ctx.AllocHeap == nil {
			return nil // no heap allocator (e.g. a bare test Vars): stay resident.
		}
		h, err := vars.Ctx.AllocHeap() // h inferred *store.Heap -- type name never written.
		if err != nil {
			return err
		}
		var encBuf []byte
		spillPush = func(r base.Vals) error {
			encBuf = base.ValsEncode(r, encBuf[:0])
			return h.PushBytes(encBuf)
		}
		var decVals base.Vals
		side.getRow = func(j int) (base.Vals, error) {
			b, e := h.Get(int64(j))
			if e != nil {
				return nil, e
			}
			decVals = base.ValsDecode(b, decVals[:0]) // b stays valid post-build (heap frozen).
			return decVals, nil
		}
		side.closeSpill = func() { h.Close() }
		return nil
	}

	rightYield := func(vals base.Vals) {
		if buildErr != nil {
			return
		}

		k, ok := mergeParseKey(vals, rightKeyIdx)
		if !ok {
			vars.Ctx.MergeStats.AddNoKeySkipped(1) // keyless build row (banner / multiline) -- can't match, skip.
			return
		}
		if seen && k < lastKey {
			buildErr = mergeJoinOrderErr("right", k, lastKey)
			return
		}
		lastKey = k
		seen = true

		for _, v := range vals {
			buildBytes += int64(len(v)) // telemetry: every row, resident or spilled.
		}

		if spillPush != nil {
			// Already spilling: encode straight into the heap (ValsEncode copies).
			if e := spillPush(vals); e != nil {
				buildErr = e
				return
			}
		} else {
			side.rows = append(side.rows, mergeCopyVals(vals))
			for _, v := range vals {
				accum += int64(len(v))
			}
			if MergeJoinBuildSpillBytes > 0 && accum > MergeJoinBuildSpillBytes {
				// Cross the budget: flush the resident payloads into the heap, then
				// serve rows from it and drop the resident slice.
				if e := startSpill(); e != nil {
					buildErr = e
					return
				}
				if spillPush != nil { // startSpill succeeded (an allocator was present).
					for _, r := range side.rows {
						if e := spillPush(r); e != nil {
							buildErr = e
							return
						}
					}
					side.rows = nil
				}
			}
		}

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

	// Telemetry (base.MergeStats, race-safe & per-request): one merge-join build.
	if vars.Ctx != nil {
		vars.Ctx.MergeStats.RecordBuild(int64(len(side.keys)), buildBytes, side.getRow != nil)
	}

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
