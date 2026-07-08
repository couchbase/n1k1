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
	"strconv"

	"github.com/buger/jsonparser"

	"github.com/couchbase/n1k1/base"
)

// OpMergeScan is the K-way sorted merge SCAN op described in
// DESIGN-merging.md, §1 "The K-way sorted merge SCAN op". It presents N
// sorted child sources as ONE globally-ordered output stream, ordered by a
// normalized int64 epoch-nanos sort key that the extract layer has already
// placed into each row as a labeled register (this op only COMPARES the
// int64; it never re-parses a timestamp string).
//
// Regimes (DESIGN-merging.md §1 "Three regimes"):
//
//   - concatenate -- when the children's key ranges are disjoint and ordered
//     (max_key(childᵢ) <= min_key(childᵢ₊₁)), the merged order is just the
//     children read back-to-back. No heap, no key comparisons, no buffering:
//     O(N), fully streaming (each child's rows are forwarded straight to the
//     downstream yield). This is the dated-log-rotation common case.
//
//   - heap -- otherwise, a classic K-way strict min-heap merge on the sort
//     key: pop the smallest head, yield it, advance that cursor, re-push its
//     new head. O(N log K).
//
//   - watermarked-near -- when a child is declared "near" (bounded disorder)
//     the strict-heap invariant no longer holds: a child's head key is not a
//     lower bound on its remaining keys (a record up to disorder_bound out of
//     order may still arrive). A bounded, REUSED reorder buffer (a small
//     min-heap over keys) holds each row until the WATERMARK -- min over live
//     cursors(frontier_key) - max disorder_bound -- passes its key, at which
//     point nothing smaller can still arrive and the row is safe to emit in
//     order (the Flink/Dataflow watermark model, applied to a bounded offline
//     stream; DESIGN-merging.md §1c). The op VALIDATES the declared bound: a
//     record arriving below a watermark we have already emitted past means the
//     bound was too small, and rather than silently mis-order the op either
//     errors or widens+warns per the late-record policy Param.
//
// The op reads only SCALAR, codegen-friendly choices from o.Params (see
// MergeScanExec for the layout) plus the K child ops in o.Children; glue will
// later derive those params from the SortedSourceMeta contract, but at this
// engine layer the op stays free of any records/ import.
//
// This op is interpreter-oriented for now: OpMergeScan delegates via a single
// "// !lz" line to MergeScanExec, whose body is deliberately free of any
// "lz" tokens so the gen-compiler copies it VERBATIM (it compiles cleanly in
// the generated intermed package but is never dispatched there, since the
// SQL/conv recognition that would emit a "merge-scan" op is a later slice).
func OpMergeScan(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	MergeScanExec(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz
}

// MergeScanExec holds the actual (non-lazy) K-way merge logic. Its params are
// intentionally named WITHOUT the "lz" prefix so every line is copied verbatim
// by the gen-compiler.
//
// Params layout (all scalar / codegen-friendly, all optional past index 0):
//
//	Params[0] int             -- keyIdx: positional index of the int64 sort-key
//	                             register within each row's Vals/Labels.
//	Params[1] string          -- regime: "concatenate" | "heap" | "auto"
//	                             (default "auto": prove disjointness from the
//	                             zone maps, else fall back to "heap").
//	Params[2] []interface{}   -- per-child sortedness: "strict" | "near" |
//	                             "none" (absent => assumed "strict").
//	Params[3] []interface{}   -- per-child min_key (int64 zone map).
//	Params[4] []interface{}   -- per-child max_key (int64 zone map).
//	Params[5] []interface{}   -- per-child disorder_bound window-nanos (int64):
//	                             how far behind an already-seen key a "near"
//	                             child's row may still arrive. Absent / 0 => the
//	                             child is treated as strict (no reorder needed).
//	Params[6] string          -- late-record policy / strictness knob:
//	                             "error" (default, the correctness-critical safe
//	                             default): a record below an already-emitted
//	                             watermark FAILS the query. "widen": widen the
//	                             effective bound, emit a Warn, and keep going
//	                             (best-effort, for exploratory use) -- never
//	                             silent (DESIGN-merging.md §1 soft options).
//
// The min/max zone maps are what let the "auto" regime PROVE disjointness
// without opening a single child (DESIGN-merging.md §1(a)). A child declared
// "near"/"none" forces the "heap" regime up into "watermarked-near".
func MergeScanExec(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	keyIdx := 0
	if len(o.Params) > 0 {
		if v, ok := o.Params[0].(int); ok {
			keyIdx = v
		}
	}

	regime := "auto"
	if len(o.Params) > 1 {
		if v, ok := o.Params[1].(string); ok && v != "" {
			regime = v
		}
	}

	sortedness := mergeStringSlice(o.Params, 2)
	minKeys := mergeInt64Slice(o.Params, 3)
	maxKeys := mergeInt64Slice(o.Params, 4)
	bounds := mergeInt64Slice(o.Params, 5)

	policy := "error"
	if len(o.Params) > 6 {
		if v, ok := o.Params[6].(string); ok && v != "" {
			policy = v
		}
	}

	regime = mergeChooseRegime(len(o.Children), regime, minKeys, maxKeys)

	// A child declared "near"/"none" violates the strict-heap invariant, so the
	// merge must run the watermarked-near reorder buffer rather than the strict
	// heap -- otherwise it could emit a row before a smaller-keyed row that has
	// not surfaced yet. (Concatenate stays valid: disjoint ordered ranges never
	// interleave, so within-child disorder never crosses a child boundary.)
	if regime == "heap" && mergeHasNear(sortedness) {
		maxBound := mergeMaxBound(sortedness, bounds)
		mergeScanWatermarked(o, vars, yieldVals, yieldErr, keyIdx, pathNext,
			maxBound, policy)
		return
	}

	if regime == "concatenate" {
		mergeScanConcatenate(o, vars, yieldVals, yieldErr, keyIdx, pathNext)
		return
	}

	mergeScanHeap(o, vars, yieldVals, yieldErr, keyIdx, pathNext)
}

// mergeScanConcatenate streams the children back-to-back in their given order.
// It performs NO key comparisons for ordering (the caller proved the ranges
// disjoint and ordered), but it DOES cheaply validate output monotonicity --
// one int64 compare per row -- as the tripwire DESIGN-merging.md §1 calls for:
// a wrong disjointness/zone-map claim is a silent-corruption bug, so the op is
// paranoid about it. Rows are forwarded straight through (the downstream copies
// per the YieldVals borrow contract), so this path buffers nothing.
func mergeScanConcatenate(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, keyIdx int, pathNext string) {
	var lastKey int64
	emitted := false

	var execErr error

	for i, child := range o.Children {
		_ = child

		childYield := func(vals base.Vals) {
			if execErr != nil {
				return
			}

			k, ok := mergeParseKey(vals, keyIdx)
			if !ok {
				execErr = fmt.Errorf("merge-scan: child %d has a missing or"+
					" non-int64 sort key at index %d", i, keyIdx)
				return
			}

			if emitted && k < lastKey {
				execErr = mergeOutOfOrderErr(i, k, lastKey)
				return
			}

			yieldVals(vals)

			lastKey = k
			emitted = true
		}

		childErr := func(err error) {
			if err != nil && execErr == nil {
				execErr = err
			}
		}

		ExecOp(o.Children[i], vars, childYield, childErr, pathNext, strconv.Itoa(i))

		if execErr != nil {
			break
		}
	}

	yieldErr(execErr)
}

// mergeScanHeap runs the classic K-way strict min-heap merge. Because n1k1's
// push ops run to completion (a child scan cannot be paused mid-yield without
// the actor-per-cursor machinery deferred to a later slice), each child is
// first drained into an in-memory MergeCursor of deep-copied rows; the cursors
// are then co-advanced through a K-entry binary min-heap keyed on the head
// sort key. State is O(total rows) here only because materialization stands in
// for resumable cursors; the heap itself is O(K). Output monotonicity is
// validated per row (cheap int64 compare), catching a source that lied about
// being strictly ordered.
func mergeScanHeap(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, keyIdx int, pathNext string) {
	cursors := make([]*MergeCursor, 0, len(o.Children))

	var execErr error

	for i := range o.Children {
		cursor := &MergeCursor{}

		childYield := func(vals base.Vals) {
			if execErr != nil {
				return
			}

			k, ok := mergeParseKey(vals, keyIdx)
			if !ok {
				execErr = fmt.Errorf("merge-scan: child %d has a missing or"+
					" non-int64 sort key at index %d", i, keyIdx)
				return
			}

			cursor.rows = append(cursor.rows, mergeCopyVals(vals))
			cursor.keys = append(cursor.keys, k)
		}

		childErr := func(err error) {
			if err != nil && execErr == nil {
				execErr = err
			}
		}

		ExecOp(o.Children[i], vars, childYield, childErr, pathNext, strconv.Itoa(i))

		if execErr != nil {
			yieldErr(execErr)
			return
		}

		cursors = append(cursors, cursor)
	}

	frontier := &MergeHeap{cursors: cursors}
	for ci, cursor := range cursors {
		if cursor.Valid() {
			frontier.Push(ci)
		}
	}

	var lastKey int64
	emitted := false

	for frontier.Len() > 0 {
		ci := frontier.Pop()
		cursor := cursors[ci]

		k := cursor.HeadKey()
		if emitted && k < lastKey {
			yieldErr(mergeOutOfOrderErr(ci, k, lastKey))
			return
		}

		yieldVals(cursor.rows[cursor.pos])

		lastKey = k
		emitted = true

		cursor.pos++
		if cursor.Valid() {
			frontier.Push(ci)
		}
	}

	yieldErr(nil)
}

// mergeScanWatermarked runs the watermarked-near K-way merge (DESIGN-merging.md
// §1c). Near sources are near-sorted: within a child a row may arrive up to
// maxBound nanos behind an already-seen key, so a child's head key is NOT a
// lower bound on its remaining keys and the strict heap could emit too early.
//
// The fix is a WATERMARK plus a bounded, REUSED reorder buffer:
//
//   - A K-entry cursor min-heap (MergeHeap) pops rows in near-sorted order --
//     the frontier is min over live cursors(head_key) (the cursor-heap top).
//   - watermark = frontier - maxBound. A buffered row is safe to emit once its
//     key <= watermark, because no live child can still produce a smaller key
//     (each child's future keys are >= its head_key - its_bound >= watermark).
//   - Safe rows drain out of the reorder buffer (a min-heap over keys) in key
//     order. When all cursors exhaust, the frontier is +inf and the buffer
//     flushes entirely.
//
// Zero-alloc steady state: the reorder buffer is a struct-of-arrays min-heap
// whose backing slices grow ONCE to the (bounded) live-buffer size and are then
// reused across every row (pop shrinks with [:n], push reuses the freed slot);
// it stores references to rows already deep-copied at drain time, so no per-row
// allocation happens in the merge loop. Keys are parsed with the zero-alloc
// mergeParseKey. The materialize-at-drain stands in for resumable cursors, same
// as mergeScanHeap.
//
// Bound validation (DESIGN-merging.md §1 soft options): a row entering the
// buffer with a key below a watermark we have ALREADY emitted past means the
// declared bound was too small. Under policy "error" (default) the query fails
// with a precise message; under "widen" the op widens the effective bound,
// emits a Warn, and continues best-effort -- never a silent mis-order. As a
// final backstop the op also validates its own OUTPUT monotonicity per row.
func mergeScanWatermarked(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, keyIdx int, pathNext string,
	maxBound int64, policy string) {
	cursors := make([]*MergeCursor, 0, len(o.Children))

	var execErr error

	for i := range o.Children {
		cursor := &MergeCursor{}

		childYield := func(vals base.Vals) {
			if execErr != nil {
				return
			}

			k, ok := mergeParseKey(vals, keyIdx)
			if !ok {
				execErr = fmt.Errorf("merge-scan: child %d has a missing or"+
					" non-int64 sort key at index %d", i, keyIdx)
				return
			}

			cursor.rows = append(cursor.rows, mergeCopyVals(vals))
			cursor.keys = append(cursor.keys, k)
		}

		childErr := func(err error) {
			if err != nil && execErr == nil {
				execErr = err
			}
		}

		ExecOp(o.Children[i], vars, childYield, childErr, pathNext, strconv.Itoa(i))

		if execErr != nil {
			yieldErr(execErr)
			return
		}

		cursors = append(cursors, cursor)
	}

	frontier := &MergeHeap{cursors: cursors}
	for ci, cursor := range cursors {
		if cursor.Valid() {
			frontier.Push(ci)
		}
	}

	var buf MergeReorderBuffer

	strict := policy != "widen" // "error" (default) is strict; "widen" tolerates.
	effBound := maxBound        // "widen" grows this so a late row can't recur.

	var lastKey int64
	emitted := false

	// emitSafe pops and yields every buffered row whose key <= watermark, in key
	// order, validating output monotonicity as the final tripwire.
	emitSafe := func(watermark int64) bool {
		for buf.Len() > 0 && buf.MinKey() <= watermark {
			k, row := buf.PopMin()

			if emitted && k < lastKey {
				// Should be unreachable given the insert-time check below, but the
				// output tripwire stays paranoid (DESIGN-merging.md: a wrong bound
				// is silent corruption -- never pass an out-of-order stream on).
				if strict {
					yieldErr(mergeOutOfOrderErr(-1, k, lastKey))
					return false
				}
				mergeWarnLate(vars, -1, k, lastKey)
			}

			yieldVals(row)
			lastKey = k
			emitted = true
		}
		return true
	}

	for frontier.Len() > 0 {
		ci := frontier.Pop()
		cursor := cursors[ci]

		k := cursor.HeadKey()
		row := cursor.rows[cursor.pos]
		cursor.pos++
		if cursor.Valid() {
			frontier.Push(ci)
		}

		// Bound validation at ingestion: a row below what we have already emitted
		// past means the declared disorder_bound was too small.
		if emitted && k < lastKey {
			if strict {
				yieldErr(mergeBoundViolationErr(ci, k, lastKey))
				return
			}
			// widen: grow the effective bound past the shortfall so subsequent
			// watermarks are conservative enough to avoid recurrence, and warn.
			if grow := lastKey - k; grow > effBound {
				effBound = grow
			}
			mergeWarnLate(vars, ci, k, lastKey)
		}

		buf.Push(k, row)

		// frontier_key = min over live cursors(head_key) = the cursor-heap top.
		watermark := mergeWatermark(frontier, cursors, effBound)

		if !emitSafe(watermark) {
			return
		}
	}

	// All cursors exhausted: nothing smaller can arrive, so flush the buffer.
	if !emitSafe(mergeMaxInt64) {
		return
	}

	yieldErr(nil)
}

// mergeWatermark returns min over live cursors(head_key) - effBound, i.e. the
// key below which no live child can still produce a row (so buffered rows at or
// below it are safe to emit). With no live cursors the watermark is +inf (flush
// everything). Guards against int64 underflow on the subtraction.
func mergeWatermark(frontier *MergeHeap, cursors []*MergeCursor, effBound int64) int64 {
	if frontier.Len() == 0 {
		return mergeMaxInt64
	}
	fk := cursors[frontier.Peek()].HeadKey()
	wm := fk - effBound
	if effBound > 0 && wm > fk { // underflowed past MinInt64
		return mergeMinInt64
	}
	return wm
}

const (
	mergeMaxInt64 = int64(^uint64(0) >> 1)
	mergeMinInt64 = -mergeMaxInt64 - 1
)

// -----------------------------------------------------

// MergeReorderBuffer is the bounded, REUSED reorder buffer for the
// watermarked-near regime (DESIGN-merging.md §1c). It is a struct-of-arrays
// binary min-heap over (key, row): parallel keys[]/rows[] slices grow ONCE to
// the (bounded) live-buffer size, then are reused for every subsequent row (Push
// reuses a freed tail slot via append's retained capacity; PopMin shrinks with
// [:n]). The rows are references to Vals already deep-copied at drain time, so
// the buffer itself allocates nothing per row in steady state. Hand-rolled (like
// MergeHeap) to avoid container/heap's per-op interface boxing.
type MergeReorderBuffer struct {
	keys []int64
	rows []base.Vals
}

// Len returns the number of buffered rows.
func (b *MergeReorderBuffer) Len() int { return len(b.keys) }

// MinKey returns the smallest buffered key (the heap root).
func (b *MergeReorderBuffer) MinKey() int64 { return b.keys[0] }

// Push inserts a (key, row) pair and restores the min-heap order.
func (b *MergeReorderBuffer) Push(key int64, row base.Vals) {
	b.keys = append(b.keys, key)
	b.rows = append(b.rows, row)
	i := len(b.keys) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if b.keys[parent] <= b.keys[i] {
			break
		}
		b.swap(i, parent)
		i = parent
	}
}

// PopMin removes and returns the (key, row) with the smallest key.
func (b *MergeReorderBuffer) PopMin() (int64, base.Vals) {
	k, row := b.keys[0], b.rows[0]
	n := len(b.keys) - 1
	b.swap(0, n)
	b.rows[n] = nil // drop the row reference so it can be GC'd once emitted
	b.keys = b.keys[:n]
	b.rows = b.rows[:n]
	b.down(0)
	return k, row
}

func (b *MergeReorderBuffer) swap(i, j int) {
	b.keys[i], b.keys[j] = b.keys[j], b.keys[i]
	b.rows[i], b.rows[j] = b.rows[j], b.rows[i]
}

func (b *MergeReorderBuffer) down(i int) {
	n := len(b.keys)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		child := left
		if right := left + 1; right < n && b.keys[right] < b.keys[left] {
			child = right
		}
		if b.keys[i] <= b.keys[child] {
			break
		}
		b.swap(i, child)
		i = child
	}
}

// -----------------------------------------------------

// MergeCursor is one child source drained into memory: parallel rows/keys
// slices plus the current read position. It stands in for the resumable,
// buffer-reusing cursor the streaming design (DESIGN-merging.md §1 cursors)
// will introduce with the actor-per-cursor slice.
type MergeCursor struct {
	rows []base.Vals
	keys []int64
	pos  int
}

// Valid reports whether the cursor still has an unread head row.
func (c *MergeCursor) Valid() bool { return c.pos < len(c.rows) }

// HeadKey returns the sort key of the cursor's current head row.
func (c *MergeCursor) HeadKey() int64 { return c.keys[c.pos] }

// -----------------------------------------------------

// MergeHeap is a tiny binary min-heap over child-cursor indices, ordered by
// each cursor's current head key. It holds at most K entries (one per live
// cursor) and never spills -- only the row bytes are large, and those live in
// the cursors, not the heap (DESIGN-merging.md §1(b)). Hand-rolled over an
// []int to avoid the interface boxing container/heap would impose per op.
type MergeHeap struct {
	idx     []int
	cursors []*MergeCursor
}

// Len returns the number of live cursors currently in the heap.
func (h *MergeHeap) Len() int { return len(h.idx) }

// Peek returns the cursor index with the smallest head key without removing it
// (the frontier cursor). Callers must check Len() > 0 first.
func (h *MergeHeap) Peek() int { return h.idx[0] }

func (h *MergeHeap) less(a, b int) bool {
	return h.cursors[h.idx[a]].HeadKey() < h.cursors[h.idx[b]].HeadKey()
}

// Push adds a cursor index and restores the min-heap order.
func (h *MergeHeap) Push(ci int) {
	h.idx = append(h.idx, ci)
	h.up(len(h.idx) - 1)
}

// Pop removes and returns the cursor index with the smallest head key.
func (h *MergeHeap) Pop() int {
	n := len(h.idx) - 1
	h.idx[0], h.idx[n] = h.idx[n], h.idx[0]
	ci := h.idx[n]
	h.idx = h.idx[:n]
	h.down(0)
	return ci
}

func (h *MergeHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			break
		}
		h.idx[i], h.idx[parent] = h.idx[parent], h.idx[i]
		i = parent
	}
}

func (h *MergeHeap) down(i int) {
	n := len(h.idx)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		child := left
		if right := left + 1; right < n && h.less(right, left) {
			child = right
		}
		if !h.less(child, i) {
			break
		}
		h.idx[i], h.idx[child] = h.idx[child], h.idx[i]
		i = child
	}
}

// -----------------------------------------------------

// mergeChooseRegime picks the cheapest legal regime. An explicit "concatenate"
// or "heap" is honored as-is; "auto" (the default) proves disjoint, ordered
// ranges from the zone maps and picks "concatenate" when it can, else "heap".
func mergeChooseRegime(numChildren int, regime string, minKeys, maxKeys []int64) string {
	if regime == "concatenate" || regime == "heap" {
		return regime
	}

	// auto: prove max_key(childᵢ) <= min_key(childᵢ₊₁) for all i.
	if numChildren > 0 &&
		len(minKeys) == numChildren && len(maxKeys) == numChildren {
		disjoint := true
		for i := 0; i+1 < numChildren; i++ {
			if maxKeys[i] > minKeys[i+1] {
				disjoint = false
				break
			}
		}
		if disjoint {
			return "concatenate"
		}
	}

	return "heap"
}

// mergeParseKey reads the int64 epoch-nanos sort key from vals[keyIdx]. The
// extract layer normalizes the key to a bare JSON integer, so a plain
// ParseInt suffices; a missing or non-integer key returns ok=false.
//
// Hot path: this runs once per row, so it must NOT allocate. jsonparser.ParseInt
// parses the digits straight out of the []byte (no string() conversion) -- the
// same zero-garbage primitive base/arith.go uses. (Garbage in the planning /
// preparation / extract phases is fine; the merge steady state is not.)
func mergeParseKey(vals base.Vals, keyIdx int) (int64, bool) {
	if keyIdx < 0 || keyIdx >= len(vals) {
		return 0, false
	}

	v := vals[keyIdx]
	if len(v) == 0 {
		return 0, false
	}

	n, err := jsonparser.ParseInt(v)
	if err != nil {
		return 0, false
	}

	return n, true
}

// mergeCopyVals returns a deep copy of vals (fresh backing bytes), so a
// drained row survives the child scan reusing its buffers.
func mergeCopyVals(vals base.Vals) base.Vals {
	out := make(base.Vals, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		c := make(base.Val, len(v))
		copy(c, v)
		out[i] = c
	}
	return out
}

// mergeOutOfOrderErr is the monotonicity-tripwire error: an emitted key that
// is smaller than the last emitted key means a source violated its declared
// (strict / disjoint) ordering -- a silent-corruption bug the op refuses to
// pass downstream.
func mergeOutOfOrderErr(child int, key, lastKey int64) error {
	return fmt.Errorf("merge-scan: child %d yielded out-of-order key %d < %d"+
		" (source violated its declared sort order)", child, key, lastKey)
}

// mergeBoundViolationErr is the watermarked-near bound-validation error: a near
// child produced a row whose key is below a watermark we already emitted past,
// so its declared disorder_bound was too small (DESIGN-merging.md §1 soft
// options, "error" policy). Distinct message from the generic out-of-order
// tripwire so the operator can point at the bound as the cause.
func mergeBoundViolationErr(child int, key, lastKey int64) error {
	return fmt.Errorf("merge-scan: child %d violated its disorder_bound at key"+
		" %d (already emitted past %d) -- widen the bound or use the 'widen'"+
		" late-record policy", child, key, lastKey)
}

// mergeWarnLate emits the non-fatal advisory for the "widen" late-record policy
// (the same Ctx.Warn stream divide-by-zero uses). Never silent: a widened bound
// changes memory behavior, so the query must surface it.
func mergeWarnLate(vars *base.Vars, child int, key, lastKey int64) {
	if vars != nil && vars.Ctx != nil && vars.Ctx.Warn != nil {
		vars.Ctx.Warn(fmt.Sprintf("merge-scan: child %d late record key %d <"+
			" already-emitted %d; widened disorder_bound", child, key, lastKey))
	}
}

// mergeHasNear reports whether any child is declared "near" or "none" -- i.e.
// not strictly sorted, so the heap regime must escalate to watermarked-near.
func mergeHasNear(sortedness []string) bool {
	for _, s := range sortedness {
		if s == "near" || s == "none" {
			return true
		}
	}
	return false
}

// mergeMaxBound returns the largest disorder_bound (window-nanos) among the
// near/none children -- the conservative watermark lag. Using the global max
// (rather than recomputing per live-cursor set) over-approximates the lag, which
// only ever buffers MORE, never emits too early, and keeps the hot loop simple.
// A strict child contributes 0. When no per-child bound is supplied but a child
// is near, the caller still runs the watermarked path with bound 0 (which
// degrades to a strict heap that tolerates equal keys) plus the tripwire.
func mergeMaxBound(sortedness []string, bounds []int64) int64 {
	var max int64
	for i := range bounds {
		s := ""
		if i < len(sortedness) {
			s = sortedness[i]
		}
		if s == "strict" {
			continue
		}
		if bounds[i] > max {
			max = bounds[i]
		}
	}
	return max
}

// mergeStringSlice reads o.Params[i] as a []string (from a []interface{}).
func mergeStringSlice(params []interface{}, i int) []string {
	if i >= len(params) || params[i] == nil {
		return nil
	}

	raw, ok := params[i].([]interface{})
	if !ok {
		return nil
	}

	out := make([]string, len(raw))
	for j, r := range raw {
		if s, ok := r.(string); ok {
			out[j] = s
		}
	}
	return out
}

// mergeInt64Slice reads o.Params[i] as a []int64 (from a []interface{}),
// tolerating int / int64 / float64 element encodings.
func mergeInt64Slice(params []interface{}, i int) []int64 {
	if i >= len(params) || params[i] == nil {
		return nil
	}

	raw, ok := params[i].([]interface{})
	if !ok {
		return nil
	}

	out := make([]int64, len(raw))
	for j, r := range raw {
		switch n := r.(type) {
		case int64:
			out[j] = n
		case int:
			out[j] = int64(n)
		case float64:
			out[j] = int64(n)
		}
	}
	return out
}
