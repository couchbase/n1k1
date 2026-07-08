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
//   - watermarked-near -- DEFERRED in this slice (see MergeScanExec). A
//     child declared "near" or "none" that would need the reorder buffer
//     raises a clear "not yet implemented" error rather than silently
//     producing a mis-ordered stream.
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
//
// The min/max zone maps are what let the "auto" regime PROVE disjointness
// without opening a single child (DESIGN-merging.md §1(a)).
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

	regime = mergeChooseRegime(len(o.Children), regime, minKeys, maxKeys)

	// The watermarked-near regime is deferred in this slice: a source that is
	// not strictly ordered would violate the heap invariant, so reject it with
	// a clear error rather than emit an out-of-order stream.
	if regime == "heap" {
		for i, s := range sortedness {
			if s == "near" || s == "none" {
				yieldErr(fmt.Errorf("merge-scan: child %d sortedness %q needs"+
					" the watermarked-near regime, not yet implemented", i, s))
				return
			}
		}
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
func mergeParseKey(vals base.Vals, keyIdx int) (int64, bool) {
	if keyIdx < 0 || keyIdx >= len(vals) {
		return 0, false
	}

	v := vals[keyIdx]
	if len(v) == 0 {
		return 0, false
	}

	n, err := strconv.ParseInt(string(v), 10, 64)
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
