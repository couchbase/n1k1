//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package base

import (
	"bytes"
	"strconv"

	"github.com/couchbase/rhmap/store"
)

// Partition-level window function kinds, computed by WindowRankValue from the
// current row's position + peer group + partition count (no frame aggregate).
const (
	WRankRowNumber   = iota // ROW_NUMBER: 1-based position.
	WRankRank               // RANK: first-peer position + 1 (gaps after ties).
	WRankDenseRank          // DENSE_RANK: count of peer groups so far (no gaps).
	WRankPercentRank        // PERCENT_RANK: (rank-1)/(N-1), in [0,1].
	WRankCumeDist           // CUME_DIST: (#rows with ORDER BY value <= current)/N, in (0,1].
	WRankNtile              // NTILE(k): 1-based bucket when the partition is split into k.
)

// WTok maps a window related configuration string to an internal
// token number, which enables faster, numeric comparisons.
var WTok = map[string]int{}

var WTokRows, WTokRange, WTokGroups, WTokUnbounded, WTokNum int
var WTokCurrentRow, WTokNoOthers, WTokGroup, WTokTies int

func init() {
	for tokenNum, tokenStr := range []string{
		"rows", "range", "groups", "unbounded", "num",
		"current-row", "no-others", "group", "ties"} {
		WTok[tokenStr] = tokenNum
	}

	WTokRows, WTokRange, WTokGroups, WTokUnbounded, WTokNum =
		WTok["rows"], WTok["range"], WTok["groups"], WTok["unbounded"], WTok["num"]

	WTokCurrentRow, WTokNoOthers, WTokGroup, WTokTies =
		WTok["current-row"], WTok["no-others"], WTok["group"], WTok["ties"]
}

// -------------------------------------------------------------------

// WindowFrame represents an immutable window frame config along with
// a mutable current window frame that's associated with a window
// partition.
type WindowFrame struct {
	Type int // Ex: "rows", "range", "groups".

	BegBoundary int     // Ex: "unbounded", "num".
	BegNum      int64   // Used when beg boundary is "num".
	BegF64      float64 // Used when beg boundary is "num" for "range" type.

	EndBoundary int     // Ex: "unbounded", "num".
	EndNum      int64   // Used when end boundary is "num".
	EndF64      float64 // Used when beg boundary is "num" for "range" type.

	Exclude int // Ex: "current-row", "no-others", "group", "ties".

	// ValIdx is used when type is "range" or "groups" and is the
	// index of the val that's used for comparisons. When type is
	// "groups", the ValIdx should refer to a rank or denseRank
	// val. When type is "range", the ValIdx refers to the val that
	// helps defines the range from val-BegF64 to val+EndF64.
	ValIdx int

	// ValComparer is used when the type is "range".
	ValComparer *ValComparer

	// --------------------------------------------------------

	// Partition is the current window partition.
	Partition *store.Heap

	// WindowFrameCurr tracks the current window frame, which is
	// updated as the caller steps through the window partition.
	WindowFrameCurr

	// TempVals helps avoid memory allocations.
	TempVals Vals

	// Ranking state (ROW_NUMBER / RANK / DENSE_RANK), maintained by StepRanking and
	// reset at each partition start (Pos == 0).
	rankVal       uint64
	denseRankVal  uint64
	rankPrevOrder []byte
	rankStarted   bool
}

// WindowRankValue computes the partition-level window function of the given kind for
// the current row (wf.Pos, 0-based) and returns it formatted as a JSON Val (appended
// into buf, which is returned for reuse). It maintains peer-group state internally,
// self-resetting at each partition start (Pos == 0). Peer detection compares the
// ValIdx column (the ORDER BY value/tuple); ROW_NUMBER and NTILE don't need it. Kept
// as a base method (not inline field access) so the gen-compiler treats it as runtime
// -- see the codegen note in op_window.go.
func (wf *WindowFrame) WindowRankValue(kind int, ntileN int64, buf []byte) (Val, []byte) {
	if wf.Pos == 0 {
		wf.rankVal, wf.denseRankVal, wf.rankStarted = 0, 0, false
		wf.rankPrevOrder = wf.rankPrevOrder[:0]
	}

	// Bump rank/denseRank at each new peer group (rank = first-peer position + 1,
	// denseRank = group count). Harmless for kinds that ignore them (ROW_NUMBER/NTILE).
	var curOrder Val
	if v, err := wf.GetValsVal(wf.Pos, wf.ValIdx); err == nil {
		curOrder = v
	}
	if !wf.rankStarted || !bytes.Equal(curOrder, wf.rankPrevOrder) {
		wf.rankVal = uint64(wf.Pos) + 1
		wf.denseRankVal++
		wf.rankPrevOrder = append(wf.rankPrevOrder[:0], curOrder...)
		wf.rankStarted = true
	}

	n := int64(wf.Partition.Len())

	switch kind {
	case WRankRank:
		buf = strconv.AppendUint(buf, wf.rankVal, 10)
	case WRankDenseRank:
		buf = strconv.AppendUint(buf, wf.denseRankVal, 10)
	case WRankPercentRank:
		pr := 0.0
		if n > 1 {
			pr = float64(wf.rankVal-1) / float64(n-1)
		}
		buf = strconv.AppendFloat(buf, pr, 'g', -1, 64)
	case WRankCumeDist:
		// (#rows with ORDER BY value <= current) / N = (last-peer position + 1) / N.
		// Walk forward while the stored ValIdx column equals the current one (peers).
		// This is a direct bytes.Equal on the ORDER BY value/tuple -- independent of the
		// frame type, so it works whether the column is a single numeric value or a
		// canonical multi-column tuple (composite ORDER BY). FindGroupEdge can't serve
		// here: it does ParseFloat64 for any non-GROUPS frame, which fails on a tuple.
		cd := 1.0
		if n > 0 {
			last := wf.Pos
			for i := wf.Pos + 1; i < n; i++ {
				v, gerr := wf.GetValsVal(i, wf.ValIdx)
				if gerr != nil || !bytes.Equal(v, curOrder) {
					break
				}
				last = i
			}
			cd = float64(last+1) / float64(n)
		}
		buf = strconv.AppendFloat(buf, cd, 'g', -1, 64)
	case WRankNtile:
		buf = strconv.AppendUint(buf, ntileBucket(wf.Pos, n, ntileN), 10)
	default: // WRankRowNumber
		buf = strconv.AppendUint(buf, uint64(wf.Pos)+1, 10)
	}

	return Val(buf), buf
}

// ntileBucket returns the 1-based bucket for row pos in a partition of n rows split
// into tiles buckets. The first (n % tiles) buckets get one extra row. When n <
// tiles, each row is its own bucket (later buckets are empty) -- and base+1 == 1, so
// there's no divide-by-zero.
func ntileBucket(pos, n, tiles int64) uint64 {
	if tiles < 1 {
		tiles = 1
	}
	if n <= 0 {
		return 1
	}
	base := n / tiles
	rem := n % tiles
	big := rem * (base + 1) // positions covered by the (base+1)-sized buckets.
	if pos < big {
		return uint64(pos/(base+1)) + 1
	}
	return uint64(rem+(pos-big)/base) + 1
}

// -------------------------------------------------------------------

// WindowFrameCurr represents the current positions of entries of a
// window frame in a window partition.
type WindowFrameCurr struct {
	// Pos is mutated as the 0-based current pos is updated.
	Pos int64

	// Include is mutated as the current pos is updated.
	// Include represents the positions included in the current
	// window frame before positions are excluded.
	Include WindowSpan

	// Excludes is mutated as the current pos is updated.
	// Excludes may be empty, or might have multiple spans when
	// the exclude config is "group" or "ties".
	Excludes []WindowSpan
}

// -------------------------------------------------------------------

// WindowSpan represents a continuous range of [Beg, End) of positions
// in the current window partition. Beg >= End means an empty span.
type WindowSpan struct {
	Beg, End int64
}

// -------------------------------------------------------------------

func (wf *WindowFrame) Init(cfg interface{}, partition *store.Heap) {
	// Default window frame cfg according to standard is...
	// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS
	parts := cfg.([]interface{})

	wf.Type = WTok[parts[0].(string)]

	wf.BegBoundary = WTok[parts[1].(string)]
	wf.EndBoundary = WTok[parts[3].(string)]

	begNum, ok := parts[2].(int)
	if ok {
		wf.BegNum = int64(begNum)
		wf.BegF64 = float64(begNum)
	} else {
		wf.BegF64, ok = parts[2].(float64)
		if ok {
			wf.BegNum = int64(wf.BegF64)
		}
	}

	endNum, ok := parts[4].(int)
	if ok {
		wf.EndNum = int64(endNum)
		wf.EndF64 = float64(endNum)
	} else {
		wf.EndF64, ok = parts[4].(float64)
		if ok {
			wf.EndNum = int64(wf.EndF64)
		}
	}

	wf.Exclude = WTok[parts[5].(string)]

	wf.Partition = partition

	wf.ValIdx = parts[6].(int)
}

// -------------------------------------------------------------------

// PartitionStart is invoked whenever a new window partition has been
// seen -- which means reseting the current window frame.
func (wf *WindowFrameCurr) PartitionStart() {
	wf.Pos = -1
	wf.Include = WindowSpan{}
	wf.Excludes = wf.Excludes[:0]
}

// -------------------------------------------------------------------

// CurrentUpdate is invoked whenever the current row is updated and
// stepped to the next row, so we update the current window frame.
func (wf *WindowFrame) CurrentUpdate(currentPos uint64) (err error) {
	wf.Pos = int64(currentPos)

	// Default to unbounded preceding.
	wf.Include.Beg = 0

	if wf.BegBoundary == WTokNum {
		// Handle cases of current-row and expr preceding|following.
		if wf.Type == WTokRows {
			wf.Include.Beg = wf.Pos + wf.BegNum
		} else if wf.Type == WTokGroups {
			var steppedAll bool
			wf.Include.Beg, steppedAll, err = wf.StepGroups(wf.BegNum)
			if err != nil {
				return err
			}
			if !steppedAll && wf.BegNum > 0 {
				// BEG = n FOLLOWING stepped past the last group: the frame starts
				// beyond the partition, so it's empty. Force Beg >= End (End is at most
				// the partition length, set below).
				wf.Include.Beg = int64(wf.Partition.Len())
			}
		} else { // wf.Type == WTokRange.
			// TODO: Assumes ASC order-by.
			wf.Include.Beg, err = wf.FindGroupEdge(wf.Pos, -1, true)
			if err != nil {
				return err
			}
		}

		if wf.Include.Beg < 0 {
			wf.Include.Beg = 0
		}
	}

	// Default to unbounded following.
	n := int64(wf.Partition.Len())

	wf.Include.End = n

	if wf.EndBoundary == WTokNum {
		// Handle cases of current-row and expr preceding|following.
		if wf.Type == WTokRows {
			wf.Include.End = wf.Pos + wf.EndNum
		} else if wf.Type == WTokGroups {
			var steppedAll bool
			wf.Include.End, steppedAll, err = wf.StepGroups(wf.EndNum)
			if err != nil {
				return err
			}
			if !steppedAll && wf.EndNum < 0 {
				// END = n PRECEDING stepped past the first group: the frame ends before
				// row 0, so it's empty. Set End = -1 (-> 0 after the +1 below), which is
				// <= any Beg >= 0.
				wf.Include.End = -1
			}
		} else { // wf.Type == WTokRange.
			// TODO: Assumes ASC order-by.
			wf.Include.End, err = wf.FindGroupEdge(wf.Pos, 1, true)
			if err != nil {
				return err
			}
		}

		// Since the range is [Beg, End), bump the end by 1.
		wf.Include.End = wf.Include.End + 1

		if wf.Include.End > n {
			wf.Include.End = n
		}
	}

	// Default to excluded rows of no-others.
	wf.Excludes = wf.Excludes[:0]

	if wf.Exclude == WTokCurrentRow {
		wf.Excludes = append(wf.Excludes, WindowSpan{wf.Pos, wf.Pos + 1})
	} else if wf.Exclude == WTokGroup || wf.Exclude == WTokTies {
		eBeg, err := wf.FindGroupEdge(wf.Pos, -1, false)
		if err != nil {
			return err
		}

		eEnd, err := wf.FindGroupEdge(wf.Pos, 1, false)
		if err != nil {
			return err
		}

		if wf.Exclude == WTokGroup {
			wf.Excludes = append(wf.Excludes, WindowSpan{eBeg, eEnd + 1})
		} else { // wf.Exclude == WTokTies.
			wf.Excludes = append(wf.Excludes, WindowSpan{eBeg, wf.Pos})
			wf.Excludes = append(wf.Excludes, WindowSpan{wf.Pos + 1, eEnd + 1})
		}
	}

	return nil
}

// -------------------------------------------------------------------

// StepGroups returns the position of the edge of a group that's n
// steps away from the current group. A positive n means stepping in
// an ascending direction, and returns the position of the last entry
// in the target group. A negative n means stepping in a descending
// direction, and returns the position of the first entry in the
// target group.
func (wf *WindowFrame) StepGroups(n int64) (int64, bool, error) {
	dir := int64(1)
	if n < 0 {
		n, dir = -n, int64(-1)
	}

	isRange := wf.Type == WTokRange

	end := int64(wf.Partition.Len())

	curr, err := wf.FindGroupEdge(wf.Pos, dir, isRange)
	if err != nil {
		return 0, false, err
	}

	for n > 0 {
		next := curr + dir
		if next < 0 || next >= end {
			break // ran off the partition before stepping all n groups
		}

		curr, err = wf.FindGroupEdge(next, dir, isRange)
		if err != nil {
			return 0, false, err
		}

		n--
	}

	// steppedAll is false when the offset ran past the partition edge. The caller uses
	// it to tell "clamp toward the open side" (BEG preceding / END following -> extend
	// to the partition edge) from "the boundary fell off the far side" (END preceding /
	// BEG following -> the frame is empty).
	return curr, n == 0, nil
}

// -------------------------------------------------------------------

// FindGroupEdge returns the position of the starting or ending member
// of a group, depending on the direction dir parameter which should
// be a 1 or -1. When 1, the ending member of the group is
// returned. When -1, the starting member of the group is returned.
func (wf *WindowFrame) FindGroupEdge(i, dir int64, isRange bool) (int64, error) {
	end := int64(wf.Partition.Len())

	valCurr, err := wf.GetValsVal(i, wf.ValIdx)
	if err != nil {
		return i, err
	}

	var f64Edge float64
	if wf.Type == WTokRange {
		f64Curr, err := ParseFloat64(valCurr)
		if err != nil {
			return i, err
		}

		if dir < 0 {
			f64Edge = f64Curr + wf.BegF64
		} else {
			f64Edge = f64Curr + wf.EndF64
		}
	}

	for {
		next := i + dir
		if next < 0 || next >= end {
			return i, nil
		}

		valNext, err := wf.GetValsVal(next, wf.ValIdx)
		if err != nil {
			return i, err
		}

		if wf.Type == WTokGroups {
			if !bytes.Equal(valCurr, valNext) {
				return i, nil
			}
		} else { // wf.Type == WTokRange.
			f64Next, err := ParseFloat64(valNext)
			if err != nil ||
				((dir < 0) && (f64Next < f64Edge)) ||
				((dir > 0) && (f64Next > f64Edge)) {
				return i, err
			}
		}

		i = next
	}
}

// -------------------------------------------------------------------

// GetValsVal returns the valIdx'th val that's in the vals entry at
// the given i position in the partition, both 0-based.
func (wf *WindowFrame) GetValsVal(i int64, valIdx int) (Val, error) {
	buf, err := wf.Partition.Get(i)
	if err != nil {
		return nil, err
	}

	wf.TempVals = ValsDecode(buf, wf.TempVals[:0])
	if len(wf.TempVals) > 0 {
		return wf.TempVals[valIdx], nil
	}

	return nil, nil
}

// -------------------------------------------------------------------

// StepToOffset navigates to the target row for the offset/navigation window
// functions (FIRST_VALUE / LAST_VALUE / NTH_VALUE / LAG / LEAD) and returns its
// decoded vals. It mirrors ExprWindowFrameStepValue's stepping:
//   - initial == -1: start before the frame (StepVals then lands on the first row);
//     num forward steps -> the num'th frame row (FIRST_VALUE=1, NTH_VALUE=n).
//   - initial ==  0: start at the current row (wf.Pos); num steps back (LAG) or
//     forward (LEAD) -> the row num away.
//   - initial ==  1: start past the frame end; num backward steps -> the last row
//     (LAST_VALUE=1).
//
// ok is false when the target falls outside the frame (e.g. LAG at the partition
// start), so the caller yields NULL. Kept as a base method (not inline field access)
// so the gen-compiler treats it as runtime -- see the codegen note in op_window.go.
func (wf *WindowFrame) StepToOffset(initial int, asc bool, num uint64, valsPre Vals) (
	vals Vals, ok bool, err error) {
	pos := int64(-1)
	if initial == 0 {
		pos = wf.Pos
	} else if initial == 1 {
		pos = int64(^uint64(0) >> 1) // MaxInt64: past the end; StepVals clamps to last.
	}

	ok = true
	for i := uint64(0); i < num && ok && err == nil; i++ {
		vals, pos, ok, err = wf.StepVals(asc, pos, valsPre)
	}

	return vals, ok, err
}

// StepVals is used for iterating through the current window frame and
// returns the next vals & position given the last seen position.
func (wf *WindowFrame) StepVals(next bool, iLast int64, valsPre Vals) (
	vals Vals, i int64, ok bool, err error) {
	if next {
		i, ok = wf.Next(iLast)
	} else {
		i, ok = wf.Prev(iLast)
	}
	if ok {
		buf, err := wf.Partition.Get(i)
		if err != nil {
			return nil, -1, false, err
		}

		vals = ValsDecode(buf, valsPre[:0])
	}

	return vals, i, ok, nil
}

// -------------------------------------------------------------------

// Next is used for iterating through the current window frame and
// returns the next position given the last seen position.
func (wf *WindowFrameCurr) Next(i int64) (int64, bool) {
	if i < wf.Include.Beg {
		i = wf.Include.Beg
	} else {
		i++
	}

	for _, exclude := range wf.Excludes {
		if i >= exclude.Beg && i < exclude.End {
			i = exclude.End
		}
	}

	if i >= wf.Include.End {
		return i, false
	}

	return i, true
}

// -------------------------------------------------------------------

// Prev is used for iterating in reverse through the current window
// frame and returns the prev position given the last seen position.
func (wf *WindowFrameCurr) Prev(i int64) (int64, bool) {
	if i >= wf.Include.End {
		i = wf.Include.End - 1
	} else {
		i--
	}

	for j := len(wf.Excludes) - 1; j >= 0; j-- {
		// Examine the Excludes in reverse in case they're adjacent.
		exclude := &wf.Excludes[j]
		if i >= exclude.Beg && i < exclude.End {
			i = exclude.Beg - 1
		}
	}

	if i < wf.Include.Beg {
		return i, false
	}

	return i, true
}

// -------------------------------------------------------------------

// Count returns the number of rows in the current frame.
func (wf *WindowFrameCurr) Count() int64 {
	s := wf.Include.End - wf.Include.Beg

	for _, e := range wf.Excludes {
		if Int64RangesOverlap(e.Beg, e.End, wf.Include.Beg, wf.Include.End) {
			s = s - (Int64Min(e.End, wf.Include.End) - Int64Max(e.Beg, wf.Include.Beg))
		}
	}

	return s
}

// -------------------------------------------------------------------

// Int64RangesOverlap returns true if the range [xBeg, xEnd) overlaps
// with the range [yBeg, yEnd).
func Int64RangesOverlap(xBeg, xEnd, yBeg, yEnd int64) bool {
	if xEnd <= yBeg || yEnd <= xBeg {
		return false
	}
	return true
}

// -------------------------------------------------------------------

// Int64Max returns the greater of a and b.
func Int64Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Int64Min returns the lesser of a and b.
func Int64Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
