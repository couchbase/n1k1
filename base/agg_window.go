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

	"github.com/couchbase/rhmap/store"
)

// WTok maps a window related configuration string to an internal
// token number, which enables faster, numeric comparisons.
var WTok = map[string]int{}

var WTokRows, WTokRange, WTokGroups, WTokUnbounded, WTokNum int
var WTokCurrentRow, WTokNoOthers, WTokGroup, WTokTies int

func init() {
	for tokenNum, tokenStr := range []string{
		"rows", "range", "groups", "unbounded", "num",
		"no-others", "group", "ties"} {
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
			wf.Include.Beg, err = wf.StepGroups(wf.BegNum)
			if err != nil {
				return err
			}
		} else { // wf.Type == WTokRange.
			// TODO: Assumes ASC order-by.
			wf.Include.Beg, err = wf.FindGroupEdge(wf.Pos, -1)
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
			wf.Include.End, err = wf.StepGroups(wf.EndNum)
			if err != nil {
				return err
			}
		} else { // wf.Type == WTokRange.
			// TODO: Assumes ASC order-by.
			wf.Include.End, err = wf.FindGroupEdge(wf.Pos, 1)
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

	if wf.Exclude != WTokNoOthers {
		if wf.Exclude == WTokCurrentRow {
			wf.Excludes = append(wf.Excludes, WindowSpan{wf.Pos, wf.Pos + 1})
		} else {
			panic("unsupported")
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
func (wf *WindowFrame) StepGroups(n int64) (int64, error) {
	if n == 0 {
		return wf.Pos, nil
	}

	dir := int64(1)
	if n < 0 {
		n, dir = -n, int64(-1)
	}

	end := int64(wf.Partition.Len())

	curr, err := wf.FindGroupEdge(wf.Pos, dir)
	if err != nil {
		return 0, err
	}

	for n > 0 {
		next := curr + dir
		if next < 0 || next >= end {
			break
		}

		curr, err = wf.FindGroupEdge(next, dir)
		if err != nil {
			return 0, err
		}

		n--
	}

	return curr, nil
}

// -------------------------------------------------------------------

// FindGroupEdge returns the position of the starting or ending member
// of a group, depending on the direction dir parameter which should
// be a 1 or -1. When 1, the ending member of the group is
// returned. When -1, the starting member of the group is returned.
func (wf *WindowFrame) FindGroupEdge(i, dir int64) (int64, error) {
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
	buf, err := wf.Partition.Get(int(i))
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
		buf, err := wf.Partition.Get(int(i))
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

	for _, exclude := range wf.Excludes {
		if Overlaps(exclude.Beg, exclude.End, wf.Include.Beg, wf.Include.End) {
			s = s - (Min(exclude.End, wf.Include.End) -
				Max(exclude.Beg, wf.Include.Beg))
		}
	}

	return s
}

// -------------------------------------------------------------------

// Overlaps returns true if the range [xBeg, xEnd) overlaps with the
// range [yBeg, yEnd).
func Overlaps(xBeg, xEnd, yBeg, yEnd int64) bool {
	if xEnd <= yBeg || yEnd <= xBeg {
		return false
	}
	return true
}

// -------------------------------------------------------------------

// Max returns the greater of a and b.
func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Min returns the lesser of a and b.
func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
