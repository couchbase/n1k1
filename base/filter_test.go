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

package base

import (
	"encoding/binary"
	"math"
	"testing"
)

func opName(op CmpOp) string {
	return []string{"==", "!=", "<", "<=", ">", ">="}[op]
}

func cmpF(a, b float64, op CmpOp) bool {
	switch op {
	case CmpEQ:
		return a == b
	case CmpNE:
		return a != b
	case CmpLT:
		return a < b
	case CmpLE:
		return a <= b
	case CmpGT:
		return a > b
	default: // CmpGE
		return a >= b
	}
}

// bitmapsEqual compares the first n bits of two dense bitmaps.
func bitmapsEqual(a, b []byte, n int) bool {
	for i := 0; i < n; i++ {
		if BitSet(a, i) != BitSet(b, i) {
			return false
		}
	}
	return true
}

func TestFilterFloat64MatchesManual(t *testing.T) {
	// Includes a value equal to the constant to exercise ==/!=/<=/>= boundaries.
	vals := []float64{1, 2.5, -3, 4, 5.5, 6, -7.25, 8, 5.5, 10, 11, -12.5, 5.5}
	col := packF64(vals)
	n := len(vals)
	const c = 5.5

	for op := CmpEQ; op <= CmpGE; op++ {
		mask := make([]byte, (n+7)/8)
		// Pre-dirty mask to prove FilterFloat64 fully rewrites it.
		for i := range mask {
			mask[i] = 0xFF
		}
		FilterFloat64(mask, col, n, op, c)

		want := bitmap(n, func(i int) bool { return cmpF(vals[i], c, op) })
		if !bitmapsEqual(mask, want, n) {
			t.Errorf("op %s: mask mismatch\n got %08b\nwant %08b", opName(op), mask, want)
		}
		if got, w := PopCount(mask, n), PopCount(want, n); got != w {
			t.Errorf("op %s: popcount=%d want %d", opName(op), got, w)
		}
	}
}

func TestFilterInt64MatchesManual(t *testing.T) {
	ints := []int64{-5, 10, 0, 7, 1000, -1, 42, 3, 7, 7}
	col := make([]byte, len(ints)*8)
	for i, v := range ints {
		binary.LittleEndian.PutUint64(col[i*8:], uint64(v))
	}
	n := len(ints)
	const c int64 = 7

	for op := CmpEQ; op <= CmpGE; op++ {
		mask := make([]byte, (n+7)/8)
		FilterInt64(mask, col, n, op, c)
		want := bitmap(n, func(i int) bool { return cmpF(float64(ints[i]), float64(c), op) })
		if !bitmapsEqual(mask, want, n) {
			t.Errorf("int op %s: mask mismatch\n got %08b\nwant %08b", opName(op), mask, want)
		}
	}
}

func TestAndOrBitmap(t *testing.T) {
	n := 20
	a := bitmap(n, func(i int) bool { return i%2 == 0 })  // even
	b := bitmap(n, func(i int) bool { return i%3 == 0 })  // multiples of 3
	wantAnd := bitmap(n, func(i int) bool { return i%2 == 0 && i%3 == 0 })
	wantOr := bitmap(n, func(i int) bool { return i%2 == 0 || i%3 == 0 })

	and := make([]byte, len(a))
	copy(and, a)
	AndBitmap(and, b)
	if !bitmapsEqual(and, wantAnd, n) {
		t.Errorf("AndBitmap mismatch\n got %08b\nwant %08b", and, wantAnd)
	}

	or := make([]byte, len(a))
	copy(or, a)
	OrBitmap(or, b)
	if !bitmapsEqual(or, wantOr, n) {
		t.Errorf("OrBitmap mismatch\n got %08b\nwant %08b", or, wantOr)
	}
}

// TestFilterThenMaskedReduce chains a predicate into a masked reducer: the fused
// WHERE+agg path. SUM over vals where vals > c must equal the manual filtered sum.
func TestFilterThenMaskedReduce(t *testing.T) {
	vals := []float64{1, 2.5, -3, 4, 5.5, 6, -7.25, 8, 9.5, 10, 11, -12.5, 13}
	col := packF64(vals)
	n := len(vals)
	const c = 4.0

	mask := make([]byte, (n+7)/8)
	FilterFloat64(mask, col, n, CmpGT, c)

	acc := AggSum.Init(nil, nil)
	SumMaskedFloat64(acc, col, mask, n)
	got := math.Float64frombits(binary.LittleEndian.Uint64(acc[:8]))

	var want float64
	for _, v := range vals {
		if v > c {
			want += v
		}
	}
	if got != want {
		t.Errorf("fused filter>%.1f then sum = %v, want %v", c, got, want)
	}
}
