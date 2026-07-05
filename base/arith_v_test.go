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

func unpackF64(b []byte, n int) []float64 {
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b[i*8:]))
	}
	return out
}

func packI64(vals []int64) []byte {
	b := make([]byte, len(vals)*8)
	for i, v := range vals {
		binary.LittleEndian.PutUint64(b[i*8:], uint64(v))
	}
	return b
}

func TestArithFloat64MatchesManual(t *testing.T) {
	a := []float64{1, 2.5, -3, 4.25, 0, 100}
	b := []float64{10, -2, 3.5, 4, 7, 0.5}
	ab, bb := packF64(a), packF64(b)
	n := len(a)

	for _, op := range []byte{'+', '-', '*'} {
		dst := make([]byte, n*8)
		ArithFloat64(dst, ab, bb, n, op)
		got := unpackF64(dst, n)
		for i := range a {
			var want float64
			switch op {
			case '+':
				want = a[i] + b[i]
			case '-':
				want = a[i] - b[i]
			case '*':
				want = a[i] * b[i]
			}
			if got[i] != want {
				t.Errorf("op %c [%d]: got %v want %v", op, i, got[i], want)
			}
		}
	}
}

func TestScaleFloat64MatchesManual(t *testing.T) {
	a := []float64{1, 2.5, -3, 4.25, 0, 100}
	ab := packF64(a)
	n := len(a)
	const c = 1.08

	for _, op := range []byte{'+', '-', '*'} {
		for _, constRight := range []bool{true, false} {
			dst := make([]byte, n*8)
			ScaleFloat64(dst, ab, c, op, constRight, n)
			got := unpackF64(dst, n)
			for i := range a {
				var want float64
				switch {
				case op == '+':
					want = a[i] + c
				case op == '*':
					want = a[i] * c
				case op == '-' && constRight:
					want = a[i] - c
				default: // '-' const on left
					want = c - a[i]
				}
				if got[i] != want {
					t.Errorf("op %c constRight=%v [%d]: got %v want %v", op, constRight, i, got[i], want)
				}
			}
		}
	}
}

func TestLoadFloat64FromInt64(t *testing.T) {
	ints := []int64{-5, 0, 7, 1000, -123456789}
	src := packI64(ints)
	n := len(ints)
	dst := make([]byte, n*8)
	LoadFloat64FromInt64(dst, src, n)
	got := unpackF64(dst, n)
	for i, v := range ints {
		if got[i] != float64(v) {
			t.Errorf("[%d] got %v want %v", i, got[i], float64(v))
		}
	}
}

// TestArithThenSum chains materialize -> masked SUM: SUM(a*b) over the packed
// product must equal the manual sum, the fused-agg path in miniature.
func TestArithThenSum(t *testing.T) {
	a := []float64{1, 2.5, -3, 4.25, 5, 6}
	b := []float64{10, -2, 3.5, 4, 2, 0.5}
	n := len(a)
	prod := make([]byte, n*8)
	ArithFloat64(prod, packF64(a), packF64(b), n, '*')

	acc := AggSum.Init(nil, nil)
	SumMaskedFloat64(acc, prod, nil, n) // nil mask = all rows
	got := math.Float64frombits(binary.LittleEndian.Uint64(acc[:8]))

	var want float64
	for i := range a {
		want += a[i] * b[i]
	}
	if got != want {
		t.Errorf("SUM(a*b) = %v, want %v", got, want)
	}
}
