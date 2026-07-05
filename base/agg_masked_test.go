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

func packF64(vals []float64) []byte {
	b := make([]byte, len(vals)*8)
	for i, f := range vals {
		binary.LittleEndian.PutUint64(b[i*8:], math.Float64bits(f))
	}
	return b
}

// bitmap builds a dense LSB-first bitmap from a predicate over indices 0..n-1.
func bitmap(n int, sel func(i int) bool) []byte {
	m := make([]byte, (n+7)/8)
	for i := 0; i < n; i++ {
		if sel(i) {
			m[i>>3] |= 1 << (uint(i) & 7)
		}
	}
	return m
}

func TestMaskedReduceMatchesManual(t *testing.T) {
	vals := []float64{1, 2.5, -3, 4, 5.5, 6, -7.25, 8, 9.5, 10, 11, -12.5, 13}
	col := packF64(vals)
	n := len(vals)

	masks := map[string]func(int) bool{
		"all":     func(i int) bool { return true },
		"none":    func(i int) bool { return false },
		"even":    func(i int) bool { return i%2 == 0 },
		"sparse":  func(i int) bool { return i == 0 || i == 7 || i == n-1 },
		"first-3": func(i int) bool { return i < 3 },
	}

	for name, sel := range masks {
		mask := bitmap(n, sel)

		var wantSum float64
		var wantCount int
		for i := 0; i < n; i++ {
			if sel(i) {
				wantSum += vals[i]
				wantCount++
			}
		}

		if got := PopCount(mask, n); got != wantCount {
			t.Errorf("%s: PopCount=%d want %d", name, got, wantCount)
		}

		acc := AggSum.Init(nil, nil)
		SumMaskedFloat64(acc, col, mask, n)
		if got := math.Float64frombits(binary.LittleEndian.Uint64(acc[:8])); got != wantSum {
			t.Errorf("%s: SumMaskedFloat64=%v want %v", name, got, wantSum)
		}

		cacc := AggCount.Init(nil, nil)
		CountMasked(cacc, mask, n)
		if got := binary.LittleEndian.Uint64(cacc[:8]); got != uint64(wantCount) {
			t.Errorf("%s: CountMasked=%d want %d", name, got, wantCount)
		}

		aacc := AggAvg.Init(nil, nil)
		AvgMaskedFloat64(aacc, col, mask, n)
		gotC := binary.LittleEndian.Uint64(aacc[:8])
		gotS := math.Float64frombits(binary.LittleEndian.Uint64(aacc[8:16]))
		if gotC != uint64(wantCount) || gotS != wantSum {
			t.Errorf("%s: AvgMasked count=%d sum=%v want count=%d sum=%v", name, gotC, gotS, wantCount, wantSum)
		}
	}

	// All-set mask must equal the unmasked scalar/vector sum exactly.
	all := bitmap(n, func(int) bool { return true })
	acc := AggSum.Init(nil, nil)
	SumMaskedFloat64(acc, col, all, n)
	masked := math.Float64frombits(binary.LittleEndian.Uint64(acc[:8]))
	scalar := sumScalar(vals, false) // from agg_v_test.go
	accV := AggSum.Init(nil, nil)
	accV, _, _ = AggSumVFloat64.Update(nil, Val(col), nil, accV, nil)
	plainV := math.Float64frombits(binary.LittleEndian.Uint64(accV[:8]))
	if masked != plainV {
		t.Errorf("all-set masked sum %v != unmasked vector sum %v", masked, plainV)
	}
	_ = scalar

	// Int64 masked matches manual.
	ints := []int64{-5, 10, 0, 7, 1000, -1, 42, 3}
	icol := make([]byte, len(ints)*8)
	for i, v := range ints {
		binary.LittleEndian.PutUint64(icol[i*8:], uint64(v))
	}
	imask := bitmap(len(ints), func(i int) bool { return i%2 == 1 })
	var iwant float64
	for i, v := range ints {
		if i%2 == 1 {
			iwant += float64(v)
		}
	}
	iacc := AggSum.Init(nil, nil)
	SumMaskedInt64(iacc, icol, imask, len(ints))
	if got := math.Float64frombits(binary.LittleEndian.Uint64(iacc[:8])); got != iwant {
		t.Errorf("SumMaskedInt64=%v want %v", got, iwant)
	}
}
