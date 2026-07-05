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

// Masked vectorized reductions (DESIGN-col.md Step 5.4a): fold a packed column
// over a dense bitmap, touching only set lanes. The bitmap is LSB-first per byte
// (bit i = mask[i>>3] & (1<<(i&7))) -- the SAME layout as an Arrow validity bitmap,
// so a WHERE selection and a null-validity bitmap AND together byte-wise, and one
// kernel serves both the null path and selection-vector WHERE. These fold in place
// into the same accumulators AggSum/AggCount/AggAvg use (so Result is reused, and
// the output is bit-identical to the scalar/unmasked path over the same lanes).

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// BitSet reports whether bit i (LSB-first) is set in a dense bitmap.
func BitSet(mask []byte, i int) bool {
	return i>>3 < len(mask) && mask[i>>3]&(1<<(uint(i)&7)) != 0
}

// PopCount counts the set bits among the first n bits of a dense bitmap.
func PopCount(mask []byte, n int) int {
	full := n >> 3
	if full > len(mask) {
		full = len(mask)
	}
	c := 0
	for i := 0; i < full; i++ {
		c += bits.OnesCount8(mask[i])
	}
	if rem := n & 7; rem > 0 && full < len(mask) {
		c += bits.OnesCount8(mask[full] & byte((1<<uint(rem))-1))
	}
	return c
}

// SumMaskedFloat64 folds values (n LE float64s) where mask bit i is set into
// AggSum's 8-byte float64 accumulator, in place.
func SumMaskedFloat64(acc, values, mask []byte, n int) {
	s := math.Float64frombits(binary.LittleEndian.Uint64(acc[:8]))
	for i := 0; i < n; i++ {
		if BitSet(mask, i) {
			s += math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:]))
		}
	}
	binary.LittleEndian.PutUint64(acc[:8], math.Float64bits(s))
}

// SumMaskedInt64 is SumMaskedFloat64 for an int64 column (each slot added as float64).
func SumMaskedInt64(acc, values, mask []byte, n int) {
	s := math.Float64frombits(binary.LittleEndian.Uint64(acc[:8]))
	for i := 0; i < n; i++ {
		if BitSet(mask, i) {
			s += float64(int64(binary.LittleEndian.Uint64(values[i*8:])))
		}
	}
	binary.LittleEndian.PutUint64(acc[:8], math.Float64bits(s))
}

// CountMasked adds the number of set bits (first n) to AggCount's 8-byte counter.
func CountMasked(acc, mask []byte, n int) {
	c := binary.LittleEndian.Uint64(acc[:8]) + uint64(PopCount(mask, n))
	binary.LittleEndian.PutUint64(acc[:8], c)
}

// AvgMaskedFloat64 folds into AggAvg's [count][sum] accumulator (count += set bits).
func AvgMaskedFloat64(acc, values, mask []byte, n int) {
	c := binary.LittleEndian.Uint64(acc[:8])
	s := math.Float64frombits(binary.LittleEndian.Uint64(acc[8:16]))
	for i := 0; i < n; i++ {
		if BitSet(mask, i) {
			c++
			s += math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:]))
		}
	}
	binary.LittleEndian.PutUint64(acc[:8], c)
	binary.LittleEndian.PutUint64(acc[8:16], math.Float64bits(s))
}

// AvgMaskedInt64 is AvgMaskedFloat64 for an int64 column.
func AvgMaskedInt64(acc, values, mask []byte, n int) {
	c := binary.LittleEndian.Uint64(acc[:8])
	s := math.Float64frombits(binary.LittleEndian.Uint64(acc[8:16]))
	for i := 0; i < n; i++ {
		if BitSet(mask, i) {
			c++
			s += float64(int64(binary.LittleEndian.Uint64(values[i*8:])))
		}
	}
	binary.LittleEndian.PutUint64(acc[:8], c)
	binary.LittleEndian.PutUint64(acc[8:16], math.Float64bits(s))
}
