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

// A nil mask means "all n lanes" (no selection / no nulls) -- so the same kernel
// serves the unmasked, filter-only, nulls-only, and filter+nulls cases. NOTE the
// SUM vs COUNT asymmetry that n1k1's aggregate semantics require: SUM folds over
// the selection-AND-validity mask (nulls contribute nothing), while COUNT counts
// the SELECTED rows regardless of validity (n1k1 COUNT(x) counts null/missing rows,
// like COUNT(*)). AVG therefore takes BOTH: count over sel, sum over sum(=sel∧val).

// MaskedSumFloat64 folds values (n LE float64s) where mask bit i is set (nil mask =
// all n) into AggSum's 8-byte float64 accumulator, in place.
func MaskedSumFloat64(acc, values, mask []byte, n int) {
	s := math.Float64frombits(binary.LittleEndian.Uint64(acc[:8]))
	if mask == nil {
		for i := 0; i < n; i++ {
			s += math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:]))
		}
	} else {
		for i := 0; i < n; i++ {
			if BitSet(mask, i) {
				s += math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:]))
			}
		}
	}
	binary.LittleEndian.PutUint64(acc[:8], math.Float64bits(s))
}

// MaskedSumInt64 is SumMaskedFloat64 for an int64 column (each slot added as float64).
func MaskedSumInt64(acc, values, mask []byte, n int) {
	s := math.Float64frombits(binary.LittleEndian.Uint64(acc[:8]))
	if mask == nil {
		for i := 0; i < n; i++ {
			s += float64(int64(binary.LittleEndian.Uint64(values[i*8:])))
		}
	} else {
		for i := 0; i < n; i++ {
			if BitSet(mask, i) {
				s += float64(int64(binary.LittleEndian.Uint64(values[i*8:])))
			}
		}
	}
	binary.LittleEndian.PutUint64(acc[:8], math.Float64bits(s))
}

// MaskedCount adds the number of set bits (first n; nil mask = n) to AggCount's
// 8-byte counter. The mask here is the SELECTION -- not ANDed with validity.
func MaskedCount(acc, mask []byte, n int) {
	c := binary.LittleEndian.Uint64(acc[:8])
	if mask == nil {
		c += uint64(n)
	} else {
		c += uint64(PopCount(mask, n))
	}
	binary.LittleEndian.PutUint64(acc[:8], c)
}

// MaskedAvgFloat64 folds into AggAvg's [count][sum] accumulator: count += selected
// rows (sel; nil = n), sum += values where the sum mask is set (nil = all n). Pass
// sel = the selection and sum = selection∧validity so AVG = sum(non-null)/count(rows).
func MaskedAvgFloat64(acc, values, sel, sum []byte, n int) {
	c := binary.LittleEndian.Uint64(acc[:8])
	s := math.Float64frombits(binary.LittleEndian.Uint64(acc[8:16]))
	if sel == nil {
		c += uint64(n)
	} else {
		c += uint64(PopCount(sel, n))
	}
	if sum == nil {
		for i := 0; i < n; i++ {
			s += math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:]))
		}
	} else {
		for i := 0; i < n; i++ {
			if BitSet(sum, i) {
				s += math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:]))
			}
		}
	}
	binary.LittleEndian.PutUint64(acc[:8], c)
	binary.LittleEndian.PutUint64(acc[8:16], math.Float64bits(s))
}

// MaskedAvgInt64 is AvgMaskedFloat64 for an int64 column.
func MaskedAvgInt64(acc, values, sel, sum []byte, n int) {
	c := binary.LittleEndian.Uint64(acc[:8])
	s := math.Float64frombits(binary.LittleEndian.Uint64(acc[8:16]))
	if sel == nil {
		c += uint64(n)
	} else {
		c += uint64(PopCount(sel, n))
	}
	if sum == nil {
		for i := 0; i < n; i++ {
			s += float64(int64(binary.LittleEndian.Uint64(values[i*8:])))
		}
	} else {
		for i := 0; i < n; i++ {
			if BitSet(sum, i) {
				s += float64(int64(binary.LittleEndian.Uint64(values[i*8:])))
			}
		}
	}
	binary.LittleEndian.PutUint64(acc[:8], c)
	binary.LittleEndian.PutUint64(acc[8:16], math.Float64bits(s))
}
