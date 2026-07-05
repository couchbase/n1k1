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

// Selection (predicate) kernels for vectorized WHERE (DESIGN-col.md Step 5.4b):
// compare a packed column against a constant to produce a dense selection bitmap
// (same LSB-first layout as the masked-reduce kernels + Arrow validity), and
// combine predicates with byte-wise AND/OR. The comparison operator is chosen
// OUTSIDE the loop (one tight branchless-per-op loop) rather than switched per
// element, matching the vectorized-kernel ethos.

import (
	"encoding/binary"
	"math"
)

// CmpOp is a comparison operator for a selection kernel.
type CmpOp int

const (
	CmpEQ CmpOp = iota
	CmpNE
	CmpLT
	CmpLE
	CmpGT
	CmpGE
)

// clearBits zeroes the first n bits' bytes of mask.
func clearBits(mask []byte, n int) {
	nb := (n + 7) >> 3
	if nb > len(mask) {
		nb = len(mask)
	}
	for i := 0; i < nb; i++ {
		mask[i] = 0
	}
}

// SelectFloat64 (re)writes mask so bit i is set iff values[i] OP c, where values
// is n little-endian float64s. mask must have >= (n+7)/8 bytes.
func SelectFloat64(mask, values []byte, n int, op CmpOp, c float64) {
	clearBits(mask, n)
	switch op {
	case CmpEQ:
		for i := 0; i < n; i++ {
			if math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:])) == c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpNE:
		for i := 0; i < n; i++ {
			if math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:])) != c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpLT:
		for i := 0; i < n; i++ {
			if math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:])) < c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpLE:
		for i := 0; i < n; i++ {
			if math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:])) <= c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpGT:
		for i := 0; i < n; i++ {
			if math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:])) > c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpGE:
		for i := 0; i < n; i++ {
			if math.Float64frombits(binary.LittleEndian.Uint64(values[i*8:])) >= c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	}
}

// SelectInt64 is SelectFloat64 for an int64 column vs an int64 constant.
func SelectInt64(mask, values []byte, n int, op CmpOp, c int64) {
	clearBits(mask, n)
	switch op {
	case CmpEQ:
		for i := 0; i < n; i++ {
			if int64(binary.LittleEndian.Uint64(values[i*8:])) == c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpNE:
		for i := 0; i < n; i++ {
			if int64(binary.LittleEndian.Uint64(values[i*8:])) != c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpLT:
		for i := 0; i < n; i++ {
			if int64(binary.LittleEndian.Uint64(values[i*8:])) < c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpLE:
		for i := 0; i < n; i++ {
			if int64(binary.LittleEndian.Uint64(values[i*8:])) <= c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpGT:
		for i := 0; i < n; i++ {
			if int64(binary.LittleEndian.Uint64(values[i*8:])) > c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	case CmpGE:
		for i := 0; i < n; i++ {
			if int64(binary.LittleEndian.Uint64(values[i*8:])) >= c {
				mask[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	}
}

// AndBitmap sets dst &= src (conjunction of predicates); OrBitmap sets dst |= src.
// Operate over dst's length (masks are same-sized in a batch).
func AndBitmap(dst, src []byte) {
	for i := range dst {
		if i < len(src) {
			dst[i] &= src[i]
		} else {
			dst[i] = 0
		}
	}
}

func OrBitmap(dst, src []byte) {
	for i := range dst {
		if i < len(src) {
			dst[i] |= src[i]
		}
	}
}
