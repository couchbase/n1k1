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

// Vectorized elementwise arithmetic kernels for aggregate operands (DESIGN-col.md
// Step 5.5): materialize `a <op> b` as a packed float64 column that the SUM/AVG
// masked reducers then fold -- so `SUM(price * qty)` runs vectorized. Everything is
// computed in float64 (matching the row engine, which parses JSON numbers as
// float64), so the result is bit-identical; an int64 source column is widened via
// LoadFloat64FromInt64. The operator is chosen OUTSIDE the loop (one tight loop per
// op), like the filter kernels. Only +, -, * -- not / (which would need x/0 -> NULL).

import (
	"encoding/binary"
	"math"
)

func ldF64(b []byte, i int) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(b[i*8:]))
}

func stF64(b []byte, i int, v float64) {
	binary.LittleEndian.PutUint64(b[i*8:], math.Float64bits(v))
}

// LoadFloat64FromInt64 writes n float64s into dst from an int64 column (widening
// each 8-byte little-endian int64 to float64), giving arithmetic a uniform float64
// view of an integer source. dst must hold n*8 bytes.
func LoadFloat64FromInt64(dst, src []byte, n int) {
	for i := 0; i < n; i++ {
		stF64(dst, i, float64(int64(binary.LittleEndian.Uint64(src[i*8:]))))
	}
}

// ArithFloat64 writes dst[i] = a[i] <op> b[i] over two float64 columns; op is one
// of '+', '-', '*'. dst must hold n*8 bytes (may alias a or b).
func ArithFloat64(dst, a, b []byte, n int, op byte) {
	switch op {
	case '+':
		for i := 0; i < n; i++ {
			stF64(dst, i, ldF64(a, i)+ldF64(b, i))
		}
	case '-':
		for i := 0; i < n; i++ {
			stF64(dst, i, ldF64(a, i)-ldF64(b, i))
		}
	case '*':
		for i := 0; i < n; i++ {
			stF64(dst, i, ldF64(a, i)*ldF64(b, i))
		}
	}
}

// ScaleFloat64 writes dst[i] = a[i] <op> c (constRight) or c <op> a[i] (else), for a
// float64 column a and scalar c; op is one of '+', '-', '*'.
func ScaleFloat64(dst, a []byte, c float64, op byte, constRight bool, n int) {
	switch op {
	case '+':
		for i := 0; i < n; i++ {
			stF64(dst, i, ldF64(a, i)+c)
		}
	case '*':
		for i := 0; i < n; i++ {
			stF64(dst, i, ldF64(a, i)*c)
		}
	case '-':
		if constRight {
			for i := 0; i < n; i++ {
				stF64(dst, i, ldF64(a, i)-c)
			}
		} else {
			for i := 0; i < n; i++ {
				stF64(dst, i, c-ldF64(a, i))
			}
		}
	}
}
