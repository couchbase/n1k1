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
	"math"
	"unsafe"
)

// VecFloat32 reinterprets a borrowed little-endian float32 buffer as a []float32
// view WITHOUT copying -- it aliases the underlying bytes (e.g. an Arrow/Parquet
// column page), so the buffer MUST outlive the view. This is the vector analog of
// the columnar scalar kernels' binary.LittleEndian reads, but traded for a true
// zero-copy borrow (DESIGN-vectors.md): a 384-dim float32 row is 1536 bytes, and the
// measured cost is the per-element decode, not the buffer. It assumes a little-endian
// host (n1k1's targets are arm64 / amd64); len(b) is rounded down to a multiple of 4.
func VecFloat32(b []byte) []float32 {
	if len(b) < 4 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)/4)
}

// VectorDistanceVFloat32 is the columnar byte-lane core of VECTOR_DISTANCE
// (DESIGN-vectors.md, "vector_distance_v_fixed_size:float32"): given `col`, `rows`
// vectors of width `dim` packed contiguously (row r == col[r*dim : (r+1)*dim]), and a
// query vector `q` (len dim), it writes each row's distance into out[:rows]. Storage
// is float32 (the win vs jsonl: no strconv.ParseFloat per element) but the
// accumulation promotes to float64, so results match the boxed / native float64
// paths (base.VectorDistanceVals) bit-for-bit on the same values -- this is the
// vectorized analog of AggSumVFloat64, just mapping N vectors to N distances instead
// of folding a column to one accumulator.
//
// A NULL result (cosine of a zero-norm vector) is written as NaN for the caller to
// map back to N1QL NULL. cbq NULLs a row if any element is outside float32 range;
// col is float32 so it is always in range, and q is checked once up front (any
// out-of-range q element -> all rows NULL, since q is shared). Unknown metric -> all
// NaN. Metric names match base.unquoteMetric's output (already unquoted, lowercased
// by the caller if needed).
func VectorDistanceVFloat32(out []float64, col []float32, q []float64, rows, dim int, metric string) {
	const maxF32 = math.MaxFloat32
	for _, x := range q { // q shared across rows: one out-of-range element NULLs them all
		if x < -maxF32 || x > maxF32 {
			fillNaN(out, rows)
			return
		}
	}

	switch metric {
	case "l2", "euclidean", "l2_squared", "euclidean_squared":
		sqrt := metric == "l2" || metric == "euclidean"
		for r := 0; r < rows; r++ {
			v := col[r*dim : r*dim+dim]
			var s float64
			for j := 0; j < dim; j++ {
				d := float64(v[j]) - q[j]
				s += d * d
			}
			if sqrt {
				s = math.Sqrt(s)
			}
			out[r] = s
		}
	case "dot":
		for r := 0; r < rows; r++ {
			v := col[r*dim : r*dim+dim]
			var s float64
			for j := 0; j < dim; j++ {
				s += float64(v[j]) * q[j]
			}
			out[r] = -s // cbq negates dot so ORDER BY ASC still means "closest"
		}
	case "cosine":
		var qn float64
		for _, x := range q {
			qn += x * x
		}
		qn = math.Sqrt(qn)
		for r := 0; r < rows; r++ {
			v := col[r*dim : r*dim+dim]
			var dot, vn float64
			for j := 0; j < dim; j++ {
				f := float64(v[j])
				dot += f * q[j]
				vn += f * f
			}
			if vn == 0 || qn == 0 {
				out[r] = math.NaN() // zero-norm -> cbq NULL
				continue
			}
			res := 1.0 - dot/(math.Sqrt(vn)*qn)
			if res == 0 {
				res = 0 // normalize IEEE -0.0 to cbq's "0"
			}
			out[r] = res
		}
	default:
		fillNaN(out, rows)
	}
}

func fillNaN(out []float64, rows int) {
	nan := math.NaN()
	for r := 0; r < rows; r++ {
		out[r] = nan
	}
}
