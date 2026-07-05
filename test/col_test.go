//go:build n1ql

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

package test

// Columnar "ceiling spike" for DESIGN-col.md § Proposed approach, Step 2: does
// fixed-width columnar beat n1k1's row-at-a-time JSON path, in PURE GO / NO SIMD,
// on Apple Silicon (arm64)?
//
// The row path is faithful to what n1k1 does today: each record is a whole JSON
// document (one []byte), and projecting a numeric field means jsonparser.GetFloat
// per row (n1k1 scans JSONL -> yields the doc Val -> project/agg reads the field,
// via the same buger/jsonparser the engine uses). The columnar paths reinterpret
// the SAME bytes-based idea (DESIGN-col.md "transpose the Val/Vals axes"):
//   - colJSONArray:  one []byte holding "[v0,v1,...]"        (encoding 1)
//   - colFixedWidth: one []byte of N little-endian float64s  (encoding 2)
//   - colNativeSlice: a real []float64 (theoretical ceiling / decode-free bound)
//
// Run just this file's benchmarks (from repo root, after the DESIGN-testing.md
// worktree bootstrap):
//
//	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' \
//	  go test -tags n1ql ./test/ -run='^$' -bench='BenchmarkCol' -benchmem
//
// Benchmarks:
//   - Col_Sum_RowJSON_narrow  -- n1k1 today: whole JSON doc/record, GetFloat/row.
//   - Col_Sum_JSONArray       -- encoding 1: one JSON array, parsed per value.
//   - Col_Sum_FixedWidth      -- encoding 2: one []byte of LE float64s.
//   - Col_Sum_NativeSlice     -- theoretical ceiling: a real []float64.
//   - Col_Sum_RowJSON_byWidth -- sweeps doc width; the "vertical stripe" axis.
//   - Col_Filter_{RowJSON,FixedWidth} -- price > 500 count.
//
// Results and full analysis live in DESIGN-col.md § Spike results. Headline:
// fixed-width is ~44x the row path (at the native-[]float64 ceiling) with NO
// SIMD, growing to ~730x as documents widen -- the win is *skipping JSON parsing*
// and *touching only one column stripe*, not vector width.

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"

	"github.com/buger/jsonparser"
)

var colSink float64
var colSinkN int

// ---- data generation ---------------------------------------------------

func colPrice(i int) float64 { return float64(i%1000) + 0.5 }

// colMakeDoc builds one JSON doc with w total fields; "price" sits at the END
// (worst case for a left-to-right parser -- and the realistic case where the
// wanted field isn't first).
func colMakeDoc(i, w int) []byte {
	b := []byte{'{'}
	for f := 0; f < w-1; f++ {
		if f > 0 {
			b = append(b, ',')
		}
		b = append(b, '"', 'f')
		b = strconv.AppendInt(b, int64(f), 10)
		b = append(b, '"', ':')
		b = strconv.AppendInt(b, int64(i*7+f), 10)
	}
	if w > 1 {
		b = append(b, ',')
	}
	b = append(b, `"price":`...)
	b = strconv.AppendFloat(b, colPrice(i), 'g', -1, 64)
	return append(b, '}')
}

func colMakeRowDocs(n, w int) [][]byte {
	docs := make([][]byte, n)
	for i := range docs {
		docs[i] = colMakeDoc(i, w)
	}
	return docs
}

func colMakeJSONArray(n int) []byte {
	b := []byte{'['}
	for i := 0; i < n; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendFloat(b, colPrice(i), 'g', -1, 64)
	}
	return append(b, ']')
}

func colMakeFixedWidth(n int) []byte {
	b := make([]byte, n*8)
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(b[i*8:], math.Float64bits(colPrice(i)))
	}
	return b
}

func colMakeNativeSlice(n int) []float64 {
	s := make([]float64, n)
	for i := range s {
		s[i] = colPrice(i)
	}
	return s
}

// ---- SUM kernels -------------------------------------------------------

func colSumRowJSON(docs [][]byte) float64 {
	var sum float64
	for _, doc := range docs {
		v, _ := jsonparser.GetFloat(doc, "price")
		sum += v
	}
	return sum
}

func colSumJSONArray(arr []byte) float64 {
	var sum float64
	jsonparser.ArrayEach(arr, func(v []byte, _ jsonparser.ValueType, _ int, _ error) {
		f, _ := strconv.ParseFloat(string(v), 64)
		sum += f
	})
	return sum
}

func colSumFixedWidth(col []byte) float64 {
	var sum float64
	for i := 0; i+8 <= len(col); i += 8 {
		sum += math.Float64frombits(binary.LittleEndian.Uint64(col[i:]))
	}
	return sum
}

func colSumNativeSlice(s []float64) float64 {
	var sum float64
	for _, v := range s {
		sum += v
	}
	return sum
}

// ---- filter-count kernels (price > 500) --------------------------------

func colFilterRowJSON(docs [][]byte) int {
	n := 0
	for _, doc := range docs {
		v, _ := jsonparser.GetFloat(doc, "price")
		if v > 500 {
			n++
		}
	}
	return n
}

func colFilterFixedWidth(col []byte) int {
	n := 0
	for i := 0; i+8 <= len(col); i += 8 {
		if math.Float64frombits(binary.LittleEndian.Uint64(col[i:])) > 500 {
			n++
		}
	}
	return n
}

// ---- benchmarks: SUM, sweep N (narrow doc, W=1) ------------------------

func colSizes() []int { return []int{64, 1024, 65536, 1048576} }

func BenchmarkCol_Sum_RowJSON_narrow(b *testing.B) {
	for _, n := range colSizes() {
		docs := colMakeRowDocs(n, 1)
		b.Run(colSz(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				colSink = colSumRowJSON(docs)
			}
			colPerValue(b, n)
		})
	}
}

func BenchmarkCol_Sum_JSONArray(b *testing.B) {
	for _, n := range colSizes() {
		arr := colMakeJSONArray(n)
		b.Run(colSz(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				colSink = colSumJSONArray(arr)
			}
			colPerValue(b, n)
		})
	}
}

func BenchmarkCol_Sum_FixedWidth(b *testing.B) {
	for _, n := range colSizes() {
		col := colMakeFixedWidth(n)
		b.Run(colSz(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				colSink = colSumFixedWidth(col)
			}
			colPerValue(b, n)
		})
	}
}

func BenchmarkCol_Sum_NativeSlice(b *testing.B) {
	for _, n := range colSizes() {
		s := colMakeNativeSlice(n)
		b.Run(colSz(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				colSink = colSumNativeSlice(s)
			}
			colPerValue(b, n)
		})
	}
}

// ---- benchmarks: doc WIDTH sweep (the "vertical stripe" axis, N=1M) -----
// The row path pays more as the doc widens (parser scans past unwanted fields);
// the fixed-width column is identical regardless -- it only touches its stripe.

func colWidths() []int { return []int{1, 5, 20, 50} }

func BenchmarkCol_Sum_RowJSON_byWidth(b *testing.B) {
	const n = 1048576
	for _, w := range colWidths() {
		docs := colMakeRowDocs(n, w)
		b.Run(colWd(w), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				colSink = colSumRowJSON(docs)
			}
			colPerValue(b, n)
		})
	}
}

// ---- benchmarks: FILTER count (price > 500), N=1M, narrow --------------

func BenchmarkCol_Filter_RowJSON(b *testing.B) {
	const n = 1048576
	docs := colMakeRowDocs(n, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		colSinkN = colFilterRowJSON(docs)
	}
	colPerValue(b, n)
}

func BenchmarkCol_Filter_FixedWidth(b *testing.B) {
	const n = 1048576
	col := colMakeFixedWidth(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		colSinkN = colFilterFixedWidth(col)
	}
	colPerValue(b, n)
}

// ---- helpers -----------------------------------------------------------

func colSz(n int) string {
	switch {
	case n >= 1048576:
		return "N=1M"
	case n >= 1024:
		return "N=" + strconv.Itoa(n/1024) + "K"
	default:
		return "N=" + strconv.Itoa(n)
	}
}

func colWd(w int) string { return "W=" + strconv.Itoa(w) }

// colPerValue reports ns spent per value processed, the comparable unit across
// benchmarks with different N.
func colPerValue(b *testing.B, n int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/value")
}
