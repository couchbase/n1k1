package colspike

// Ceiling spike for DESIGN-col.md: does fixed-width columnar beat n1k1's
// row-at-a-time JSON path on Apple Silicon (arm64), PURE-GO SCALAR, no SIMD?
//
// Row path is faithful to what n1k1 does today: each record is a whole JSON
// document (one []byte), and projecting a numeric field means jsonparser.GetFloat
// per row (n1k1 scans JSONL -> yields the doc Val -> project/agg reads the field).
//
// Columnar paths reinterpret the SAME bytes-based idea:
//   - colJSONArray: one []byte holding "[v0,v1,...]" (DESIGN-col.md encoding 1)
//   - colFixedWidth: one []byte of N little-endian float64s (encoding 2)
//   - nativeSlice: a real []float64 (the theoretical ceiling / decode-free bound)

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"

	"github.com/buger/jsonparser"
)

var sink float64
var sinkN int

// ---- data generation ---------------------------------------------------

func price(i int) float64 { return float64(i%1000) + 0.5 }

// wide JSON doc with W total fields; "price" sits at the END (worst case for a
// left-to-right parser -- and the realistic case where the field you want isn't
// first). Returns one doc's bytes.
func makeDoc(i, w int) []byte {
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
	b = strconv.AppendFloat(b, price(i), 'g', -1, 64)
	b = append(b, '}')
	return b
}

func makeRowDocs(n, w int) [][]byte {
	docs := make([][]byte, n)
	for i := range docs {
		docs[i] = makeDoc(i, w)
	}
	return docs
}

func makeJSONArray(n int) []byte {
	b := []byte{'['}
	for i := 0; i < n; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendFloat(b, price(i), 'g', -1, 64)
	}
	return append(b, ']')
}

func makeFixedWidth(n int) []byte {
	b := make([]byte, n*8)
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(b[i*8:], math.Float64bits(price(i)))
	}
	return b
}

func makeNativeSlice(n int) []float64 {
	s := make([]float64, n)
	for i := range s {
		s[i] = price(i)
	}
	return s
}

// ---- SUM kernels -------------------------------------------------------

func sumRowJSON(docs [][]byte) float64 {
	var sum float64
	for _, doc := range docs {
		v, _ := jsonparser.GetFloat(doc, "price")
		sum += v
	}
	return sum
}

func sumJSONArray(arr []byte) float64 {
	var sum float64
	jsonparser.ArrayEach(arr, func(v []byte, _ jsonparser.ValueType, _ int, _ error) {
		f, _ := strconv.ParseFloat(string(v), 64)
		sum += f
	})
	return sum
}

func sumFixedWidth(col []byte) float64 {
	var sum float64
	for i := 0; i+8 <= len(col); i += 8 {
		sum += math.Float64frombits(binary.LittleEndian.Uint64(col[i:]))
	}
	return sum
}

func sumNativeSlice(s []float64) float64 {
	var sum float64
	for _, v := range s {
		sum += v
	}
	return sum
}

// ---- filter-count kernels (price > 500) --------------------------------

func filterRowJSON(docs [][]byte) int {
	n := 0
	for _, doc := range docs {
		v, _ := jsonparser.GetFloat(doc, "price")
		if v > 500 {
			n++
		}
	}
	return n
}

func filterFixedWidth(col []byte) int {
	n := 0
	for i := 0; i+8 <= len(col); i += 8 {
		if math.Float64frombits(binary.LittleEndian.Uint64(col[i:])) > 500 {
			n++
		}
	}
	return n
}

// ---- benchmarks: SUM, sweep N (narrow doc, W=1) ------------------------

func benchSizes() []int { return []int{64, 1024, 65536, 1048576} }

func BenchmarkSum_RowJSON_narrow(b *testing.B) {
	for _, n := range benchSizes() {
		docs := makeRowDocs(n, 1)
		b.Run(sz(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = sumRowJSON(docs)
			}
			perValue(b, n)
		})
	}
}

func BenchmarkSum_ColJSONArray(b *testing.B) {
	for _, n := range benchSizes() {
		arr := makeJSONArray(n)
		b.Run(sz(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = sumJSONArray(arr)
			}
			perValue(b, n)
		})
	}
}

func BenchmarkSum_ColFixedWidth(b *testing.B) {
	for _, n := range benchSizes() {
		col := makeFixedWidth(n)
		b.Run(sz(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = sumFixedWidth(col)
			}
			perValue(b, n)
		})
	}
}

func BenchmarkSum_NativeSlice(b *testing.B) {
	for _, n := range benchSizes() {
		s := makeNativeSlice(n)
		b.Run(sz(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = sumNativeSlice(s)
			}
			perValue(b, n)
		})
	}
}

// ---- benchmarks: doc WIDTH sweep (the "vertical stripe" axis, N=1M) -----
// Row path pays more as the doc widens (parser scans past unwanted fields);
// the fixed-width column is identical regardless -- it only touches its stripe.

func benchWidths() []int { return []int{1, 5, 20, 50} }

func BenchmarkSum_RowJSON_byWidth(b *testing.B) {
	const n = 1048576
	for _, w := range benchWidths() {
		docs := makeRowDocs(n, w)
		b.Run(wd(w), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = sumRowJSON(docs)
			}
			perValue(b, n)
		})
	}
}

// ---- benchmarks: FILTER count (price > 500), N=1M, narrow --------------

func BenchmarkFilter_RowJSON(b *testing.B) {
	const n = 1048576
	docs := makeRowDocs(n, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkN = filterRowJSON(docs)
	}
	perValue(b, n)
}

func BenchmarkFilter_ColFixedWidth(b *testing.B) {
	const n = 1048576
	col := makeFixedWidth(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkN = filterFixedWidth(col)
	}
	perValue(b, n)
}

// ---- helpers -----------------------------------------------------------

func sz(n int) string {
	switch {
	case n >= 1048576:
		return "N=1M"
	case n >= 1024:
		return "N=" + strconv.Itoa(n/1024) + "K"
	default:
		return "N=" + strconv.Itoa(n)
	}
}

func wd(w int) string { return "W=" + strconv.Itoa(w) }

// perValue reports ns spent per value processed, the comparable unit across
// benchmarks with different N.
func perValue(b *testing.B, n int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/value")
}
