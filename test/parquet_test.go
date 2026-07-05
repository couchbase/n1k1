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

// Parquet columnar-SOURCE prototype for DESIGN-col.md § Proposed approach,
// Step 3. It proves, with a real pure-Go Parquet file, the three things the
// design says a columnar source buys n1k1:
//
//  1. Projection pushdown ("only these columns") -- read one column of a wide
//     file instead of the whole row (§ Pushdown, down direction).
//  2. Free schema/stats metadata from the footer -- physical types, null_count,
//     min/max -- with NO data pages read (§ Pushdown, up direction). null_count=0
//     means "no validity bitmap needed" => the unmasked kernel (§ the tail).
//  3. A parse-free fixed-width column: Arrow hands back a borrowed contiguous
//     []float64 the fast SUM kernel runs straight over -- at the same ceiling the
//     § Spike results measured, but with NO JSON parse to build it.
//
// Uses apache/arrow-go/v18 (already an indirect dep of n1k1; the same library
// glue's iceberg_reader builds on). This is a *prototype* -- it reads Arrow's
// materialized column, i.e. it does NOT yet demonstrate the zero-copy borrow all
// the way into a n1k1 op; that is the Step-5 integration.
//
//   go test -tags n1ql ./test/ -run TestParquetReport -v      # metadata + pushdown
//   go test -tags n1ql ./test/ -run='^$' -bench=BenchmarkPQ    # SUM: arrow vs row-JSON

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/buger/jsonparser"

	"github.com/couchbase/n1k1/glue"
	"github.com/couchbase/n1k1/records"
)

const pqPriceCol = 1 // leaf index: id=0, price=1, then f0..f{width-1}.

func pqPrice(i int) float64 { return float64(i%1000) + 0.5 }

// pqWrite writes n rows of {id int64, price float64, f0..f{width-1} string}
// (Snappy + dictionary, one row group) -- a "wide" record where a query wants
// only the one numeric column.
func pqWrite(t testing.TB, path string, n, width int) {
	mem := memory.NewGoAllocator()
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}
	for f := 0; f < width; f++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("f%d", f), Type: arrow.BinaryTypes.String})
	}
	schema := arrow.NewSchema(fields, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for i := 0; i < n; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.Float64Builder).Append(pqPrice(i))
		for f := 0; f < width; f++ {
			b.Field(2 + f).(*array.StringBuilder).Append("v" + strconv.Itoa(i) + "_" + strconv.Itoa(f))
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	fout, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fout.Close()
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
	)
	if err := pqarrow.WriteTable(tbl, fout, int64(n), props, pqarrow.DefaultWriterProps()); err != nil {
		t.Fatal(err)
	}
}

func pqAllRowGroups(pf *file.Reader) []int {
	rgs := make([]int, pf.NumRowGroups())
	for i := range rgs {
		rgs[i] = i
	}
	return rgs
}

// pqSumColumn sums a projected single-column table's float64 values -- straight
// over the borrowed Arrow buffer, NO JSON parse.
func pqSumColumn(tbl arrow.Table) float64 {
	var sum float64
	for _, chunk := range tbl.Column(0).Data().Chunks() {
		for _, v := range chunk.(*array.Float64).Float64Values() {
			sum += v
		}
	}
	return sum
}

// ---- (1)+(2) metadata + projection-pushdown report --------------------

func TestParquetReport(t *testing.T) {
	const n, width = 1 << 17, 12 // modest: keeps the suite fast; benchmarks go bigger.
	path := filepath.Join(t.TempDir(), "data.parquet")
	pqWrite(t, path, n, width)

	fi, _ := os.Stat(path)
	pf, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	md := pf.MetaData()

	t.Logf("file: %d rows, %d cols, %d row groups, %.1f MB on disk",
		md.NumRows, md.Schema.NumColumns(), md.NumRowGroups(), float64(fi.Size())/1e6)

	// (2) Schema + per-column stats straight from the footer -- "metadata up" is free.
	t.Log("--- footer metadata (no data pages read) ---")
	var priceBytes, totalBytes int64
	for c := 0; c < md.Schema.NumColumns(); c++ {
		cc, _ := md.RowGroup(0).ColumnChunk(c)
		totalBytes += cc.TotalCompressedSize()
		if c == pqPriceCol {
			priceBytes = cc.TotalCompressedSize()
		}
		if c > 2 && c != pqPriceCol {
			continue // don't spam every filler column
		}
		line := fmt.Sprintf("  col[%d] %-7s type=%-10s", c, md.Schema.Column(c).Name(), md.Schema.Column(c).PhysicalType())
		if st, err := cc.Statistics(); err == nil && st != nil {
			line += fmt.Sprintf(" null_count=%d", st.NullCount())
			if fst, ok := st.(*metadata.Float64Statistics); ok && fst.HasMinMax() {
				line += fmt.Sprintf(" min=%.1f max=%.1f", fst.Min(), fst.Max())
			}
		}
		t.Log(line)
	}
	t.Log("  => price null_count=0 => NO validity bitmap => unmasked kernel")

	// (1) Projection pushdown: read only price vs all columns.
	pr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rgs := pqAllRowGroups(pf)

	t0 := time.Now()
	projTbl, err := pr.ReadRowGroups(ctx, []int{pqPriceCol}, rgs)
	if err != nil {
		t.Fatal(err)
	}
	projDur := time.Since(t0)
	defer projTbl.Release()

	t0 = time.Now()
	allTbl, err := pr.ReadTable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	allDur := time.Since(t0)
	defer allTbl.Release()

	t.Log("--- projection pushdown (read+decode) ---")
	t.Logf("  price only : %6.2f ms,  ~%.2f MB of column-chunk bytes",
		float64(projDur.Microseconds())/1000, float64(priceBytes)/1e6)
	t.Logf("  all %2d cols: %6.2f ms,  ~%.2f MB of column-chunk bytes",
		md.Schema.NumColumns(), float64(allDur.Microseconds())/1000, float64(totalBytes)/1e6)
	t.Logf("  => projection reads %.1f%% of the bytes, %.0fx faster to materialize",
		100*float64(priceBytes)/float64(totalBytes), float64(allDur)/float64(projDur))

	if got := pqSumColumn(projTbl); got == 0 {
		t.Fatal("unexpected zero sum")
	}
}

// ---- end-to-end: run a real SQL++ query against a .parquet keyspace --------
// This is the actual Step-3 feature (not a benchmark): a .parquet file dropped
// into a keyspace dir is queryable through n1k1's normal FileStore/scan path,
// via the records.parquetSource transpose-to-rows.

func TestParquetQueryEndToEnd(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "orders")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	// 6 rows of {id, price, f0}; id=i, price=i+0.5.
	pqWrite(t, filepath.Join(ks, "orders.parquet"), 6, 1)

	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// (a) projection + ORDER BY over the parquet-sourced rows.
	res, err := sess.Run(`SELECT id, price FROM orders ORDER BY id`)
	if err != nil {
		t.Fatalf("Run select: %v", err)
	}
	if res.Count != 6 {
		t.Fatalf("row count = %d, want 6; rows=%v", res.Count, rowStrings(res))
	}
	if got := string(res.Rows[0]); got != `{"id":0,"price":0.5}` {
		t.Errorf("row[0] = %s, want {\"id\":0,\"price\":0.5}", got)
	}
	if got := string(res.Rows[5]); got != `{"id":5,"price":5.5}` {
		t.Errorf("row[5] = %s, want {\"id\":5,\"price\":5.5}", got)
	}

	// (b) filter + aggregate: proves the values are real numbers, not strings.
	res, err = sess.Run(`SELECT COUNT(*) AS c, SUM(price) AS s FROM orders WHERE price > 2`)
	if err != nil {
		t.Fatalf("Run agg: %v", err)
	}
	// price>2 keeps 2.5,3.5,4.5,5.5 => c=4, s=16.
	if got := string(res.Rows[0]); got != `{"c":4,"s":16}` {
		t.Errorf("agg = %s, want {\"c\":4,\"s\":16}", got)
	}
	t.Logf("OK: SELECT over orders.parquet -> %d rows; agg over WHERE price>2 -> %s",
		6, string(res.Rows[0]))
}

// TestParquetSidecars exercises the Step-4 optional capability interfaces on the
// Parquet source in isolation (no glue): ColumnsSource (schema/stats from the
// footer) and ColumnsProjector (only-these-columns pushdown).
func TestParquetSidecars(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.parquet")
	pqWrite(t, path, 4, 1) // columns: id, price, f0

	// ColumnsSource: types + null_count + min/max, no data pages read.
	src, err := records.OpenFile(path, "")
	if err != nil {
		t.Fatal(err)
	}
	cs, ok := src.(records.ColumnsSource)
	if !ok {
		t.Fatal("parquet source should implement records.ColumnsSource")
	}
	byName := map[string]records.ColumnMeta{}
	for _, c := range cs.Columns() {
		byName[c.Name] = c
	}
	if c := byName["id"]; c.Type != "INT64" || c.NullCount != 0 {
		t.Errorf("id meta = %+v, want INT64 null_count=0", c)
	}
	if c := byName["price"]; c.Type != "DOUBLE" || c.NullCount != 0 || c.Min == nil {
		t.Errorf("price meta = %+v, want DOUBLE null_count=0 with min/max", c)
	}
	src.Close()

	// ColumnsProjector: only "price" is decoded and yielded.
	src, err = records.OpenFile(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	pj, ok := src.(records.ColumnsProjector)
	if !ok {
		t.Fatal("parquet source should implement records.ColumnsProjector")
	}
	if err := pj.ProjectColumns([]string{"price"}); err != nil {
		t.Fatalf("ProjectColumns: %v", err)
	}
	var rec records.Record
	rows := 0
	for {
		more, err := src.Next(&rec)
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			break
		}
		rows++
		if doc := string(rec.Doc); !strings.Contains(doc, `"price"`) ||
			strings.Contains(doc, `"id"`) || strings.Contains(doc, `"f0"`) {
			t.Fatalf("projected doc = %s, want only price", doc)
		}
	}
	if rows != 4 {
		t.Fatalf("projected rows = %d, want 4", rows)
	}

	// Unknown column is an error.
	src3, _ := records.OpenFile(path, "")
	defer src3.Close()
	if err := src3.(records.ColumnsProjector).ProjectColumns([]string{"nope"}); err == nil {
		t.Error("ProjectColumns(nope) should error on unknown column")
	}
	t.Log("OK: ColumnsSource schema + ColumnsProjector projection verified")
}

func rowStrings(res *glue.Result) []string {
	out := make([]string, len(res.Rows))
	for i, r := range res.Rows {
		out[i] = string(r)
	}
	return out
}

// ---- (3) SUM kernel: arrow column (parse-free) vs row-JSON baseline ----

var pqBenchPath string
var pqSink float64

func pqSetupBench(b *testing.B) string {
	if pqBenchPath == "" {
		dir, _ := os.MkdirTemp("", "pqbench")
		pqBenchPath = filepath.Join(dir, "bench.parquet")
		pqWrite(b, pqBenchPath, 1<<20, 8)
	}
	return pqBenchPath
}

// Columnar source, full path: open parquet, project+read price, sum. Re-opens
// each iteration (worst case; a real query reads once), so this includes Snappy
// decode + Arrow materialization.
func BenchmarkPQ_Sum_ArrowColumn_full(b *testing.B) {
	path := pqSetupBench(b)
	pf, _ := file.OpenParquetFile(path, false)
	n := int(pf.NumRows())
	pf.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pf, _ := file.OpenParquetFile(path, false)
		pr, _ := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		tbl, _ := pr.ReadRowGroups(context.Background(), []int{pqPriceCol}, pqAllRowGroups(pf))
		pqSink = pqSumColumn(tbl)
		tbl.Release()
		pf.Close()
	}
	pqPerValue(b, n)
}

// Kernel only, over an already-materialized Arrow column (parse-free bound --
// compare against § Spike results fixed-width ~0.9 ns/value).
func BenchmarkPQ_Sum_ArrowColumn_kernel(b *testing.B) {
	path := pqSetupBench(b)
	pf, _ := file.OpenParquetFile(path, false)
	pr, _ := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	tbl, _ := pr.ReadRowGroups(context.Background(), []int{pqPriceCol}, pqAllRowGroups(pf))
	defer tbl.Release()
	defer pf.Close()
	n := int(pf.NumRows())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pqSink = pqSumColumn(tbl)
	}
	pqPerValue(b, n)
}

// n1k1-today baseline: the same data as JSON docs, jsonparser.GetFloat per row
// (i.e. transpose parquet -> JSON rows, run today's row engine).
func BenchmarkPQ_Sum_RowJSON(b *testing.B) {
	const n = 1 << 20
	docs := make([][]byte, n)
	for i := range docs {
		d := []byte(`{"id":`)
		d = strconv.AppendInt(d, int64(i), 10)
		d = append(d, `,"price":`...)
		d = strconv.AppendFloat(d, pqPrice(i), 'g', -1, 64)
		docs[i] = append(d, '}')
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum float64
		for _, doc := range docs {
			v, _ := jsonparser.GetFloat(doc, "price")
			sum += v
		}
		pqSink = sum
	}
	pqPerValue(b, n)
}

func pqPerValue(b *testing.B, n int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/value")
}
