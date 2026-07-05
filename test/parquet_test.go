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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
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
func pqWrite(t testing.TB, path string, n, width int) { pqWriteBase(t, path, 0, n, width) }

// pqWriteBase is pqWrite with an id/base offset, so multiple part files get
// disjoint ids (id = base+i, price = pqPrice(base+i)).
func pqWriteBase(t testing.TB, path string, base, n, width int) {
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
		id := base + i
		b.Field(0).(*array.Int64Builder).Append(int64(id))
		b.Field(1).(*array.Float64Builder).Append(pqPrice(id))
		for f := 0; f < width; f++ {
			b.Field(2 + f).(*array.StringBuilder).Append("v" + strconv.Itoa(id) + "_" + strconv.Itoa(f))
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

	// Unknown columns are tolerated (skipped), not errors: a field absent from the
	// file reads as MISSING downstream anyway. A mix keeps the known one.
	src3, _ := records.OpenFile(path, "")
	defer src3.Close()
	if err := src3.(records.ColumnsProjector).ProjectColumns([]string{"price", "nope"}); err != nil {
		t.Fatalf("ProjectColumns(price,nope): %v", err)
	}
	var r3 records.Record
	if ok, err := src3.Next(&r3); err != nil || !ok {
		t.Fatalf("Next: ok=%v err=%v", ok, err)
	}
	if doc := string(r3.Doc); !strings.Contains(doc, `"price"`) ||
		strings.Contains(doc, `"nope"`) || strings.Contains(doc, `"id"`) {
		t.Fatalf("mixed projection doc = %s, want only price", doc)
	}
	t.Log("OK: ColumnsSource schema + ColumnsProjector projection (incl. tolerant unknown)")
}

// TestParquetProjectionDifferential is the correctness guardrail for Step-4
// caller-side pushdown: for a battery of query shapes, results with column
// projection ON must exactly equal results with it forced OFF. It also asserts
// the projection actually fires (and is correctly absent for SELECT *).
func TestParquetProjectionDifferential(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "sales2")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	// Two part files (exercises the walkSource projection forwarding); cols:
	// id, price, f0, f1.  ids 0..9 and 100..109.
	pqWrite(t, filepath.Join(ks, "part-0.parquet"), 10, 2)
	pqWriteBase(t, filepath.Join(ks, "part-1.parquet"), 100, 10, 2)

	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}

	queries := []struct {
		name    string
		stmt    string
		projects bool // whether pushdown should fire (a determinable field subset)
	}{
		{"project-order", `SELECT id, price FROM sales2 ORDER BY id`, true},
		{"filter-agg", `SELECT COUNT(*) AS c, SUM(price) AS s FROM sales2 WHERE price > 3`, true},
		{"one-field", `SELECT f0 FROM sales2 ORDER BY id`, true},
		// Aliased keyspace: cbq's formalizer rewrites field refs to the alias, and
		// EarlyProjection resolves them to bare column names -- so projection still
		// fires correctly (the alias never reaches the source).
		{"alias-explicit", `SELECT o.price FROM sales2 AS o ORDER BY o.id`, true},
		{"alias-where", `SELECT id, price FROM sales2 s WHERE s.price > 3 ORDER BY s.id`, true},
		// A field set IS pushed (the planner names it), but the Parquet source finds
		// no such column and harmlessly falls back to read-all -- so the request
		// still "fires" at the source. Correctness is proven by the ON==OFF compare.
		{"absent-field", `SELECT nonexistent FROM sales2 ORDER BY id`, true},
		{"star", `SELECT * FROM sales2 ORDER BY id`, false}, // whole doc => no pushdown attached
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			glue.DisableColumnProjection = true
			base := runSortedRows(t, sess, q.stmt)
			glue.DisableColumnProjection = false
			before := atomic.LoadInt64(&glue.ColumnProjectionApplied)
			got := runSortedRows(t, sess, q.stmt)
			applied := atomic.LoadInt64(&glue.ColumnProjectionApplied) - before

			if strings.Join(base, "\n") != strings.Join(got, "\n") {
				t.Fatalf("projection changed results!\n OFF: %v\n ON:  %v", base, got)
			}
			if q.projects && applied == 0 {
				t.Errorf("expected projection to fire, but it didn't")
			}
			if !q.projects && applied != 0 {
				t.Errorf("expected NO projection, but %d fired", applied)
			}
		})
	}
	glue.DisableColumnProjection = false
}

func runSortedRows(t *testing.T, sess *glue.Session, stmt string) []string {
	t.Helper()
	res, err := sess.Run(stmt)
	if err != nil {
		t.Fatalf("Run(%q): %v", stmt, err)
	}
	out := make([]string, len(res.Rows))
	for i, r := range res.Rows {
		out[i] = string(r)
	}
	sort.Strings(out)
	return out
}

// TestParquetSumVectorizedDifferential is the Step-5.1 correctness guardrail: a
// battery of SUM queries over a Parquet keyspace must return identical results
// with the vectorized columnar-agg lane ON vs forced OFF (row path). It also
// checks the vectorized lane fires when it should and correctly bails otherwise.
func TestParquetSumVectorizedDifferential(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "sales3")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	// Two part files, cols id(int64), price(float64), f0(string); disjoint ids.
	pqWrite(t, filepath.Join(ks, "part-0.parquet"), 30, 1)
	pqWriteBase(t, filepath.Join(ks, "part-1.parquet"), 1000, 30, 1)

	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}

	queries := []struct {
		name       string
		stmt       string
		vectorizes bool
	}{
		{"sum-float", `SELECT SUM(price) AS s FROM sales3`, true},
		{"sum-int", `SELECT SUM(id) AS s FROM sales3`, true},
		{"avg-float", `SELECT AVG(price) AS a FROM sales3`, true},
		{"avg-int", `SELECT AVG(id) AS a FROM sales3`, true},
		{"multi-agg", `SELECT SUM(price) AS sp, SUM(id) AS si FROM sales3`, true},
		{"mixed-agg", `SELECT SUM(price) AS s, COUNT(price) AS c, AVG(price) AS a FROM sales3`, true},
		{"aliased", `SELECT SUM(o.price) AS s FROM sales3 AS o`, true},
		// 5.4c: single-comparison WHERE fuses into the columnar-agg lane.
		{"where-gt-float", `SELECT SUM(price) AS s FROM sales3 WHERE price > 10`, true},
		{"where-ge-float", `SELECT SUM(price) AS s FROM sales3 WHERE price >= 10`, true},
		{"where-lt-float", `SELECT SUM(price) AS s FROM sales3 WHERE price < 50`, true},
		{"where-le-int", `SELECT SUM(price) AS s FROM sales3 WHERE id <= 15`, true},
		{"where-gt-int", `SELECT AVG(price) AS a FROM sales3 WHERE id > 20`, true},
		{"where-eq-int", `SELECT SUM(price) AS s FROM sales3 WHERE id = 5`, true},
		{"where-const-first", `SELECT SUM(price) AS s FROM sales3 WHERE 10 < price`, true}, // flipped operand order
		{"where-count", `SELECT COUNT(price) AS c FROM sales3 WHERE price > 10`, true},     // count+filter → columnar, not metadata
		{"where-multi", `SELECT SUM(price) AS sp, COUNT(id) AS c FROM sales3 WHERE price > 10`, true},
		// Bails to the row path: predicate not a single numeric field-vs-const compare.
		{"where-noninteger-int", `SELECT SUM(price) AS s FROM sales3 WHERE id > 10.5`, false}, // non-int const vs int col
		{"where-field-field", `SELECT SUM(price) AS s FROM sales3 WHERE price > id`, false},   // field vs field
		{"where-and", `SELECT SUM(price) AS s FROM sales3 WHERE price > 10 AND id < 20`, false}, // conjunction
		{"where-string", `SELECT SUM(price) AS s FROM sales3 WHERE f0 = "x"`, false},           // non-numeric predicate col
		{"count-star", `SELECT COUNT(*) AS c FROM sales3`, false},                              // not SUM
		{"sum-string", `SELECT SUM(f0) AS s FROM sales3`, false},                               // non-numeric column
		{"grouped", `SELECT f0, SUM(price) AS s FROM sales3 GROUP BY f0`, false},               // has GROUP BY
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			glue.DisableVectorizedAgg = true
			base := runSortedRows(t, sess, q.stmt)
			glue.DisableVectorizedAgg = false
			before := atomic.LoadInt64(&glue.VectorizedAggApplied)
			got := runSortedRows(t, sess, q.stmt)
			applied := atomic.LoadInt64(&glue.VectorizedAggApplied) - before

			if strings.Join(base, "\n") != strings.Join(got, "\n") {
				t.Fatalf("vectorized changed results!\n OFF: %v\n ON:  %v", base, got)
			}
			if q.vectorizes && applied == 0 {
				t.Errorf("expected the columnar-agg lane to fire, but it didn't")
			}
			if !q.vectorizes && applied != 0 {
				t.Errorf("expected the row path, but the columnar-agg lane fired %d times", applied)
			}
		})
	}
	glue.DisableVectorizedAgg = false
}

// TestParquetMetadataAggDifferential guards the metadata-agg lane (COUNT/MIN/MAX
// answered from footer stats, zero scan): results must equal the row path, the
// lane fires when it should, and it bails (row path) for non-numeric MIN/MAX or
// when any aggregate needs a scan.
func TestParquetMetadataAggDifferential(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "sales4")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	pqWrite(t, filepath.Join(ks, "part-0.parquet"), 30, 1)       // id 0..29,     price i+0.5
	pqWriteBase(t, filepath.Join(ks, "part-1.parquet"), 1000, 30, 1) // id 1000..1029, price 1000+i+0.5

	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}

	queries := []struct {
		name string
		stmt string
		meta bool
	}{
		{"count-star", `SELECT COUNT(*) AS c FROM sales4`, true},
		{"count-field", `SELECT COUNT(id) AS c FROM sales4`, true},
		{"min-float", `SELECT MIN(price) AS m FROM sales4`, true},
		{"max-float", `SELECT MAX(price) AS m FROM sales4`, true},
		{"min-int", `SELECT MIN(id) AS m FROM sales4`, true},
		{"max-int", `SELECT MAX(id) AS m FROM sales4`, true},
		{"count-min-max", `SELECT COUNT(*) AS c, MIN(price) AS mn, MAX(id) AS mx FROM sales4`, true},
		{"min-string", `SELECT MIN(f0) AS m FROM sales4`, false},              // non-numeric → row path
		{"sum-plus-min", `SELECT SUM(price) AS s, MIN(price) AS m FROM sales4`, false}, // needs scan + min → row path
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			glue.DisableVectorizedAgg = true
			base := runSortedRows(t, sess, q.stmt)
			glue.DisableVectorizedAgg = false
			before := atomic.LoadInt64(&glue.MetadataAggApplied)
			got := runSortedRows(t, sess, q.stmt)
			applied := atomic.LoadInt64(&glue.MetadataAggApplied) - before

			if strings.Join(base, "\n") != strings.Join(got, "\n") {
				t.Fatalf("metadata-agg changed results!\n OFF: %v\n ON:  %v", base, got)
			}
			if q.meta && applied == 0 {
				t.Errorf("expected the metadata-agg lane to fire, but it didn't")
			}
			if !q.meta && applied != 0 {
				t.Errorf("expected the row path, but the metadata-agg lane fired %d times", applied)
			}
		})
	}
	glue.DisableVectorizedAgg = false
}

func rowStrings(res *glue.Result) []string {
	out := make([]string, len(res.Rows))
	for i, r := range res.Rows {
		out[i] = string(r)
	}
	return out
}

// ---- hand-rolled zero-alloc transpose: equivalence + allocation guard --------

// pqWriteVaried writes one row group covering the fast writer's type range plus
// nulls and nasty strings, so the equivalence test exercises every branch.
func pqWriteVaried(t testing.TB, path string) int {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "u", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	strs := []string{`plain`, `has "quotes"`, "tab\tnl\ncr\r", "unicode→π", "ctrl\x01\x1fx", ""}
	bld := array.NewRecordBuilder(mem, schema)
	defer bld.Release()
	for i := range strs {
		bld.Field(0).(*array.Int64Builder).Append(int64(i*100 - 250)) // incl. negative
		bld.Field(1).(*array.Uint32Builder).Append(uint32(i * 7))
		bld.Field(2).(*array.Float64Builder).Append(float64(i) + 0.25)
		bld.Field(3).(*array.StringBuilder).Append(strs[i])
		bld.Field(4).(*array.BooleanBuilder).Append(i%2 == 0)
		if i%3 == 0 {
			bld.Field(5).(*array.Int32Builder).AppendNull()
		} else {
			bld.Field(5).(*array.Int32Builder).Append(int32(i))
		}
	}
	rec := bld.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()
	fout, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fout.Close()
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	if err := pqarrow.WriteTable(tbl, fout, int64(len(strs)), props, pqarrow.DefaultWriterProps()); err != nil {
		t.Fatal(err)
	}
	return len(strs)
}

// drainDocs reads every record doc from a parquet file, copying each (borrowed)
// doc out; disableFast selects the RecordToJSON fallback vs the fast writer.
func drainDocs(t testing.TB, path string, disableFast bool) [][]byte {
	records.DisableFastTranspose = disableFast
	defer func() { records.DisableFastTranspose = false }()
	src, err := records.OpenFile(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	var out [][]byte
	var rec records.Record
	for {
		ok, err := src.Next(&rec)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		out = append(out, append([]byte(nil), rec.Doc...))
	}
	return out
}

// TestParquetFastTransposeEquivalence proves the hand-rolled zero-alloc writer
// produces JSON semantically identical to arrow's array.RecordToJSON (both must
// be valid JSON that unmarshals to equal values), across ints/uints/floats/
// strings-with-escapes/bools/nulls.
func TestParquetFastTransposeEquivalence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "varied.parquet")
	n := pqWriteVaried(t, path)

	fast := drainDocs(t, path, false)
	slow := drainDocs(t, path, true)

	if len(fast) != n || len(slow) != n {
		t.Fatalf("row counts: fast=%d slow=%d want %d", len(fast), len(slow), n)
	}
	for i := range fast {
		var mf, ms map[string]interface{}
		if err := json.Unmarshal(fast[i], &mf); err != nil {
			t.Fatalf("fast doc %d is not valid JSON: %s: %v", i, fast[i], err)
		}
		if err := json.Unmarshal(slow[i], &ms); err != nil {
			t.Fatalf("slow doc %d: %v", i, err)
		}
		if !reflect.DeepEqual(mf, ms) {
			t.Errorf("row %d differs:\n fast=%s\n slow=%s", i, fast[i], slow[i])
		}
	}
	t.Logf("OK: fast writer == RecordToJSON across %d varied rows", n)
}

// BenchmarkParquetTransposeDrain guards the zero-alloc property of the fast
// transpose (compare -benchmem allocs/op against DisableFastTranspose).
func BenchmarkParquetTransposeDrain(b *testing.B) {
	dir, _ := os.MkdirTemp("", "pqtd")
	path := filepath.Join(dir, "d.parquet")
	pqWrite(b, path, 1<<16, 4) // 65536 rows, cols id,price,f0..f3
	b.ReportAllocs()
	b.ResetTimer()
	var rows int
	for i := 0; i < b.N; i++ {
		src, err := records.OpenFile(path, "")
		if err != nil {
			b.Fatal(err)
		}
		var rec records.Record
		for {
			ok, err := src.Next(&rec)
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
			rows++
			_ = rec.Doc
		}
		src.Close()
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(rows), "ns/row")
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
