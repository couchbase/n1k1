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

// Vector queries over a real Parquet keyspace with a list<float32> vec column
// (DESIGN-vectors.md). Ground truth is the row lane (cbq's boxed / n1k1's native
// VECTOR_DISTANCE over the JSON-materialized vec).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
	"github.com/couchbase/n1k1/records"
)

// writeVecKS writes n rows of {id int64, vec list<float32>[dim]} (element non-nullable,
// list field nullable) -- the vec column shape an INSERT INTO parquet would produce.
func writeVecKS(t testing.TB, path string, n, dim int) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "vec", Type: arrow.ListOfNonNullable(arrow.PrimitiveTypes.Float32), Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	idB := b.Field(0).(*array.Int64Builder)
	vecB := b.Field(1).(*array.ListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < n; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dim; j++ {
			// Exact binary fractions (k/256) so the float32 storage, its float64 promotion,
			// and the JSON the row lane materializes are all the SAME value -- the columnar
			// vs row-lane differential isolates the math, not float formatting. Distinct
			// per row (magnitude + direction vary) so top-K distances don't tie, keeping
			// the ORDER deterministic across both lanes.
			valB.Append(float32(i*dim+j+1) / 256.0)
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pqarrow.WriteTable(tbl, f, int64(n), parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatal(err)
	}
}

// TestParquetNestedColumnProjection is a regression test: projecting a nested (list)
// column ALONGSIDE a scalar must not drop the nested column. The projection pushdown
// used to resolve names by leaf name only, and a list column's leaf is "vec.list.element"
// (not "vec"), so `SELECT id, VECTOR_DISTANCE(vec,...)` read only `id` and VECTOR_DISTANCE
// saw a MISSING vec -> the `d` field vanished. ProjectColumns now maps a top-level field
// to all its leaves.
func TestParquetNestedColumnProjection(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "vecs")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	writeVecKS(t, filepath.Join(ks, "part-0.parquet"), 4, 4)
	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}
	res, err := sess.Run(`SELECT t.id, VECTOR_DISTANCE(t.vec, [0.1,0.2,0.3,0.4], "l2_squared") AS d FROM vecs t ORDER BY t.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 4 {
		t.Fatalf("got %d rows, want 4", len(res.Rows))
	}
	for i, r := range res.Rows {
		var m map[string]interface{}
		if err := json.Unmarshal(r, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["id"]; !ok {
			t.Errorf("row %d missing id: %s", i, r)
		}
		if _, ok := m["d"]; !ok {
			t.Errorf("row %d missing d (nested vec column dropped by projection): %s", i, r)
		}
	}
}

// TestVectorColumnarMatchesRowLane is the differential correctness anchor for the
// columnar map: VectorColumnarScan (reads the vec column as borrowed float32, computes
// on the byte lane, transposes to rows) must produce the SAME per-row distances as the
// row lane (cbq/native VECTOR_DISTANCE over the JSON-materialized vec). Compared
// numerically per id, across all metrics. Order/limit is the existing row-lane
// machinery, so proving the rows match proves the whole columnar top-K.
func TestVectorColumnarMatchesRowLane(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "vecs")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	const n, dim = 50, 4
	part := filepath.Join(ks, "part-0.parquet")
	writeVecKS(t, part, n, dim)

	q := []float64{0.125, 0.25, 0.375, 0.5}
	const qlit = "[0.125,0.25,0.375,0.5]"

	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}

	num := func(v json.RawMessage) (float64, bool) {
		if len(v) == 0 || string(v) == "null" {
			return 0, false
		}
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}

	for _, metric := range []string{"cosine", "l2", "l2_squared", "dot"} {
		// Oracle: the row lane over the same Parquet keyspace.
		res, err := sess.Run(`SELECT t.id, VECTOR_DISTANCE(t.vec, ` + qlit + `, "` + metric + `") AS d FROM vecs t`)
		if err != nil {
			t.Fatalf("%s row lane: %v", metric, err)
		}
		oracle := map[int64]float64{}
		for _, r := range res.Rows {
			var m struct {
				ID int64           `json:"id"`
				D  json.RawMessage `json:"d"`
			}
			if err := json.Unmarshal(r, &m); err != nil {
				t.Fatal(err)
			}
			if f, ok := num(m.D); ok {
				oracle[m.ID] = f
			}
		}
		if len(oracle) != n {
			t.Fatalf("%s: oracle produced %d numeric distances, want %d", metric, len(oracle), n)
		}

		// Columnar: read the Parquet part directly as a VectorBatchSource.
		src, err := records.OpenFile(part, "")
		if err != nil {
			t.Fatal(err)
		}
		vbs, ok := src.(records.VectorBatchSource)
		if !ok {
			t.Fatalf("part is not a VectorBatchSource")
		}
		seen := 0
		err = glue.VectorColumnarScan(vbs, "vec", []string{"id"}, q, metric, func(row base.Vals) error {
			id, _ := strconv.ParseInt(string(row[0]), 10, 64)
			cf, ok := num(json.RawMessage(row[1]))
			if !ok {
				t.Errorf("%s id %d: columnar distance not numeric: %q", metric, id, row[1])
				return nil
			}
			if of := oracle[id]; cf != of { // bit-exact: identical inputs + identical float64 ops
				t.Errorf("%s id %d: columnar %v != row-lane %v", metric, id, cf, of)
			}
			seen++
			return nil
		})
		src.Close()
		if err != nil {
			t.Fatalf("%s VectorColumnarScan: %v", metric, err)
		}
		if seen != n {
			t.Errorf("%s: columnar produced %d rows, want %d", metric, seen, n)
		}
	}
}

// runVecRows runs a query and returns its result rows as strings.
func runVecRows(t *testing.T, sess *glue.Session, q string) []string {
	t.Helper()
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	out := make([]string, len(res.Rows))
	for i, r := range res.Rows {
		out[i] = string(r)
	}
	return out
}

// TestVectorTopKColumnarDifferential is the end-to-end proof of the query integration:
// a real top-K vector query (ORDER BY VECTOR_DISTANCE(...) LIMIT k) must produce
// IDENTICAL rows with the columnar rewrite ON vs OFF -- and the fused columnar op must
// actually fire (VectorColumnarApplied increments). Distinct per-row vectors keep the
// ordering unambiguous. Covers a single- and a multi-file keyspace, ASC/DESC, OFFSET.
func TestVectorTopKColumnarDifferential(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "vecs")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	const dim = 4
	writeVecKS(t, filepath.Join(ks, "part-0.parquet"), 40, dim)
	writeVecKS(t, filepath.Join(ks, "part-1.parquet"), 25, dim) // multi-file keyspace

	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}

	const qlit = `[0.1, 0.2, 0.3, 0.4]`
	queries := []struct{ name, q string }{
		{"cosine-top5", `SELECT t.id, VECTOR_DISTANCE(t.vec, ` + qlit + `, "cosine") AS d FROM vecs t ORDER BY d ASC LIMIT 5`},
		{"l2-top8", `SELECT t.id, VECTOR_DISTANCE(t.vec, ` + qlit + `, "l2") AS d FROM vecs t ORDER BY d ASC LIMIT 8`},
		{"l2sq-offset", `SELECT t.id, VECTOR_DISTANCE(t.vec, ` + qlit + `, "l2_squared") AS d FROM vecs t ORDER BY d ASC LIMIT 5 OFFSET 4`},
		{"dot-desc", `SELECT t.id, VECTOR_DISTANCE(t.vec, ` + qlit + `, "dot") AS d FROM vecs t ORDER BY d DESC LIMIT 6`},
	}
	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			glue.DisableColumnarOptimize = true
			off := runVecRows(t, sess, q.q)
			glue.DisableColumnarOptimize = false

			before := atomic.LoadInt64(&glue.VectorColumnarApplied)
			on := runVecRows(t, sess, q.q)
			applied := atomic.LoadInt64(&glue.VectorColumnarApplied) - before

			if applied == 0 {
				t.Errorf("columnar VECTOR_DISTANCE op did not fire")
			}
			if !reflect.DeepEqual(off, on) {
				t.Errorf("columnar != row lane\n OFF: %v\n ON:  %v", off, on)
			}
		})
	}
	glue.DisableColumnarOptimize = false
}

// TestVectorColumnarNoMisfire guards the rewrite: a top-K query over the same Parquet
// keyspace that is NOT a VECTOR_DISTANCE map (a plain arithmetic projection) must run
// correctly and must NOT trigger the columnar vector op.
func TestVectorColumnarNoMisfire(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "vecs")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	writeVecKS(t, filepath.Join(ks, "part-0.parquet"), 20, 4)
	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}

	before := atomic.LoadInt64(&glue.VectorColumnarApplied)
	rows := runVecRows(t, sess, `SELECT t.id, (t.id + 100) AS d FROM vecs t ORDER BY d ASC LIMIT 3`)
	if atomic.LoadInt64(&glue.VectorColumnarApplied) != before {
		t.Errorf("columnar vector op fired on a non-VECTOR_DISTANCE query")
	}
	if len(rows) != 3 || rows[0] != `{"id":0,"d":100}` {
		t.Errorf("unexpected rows: %v", rows)
	}
}
