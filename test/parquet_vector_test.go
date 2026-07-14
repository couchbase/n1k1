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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/couchbase/n1k1/glue"
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
			valB.Append(float32((i*7+j)%13) / 13.0)
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
