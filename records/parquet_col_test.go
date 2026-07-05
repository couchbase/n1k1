//go:build !js

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

package records

// Tests for the Step-5 prerequisites (DESIGN-col.md): ColumnsSource on a
// multi-file keyspace (walkSource.Columns) and ColumnBatchSource (parquetSource /
// walkSource NextColumns yielding borrowed little-endian value buffers, no
// transpose). Self-contained in records -- no glue, no EE bootstrap.

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// writeColTestParquet writes n rows of {id int64, price float64} with
// id = base+i and price = float64(base+i)+0.5.
func writeColTestParquet(t *testing.T, path string, base, n int) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for i := 0; i < n; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(base + i))
		b.Field(1).(*array.Float64Builder).Append(float64(base+i) + 0.5)
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

// sumF64Batches drains a ColumnBatchSource, summing column 0 as float64, and
// returns the sum and total row count.
func sumF64Batches(t *testing.T, cbs ColumnBatchSource) (float64, int) {
	var sum float64
	var rows int
	for {
		cols, _, r, ok, err := cbs.NextColumns()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if len(cols) != 1 {
			t.Fatalf("projected 1 column, got %d buffers", len(cols))
		}
		buf := cols[0]
		for i := 0; i+8 <= len(buf); i += 8 {
			sum += math.Float64frombits(binary.LittleEndian.Uint64(buf[i:]))
		}
		rows += r
	}
	return sum, rows
}

func TestParquetColumnBatchSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.parquet")
	writeColTestParquet(t, path, 0, 100) // price = i+0.5, i in 0..99

	src, err := OpenFile(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	if err := src.(ColumnsProjector).ProjectColumns([]string{"price"}); err != nil {
		t.Fatal(err)
	}
	cbs, ok := src.(ColumnBatchSource)
	if !ok {
		t.Fatal("parquet source should implement ColumnBatchSource")
	}

	sum, rows := sumF64Batches(t, cbs)
	if rows != 100 {
		t.Fatalf("rows=%d want 100", rows)
	}
	if sum != 5000 { // sum(0..99) + 100*0.5 = 4950 + 50
		t.Fatalf("sum=%v want 5000", sum)
	}
}

func TestWalkSourceColumnsAndBatches(t *testing.T) {
	dir := t.TempDir()
	p0 := filepath.Join(dir, "part-0.parquet")
	p1 := filepath.Join(dir, "part-1.parquet")
	writeColTestParquet(t, p0, 0, 50)    // price = i+0.5,        i in 0..49  -> 1250
	writeColTestParquet(t, p1, 1000, 50) // price = 1000+i+0.5,   i in 0..49  -> 51250

	ws := WalkPrelisted(dir, []string{p0, p1}, WalkOptions{})
	defer ws.Close()

	// ColumnsSource: schema from the first file's footer.
	cols := ws.(ColumnsSource).Columns()
	if len(cols) != 2 || cols[0].Name != "id" || cols[1].Name != "price" || cols[1].Type != "DOUBLE" {
		t.Fatalf("Columns() = %+v, want [id INT64, price DOUBLE]", cols)
	}
	if cols[1].NullCount != 0 {
		t.Errorf("price NullCount=%d, want 0", cols[1].NullCount)
	}

	// ColumnBatchSource: project price, sum across both part files.
	if err := ws.(ColumnsProjector).ProjectColumns([]string{"price"}); err != nil {
		t.Fatal(err)
	}
	sum, rows := sumF64Batches(t, ws.(ColumnBatchSource))
	if rows != 100 {
		t.Fatalf("rows=%d want 100", rows)
	}
	if sum != 52500 { // 1250 + 51250
		t.Fatalf("sum=%v want 52500", sum)
	}
}
