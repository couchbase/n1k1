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

// The nullability contract for a vec column (DESIGN-vectors.md). When an
// INSERT INTO ...parquet SELECT VECTORIZE_BATCH(f.body, ...) hits a row whose body
// is MISSING or non-string, that row's vec is NULL -- represented natively by
// arrow/parquet's row-level validity bitmap (a zero-length list), NOT a sentinel
// vector. This test pins the round-trip so the reader/writer can rely on it:
//   - list ELEMENT non-nullable (a coord is always a real float),
//   - list FIELD nullable (a row's vec can be NULL),
//   - null rows are zero-length (offsets don't advance), so the child buffer holds
//     ONLY the present vectors, contiguous and borrowable zero-copy;
//   - therefore per-row indexing with nulls MUST use the list offsets, not r*dim.

import (
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/couchbase/n1k1/base"
)

func TestVecParquetNullContract(t *testing.T) {
	const dim = 4
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

	null := map[int]bool{2: true, 4: true} // rows 2 and 4: missing/non-string body -> NULL vec
	const nRows = 6
	for i := 0; i < nRows; i++ {
		idB.Append(int64(i))
		if null[i] {
			vecB.AppendNull()
			continue
		}
		vecB.Append(true)
		for j := 0; j < dim; j++ {
			valB.Append(float32(i*10 + j))
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	path := t.TempDir() + "/nulls.parquet"
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	// A nullable list of a non-nullable element WRITES (a FixedSizeList null would
	// error: "lists with non-zero length null components are not supported").
	if err := pqarrow.WriteTable(tbl, f, 8192, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("write nullable-list vec column: %v", err)
	}
	f.Close()

	rf, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	pf, err := file.NewParquetReader(rf)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	pr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 8192}, mem)
	if err != nil {
		t.Fatal(err)
	}
	rr, err := pr.GetRecordReader(context.Background(), []int{1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	rrec, err := rr.Read()
	if err != nil {
		t.Fatal(err)
	}

	lst := rrec.Column(0).(*array.List)
	offs := lst.Offsets()
	child := lst.ListValues().(*array.Float32)
	// Whole child borrowed zero-copy as one []float32; only present vecs, contiguous.
	all := base.VecFloat32(child.Data().Buffers()[1].Bytes())
	if len(all) != 4*dim { // 4 present rows
		t.Fatalf("child has %d floats, want %d (only present vecs)", len(all), 4*dim)
	}

	for r := 0; r < nRows; r++ {
		vlen := int(offs[r+1] - offs[r])
		if null[r] {
			if !lst.IsNull(r) || vlen != 0 {
				t.Errorf("row %d: IsNull=%v len=%d, want null + zero-length", r, lst.IsNull(r), vlen)
			}
			continue
		}
		if lst.IsNull(r) || vlen != dim {
			t.Fatalf("row %d: IsNull=%v len=%d, want present + len %d", r, lst.IsNull(r), vlen, dim)
		}
		// Per-row slice via OFFSETS (not r*dim -- nulls make stride irregular).
		v := all[offs[r]:offs[r+1]]
		for j := 0; j < dim; j++ {
			if v[j] != float32(r*10+j) {
				t.Errorf("row %d elem %d = %v, want %v", r, j, v[j], float32(r*10+j))
			}
		}
	}

	// Read the SAME file back through the VectorBatchSource and confirm it surfaces
	// the vec rows (present via offsets, null as zero-length) + the id scalar column.
	src, err := OpenFile(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	vbs, ok := src.(VectorBatchSource)
	if !ok {
		t.Fatalf("parquet source is not a VectorBatchSource")
	}
	vb, ok, err := vbs.NextVectorBatch("vec", []string{"id"})
	if err != nil || !ok {
		t.Fatalf("NextVectorBatch ok=%v err=%v", ok, err)
	}
	if vb.Rows != nRows || vb.Dim != dim || vb.Regular {
		t.Fatalf("batch rows=%d dim=%d regular=%v, want %d/%d/false(nulls present)", vb.Rows, vb.Dim, vb.Regular, nRows, dim)
	}
	ids := vb.Scalars[0]
	for r := 0; r < nRows; r++ {
		rv := vb.RowVec(r)
		if null[r] {
			if rv != nil {
				t.Errorf("row %d: RowVec = %v, want nil (NULL)", r, rv)
			}
		} else {
			if len(rv) != dim || rv[0] != float32(r*10) {
				t.Errorf("row %d: RowVec = %v, want [%d..]", r, rv, r*10)
			}
		}
		if gotID := int64(binary.LittleEndian.Uint64(ids[r*8:])); gotID != int64(r) {
			t.Errorf("row %d: id = %d, want %d", r, gotID, r)
		}
	}
}
