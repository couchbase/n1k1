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

// De-risk benchmark for DESIGN-vectors.md's columnar VECTOR_DISTANCE: prove the
// ~60ms in-memory ceiling survives a REAL Parquet round-trip. Writes N rows of
// {id int64, vec FixedSizeList<float32>[dim]}, then reads the vec column back as the
// borrowed little-endian float32 child buffer (base.VecFloat32, zero copy -- no
// per-element ParseFloat, no JSON materialization) and runs base.VectorDistanceVFloat32
// + a top-K scan. Compare the logged time to jsonl+native 6.8s / jsonl+boxed 11.2s at
// the same 100K x 384 shape. Small N by default (correctness in the suite); set
// N1K1_VEC_ROWS / N1K1_VEC_DIM for the headline run.

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/couchbase/n1k1/base"
)

// writeVecParquet writes n rows of {id int64, vec FixedSizeList<float32>[dim]} with a
// deterministic pseudo-random unit-ish vector per row (LCG, no math/rand for
// reproducibility). Returns the query vector = row 0's vector as a []float64.
func writeVecParquet(t testing.TB, path string, n, dim int) []float64 {
	mem := memory.NewGoAllocator()
	// The vec column is a variable List<float32> (NOT FixedSizeList -- pqarrow can't
	// write FixedSizeList nulls), element non-nullable (a coord is always a real
	// float), list field nullable (a row's vec can be NULL when the source text is
	// missing/non-string). All-present rows give regular offsets 0,dim,2dim,.. so the
	// contiguous zero-copy borrow still holds; see TestVecParquetNullContract for nulls.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "vec", Type: arrow.ListOfNonNullable(arrow.PrimitiveTypes.Float32), Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	idB := b.Field(0).(*array.Int64Builder)
	vecB := b.Field(1).(*array.ListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	seed := uint64(0x243f6a8885a308d3)
	nextF32 := func() float32 {
		seed = seed*6364136223846793005 + 1442695040888963407
		return float32(int64(seed>>11))/float32(1<<52)*2 - 1
	}

	query := make([]float64, dim)
	idB.Reserve(n)
	valB.Reserve(n * dim)
	for i := 0; i < n; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dim; j++ {
			f := nextF32()
			valB.Append(f)
			if i == 0 {
				query[j] = float64(f)
			}
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
	// Chunk size = one row group of 8192 rows (a realistic page/batch granularity).
	if err := pqarrow.WriteTable(tbl, f, 8192, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatal(err)
	}
	return query
}

func envInt(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

// TestVectorDistanceParquetCeiling measures a columnar VECTOR_DISTANCE top-K over a
// real Parquet file, reading the vec column as a zero-copy borrowed float32 buffer.
func TestVectorDistanceParquetCeiling(t *testing.T) {
	rows := envInt("N1K1_VEC_ROWS", 3000)
	dim := envInt("N1K1_VEC_DIM", 384)
	const k = 10
	const metric = "cosine"

	dir := os.Getenv("CLAUDE_JOB_DIR")
	if dir != "" {
		dir = filepath.Join(dir, "tmp")
	} else {
		dir = t.TempDir()
	}
	path := filepath.Join(dir, "vecbench.parquet")

	wStart := time.Now()
	query := writeVecParquet(t, path, rows, dim)
	tWrite := time.Since(wStart)

	// findVecCol drains the Parquet, borrowing each batch's FixedSizeList<float32>
	// child buffer as a []float32 (VecFloat32, no copy) and folding a callback over
	// each batch. Returns the total rows seen.
	drain := func(fn func(vec []float32, batchRows, dim int)) int {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		pf, err := file.NewParquetReader(f)
		if err != nil {
			t.Fatal(err)
		}
		defer pf.Close()
		pr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 8192}, memory.NewGoAllocator())
		if err != nil {
			t.Fatal(err)
		}
		rr, err := pr.GetRecordReader(context.Background(), []int{1}, nil) // vec column only
		if err != nil {
			t.Fatal(err)
		}
		defer rr.Release()
		total := 0
		for {
			rec, err := rr.Read()
			if err != nil {
				break // io.EOF
			}
			if rec.NumRows() == 0 {
				rec.Release()
				break
			}
			// Parquet has no fixed-size-list type -> a FixedSizeList<float32>[dim]
			// round-trips as a variable List<float32>. The child float32 values are
			// still contiguous (every list has exactly dim elems, offsets 0,dim,2dim,..),
			// so we borrow child[offs[0]:offs[n]] zero-copy. (The real reader will do the
			// same: expose an arrow.LIST-of-float32 column's contiguous child buffer.)
			lst := rec.Column(0).(*array.List)
			offs := lst.Offsets() // len n+1, element indices into the child
			child := lst.ListValues().(*array.Float32)
			data := child.Data()
			n := int(rec.NumRows())
			start := int(offs[0]) + data.Offset()
			end := int(offs[n]) + data.Offset()
			if end-start != n*dim {
				t.Fatalf("non-fixed vec column: %d child elems for %d rows x %d dim", end-start, n, dim)
			}
			raw := data.Buffers()[1].Bytes() // little-endian float32 bytes, borrowed
			vec := base.VecFloat32(raw)[start:end]
			fn(vec, n, dim)
			total += n
			rec.Release()
		}
		return total
	}

	// Read-only pass: touch every vector element (sum) to attribute Parquet decode/IO
	// alone, separate from the distance compute.
	var sink float64
	rStart := time.Now()
	nRead := drain(func(vec []float32, n, dim int) {
		for _, x := range vec {
			sink += float64(x)
		}
	})
	tRead := time.Since(rStart)
	if nRead != rows {
		t.Fatalf("read %d rows, want %d", nRead, rows)
	}

	// Full pass: read + kernel + top-K (a bounded max-at-front slice).
	type hit struct {
		id int
		d  float64
	}
	best := make([]hit, 0, k+1)
	worst := func() float64 {
		if len(best) < k {
			return 1e308
		}
		return best[len(best)-1].d
	}
	rowBase := 0
	var out []float64
	fStart := time.Now()
	drain(func(vec []float32, n, dim int) {
		if cap(out) < n {
			out = make([]float64, n)
		}
		out = out[:n]
		base.VectorDistanceVFloat32(out, vec, query, n, dim, metric)
		for i := 0; i < n; i++ {
			d := out[i]
			if d != d { // NaN (NULL) never ranks
				continue
			}
			if d >= worst() {
				continue
			}
			best = append(best, hit{rowBase + i, d})
			sort.Slice(best, func(a, b int) bool { return best[a].d < best[b].d })
			if len(best) > k {
				best = best[:k]
			}
		}
		rowBase += n
	})
	tFull := time.Since(fStart)

	fi, _ := os.Stat(path)
	if (false) {
		t.Logf("columnar VECTOR_DISTANCE over Parquet: rows=%d dim=%d metric=%s", rows, dim, metric)
		t.Logf("  file=%.1f MB  write=%v", float64(fi.Size())/1e6, tWrite.Round(time.Millisecond))
		t.Logf("  read-only (decode vec column)   = %v", tRead.Round(time.Millisecond))
		t.Logf("  full (read + kernel + top-%d)    = %v", k, tFull.Round(time.Millisecond))
		t.Logf("  compute alone (full - read)     ~ %v", (tFull - tRead).Round(time.Millisecond))
		t.Logf("  vs jsonl+native 6.8s, jsonl+boxed 11.2s (100K x 384)")
	}
	// Row 0 is the query itself -> cosine distance 0 -> must be the top hit.
	if len(best) == 0 || best[0].id != 0 {
		t.Fatalf("top hit = %+v, want id 0 (the query row, distance 0)", best)
	}
	_ = sink
}
