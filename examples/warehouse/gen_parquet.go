//go:build ignore

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

// gen_parquet.go writes the example Parquet dataset for DESIGN-data.md scenario K:
//
//	examples/warehouse/default/sales/part-0.parquet
//	examples/warehouse/default/sales/part-1.parquet
//
// A keyspace ("sales") is the union of the two part files (like a partitioned
// export). Columns: id, region, product, units, amount -- so GROUP BY / SUM
// examples work. The .parquet files are checked in (small, binary); regenerate
// with, from the repo root:
//
//	go run examples/warehouse/gen_parquet.go            # default output dir
//	go run examples/warehouse/gen_parquet.go <outDir>
//
// (Needs the DESIGN-testing.md worktree bootstrap when run from a fresh worktree,
// since apache/arrow-go pulls the module graph. pyarrow isn't a repo dep, so this
// lives here rather than in examples/generate.py.)
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

var regions = []string{"west", "east", "north", "south"}
var products = []string{"widget", "gadget", "gizmo"}

func main() {
	out := filepath.Join("examples", "warehouse", "default", "sales")
	if len(os.Args) > 1 {
		out = os.Args[1]
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		panic(err)
	}
	writePart(filepath.Join(out, "part-0.parquet"), 1000, 120)
	writePart(filepath.Join(out, "part-1.parquet"), 2000, 120)
}

func writePart(path string, baseID, n int) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "region", Type: arrow.BinaryTypes.String},
		{Name: "product", Type: arrow.BinaryTypes.String},
		{Name: "units", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for i := 0; i < n; i++ {
		units := int64(i%7) + 1
		price := 4.99 + float64(i%5)*5.0 // 4.99, 9.99, 14.99, 19.99, 24.99
		b.Field(0).(*array.Int64Builder).Append(int64(baseID + i))
		b.Field(1).(*array.StringBuilder).Append(regions[i%len(regions)])
		b.Field(2).(*array.StringBuilder).Append(products[i%len(products)])
		b.Field(3).(*array.Int64Builder).Append(units)
		b.Field(4).(*array.Float64Builder).Append(float64(units) * price)
	}
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
	)
	if err := pqarrow.WriteTable(tbl, f, int64(n), props, pqarrow.DefaultWriterProps()); err != nil {
		panic(err)
	}
	fmt.Printf("wrote %s (%d rows)\n", path, n)
}
