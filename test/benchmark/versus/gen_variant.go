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

// gen_variant.go re-encodes a JSONL container into a Parquet file whose `order`
// column is an Apache Parquet VARIANT (arrow-go's native VARIANT extension type),
// so the versus benchmark can query the SAME order docs stored as VARIANT-in-parquet
// -- read natively by n1k1's records/parquet.go (cbq can't: iceberg-go has no VARIANT).
// Reads the jsonl (not Python's PRNG) so the docs are byte-identical to orders_jsonl.
//
//	go run test/benchmark/versus/gen_variant.go <data.jsonl> <out.parquet>
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	av "github.com/apache/arrow-go/v18/parquet/variant"
)

func main() {
	if len(os.Args) < 3 {
		fatal(fmt.Errorf("usage: gen_variant.go <data.jsonl> <out.parquet>"))
	}
	in, out := os.Args[1], os.Args[2]

	f, err := os.Open(in)
	if err != nil {
		fatal(err)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	var ids, docs []string
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		ids = append(ids, strconv.Itoa(len(ids)))
		docs = append(docs, line)
	}
	f.Close()
	if err := sc.Err(); err != nil {
		fatal(err)
	}

	mem := memory.DefaultAllocator

	idB := array.NewStringBuilder(mem)
	for _, s := range ids {
		idB.Append(s)
	}
	idArr := idB.NewArray()

	vt := extensions.NewDefaultVariantType()
	vB := extensions.NewVariantBuilder(mem, vt)
	for _, s := range docs {
		v, e := av.ParseJSON(s, false)
		if e != nil {
			fatal(fmt.Errorf("ParseJSON line %d: %v", len(ids), e))
		}
		vB.Append(v)
	}
	vArr := vB.NewArray()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "order", Type: vt, Nullable: true},
	}, nil)
	rec := array.NewRecord(schema, []arrow.Array{idArr, vArr}, int64(len(ids)))
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})

	fout, err := os.Create(out)
	if err != nil {
		fatal(err)
	}
	if err := pqarrow.WriteTable(tbl, fout, max(1, tbl.NumRows()),
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithStats(false)),
		pqarrow.DefaultWriterProps()); err != nil {
		fatal(err)
	}
	fout.Close()
	fmt.Printf("gen_variant: wrote %d VARIANT docs to %s\n", len(docs), out)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "gen_variant:", err)
	os.Exit(1)
}
