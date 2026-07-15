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

// Benchmarks characterizing the VARIANT fidelity path (DESIGN-variant.md §4.3): the
// per-row scan cost of the whole-row `V`-object assembly vs the Phase-0 JSON render, and
// the per-navigation cost of walking a carried `V`. These quantify the two alloc risks
// the design flagged as "benchmark before optimizing".

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	av "github.com/apache/arrow-go/v18/parquet/variant"

	"github.com/couchbase/n1k1/base"
)

const benchOrderJSON = `{"customer":{"name":"Ada","address":{"city":"London","geo":{"lat":51.5,"lon":-0.12}}},` +
	`"orderlines":[{"sku":"A1","qty":2},{"sku":"B2","qty":1}],"total":9.99}`

// buildOrdersBatch builds an in-memory {id string, order VARIANT} arrow batch of n rows,
// each order a moderately nested object (from benchOrderJSON).
func buildOrdersBatch(tb testing.TB, n int) arrow.RecordBatch {
	tb.Helper()
	mem := memory.DefaultAllocator
	vt := extensions.NewDefaultVariantType()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "order", Type: vt, Nullable: true},
	}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()
	idB := rb.Field(0).(*array.StringBuilder)
	orderB := rb.Field(1).(*extensions.VariantBuilder)
	for i := 0; i < n; i++ {
		idB.Append(fmt.Sprintf("o%d", i))
		ov, err := av.ParseJSON(benchOrderJSON, false)
		if err != nil {
			tb.Fatal(err)
		}
		orderB.Append(ov)
	}
	return rb.NewRecord()
}

const benchBatchRows = 256

// BenchmarkBatchRenderJSON: Phase-0 render of a VARIANT-bearing batch to NDJSON rows.
func BenchmarkBatchRenderJSON(b *testing.B) {
	rec := buildOrdersBatch(b, benchBatchRows)
	defer rec.Release()
	var buf []byte
	var lines [][]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		if buf, err = arrowBatchToNDJSON(buf, rec); err != nil {
			b.Fatal(err)
		}
		lines = splitNDJSON(buf, lines[:0])
	}
	_ = lines
}

// BenchmarkBatchRenderVariant: fidelity-mode render of the same batch to whole-row `V`
// objects (the assembler, builder + buffers reused across rows). Compare allocs/op and
// ns/op to BenchmarkBatchRenderJSON, dividing by benchBatchRows for per-row figures.
func BenchmarkBatchRenderVariant(b *testing.B) {
	rec := buildOrdersBatch(b, benchBatchRows)
	defer rec.Release()
	var buf []byte
	var lines [][]byte
	var offs []int
	var asm variantRowAssembler
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		if buf, lines, offs, err = arrowBatchToVariantRows(buf, lines[:0], offs, rec, &asm); err != nil {
			b.Fatal(err)
		}
	}
	_ = lines
}

// BenchmarkVariantPathGetScalar: navigate a 3-deep scalar leaf (order.customer.name) of a
// carried `V` doc, projecting it to JSON. Exercises the metadata-parse-per-navigation cost.
func BenchmarkVariantPathGetScalar(b *testing.B) {
	rec := buildOrdersBatch(b, 1)
	defer rec.Release()
	var asm variantRowAssembler
	doc, err := asm.appendRow(nil, rec, 0)
	if err != nil {
		b.Fatal(err)
	}
	path := []string{"order", "customer", "name"}
	var out base.Val
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, out = base.VariantPathGet(doc, path, out)
	}
	_ = out
}

// BenchmarkVariantPathGetContainer: navigate to a container leaf (order), returned as a
// self-contained `V` envelope (the zero-re-encode reframe).
func BenchmarkVariantPathGetContainer(b *testing.B) {
	rec := buildOrdersBatch(b, 1)
	defer rec.Release()
	var asm variantRowAssembler
	doc, err := asm.appendRow(nil, rec, 0)
	if err != nil {
		b.Fatal(err)
	}
	path := []string{"order"}
	var out base.Val
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, out = base.VariantPathGet(doc, path, out)
	}
	_ = out
}
