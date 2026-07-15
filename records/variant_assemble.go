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

// Whole-row VARIANT assembly (DESIGN-variant.md §4.1, Phase-1 step 2). The scan's
// fidelity mode renders a row that carries a VARIANT column as ONE whole-row VARIANT
// object (base's `V` carrier), instead of the Phase-0 JSON object — so a VARIANT
// date/decimal/uuid keeps its typed identity through the engine and out to a write-back.
//
// The linchpin is nesting an already-encoded VARIANT cell (which references its OWN
// metadata dictionary by field-id) into the row object without corrupting those ids. We
// exploit two facts about arrow-go's variant.Builder: AddKey assigns ids by insertion
// order and never renumbers, and Build writes the dictionary in insertion order. So we
// SEED the row builder's dictionary with the cell's dict keys first (ids 0..k-1 match the
// cell's), then UnsafeAppendEncoded the cell's value bytes verbatim (its ids stay valid)
// — no re-encode of the VARIANT payload. Scalar columns carry no field-ids, so they are
// re-encoded cheaply through their existing JSON rendering (appendArrowValueJSON ->
// ParseJSON -> splice). This assembly is only reached on the opt-in fidelity path.
//
// Supported shape: scalar/list columns + AT MOST ONE VARIANT column (what the Phase-2a
// writer produces and the read tests use). Other shapes (>1 VARIANT column, or a scalar
// column that renders to a JSON object) return an error so the caller can fall back to
// the Phase-0 JSON row.

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	av "github.com/apache/arrow-go/v18/parquet/variant"

	"github.com/couchbase/n1k1/base"
)

// variantRowAssembler assembles whole-row VARIANT objects, reusing its scratch buffers
// across rows to hold per-row allocations down. Not concurrent-safe.
//
// NOTE: it deliberately does NOT reuse an av.Builder across rows. arrow-go's
// Builder.Reset clears the buffer/dictionary but NOT its internal totalDictSize
// accumulator, so a reused builder's per-row Build() sizes metadata by the SUM of every
// row's dictionary — an O(N^2) allocation blowup (measured 221MB / 256-row batch before
// this fix). A fresh builder per row keeps it linear; buf/fields reuse is unaffected.
type variantRowAssembler struct {
	jsonBuf []byte
	fields  []av.FieldEntry
}

// arrowBatchToVariantRows renders each row of batch as a whole-row VARIANT `V`-carrier
// object into buf (reset first), returning per-row slices in lines. offs is a reused
// offset scratch: rows are all appended before any slice is taken, so a realloc of buf
// mid-batch can't invalidate an earlier line. Mirrors arrowBatchToNDJSON's buffer
// contract for the fidelity path.
func arrowBatchToVariantRows(buf []byte, lines [][]byte, offs []int, batch arrow.RecordBatch, asm *variantRowAssembler) ([]byte, [][]byte, []int, error) {
	buf = buf[:0]
	offs = append(offs[:0], 0)
	n := int(batch.NumRows())
	for row := 0; row < n; row++ {
		var err error
		if buf, err = asm.appendRow(buf, batch, row); err != nil {
			return buf, lines, offs, err
		}
		offs = append(offs, len(buf))
	}
	for i := 0; i < n; i++ {
		lines = append(lines, buf[offs[i]:offs[i+1]])
	}
	return buf, lines, offs, nil
}

// batchHasVariant reports the index of the (single) VARIANT column in batch, or -1 if
// none. It returns ok=false if the batch has more than one VARIANT column (unsupported).
func batchHasVariant(batch arrow.RecordBatch) (col int, ok bool) {
	col = -1
	for c := 0; c < int(batch.NumCols()); c++ {
		if _, isV := batch.Column(c).(*extensions.VariantArray); isV {
			if col >= 0 {
				return -1, false // >1 VARIANT column: unsupported
			}
			col = c
		}
	}
	return col, true
}

// appendRow assembles row `row` of batch as a whole-row VARIANT object and appends it to
// dst as base's `V`-carrier envelope.
func (a *variantRowAssembler) appendRow(dst []byte, batch arrow.RecordBatch, row int) ([]byte, error) {
	variantCol, ok := batchHasVariant(batch)
	if !ok {
		return dst, fmt.Errorf("appendRowVariant: multiple VARIANT columns unsupported")
	}

	var bld av.Builder // fresh per row — see the type doc (Reset leaks totalDictSize)

	// Seed the row dict with the VARIANT cell's dict keys (in id order) so the cell's
	// value bytes can be spliced with its field-ids intact.
	var cell av.Value
	haveCell := false
	if variantCol >= 0 {
		va := batch.Column(variantCol).(*extensions.VariantArray)
		if !va.IsNull(row) {
			v, err := va.Value(row)
			if err != nil {
				return dst, err
			}
			cell, haveCell = v, true
			md := cell.Metadata()
			for id := uint32(0); id < md.DictionarySize(); id++ {
				k, err := md.KeyAt(id)
				if err != nil {
					return dst, err
				}
				bld.AddKey(k)
			}
		}
	}

	schema := batch.Schema()
	start := bld.Offset()
	a.fields = a.fields[:0]
	for c := 0; c < int(batch.NumCols()); c++ {
		a.fields = append(a.fields, bld.NextField(start, schema.Field(c).Name))
		if c == variantCol {
			if haveCell {
				if err := bld.UnsafeAppendEncoded(cell.Bytes()); err != nil {
					return dst, err
				}
			} else if err := bld.AppendNull(); err != nil { // a null VARIANT cell -> variant null
				return dst, err
			}
			continue
		}
		// Scalar/list column: reuse the JSON renderer, then splice the (dict-less) value.
		a.jsonBuf = appendArrowValueJSON(a.jsonBuf[:0], batch.Column(c), row)
		sv, err := av.ParseJSONBytes(a.jsonBuf, false)
		if err != nil {
			return dst, err
		}
		if sv.Metadata().DictionarySize() > 0 {
			return dst, fmt.Errorf("appendRowVariant: column %q rendered to an object; unsupported",
				schema.Field(c).Name)
		}
		if err := bld.UnsafeAppendEncoded(sv.Bytes()); err != nil {
			return dst, err
		}
	}
	if err := bld.FinishObject(start, a.fields); err != nil {
		return dst, err
	}
	v, err := bld.Build()
	if err != nil {
		return dst, err
	}
	// Copy the built bytes into dst (bld's Build() Value references bld's own buffer,
	// which goes out of scope at return — we retain only the copy in dst).
	return base.AppendVariantEnvelope(dst, v.Metadata().Bytes(), v.Bytes()), nil
}
