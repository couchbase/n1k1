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

// Parquet record source -- "transpose to rows": each Parquet row becomes one
// JSON object built from the file's columns, so n1k1 can *query* a .parquet file
// at all. This is DESIGN-col.md's Step 3 (the correctness path); it deliberately
// does NOT exploit columnar/vectorized execution yet (that's the column-batch
// roadmap in DESIGN-col.md). Built on apache/arrow-go/v18 -- the same library
// glue's iceberg_reader uses.
//
// Guarded !js: arrow-go's Parquet reader (assembly, large surface) does not build
// for GOOS=js/wasm, so the browser build gets the stub in parquet_js.go, matching
// how idx_si/idx_fts are build-tag-guarded for wasm.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// DisableFastTranspose forces the allocating array.RecordToJSON fallback instead
// of the hand-rolled zero-alloc writer. Used by the equivalence test to prove the
// two paths produce semantically identical JSON.
var DisableFastTranspose bool

// parquetSource streams a Parquet file as JSON records. It reads one Arrow
// record batch at a time, renders it to newline-delimited JSON (one object per
// row via array.RecordToJSON), and yields each line. Doc borrows the render
// buffer and is valid only until the batch is exhausted and the next one loads.
type parquetSource struct {
	pf       *file.Reader
	pr       *pqarrow.FileReader
	rr       pqarrow.RecordReader
	proj     []int // leaf column indices to read; nil => all columns
	idPrefix string
	row      int

	buf   []byte   // current batch rendered as NDJSON (reused across batches)
	lines [][]byte // per-row slices into buf
	li    int      // next line index
	idBuf []byte
	done  bool

	curBatch arrow.RecordBatch // held for the column-batch path; released on next NextColumns/Close

	rrVec       pqarrow.RecordReader // vector-batch path (NextVectorBatch)
	curBatchVec arrow.RecordBatch    // held for the vector-batch path; released on next call/Close
}

func newParquetSource(path, idPrefix string) (Source, error) {
	pf, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, err
	}
	pr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		pf.Close()
		return nil, err
	}
	// The RecordReader is created lazily on the first Next so an optional
	// ProjectColumns (which must precede iteration) can restrict its columns.
	return &parquetSource{pf: pf, pr: pr, idPrefix: idPrefix}, nil
}

// ProjectColumns implements ColumnsProjector: read only the named columns. Must
// be called before the first Next. Unknown names are skipped, not errors: a
// field absent from this file reads as MISSING downstream whether or not we
// projected it, so dropping it from the read is safe (and correct for SQL++'s
// schemaless model). If none of the names resolve, the projection is left off
// (read all columns) rather than reading an empty, zero-column batch.
func (s *parquetSource) ProjectColumns(names []string) error {
	if s.rr != nil {
		return fmt.Errorf("records: ProjectColumns must be called before Next")
	}
	sch := s.pf.MetaData().Schema
	// Map each TOP-LEVEL field to its leaf column indices. A nested column (a
	// list<float32> vec, a struct, ...) has leaves under "field.list.element" /
	// "field.subfield", so ColumnIndexByName(topField) returns -1 -- projecting by
	// leaf-name-only would silently DROP the whole nested column when a sibling
	// scalar resolves (reading only the sibling). Grouping leaves by their first
	// path element fixes that: requesting "vec" pulls in vec's element leaf.
	leavesByField := map[string][]int{}
	for c := 0; c < sch.NumColumns(); c++ {
		p := sch.Column(c).Path() // "id" | "vec.list.element" | ...
		top := p
		if i := strings.IndexByte(p, '.'); i >= 0 {
			top = p[:i]
		}
		leavesByField[top] = append(leavesByField[top], c)
	}
	// Preserve the requested field order (a field's leaves in schema order within it):
	// the scalar columnar path (DatastoreAggColumnar) maps cols positionally to the
	// projected-names order, so reordering here would fold the wrong column.
	proj := make([]int, 0, len(names))
	seen := map[int]bool{}
	for _, n := range names {
		for _, c := range leavesByField[n] {
			if !seen[c] {
				proj = append(proj, c)
				seen[c] = true
			}
		}
	}
	if len(proj) > 0 {
		s.proj = proj
	}
	return nil
}

// Columns implements ColumnsSource: describe columns from the footer (types,
// null counts, min/max) with no data pages read.
func (s *parquetSource) Columns() []ColumnMeta {
	md := s.pf.MetaData()
	sch := md.Schema
	out := make([]ColumnMeta, 0, sch.NumColumns())
	for c := 0; c < sch.NumColumns(); c++ {
		typ := sch.Column(c).PhysicalType().String()
		cm := ColumnMeta{Name: sch.Column(c).Name(), Type: typ, Count: md.NumRows, NullCount: -1}
		// Aggregate stats across row groups: null count sums; Min-of-mins,
		// Max-of-maxs. A row group missing min/max makes the whole column's
		// min/max unknown (nil) -- so a metadata-only MIN/MAX can't be trusted.
		var haveNull bool
		minMaxOK := md.NumRowGroups() > 0
		for rg := 0; rg < md.NumRowGroups(); rg++ {
			cc, err := md.RowGroup(rg).ColumnChunk(c)
			if err != nil {
				minMaxOK = false
				continue
			}
			st, err := cc.Statistics()
			if err != nil || st == nil {
				minMaxOK = false
				continue
			}
			if st.HasNullCount() {
				if !haveNull {
					cm.NullCount, haveNull = 0, true
				}
				cm.NullCount += st.NullCount()
			}
			if !st.HasMinMax() {
				minMaxOK = false
				continue
			}
			emin, emax := st.EncodeMin(), st.EncodeMax()
			if cm.Min == nil {
				cm.Min, cm.Max = emin, emax
			} else {
				cm.Min = pickStat(typ, cm.Min, emin, true)
				cm.Max = pickStat(typ, cm.Max, emax, false)
			}
		}
		if !minMaxOK {
			cm.Min, cm.Max = nil, nil
		}
		out = append(out, cm)
	}
	return out
}

func (s *parquetSource) Next(rec *Record) (bool, error) {
	if s.rr == nil {
		// nil rowGroups => all row groups; s.proj (nil => all columns).
		rr, err := s.pr.GetRecordReader(context.Background(), s.proj, nil)
		if err != nil {
			return false, err
		}
		s.rr = rr
	}
	for s.li >= len(s.lines) {
		if s.done {
			return false, nil
		}
		batch, err := s.rr.Read()
		if err == io.EOF {
			s.done = true
			return false, nil
		}
		if err != nil {
			return false, err
		}
		s.buf, err = arrowBatchToNDJSON(s.buf, batch)
		batch.Release()
		if err != nil {
			return false, err
		}
		s.lines = splitNDJSON(s.buf, s.lines[:0])
		s.li = 0
	}

	rec.Doc = s.lines[s.li]
	s.idBuf = appendRecordID(s.idBuf[:0], s.idPrefix, s.row)
	rec.ID = s.idBuf
	s.li++
	s.row++
	return true, nil
}

// NextColumns implements ColumnBatchSource: read one Arrow record batch and hand
// back each projected column's raw little-endian value buffer, borrowed from the
// batch (valid until the next NextColumns/Close). The vectorized aggregates
// consume these directly -- no transpose, no JSON. Only fixed-width 8-byte numeric
// columns are supported for now; anything else errors (caller falls back to rows).
func (s *parquetSource) NextColumns() (cols, valids [][]byte, rows int, ok bool, err error) {
	if s.rr == nil {
		rr, e := s.pr.GetRecordReader(context.Background(), s.proj, nil)
		if e != nil {
			return nil, nil, 0, false, e
		}
		s.rr = rr
	}
	if s.curBatch != nil {
		s.curBatch.Release() // free the previous batch; its buffers were borrowed
		s.curBatch = nil
	}
	batch, e := s.rr.Read()
	if e == io.EOF {
		return nil, nil, 0, false, nil
	}
	if e != nil {
		return nil, nil, 0, false, e
	}
	s.curBatch = batch
	rows = int(batch.NumRows())
	for _, c := range batch.Columns() {
		b, e := arrowValueBytes(c)
		if e != nil {
			return nil, nil, 0, false, e
		}
		cols = append(cols, b)
		valids = append(valids, arrowValidityBytes(c))
	}
	return cols, valids, rows, true, nil
}

// arrowValueBytes returns the raw little-endian value buffer of a fixed-width
// 8-byte numeric column, sliced to the array's [offset, offset+len) window (no
// copy). Errors for any other type. (float32/int32 (4-byte) etc. come later.)
func arrowValueBytes(a arrow.Array) ([]byte, error) {
	switch a.DataType().ID() {
	case arrow.FLOAT64, arrow.INT64, arrow.UINT64:
		const w = 8
		buf := a.Data().Buffers()[1].Bytes()
		off := a.Data().Offset() * w
		return buf[off : off+a.Len()*w], nil
	default:
		return nil, fmt.Errorf("records: column type %s is not a fixed 8-byte numeric column", a.DataType())
	}
}

// arrowValidityBytes returns the column's validity bitmap normalized to bit 0 ==
// row 0 (LSB-first, 1 == valid) -- the exact layout base's masked/selection kernels
// expect, so it doubles as a selection mask (include the non-null lanes). Returns
// nil when the column has no nulls (all lanes valid; caller uses the unmasked path).
// A byte-aligned array offset borrows the buffer directly; the rare unaligned offset
// is copied bit-by-bit into a fresh, aligned bitmap.
func arrowValidityBytes(a arrow.Array) []byte {
	data := a.Data()
	bufs := data.Buffers()
	if len(bufs) == 0 || bufs[0] == nil || a.NullN() == 0 {
		return nil
	}
	raw := bufs[0].Bytes()
	off, n := data.Offset(), a.Len()
	if off%8 == 0 {
		return raw[off/8 : (off+n+7)/8] // bit off is bit 0 of this slice
	}
	out := make([]byte, (n+7)/8)
	for i := 0; i < n; i++ {
		if j := off + i; raw[j>>3]&(1<<(uint(j)&7)) != 0 {
			out[i>>3] |= 1 << (uint(i) & 7)
		}
	}
	return out
}

func (s *parquetSource) Close() error {
	if s.curBatch != nil {
		s.curBatch.Release()
		s.curBatch = nil
	}
	if s.rr != nil {
		s.rr.Release()
	}
	if s.curBatchVec != nil {
		s.curBatchVec.Release()
		s.curBatchVec = nil
	}
	if s.rrVec != nil {
		s.rrVec.Release()
	}
	if s.pf != nil {
		return s.pf.Close()
	}
	return nil
}

// fastRenderable reports whether every column is a type appendArrowValueJSON
// handles directly. If not (timestamps, decimals, nested lists/structs, binary,
// ...), the caller falls back to the allocating array.RecordToJSON.
func fastRenderable(rec arrow.RecordBatch) bool {
	for _, c := range rec.Columns() {
		switch c.(type) {
		case *array.Boolean,
			*array.Int8, *array.Int16, *array.Int32, *array.Int64,
			*array.Uint8, *array.Uint16, *array.Uint32, *array.Uint64,
			*array.Float32, *array.Float64,
			*array.String, *array.LargeString:
		default:
			return false
		}
	}
	return true
}

// arrowBatchToNDJSON renders an Arrow batch to newline-delimited JSON into buf (reused):
// the zero-alloc fast path when every column is fastRenderable, else the allocating
// array.RecordToJSON fallback for exotic types (timestamps/decimals/lists/structs).
// Shared by the Parquet source (Next) and the Iceberg source (iceberg.go), so both
// transpose Arrow batches identically.
func arrowBatchToNDJSON(buf []byte, batch arrow.RecordBatch) ([]byte, error) {
	if !DisableFastTranspose && fastRenderable(batch) {
		return appendRecordsNDJSON(buf[:0], batch), nil
	}
	bb := bytes.NewBuffer(buf[:0])
	err := array.RecordToJSON(batch, bb)
	return bb.Bytes(), err
}

// appendRecordsNDJSON renders every row of rec as a JSON object on its own line,
// appending into dst. Keys are in column order (downstream reads by name, so
// order is irrelevant). Zero allocation: numbers/bools/strings append directly,
// and arrow's String.Value is a zero-copy substring.
func appendRecordsNDJSON(dst []byte, rec arrow.RecordBatch) []byte {
	fields := rec.Schema().Fields()
	cols := rec.Columns()
	n := int(rec.NumRows())
	for i := 0; i < n; i++ {
		dst = append(dst, '{')
		for j, c := range cols {
			if j > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, fields[j].Name)
			dst = append(dst, ':')
			dst = appendArrowValueJSON(dst, c, i)
		}
		dst = append(dst, '}', '\n')
	}
	return dst
}

// appendArrowValueJSON appends arr[i] as a JSON value. A null becomes JSON null
// (SQL++ NULL, distinct from a missing/absent field). Assumes arr's type passed
// fastRenderable.
func appendArrowValueJSON(dst []byte, arr arrow.Array, i int) []byte {
	if arr.IsNull(i) {
		return append(dst, "null"...)
	}
	switch a := arr.(type) {
	case *array.Boolean:
		if a.Value(i) {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case *array.Int8:
		return strconv.AppendInt(dst, int64(a.Value(i)), 10)
	case *array.Int16:
		return strconv.AppendInt(dst, int64(a.Value(i)), 10)
	case *array.Int32:
		return strconv.AppendInt(dst, int64(a.Value(i)), 10)
	case *array.Int64:
		return strconv.AppendInt(dst, a.Value(i), 10)
	case *array.Uint8:
		return strconv.AppendUint(dst, uint64(a.Value(i)), 10)
	case *array.Uint16:
		return strconv.AppendUint(dst, uint64(a.Value(i)), 10)
	case *array.Uint32:
		return strconv.AppendUint(dst, uint64(a.Value(i)), 10)
	case *array.Uint64:
		return strconv.AppendUint(dst, a.Value(i), 10)
	case *array.Float32:
		return appendJSONFloat(dst, float64(a.Value(i)))
	case *array.Float64:
		return appendJSONFloat(dst, a.Value(i))
	case *array.String:
		return appendJSONString(dst, a.Value(i))
	case *array.LargeString:
		return appendJSONString(dst, a.Value(i))
	}
	return append(dst, "null"...) // unreachable when gated by fastRenderable
}

// appendJSONFloat appends f as JSON. NaN/±Inf aren't representable in JSON, so
// they become null (matching n1k1's number handling; see TODO.md).
func appendJSONFloat(dst []byte, f float64) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(dst, "null"...)
	}
	return strconv.AppendFloat(dst, f, 'g', -1, 64)
}

// appendJSONString appends s as a JSON string literal (RFC 8259 escaping: ",
// \, and control chars < 0x20; raw UTF-8 bytes pass through). No allocation.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b >= 0x20 && b != '"' && b != '\\' {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch b {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			const hex = "0123456789abcdef"
			dst = append(dst, '\\', 'u', '0', '0', hex[b>>4], hex[b&0xf])
		}
		start = i + 1
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// splitNDJSON slices b into its non-empty newline-delimited lines, reusing dst.
func splitNDJSON(b []byte, dst [][]byte) [][]byte {
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			if len(bytes.TrimSpace(b)) > 0 {
				dst = append(dst, b)
			}
			break
		}
		if line := b[:i]; len(bytes.TrimSpace(line)) > 0 {
			dst = append(dst, line)
		}
		b = b[i+1:]
	}
	return dst
}
