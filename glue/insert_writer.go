//go:build n1ql && !js

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

package glue

// INSERT INTO `<name>.parquet` writer (DESIGN-vectors.md): the write half of the vector
// story -- `INSERT INTO vecs.parquet SELECT VECTORIZE_BATCH(...) ...` produces the
// columnar list<float32> files the columnar VECTOR_DISTANCE path reads. It streams the
// source docs into an Arrow RecordBuilder and flushes row groups via pqarrow (no full
// materialization), committing atomically (temp + rename) like the JSONL writer.
//
// Schema is inferred from the FIRST doc (fields sorted for determinism): number ->
// INT64 (integral) or DOUBLE, string -> UTF8, bool -> BOOLEAN, and a numeric ARRAY ->
// list<float32> (element non-nullable, list field nullable -- the vec-column contract,
// so a missing/NULL vec row is a zero-length list, not a sentinel). Every column is
// nullable: a doc missing a field appends NULL. First-row-defines-schema is a STRICT
// contract -- the first row must carry every column; a later doc bearing a field the
// first row lacked is an error (nowhere to put it in the fixed schema), as is a value
// whose type conflicts with the inferred column (a fractional number into an INT64
// column, a non-numeric array element). Faithful or refuse -- never a silent coercion
// or drop.
//
// TODO(future): widen the accepted shapes. For complex values (nested objects,
// non-numeric/nested arrays) that error today, two options: (1) Parquet's VARIANT
// logical type (a self-describing column), or (2) stringify the complex value into a
// UTF8 column (queried back via the JSON functions). Both deferred -- the vector use
// case only needs flat scalars + a numeric vec array. See DESIGN-vectors.md.

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/couchbase/query/value"
)

// newInsertWriter picks the output format by target extension: .parquet -> parquetWriter,
// otherwise the JSON Lines writer. (The js build has a jsonl-only variant.)
func newInsertWriter(path, seedFrom, mode string) (insertWriter, error) {
	if strings.EqualFold(filepath.Ext(path), ".parquet") {
		return newParquetWriter(path, mode)
	}
	return newJSONLWriter(path, seedFrom)
}

// parquetRowGroup is how many rows accumulate in the builder before a row group flushes.
const parquetRowGroup = 8192

type parquetWriter struct {
	path, tmp string
	f         *os.File
	mem       memory.Allocator
	fw        *pqarrow.FileWriter
	bld       *array.RecordBuilder
	appenders []func(value.Value) error // one per schema field, positional
	names     []string                  // schema field names (sorted)
	nameSet   map[string]bool           // schema field names, for the extra-field check
	nameBuf   []string                  // reused buffer for doc.FieldNames per row
	batch     int                       // rows buffered in bld since the last flush
	n         int
	err       error
}

func newParquetWriter(path, mode string) (*parquetWriter, error) {
	if mode == insertModeAppend {
		return nil, fmt.Errorf(`INSERT INTO %q: "append" mode is not supported for a .parquet target `+
			`(a Parquet file can't be appended in place; use OPTIONS {"mode":"overwrite"})`, path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("INSERT: creating %q: %w", filepath.Dir(path), err)
	}
	tmp := path + ".n1k1-insert.tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("INSERT: creating temp file: %w", err)
	}
	return &parquetWriter{path: path, tmp: tmp, f: f, mem: memory.NewGoAllocator()}, nil
}

func (pw *parquetWriter) writeErr() error { return pw.err }
func (pw *parquetWriter) rows() int       { return pw.n }

func (pw *parquetWriter) setErr(e error) {
	if pw.err == nil {
		pw.err = e
	}
}

func (pw *parquetWriter) write(doc value.Value) {
	if pw.err != nil || pqIsNull(doc) {
		return
	}
	if pw.bld == nil {
		if e := pw.start(doc); e != nil {
			pw.setErr(e)
			return
		}
	}
	// First-row-defines-schema is a strict contract: a later row carrying a field the
	// first row didn't have has nowhere to go in the fixed Parquet schema, so refuse it
	// rather than silently drop the value. (A field the schema HAS but this row lacks is
	// fine -- it writes NULL.)
	pw.nameBuf = doc.FieldNames(pw.nameBuf[:0])
	for _, fn := range pw.nameBuf {
		if !pw.nameSet[fn] {
			pw.setErr(fmt.Errorf("INSERT: row %d has field %q, which is absent from the schema "+
				"inferred from the first row %v (a .parquet target has a fixed schema; the first "+
				"row must carry every column)", pw.n, fn, pw.names))
			return
		}
	}
	for i, name := range pw.names {
		fv, _ := doc.Field(name) // absent -> nil -> appended as NULL
		if e := pw.appenders[i](fv); e != nil {
			pw.setErr(fmt.Errorf("INSERT: row %d field %q: %w", pw.n, name, e))
			return
		}
	}
	pw.batch++
	pw.n++
	if pw.batch >= parquetRowGroup {
		pw.flush()
	}
}

// start infers the schema from the first doc and builds the RecordBuilder + per-field
// appenders + the pqarrow file writer.
func (pw *parquetWriter) start(doc value.Value) error {
	names := doc.FieldNames(nil)
	if len(names) == 0 {
		return fmt.Errorf("cannot INSERT into a .parquet target: the first row is an empty object (no columns to infer)")
	}
	sort.Strings(names)

	fields := make([]arrow.Field, len(names))
	kinds := make([]string, len(names))
	for i, name := range names {
		fv, _ := doc.Field(name)
		k, dt, ok := inferParquetKind(fv)
		if !ok {
			return fmt.Errorf("cannot infer a Parquet type for field %q from the first row (type %v)", name, valType(fv))
		}
		fields[i] = arrow.Field{Name: name, Type: dt, Nullable: true}
		kinds[i] = k
	}

	schema := arrow.NewSchema(fields, nil)
	fw, err := pqarrow.NewFileWriter(schema, pw.f, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	bld := array.NewRecordBuilder(pw.mem, schema)

	pw.fw = fw
	pw.bld = bld
	pw.names = names
	pw.nameSet = make(map[string]bool, len(names))
	for _, n := range names {
		pw.nameSet[n] = true
	}
	pw.appenders = make([]func(value.Value) error, len(names))
	for i, k := range kinds {
		pw.appenders[i] = makeParquetAppender(k, bld.Field(i))
	}
	return nil
}

func (pw *parquetWriter) flush() {
	if pw.batch == 0 || pw.err != nil {
		return
	}
	rec := pw.bld.NewRecord() // also resets the builder for reuse
	if e := pw.fw.Write(rec); e != nil {
		pw.setErr(e)
	}
	rec.Release()
	pw.batch = 0
}

func (pw *parquetWriter) finish() error {
	if pw.err != nil {
		return pw.err
	}
	if pw.bld == nil {
		// Zero rows: no schema to infer. Leave an empty file (an empty .parquet is a
		// degenerate keyspace part; a JSONL INSERT of zero rows is likewise 0 bytes).
		if e := pw.f.Close(); e != nil {
			return e
		}
		return os.Rename(pw.tmp, pw.path)
	}
	pw.flush()
	if pw.err != nil {
		return pw.err
	}
	if e := pw.fw.Close(); e != nil { // writes the footer AND closes the underlying file
		return e
	}
	return os.Rename(pw.tmp, pw.path)
}

func (pw *parquetWriter) abort() {
	if pw.fw != nil {
		pw.fw.Close() // also closes pw.f
	} else {
		pw.f.Close()
	}
	os.Remove(pw.tmp)
}

// inferParquetKind maps a first-row field value to (kind, arrow type). Number ->
// INT64 (integral) or DOUBLE; string -> UTF8; bool -> BOOLEAN; numeric array ->
// list<float32> (the vec column, element non-nullable). A null/missing first value or
// an unsupported type (object, non-numeric array) is not inferable.
func inferParquetKind(fv value.Value) (string, arrow.DataType, bool) {
	if pqIsNull(fv) {
		return "", nil, false
	}
	switch fv.Type() {
	case value.BOOLEAN:
		return "bool", arrow.FixedWidthTypes.Boolean, true
	case value.NUMBER:
		f, _ := pqNumFloat(fv)
		if isIntegral(f) {
			return "int64", arrow.PrimitiveTypes.Int64, true
		}
		return "double", arrow.PrimitiveTypes.Float64, true
	case value.STRING:
		return "string", arrow.BinaryTypes.String, true
	case value.ARRAY:
		return "vec", arrow.ListOfNonNullable(arrow.PrimitiveTypes.Float32), true
	}
	return "", nil, false
}

// makeParquetAppender returns a closure that appends one field value to the given
// column builder (a NULL/missing value -> AppendNull), erroring on a type conflict.
func makeParquetAppender(kind string, fb array.Builder) func(value.Value) error {
	switch kind {
	case "bool":
		b := fb.(*array.BooleanBuilder)
		return func(v value.Value) error {
			if pqIsNull(v) {
				b.AppendNull()
				return nil
			}
			bv, ok := v.Actual().(bool)
			if !ok {
				return fmt.Errorf("expected boolean, got %v", valType(v))
			}
			b.Append(bv)
			return nil
		}
	case "int64":
		b := fb.(*array.Int64Builder)
		return func(v value.Value) error {
			if pqIsNull(v) {
				b.AppendNull()
				return nil
			}
			f, ok := pqNumFloat(v)
			if !ok {
				return fmt.Errorf("expected number, got %v", valType(v))
			}
			if !isIntegral(f) {
				return fmt.Errorf("non-integer %v in an integer column (first row was integral)", f)
			}
			b.Append(int64(f))
			return nil
		}
	case "double":
		b := fb.(*array.Float64Builder)
		return func(v value.Value) error {
			if pqIsNull(v) {
				b.AppendNull()
				return nil
			}
			f, ok := pqNumFloat(v)
			if !ok {
				return fmt.Errorf("expected number, got %v", valType(v))
			}
			b.Append(f)
			return nil
		}
	case "string":
		b := fb.(*array.StringBuilder)
		return func(v value.Value) error {
			if pqIsNull(v) {
				b.AppendNull()
				return nil
			}
			s, ok := v.Actual().(string)
			if !ok {
				return fmt.Errorf("expected string, got %v", valType(v))
			}
			b.Append(s)
			return nil
		}
	default: // "vec": list<float32>
		lb := fb.(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Float32Builder)
		return func(v value.Value) error {
			if pqIsNull(v) {
				lb.AppendNull() // row-level NULL vec = zero-length list (the contract)
				return nil
			}
			if v.Type() != value.ARRAY {
				return fmt.Errorf("expected array, got %v", valType(v))
			}
			arr, _ := v.Actual().([]interface{})
			lb.Append(true)
			for i := range arr {
				ev, _ := v.Index(i)
				f, ok := pqNumFloat(ev)
				if !ok {
					return fmt.Errorf("non-numeric element %d in a vector column", i)
				}
				vb.Append(float32(f))
			}
			return nil
		}
	}
}

func pqIsNull(v value.Value) bool {
	return v == nil || v.Type() == value.MISSING || v.Type() == value.NULL
}

func pqNumFloat(v value.Value) (float64, bool) {
	if v == nil || v.Type() != value.NUMBER {
		return 0, false
	}
	switch a := v.Actual().(type) {
	case float64:
		return a, true
	case int64:
		return float64(a), true
	}
	return 0, false
}

func isIntegral(f float64) bool {
	return !math.IsInf(f, 0) && !math.IsNaN(f) && f == math.Trunc(f) && math.Abs(f) < 1<<53
}

func valType(v value.Value) interface{} {
	if v == nil {
		return "missing"
	}
	return v.Type()
}
