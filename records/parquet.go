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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

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

	buf   bytes.Buffer // current batch rendered as NDJSON
	lines [][]byte     // per-row slices into buf
	li    int          // next line index
	idBuf []byte
	done  bool
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

// ProjectColumns implements ColumnProjector: read only the named columns. Must
// be called before the first Next.
func (s *parquetSource) ProjectColumns(names []string) error {
	if s.rr != nil {
		return fmt.Errorf("records: ProjectColumns must be called before Next")
	}
	sch := s.pf.MetaData().Schema
	proj := make([]int, 0, len(names))
	for _, n := range names {
		i := sch.ColumnIndexByName(n)
		if i < 0 {
			return fmt.Errorf("records: parquet has no column %q", n)
		}
		proj = append(proj, i)
	}
	s.proj = proj
	return nil
}

// Columns implements ColumnsSource: describe columns from the footer (types,
// null counts, min/max) with no data pages read.
func (s *parquetSource) Columns() []ColumnMeta {
	md := s.pf.MetaData()
	sch := md.Schema
	out := make([]ColumnMeta, 0, sch.NumColumns())
	for c := 0; c < sch.NumColumns(); c++ {
		cm := ColumnMeta{Name: sch.Column(c).Name(), Type: sch.Column(c).PhysicalType().String(), NullCount: -1}
		// Aggregate stats across row groups: null count sums; min/max from RG 0
		// (a coarse zone-map bound -- adequate for the column-meta contract).
		var haveNull bool
		for rg := 0; rg < md.NumRowGroups(); rg++ {
			cc, err := md.RowGroup(rg).ColumnChunk(c)
			if err != nil {
				continue
			}
			st, err := cc.Statistics()
			if err != nil || st == nil {
				continue
			}
			if st.HasNullCount() {
				if !haveNull {
					cm.NullCount, haveNull = 0, true
				}
				cm.NullCount += st.NullCount()
			}
			if rg == 0 && st.HasMinMax() {
				cm.Min, cm.Max = st.EncodeMin(), st.EncodeMax()
			}
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
		s.buf.Reset()
		err = array.RecordToJSON(batch, &s.buf)
		batch.Release()
		if err != nil {
			return false, err
		}
		s.lines = splitNDJSON(s.buf.Bytes(), s.lines[:0])
		s.li = 0
	}

	rec.Doc = s.lines[s.li]
	s.idBuf = appendRecordID(s.idBuf[:0], s.idPrefix, s.row)
	rec.ID = s.idBuf
	s.li++
	s.row++
	return true, nil
}

func (s *parquetSource) Close() error {
	if s.rr != nil {
		s.rr.Release()
	}
	if s.pf != nil {
		return s.pf.Close()
	}
	return nil
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
