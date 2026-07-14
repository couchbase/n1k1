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

// VectorBatchSource feeds the columnar VECTOR_DISTANCE top-K path (DESIGN-vectors.md):
// per batch it hands back the list<float32> vec column as a borrowed contiguous child
// buffer + per-row offsets (zero copy, no JSON), plus the requested scalar side columns
// (id, ...) as 8-byte little-endian buffers. It's the read half of the map->order->limit
// executor; the write half (INSERT INTO parquet) produces these files.

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/couchbase/n1k1/base"
)

// VectorField implements VectorSchemaSource: field is a vec column iff its leaf lives
// under "field.list...." (a nested list) with a FLOAT (float32) physical type.
func (s *parquetSource) VectorField(field string) bool {
	sch := s.pf.MetaData().Schema
	prefix := field + "."
	for c := 0; c < sch.NumColumns(); c++ {
		col := sch.Column(c)
		if strings.HasPrefix(col.Path(), prefix) {
			return col.PhysicalType().String() == "FLOAT"
		}
	}
	return false
}

// ScalarField implements VectorSchemaSource: field is a top-level scalar side column the
// columnar reader can carry -- a fixed 8-byte numeric (INT64/DOUBLE) or a UTF8 string
// (BYTE_ARRAY, e.g. a string doc id). Its leaf path must equal field (no nesting).
func (s *parquetSource) ScalarField(field string) bool {
	sch := s.pf.MetaData().Schema
	for c := 0; c < sch.NumColumns(); c++ {
		col := sch.Column(c)
		if col.Path() == field {
			switch col.PhysicalType().String() {
			case "INT64", "DOUBLE", "BYTE_ARRAY":
				return true
			}
			return false
		}
	}
	return false
}

// VectorField / ScalarField for a multi-file keyspace: consult the first part's footer
// (parts are homogeneous, the normal partitioned-export case).
func (w *walkSource) VectorField(field string) bool { return w.firstVecSchema(field, true) }
func (w *walkSource) ScalarField(field string) bool { return w.firstVecSchema(field, false) }

func (w *walkSource) firstVecSchema(field string, vec bool) bool {
	if len(w.files) == 0 {
		return false
	}
	s, err := OpenFile(w.files[0], "")
	if err != nil {
		return false
	}
	defer s.Close()
	vss, ok := s.(VectorSchemaSource)
	if !ok {
		return false
	}
	if vec {
		return vss.VectorField(field)
	}
	return vss.ScalarField(field)
}

// NextVectorBatch implements VectorBatchSource for a single Parquet file. It reads all
// columns (projection pushdown for the nested vec leaf is a later optimization) and
// picks out the named vec + scalar columns by schema field name.
func (s *parquetSource) NextVectorBatch(vecField string, scalarFields []string) (VectorBatch, bool, error) {
	if s.rrVec == nil {
		rr, err := s.pr.GetRecordReader(context.Background(), nil, nil)
		if err != nil {
			return VectorBatch{}, false, err
		}
		s.rrVec = rr
	}
	if s.curBatchVec != nil {
		s.curBatchVec.Release()
		s.curBatchVec = nil
	}
	batch, err := s.rrVec.Read()
	if err == io.EOF {
		return VectorBatch{}, false, nil
	}
	if err != nil {
		return VectorBatch{}, false, err
	}
	s.curBatchVec = batch
	return arrowVectorBatch(batch, vecField, scalarFields)
}

// arrowVectorBatch extracts the vec list column + scalar columns from an Arrow batch
// into a VectorBatch, borrowing the buffers (no copy of the float32 data).
func arrowVectorBatch(batch arrow.RecordBatch, vecField string, scalarFields []string) (VectorBatch, bool, error) {
	schema := batch.Schema()
	rows := int(batch.NumRows())
	vb := VectorBatch{Rows: rows}

	vi := schema.FieldIndices(vecField)
	if len(vi) == 0 {
		return VectorBatch{}, false, fmt.Errorf("records: vec field %q not found", vecField)
	}
	lst, ok := batch.Column(vi[0]).(*array.List)
	if !ok {
		return VectorBatch{}, false, fmt.Errorf("records: vec field %q is %T, want list<float32>", vecField, batch.Column(vi[0]))
	}
	child, ok := lst.ListValues().(*array.Float32)
	if !ok {
		return VectorBatch{}, false, fmt.Errorf("records: vec field %q element is %T, want float32", vecField, lst.ListValues())
	}
	offs := lst.Offsets() // len rows+1, indices into the child's logical values
	childOff := child.Data().Offset()
	all := base.VecFloat32(child.Data().Buffers()[1].Bytes())
	// Window the child to this list's value range, rebased so row 0 starts at 0.
	base0 := int(offs[0])
	vb.Vec = all[childOff+base0 : childOff+int(offs[rows])]

	// Dim = the width of the first present (non-empty) row.
	for r := 0; r < rows; r++ {
		if offs[r+1] > offs[r] {
			vb.Dim = int(offs[r+1] - offs[r])
			break
		}
	}
	// Regular (fast path) iff no nulls and the child is exactly rows*Dim contiguous.
	if lst.NullN() == 0 && vb.Dim > 0 && int(offs[rows]-offs[0]) == rows*vb.Dim {
		vb.Regular = true
	} else {
		vb.Offsets = make([]int32, rows+1)
		for r := 0; r <= rows; r++ {
			vb.Offsets[r] = offs[r] - offs[0]
		}
	}

	for _, name := range scalarFields {
		si := schema.FieldIndices(name)
		if len(si) == 0 {
			return VectorBatch{}, false, fmt.Errorf("records: scalar field %q not found", name)
		}
		col := batch.Column(si[0])
		switch col.DataType().ID() {
		case arrow.INT64, arrow.UINT64, arrow.FLOAT64:
			b, err := arrowValueBytes(col)
			if err != nil {
				return VectorBatch{}, false, fmt.Errorf("records: scalar field %q: %v", name, err)
			}
			typ := "INT64"
			if col.DataType().ID() == arrow.FLOAT64 {
				typ = "DOUBLE"
			}
			vb.Scalars = append(vb.Scalars, b)
			vb.ScalarStrings = append(vb.ScalarStrings, nil)
			vb.ScalarTypes = append(vb.ScalarTypes, typ)
		case arrow.STRING:
			sa := col.(*array.String)
			ss := make([]string, rows)
			for r := 0; r < rows; r++ {
				if sa.IsValid(r) {
					ss[r] = sa.Value(r)
				}
			}
			vb.Scalars = append(vb.Scalars, nil)
			vb.ScalarStrings = append(vb.ScalarStrings, ss)
			vb.ScalarTypes = append(vb.ScalarTypes, "UTF8")
		default:
			return VectorBatch{}, false, fmt.Errorf("records: scalar field %q type %s unsupported", name, col.DataType())
		}
		vb.ScalarValids = append(vb.ScalarValids, arrowValidityBytes(col))
	}
	return vb, true, nil
}

// NextVectorBatch implements VectorBatchSource across a multi-file keyspace, opening
// each part lazily and draining its batches before advancing (mirrors NextColumns).
func (w *walkSource) NextVectorBatch(vecField string, scalarFields []string) (VectorBatch, bool, error) {
	for {
		if w.curVB == nil {
			if w.iVB >= len(w.files) {
				return VectorBatch{}, false, nil
			}
			s, e := OpenFile(w.files[w.iVB], "")
			if e != nil {
				return VectorBatch{}, false, e
			}
			vbs, ok := s.(VectorBatchSource)
			if !ok {
				s.Close()
				return VectorBatch{}, false, fmt.Errorf("records: %s is not a VectorBatchSource", w.files[w.iVB])
			}
			w.curVB, w.curVBSrc = vbs, s
		}
		vb, ok, err := w.curVB.NextVectorBatch(vecField, scalarFields)
		if err != nil {
			return VectorBatch{}, false, err
		}
		if ok {
			return vb, true, nil
		}
		w.curVBSrc.Close()
		w.curVB, w.curVBSrc = nil, nil
		w.iVB++
	}
}
