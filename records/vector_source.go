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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/couchbase/n1k1/base"
)

// VectorBatch is one batch of a VectorBatchSource. Vec is the borrowed contiguous
// float32 child buffer of the present vectors (valid until the next NextVectorBatch/
// Close). When Regular (no null rows and every row has Dim elements) row r is
// Vec[r*Dim:(r+1)*Dim] and Offsets is nil; otherwise a NULL vec is a zero-length row
// and row r is Vec[Offsets[r]:Offsets[r+1]] (len Dim, or 0 == NULL). Scalars[i] is the
// i-th requested scalar field's packed 8-byte values; ScalarTypes[i] is "INT64" or
// "DOUBLE" (for formatting); ScalarValids[i] is its validity bitmap, or nil if no nulls.
type VectorBatch struct {
	Rows         int
	Dim          int
	Vec          []float32
	Offsets      []int32 // nil when Regular
	Regular      bool
	Scalars      [][]byte
	ScalarTypes  []string
	ScalarValids [][]byte
}

// RowVec returns row r's vector slice (nil for a NULL/zero-length row).
func (b *VectorBatch) RowVec(r int) []float32 {
	if b.Regular {
		return b.Vec[r*b.Dim : r*b.Dim+b.Dim]
	}
	lo, hi := b.Offsets[r], b.Offsets[r+1]
	if lo == hi {
		return nil
	}
	return b.Vec[lo:hi]
}

// VectorBatchSource is a Source that yields a vec list column + scalar side columns
// directly, skipping the JSON transpose. NextVectorBatch advances one batch; the
// field names are fixed on the first call. Buffers are borrowed until the next call.
type VectorBatchSource interface {
	NextVectorBatch(vecField string, scalarFields []string) (VectorBatch, bool, error)
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
		b, err := arrowValueBytes(col)
		if err != nil {
			return VectorBatch{}, false, fmt.Errorf("records: scalar field %q: %v", name, err)
		}
		var typ string
		switch col.DataType().ID() {
		case arrow.INT64, arrow.UINT64:
			typ = "INT64"
		case arrow.FLOAT64:
			typ = "DOUBLE"
		default:
			return VectorBatch{}, false, fmt.Errorf("records: scalar field %q type %s unsupported", name, col.DataType())
		}
		vb.Scalars = append(vb.Scalars, b)
		vb.ScalarTypes = append(vb.ScalarTypes, typ)
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
