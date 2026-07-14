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

// Pure-Go vec-source types (no Arrow), so walkSource's fields + glue's type assertions
// compile for every target incl. GOOS=js/wasm; the Arrow-backed implementations live in
// vector_source.go (!js). See DESIGN-vectors.md.

// VectorBatch is one batch of a VectorBatchSource. Vec is the borrowed contiguous
// float32 child buffer of the present vectors (valid until the next NextVectorBatch/
// Close). When Regular (no null rows and every row has Dim elements) row r is
// Vec[r*Dim:(r+1)*Dim] and Offsets is nil; otherwise a NULL vec is a zero-length row
// and row r is Vec[Offsets[r]:Offsets[r+1]] (len Dim, or 0 == NULL). Scalars[i] is the
// i-th requested scalar field's values; ScalarTypes[i] is "INT64"/"DOUBLE" (packed
// 8-byte in Scalars[i], ScalarStrings[i] nil) or "UTF8" (per-row strings in
// ScalarStrings[i], Scalars[i] nil); ScalarValids[i] is its validity bitmap, or nil if
// no nulls.
type VectorBatch struct {
	Rows          int
	Dim           int
	Vec           []float32
	Offsets       []int32 // nil when Regular
	Regular       bool
	Scalars       [][]byte
	ScalarStrings [][]string
	ScalarTypes   []string
	ScalarValids  [][]byte
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

// VectorSchemaSource reports, from the footer alone (no data pages), whether a named
// top-level field is a list-of-float32 vec column -- the plan-time check that gates the
// columnar VECTOR_DISTANCE rewrite (glue). ScalarField reports whether a field is a
// fixed 8-byte numeric scalar (INT64/DOUBLE), so the rewrite can confirm the id/side
// columns are readable by NextVectorBatch.
type VectorSchemaSource interface {
	VectorField(field string) bool
	ScalarField(field string) bool
}
