//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package base

import (
	"encoding/binary"
	"fmt"

	"github.com/couchbase/rhmap/heap"
	"github.com/couchbase/rhmap/store"
)

// ValsLessFunc is the signature for comparing two vals.
type ValsLessFunc func(valsA, valsB Vals) bool

// ---------------------------------------------

// ValsProjectedEncode encodes a vals-projected item.
func ValsProjectedEncode(vals, projected Vals, out []byte,
	valComparer *ValComparer) (rv []byte, err error) {
	var buf8 [8]byte
	out = append(out, buf8[:]...) // Prepend space for projected len.

	// Encode projected before vals as it's accessed more often.
	out, err = ValsEncodeCanonical(projected, out, valComparer)
	if err != nil {
		return out, fmt.Errorf(
			"ValsProjectedEncode, projected: %v, err: %v\n",
			projected, err)
	}

	// Write projected len into the earlier prepended space.
	binary.LittleEndian.PutUint64(buf8[:], uint64(len(out)-8))
	copy(out[:8], buf8[:])

	// Encode vals.
	out = ValsEncode(vals, out)

	return out, nil
}

// ValsProjectedDecode decodes a vals-projected item.
func ValsProjectedDecodeVals(b []byte, out Vals) Vals {
	n := int(binary.LittleEndian.Uint64(b[:8])) // Projected length.
	return ValsDecode(b[8+n:], out)
}

// ValsProjectedDecodeProjected decodes only the projected part of an
// encoded vals-projected item.
func ValsProjectedDecodeProjected(b []byte, out Vals) Vals {
	n := int(binary.LittleEndian.Uint64(b[:8])) // Projected length.
	return ValsDecode(b[8:8+n], out)
}

// ---------------------------------------------

type HeapValsProjected struct{ heap.Heap }

// CreateHeapValsProjected creates a max-heap for ValsProjected items
// with an associated ValsLessFunc for the projected data. When the
// heap becomes too big, it will automatically spill to temp files.
func CreateHeapValsProjected(ctx *Ctx,
	valsLessFunc ValsLessFunc) *HeapValsProjected {
	pathPrefix := ctx.TempDir + "_hvp"     // TODO: Config.
	heapChunkSizeBytes := 1024 * 1024      // TODO: Config.
	dataChunkSizeBytes := 16 * 1024 * 1024 // TODO: Config.

	var pa, pb Vals

	return &HeapValsProjected{heap.Heap{
		LessFunc: func(a, b []byte) bool {
			pa = ValsProjectedDecodeProjected(a, pa[:0])
			pb = ValsProjectedDecodeProjected(b, pb[:0])

			// Reverse a & b so that we have a max-heap.
			return valsLessFunc(pb, pa)
		},
		Heap: &store.Chunks{
			PathPrefix:     pathPrefix,
			FileSuffix:     ".heap",
			ChunkSizeBytes: heapChunkSizeBytes,
		},
		Data: &store.Chunks{
			PathPrefix:     pathPrefix,
			FileSuffix:     ".data",
			ChunkSizeBytes: dataChunkSizeBytes,
		},
	}}
}
