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

// Bridge for the VARIANT fidelity carrier (DESIGN-variant.md §4.1, Phase 1): base owns
// the arrow-go-free carrier framing + classification (base/variant.go); the actual
// decode/projection lives HERE, where importing the arrow-go-backed ./variant library is
// allowed — keeping base itself arrow-go-free so it still builds under GOOS=js/wasm. The
// hook is installed on init, so any binary that links records (the package that scans
// VARIANT) can project a carried `V` value back to JSON. Guarded `!js` because a `V`
// value only ever originates from the Parquet scan, which is itself `!js`.

import (
	av "github.com/apache/arrow-go/v18/parquet/variant"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/variant"
)

func init() {
	// Both hooks go straight to the byte-level variant walkers -- no av.New, so no
	// per-call av.NewMetadata make([][]byte, dictSize) dictionary build.
	base.VariantAppendJSON = variant.AppendJSONBytes

	base.VariantPathGet = variantPathGet
}

// variantPathGet navigates `path` through the VARIANT-carrier val and returns the
// reached value: a scalar leaf projected to JSON into valOut, or a container leaf as a
// self-contained `V` envelope in valOut (its bytes carry the shared metadata, so the
// child's field-ids stay valid — a zero-re-encode reframe).
//
// It is allocation-free on the hot path: variant.PathGet walks the value bytes by offset
// (no av.Value boxing per node, no av.New metadata-dictionary build), returning a
// zero-copy sub-slice of the carrier; the leaf is then either re-enveloped (a byte copy
// into the reused valOut buffer) or rendered by variant.AppendJSONBytes (also byte-level).
func variantPathGet(val base.Val, path []string, valOut base.Val) (base.Val, base.Val) {
	meta, value, ok := base.SplitVariantEnvelope(val)
	if !ok {
		return base.ValMissing, valOut
	}
	child, ok := variant.PathGet(meta, value, path)
	if !ok {
		return base.ValMissing, valOut
	}
	switch av.BasicType(child[0] & 0x03) {
	case av.BasicObject, av.BasicArray:
		valOut = base.AppendVariantEnvelope(valOut[:0], meta, child)
	default:
		valOut = variant.AppendJSONBytes(valOut[:0], meta, child)
	}
	return valOut, valOut
}
