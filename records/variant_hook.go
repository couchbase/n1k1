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
	"strconv"

	av "github.com/apache/arrow-go/v18/parquet/variant"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/variant"
)

func init() {
	base.VariantAppendJSON = func(dst, meta, value []byte) []byte {
		v, err := av.New(meta, value)
		if err != nil {
			return append(dst, "null"...)
		}
		return variant.AppendJSON(dst, v)
	}

	base.VariantPathGet = variantPathGet
}

// variantPathGet navigates `path` through the VARIANT-carrier val and returns the
// reached value: a scalar leaf projected to JSON into valOut, or a container leaf as a
// self-contained `V` envelope in valOut (its bytes carry the shared metadata, so the
// child's field-ids stay valid — a zero-re-encode reframe). Uses the arrow-go view API;
// obtaining a container from a Value boxes one struct per descended node (acceptable on
// the opt-in fidelity path — an unboxed offset walk is a later optimization).
func variantPathGet(val base.Val, path []string, valOut base.Val) (base.Val, base.Val) {
	meta, value, ok := base.SplitVariantEnvelope(val)
	if !ok {
		return base.ValMissing, valOut
	}
	cur, err := av.New(meta, value)
	if err != nil {
		return base.ValMissing, valOut
	}
	for _, p := range path {
		switch cur.BasicType() {
		case av.BasicObject:
			f, err := cur.Value().(av.ObjectValue).ValueByKey(p)
			if err != nil {
				return base.ValMissing, valOut
			}
			cur = f.Value
		case av.BasicArray:
			idx, err := strconv.Atoi(p)
			if err != nil {
				return base.ValMissing, valOut
			}
			arr := cur.Value().(av.ArrayValue)
			if idx < 0 || uint32(idx) >= arr.Len() {
				return base.ValMissing, valOut
			}
			e, err := arr.Value(uint32(idx))
			if err != nil {
				return base.ValMissing, valOut
			}
			cur = e
		default:
			return base.ValMissing, valOut // can't navigate into a scalar
		}
	}
	switch cur.BasicType() {
	case av.BasicObject, av.BasicArray:
		valOut = base.AppendVariantEnvelope(valOut[:0], cur.Metadata().Bytes(), cur.Bytes())
	default:
		valOut = variant.AppendJSON(valOut[:0], cur)
	}
	return valOut, valOut
}
