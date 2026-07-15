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
	base.VariantAppendJSON = func(dst, meta, value []byte) []byte {
		v, err := av.New(meta, value)
		if err != nil {
			return append(dst, "null"...)
		}
		return variant.AppendJSON(dst, v)
	}
}
