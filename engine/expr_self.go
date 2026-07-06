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

package engine

import (
	"github.com/couchbase/n1k1/base"
)

// Native whole-row `self` / SELECT * projection. Instead of boxing the row
// into a value.Value and serializing it (the fallback's Convert -> Self ->
// WriteJSON, which allocates one value.Value plus its JSON per row), ExprSelf
// assembles the row object's JSON bytes directly from the selected label vals
// into a reused buffer -- zero steady-state garbage, like the other native
// exprs. See base.ValsSelfObject and DESIGN-exprs.md ("self-projection byte
// path").
//
// The glue optimizer emits the "self" param only for row shapes it can
// reproduce byte-correctly: plain .["name"] field labels (attachments and
// binding names handled by glue, which passes only the labels to emit). Other
// shapes (whole-row ".", nested paths, ".*") stay boxed via exprTree.
//
// params is a flat list of (key, label) string pairs: key is the emitted
// object key, label is the source label whose val supplies the value. Key order
// is the given order (NOT cbq's sorted order); result comparison is
// key-order-insensitive.

func init() {
	ExprCatalog["self"] = ExprSelf
}

func ExprSelf(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var keyTokens [][]byte
	var indices []int

	for i := 0; i+1 < len(params); i += 2 {
		keyTokens = append(keyTokens, base.SelfKeyToken(params[i].(string)))
		indices = append(indices, labels.IndexOf(params[i+1].(string)))
	}

	var lzKeyTokens [][]byte = keyTokens // <== varLift: lzKeyTokens by path
	var lzIndices []int = indices        // <== varLift: lzIndices by path

	var lzJoined []byte // <== varLift: lzJoined by path

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		if LzScope {
			lzBytes := lzJoined[:0]

			lzJoined = base.ValsSelfObject(lzKeyTokens, lzIndices, lzVals, lzBytes)

			lzVal = base.Val(lzJoined)
		}

		return lzVal
	}

	return lzExprFunc
}
