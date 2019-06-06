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

package n1k1

import (
	"encoding/binary" // <== genCompiler:hide

	"strconv"

	"github.com/couchbase/n1k1/base"
)

// ExprCatalog is the default registry of known expression functions.
var ExprCatalog = map[string]base.ExprCatalogFunc{
	"json":        ExprJson,
	"labelPath":   ExprLabelPath,
	"labelUint64": ExprLabelUint64,

	"valsEncode":          ExprValsEncode,
	"valsEncodeCanonical": ExprValsEncodeCanonical,
}

// -----------------------------------------------------

func MakeExprFunc(lzVars *base.Vars, labels base.Labels,
	expr []interface{}, path, pathItem string) (lzExprFunc base.ExprFunc) {
	pathNext := EmitPush(path, pathItem)

	lzExprFunc =
		lzVars.Ctx.ExprCatalog[expr[0].(string)](lzVars, labels, expr[1:], pathNext) // !lz

	EmitPop(path, pathItem)

	return lzExprFunc
}

// -----------------------------------------------------

func ExprJson(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	json := []byte(params[0].(string))

	var lzValJson base.Val = base.Val(json) // <== varLift: lzValJson by path

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzValJson
		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------

func ExprLabelPath(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	idx := labels.IndexOf(params[0].(string))
	if idx >= 0 {
		var valPath []string
		for _, param := range params[1:] {
			valPath = append(valPath, param.(string))
		}

		var lzValOut base.Val // <== varLift: lzValOut by path

		var lzValPath []string = valPath // <== varLift: lzValPath by path

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal = lzVals[idx]

			if len(params) > 1 { // !lz
				lzVal, lzValOut = base.ValPathGet(lzVal, lzValPath, lzValOut)
			} else { // !lz
				_ = lzValPath
			} // !lz

			return lzVal
		}
	} else {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal = base.ValMissing
			return lzVal
		}
	}

	return lzExprFunc
}

// -----------------------------------------------------

// ExprLabelUint64 converts the binary encoded uint64 at the label
// position to JSON integer representation.
func ExprLabelUint64(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	idx := labels.IndexOf(params[0].(string))
	if idx >= 0 {
		var lzBufPre []byte // <== varLift: lzBufPre by path

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzN := binary.LittleEndian.Uint64(lzVals[idx])

			lzBuf := strconv.AppendUint(lzBufPre[:0], lzN, 10)

			lzVal = base.Val(lzBuf)

			lzBufPre = lzBuf

			return lzVal
		}
	} else {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal = base.ValMissing
			return lzVal
		}
	}

	return lzExprFunc
}

// -----------------------------------------------------

// ExprValsEncode encodes vals using base.ValsEncode(). The result is
// non-JSON / BINARY and can be parsed using base.ValsDecode().
func ExprValsEncode(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzJoined []byte // <== varLift: lzJoined by path

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		if LzScope {
			var lzErr error

			lzBytes := lzJoined[:0]

			lzJoined = base.ValsEncode(lzVals, lzBytes)
			if lzErr == nil {
				lzVal = base.Val(lzJoined)
			}
		}

		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------

// ExprValsEncodeCanonical encodes the vals using
// base.ValsEncodeCanonical(). The result is non-JSON / BINARY and can
// be parsed using base.ValsDecode().
func ExprValsEncodeCanonical(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzJoined []byte // <== varLift: lzJoined by path

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		if LzScope {
			var lzErr error

			lzBytes := lzJoined[:0]

			lzJoined, lzErr = base.ValsEncodeCanonical(lzVals, lzBytes, lzVars.Ctx.ValComparer)
			if lzErr == nil {
				lzVal = base.Val(lzJoined)
			}
		}

		return lzVal
	}

	return lzExprFunc
}
