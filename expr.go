package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

// LzScope is used to mark variable scopes as lz (ex: IF block).
const LzScope = true

// -----------------------------------------------------

// Marks the start of a nested "emit capture" area.
var EmitPush = func(path, pathItem string) string {
	return path + "_" + pathItem // Placeholder for compiler.
}

// Marks the end of a nested "emit capture" area.
var EmitPop = func(path, pathItem string) {} // Placeholder for compiler.

// -----------------------------------------------------

// ExprCatalog is a registry of all the known expression functions.
var ExprCatalog = map[string]ExprCatalogFunc{
	"json":      ExprJson,
	"fieldPath": ExprFieldPath,
}

type ExprCatalogFunc func(lzVars *base.Vars, fields base.Fields,
	types base.Types, params []interface{}, path string) base.ExprFunc

// -----------------------------------------------------

func MakeExprFunc(lzVars *base.Vars, fields base.Fields, types base.Types,
	expr []interface{}, path, pathItem string) (lzExprFunc base.ExprFunc) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	lzExprFunc =
		ExprCatalog[expr[0].(string)](lzVars, fields, types, expr[1:], pathNext) // !lz

	return lzExprFunc
}

// -----------------------------------------------------

func ExprJson(lzVars *base.Vars, fields base.Fields, types base.Types,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	json := []byte(params[0].(string))

	var lzValJson base.Val = base.Val(json) // <== varLift: lzValJson by path

	lzExprFunc = func(lzVals base.Vals) (lzVal base.Val) {
		lzVal = lzValJson
		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------

func ExprFieldPath(lzVars *base.Vars, fields base.Fields, types base.Types,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	idx := fields.IndexOf(params[0].(string))
	if idx >= 0 {
		lzExprFunc = func(lzVals base.Vals) (lzVal base.Val) {
			lzVal = lzVals[idx]

			if len(params) > 1 { // !lz
				var parts []string                 // !lz
				for _, param := range params[1:] { // !lz
					parts = append(parts, param.(string)) // !lz
				} // !lz

				lzVal = base.ValPathGet(lzVal, parts)
			} // !lz

			return lzVal
		}
	} else {
		lzExprFunc = func(lzVals base.Vals) (lzVal base.Val) {
			lzVal = base.ValMissing
			return lzVal
		}
	}

	return lzExprFunc
}
