package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

// LzScope is used to mark variable scopes as lz (ex: IF block).
const LzScope = true

// LzErrNil is always the nil error.
var LzErrNil error

// -----------------------------------------------------

// ExprCatalog is a registry of all the known expression functions.
var ExprCatalog = map[string]ExprCatalogFunc{}

func init() {
	ExprCatalog["json"] = ExprJson
	ExprCatalog["field"] = ExprField
}

type ExprCatalogFunc func(fields base.Fields, types base.Types,
	params []interface{}, outTypes base.Types, path string) (
	lzExprFunc base.ExprFunc)

// -----------------------------------------------------

func MakeExprFunc(fields base.Fields, types base.Types,
	expr []interface{}, outTypes base.Types, path, pathItem string) (
	lzExprFunc base.ExprFunc) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	lzExprFunc =
		ExprCatalog[expr[0].(string)](fields, types, expr[1:], outTypes, pathNext)

	return lzExprFunc
}

var EmitPush = func(path, pathItem string) string {
	return path + "_" + pathItem // Placeholder for compiler.
}

// Placeholder for compiler.
var EmitPop = func(path, pathItem string) {} // Placeholder for compiler.

// -----------------------------------------------------

func ExprJson(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lzExprFunc base.ExprFunc) {
	json := []byte(params[0].(string))
	jsonType := base.JsonTypes[json[0]] // Might be "".

	base.SetLastType(outTypes, jsonType)

	var lzValJson base.Val = base.Val(json) // <== varLift: lzValJson by path

	lzExprFunc = func(lzVals base.Vals) (lzVal base.Val) {
		lzVal = lzValJson // <== varLift: lzValJson by path
		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------

func ExprField(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lzExprFunc base.ExprFunc) {
	idx := fields.IndexOf(params[0].(string))
	if idx < 0 {
		base.SetLastType(outTypes, "")
	} else {
		base.SetLastType(outTypes, types[idx])
	}

	if idx >= 0 {
		lzExprFunc = func(lzVals base.Vals) (lzVal base.Val) {
			lzVal = lzVals[idx]
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
