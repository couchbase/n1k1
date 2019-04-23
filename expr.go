package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

// LazyScope is used to mark variable scopes as lazy (ex: IF block).
const LazyScope = true

// LazyErrNil is always the nil error.
var LazyErrNil error

// -----------------------------------------------------

// ExprCatalog is a registry of all the known expression functions.
var ExprCatalog = map[string]ExprCatalogFunc{}

func init() {
	ExprCatalog["json"] = ExprJson
	ExprCatalog["field"] = ExprField
}

type ExprCatalogFunc func(fields base.Fields, types base.Types,
	params []interface{}, outTypes base.Types, path string) (
	lazyExprFunc base.ExprFunc)

// -----------------------------------------------------

func MakeExprFunc(fields base.Fields, types base.Types,
	expr []interface{}, outTypes base.Types, path, pathItem string) (
	lazyExprFunc base.ExprFunc) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	lazyExprFunc =
		ExprCatalog[expr[0].(string)](fields, types, expr[1:], outTypes, pathNext)

	return lazyExprFunc
}

var EmitPush = func(path, pathItem string) string {
	return path + "_" + pathItem
}

// Placeholder for compiler.
var EmitPop = func(path, pathItem string) {} // Placeholder for compiler.

// -----------------------------------------------------

func ExprJson(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	json := []byte(params[0].(string))
	jsonType := base.JsonTypes[json[0]] // Might be "".

	base.SetLastType(outTypes, jsonType)

	var lazyValJson base.Val = base.Val(json) // <== varLift: lazyValJson by path

	lazyExprFunc = func(lazyVals base.Vals) (lazyVal base.Val) {
		lazyVal = lazyValJson // <== varLift: lazyValJson by path
		return lazyVal
	}

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprField(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	idx := fields.IndexOf(params[0].(string))
	if idx < 0 {
		base.SetLastType(outTypes, "")
	} else {
		base.SetLastType(outTypes, types[idx])
	}

	if idx >= 0 {
		lazyExprFunc = func(lazyVals base.Vals) (lazyVal base.Val) {
			lazyVal = lazyVals[idx]
			return lazyVal
		}
	} else {
		lazyExprFunc = func(lazyVals base.Vals) (lazyVal base.Val) {
			lazyVal = base.ValMissing
			return lazyVal
		}
	}

	return lazyExprFunc
}
