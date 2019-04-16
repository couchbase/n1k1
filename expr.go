package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

// LazyScope is used to mark variable scopes as lazy (ex: IF block).
const LazyScope = true

var LazyErrNil error

// -----------------------------------------------------

var ExprCatalog = map[string]ExprCatalogFunc{}

func init() {
	ExprCatalog["eq"] = ExprEq
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
	EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	if len(pathItem) > 0 {
		path = path + "_" + pathItem
	}

	ecf := ExprCatalog[expr[0].(string)]

	lazyExprFunc =
		ecf(fields, types, expr[1:], outTypes, path) // <== inlineOk

	return lazyExprFunc
}

var EmitPush = func(path, pathItem string) {}
var EmitPop = func(path, pathItem string) {}

// -----------------------------------------------------

func ExprJson(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	json := []byte(params[0].(string))
	jsonType := base.JsonTypes[json[0]] // Might be "".

	base.SetLastType(outTypes, jsonType)

	if LazyScope {
		var lazyValJson base.Val // <== varLift: lazyValJson by path

		lazyValJson = base.Val(json) // <== varLift: lazyValJson by path

		lazyExprFunc = func(lazyVals base.Vals) (lazyVal base.Val) {
			lazyVal = lazyValJson // <== varLift: lazyValJson by path

			return lazyVal
		}
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

	lazyExprFunc = func(lazyVals base.Vals) (lazyVal base.Val) {
		if idx >= 0 { // <== inlineOk
			lazyVal = lazyVals[idx]
		} else { // <== inlineOk
			lazyVal = base.ValMissing
		} // <== inlineOk

		return lazyVal
	}

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprEq(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})
	exprB := params[1].([]interface{})

	if LazyScope {
		lazyExprFunc =
			MakeExprFunc(fields, types, exprA, outTypes, path, "lazyA") // <== inlineOk
		lazyA := lazyExprFunc
		base.TakeLastType(outTypes) // <== inlineOk

		lazyExprFunc =
			MakeExprFunc(fields, types, exprB, outTypes, path, "lazyB") // <== inlineOk
		lazyB := lazyExprFunc
		base.TakeLastType(outTypes) // <== inlineOk

		// TODO: consider inlining this one day...

		lazyExprFunc = func(lazyVals base.Vals) (lazyVal base.Val) {
			lazyVal = lazyA(lazyVals)
			lazyValA := lazyVal

			lazyVal = lazyB(lazyVals)
			lazyValB := lazyVal

			lazyVal = base.ValEqual(lazyValA, lazyValB)

			return lazyVal
		}
	}

	base.SetLastType(outTypes, "bool")

	return lazyExprFunc
}
