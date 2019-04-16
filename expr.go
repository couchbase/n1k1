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

	ExprCatalog["or"] = ExprOr
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
		ecf(fields, types, expr[1:], outTypes, path) // <== notLazy

	return lazyExprFunc
}

var EmitPush = func(path, pathItem string) {} // Placeholder for compiler.
var EmitPop = func(path, pathItem string) {}  // Placeholder for compiler.

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
		if idx >= 0 { // <== notLazy
			lazyVal = lazyVals[idx]
		} else { // <== notLazy
			lazyVal = base.ValMissing
		} // <== notLazy

		return lazyVal
	}

	return lazyExprFunc
}

// -----------------------------------------------------

func MakeBinaryExprFunc(fields base.Fields, types base.Types,
	params []interface{}, outTypes base.Types, path string,
	binaryExprFunc base.BinaryExprFunc) (
	lazyExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})
	exprB := params[1].([]interface{})

	var lazyA base.ExprFunc // <== notLazy
	_ = lazyA               // <== notLazy
	var lazyB base.ExprFunc // <== notLazy
	_ = lazyB               // <== notLazy
	var lazyVals base.Vals  // <== notLazy
	_ = lazyVals            // <== notLazy

	if LazyScope {
		var lazyA base.ExprFunc
		_ = lazyA
		var lazyB base.ExprFunc
		_ = lazyB

		lazyExprFunc =
			MakeExprFunc(fields, types, exprA, outTypes, path, "lazyA") // <== notLazy
		lazyA = lazyExprFunc
		base.TakeLastType(outTypes) // <== notLazy

		lazyExprFunc =
			MakeExprFunc(fields, types, exprB, outTypes, path, "lazyB") // <== notLazy
		lazyB = lazyExprFunc
		base.TakeLastType(outTypes) // <== notLazy

		// TODO: consider inlining this one day...

		lazyExprFunc = func(lazyVals base.Vals) (lazyVal base.Val) {
			lazyVal =
				binaryExprFunc(lazyA, lazyB, lazyVals) // <== notLazy

			return lazyVal
		}
	}

	base.SetLastType(outTypes, "bool")

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprEq(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	var binaryExprFunc base.BinaryExprFunc

	binaryExprFunc = func(lazyA, lazyB base.ExprFunc, lazyVals base.Vals) (lazyVal base.Val) { // <== notLazy
		if LazyScope {
			lazyVal =
				lazyA(lazyVals) // <== expandEmitCaptured: path lazyA
			lazyValA := lazyVal

			lazyVal =
				lazyB(lazyVals) // <== expandEmitCaptured: path lazyB
			lazyValB := lazyVal

			lazyVal = base.ValEqual(lazyValA, lazyValB)
		}

		return lazyVal
	} // <== notLazy

	lazyExprFunc =
		MakeBinaryExprFunc(fields, types, params, outTypes, path, binaryExprFunc) // <== notLazy

	base.SetLastType(outTypes, "bool")

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprOr(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	var binaryExprFunc base.BinaryExprFunc

	binaryExprFunc = func(lazyA, lazyB base.ExprFunc, lazyVals base.Vals) (lazyVal base.Val) { // <== notLazy
		lazyVal =
			lazyA(lazyVals) // <== expandEmitCaptured: path lazyA
		if base.ValEqualTrue(lazyVal) {
			return lazyVal
		}

		lazyVal =
			lazyB(lazyVals) // <== expandEmitCaptured: path lazyB
		if base.ValEqualTrue(lazyVal) {
			return lazyVal
		}

		lazyVal = base.ValFalse

		return lazyVal
	} // <== notLazy

	lazyExprFunc =
		MakeBinaryExprFunc(fields, types, params, outTypes, path, binaryExprFunc) // <== notLazy

	base.SetLastType(outTypes, "bool")

	return lazyExprFunc
}
