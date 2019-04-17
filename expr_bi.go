package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func init() {
	ExprCatalog["eq"] = ExprEq
	ExprCatalog["or"] = ExprOr
	ExprCatalog["and"] = ExprAnd
}

// MakeBiExprFunc is for two-argument or "binary" expressions.
func MakeBiExprFunc(fields base.Fields, types base.Types,
	params []interface{}, outTypes base.Types, path string,
	biExprFunc base.BiExprFunc) (
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
				biExprFunc(lazyA, lazyB, lazyVals) // <== notLazy

			return lazyVal
		}
	}

	base.SetLastType(outTypes, "bool")

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprEq(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	var biExprFunc base.BiExprFunc

	biExprFunc = func(lazyA, lazyB base.ExprFunc, lazyVals base.Vals) (lazyVal base.Val) { // <== notLazy
		if LazyScope {
			lazyVal = lazyA(lazyVals) // <== emitCaptured: path lazyA
			lazyValA := lazyVal

			lazyVal = lazyB(lazyVals) // <== emitCaptured: path lazyB
			lazyValB := lazyVal

			lazyVal = base.ValEqual(lazyValA, lazyValB)
		}

		return lazyVal
	} // <== notLazy

	lazyExprFunc =
		MakeBiExprFunc(fields, types, params, outTypes, path, biExprFunc) // <== notLazy

	base.SetLastType(outTypes, "bool")

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprOr(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	var biExprFunc base.BiExprFunc

	biExprFunc = func(lazyA, lazyB base.ExprFunc, lazyVals base.Vals) (lazyVal base.Val) { // <== notLazy
		// Implemented this way since compiler only allows return on last line.
		// TODO: This might not match N1QL logical OR semantics.
		lazyVal = lazyA(lazyVals) // <== emitCaptured: path lazyA
		if !base.ValEqualTrue(lazyVal) {
			lazyVal = lazyB(lazyVals) // <== emitCaptured: path lazyB
		}

		return lazyVal
	} // <== notLazy

	lazyExprFunc =
		MakeBiExprFunc(fields, types, params, outTypes, path, biExprFunc) // <== notLazy

	base.SetLastType(outTypes, "bool")

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprAnd(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	var biExprFunc base.BiExprFunc

	biExprFunc = func(lazyA, lazyB base.ExprFunc, lazyVals base.Vals) (lazyVal base.Val) { // <== notLazy
		// Implemented this way since compiler only allows return on last line.
		// TODO: This might not match N1QL logical AND semantics.
		lazyVal = lazyA(lazyVals) // <== emitCaptured: path lazyA
		if base.ValEqualTrue(lazyVal) {
			lazyVal = lazyB(lazyVals) // <== emitCaptured: path lazyB
		}

		return lazyVal
	} // <== notLazy

	lazyExprFunc =
		MakeBiExprFunc(fields, types, params, outTypes, path, biExprFunc) // <== notLazy

	base.SetLastType(outTypes, "bool")

	return lazyExprFunc
}
