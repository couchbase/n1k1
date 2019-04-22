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
	biExprFunc base.BiExprFunc, outType string) (
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
			MakeExprFunc(fields, types, exprA, outTypes, path, "A") // <== notLazy
		lazyA = lazyExprFunc
		base.TakeLastType(outTypes) // <== notLazy

		lazyExprFunc =
			MakeExprFunc(fields, types, exprB, outTypes, path, "B") // <== notLazy
		lazyB = lazyExprFunc
		base.TakeLastType(outTypes) // <== notLazy

		// TODO: consider inlining this one day...

		lazyExprFunc = func(lazyVals base.Vals) (lazyVal base.Val) {
			lazyVal =
				biExprFunc(lazyA, lazyB, lazyVals) // <== notLazy

			return lazyVal
		}
	}

	base.SetLastType(outTypes, outType)

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprEq(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	var biExprFunc base.BiExprFunc

	biExprFunc = func(lazyA, lazyB base.ExprFunc, lazyVals base.Vals) (lazyVal base.Val) { // <== notLazy
		if LazyScope {
			lazyVal = lazyA(lazyVals) // <== emitCaptured: path "A"
			lazyValA := lazyVal

			lazyVal = lazyB(lazyVals) // <== emitCaptured: path "B"
			lazyValB := lazyVal

			lazyVal = base.ValEqual(lazyValA, lazyValB)
		}

		return lazyVal
	} // <== notLazy

	lazyExprFunc =
		MakeBiExprFunc(fields, types, params, outTypes, path, biExprFunc, "") // <== notLazy

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprOr(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	var biExprFunc base.BiExprFunc

	biExprFunc = func(lazyA, lazyB base.ExprFunc, lazyVals base.Vals) (lazyVal base.Val) { // <== notLazy
		// Implemented this way since compiler only allows return on last line.
		// TODO: This might not match N1QL logical OR semantics.
		lazyVal = lazyA(lazyVals) // <== emitCaptured: path "A"
		if !base.ValEqualTrue(lazyVal) {
			lazyVal = lazyB(lazyVals) // <== emitCaptured: path "B"
		}

		return lazyVal
	} // <== notLazy

	lazyExprFunc =
		MakeBiExprFunc(fields, types, params, outTypes, path, biExprFunc, "") // <== notLazy

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprAnd(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lazyExprFunc base.ExprFunc) {
	var biExprFunc base.BiExprFunc

	biExprFunc = func(lazyA, lazyB base.ExprFunc, lazyVals base.Vals) (lazyVal base.Val) { // <== notLazy
		// Implemented this way since compiler only allows return on last line.
		// TODO: This might not match N1QL logical AND semantics.
		lazyVal = lazyA(lazyVals) // <== emitCaptured: path "A"
		if base.ValEqualTrue(lazyVal) {
			lazyVal = lazyB(lazyVals) // <== emitCaptured: path "B"
		}

		return lazyVal
	} // <== notLazy

	lazyExprFunc =
		MakeBiExprFunc(fields, types, params, outTypes, path, biExprFunc, "") // <== notLazy

	return lazyExprFunc
}
