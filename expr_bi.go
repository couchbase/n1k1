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
	lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})
	exprB := params[1].([]interface{})

	var lzA base.ExprFunc // <== notLz
	var lzB base.ExprFunc // <== notLz
	var lzVals base.Vals  // <== notLz

	_, _, _ = lzA, lzB, lzVals // <== notLz

	if LzScope {
		var lzA base.ExprFunc
		var lzB base.ExprFunc

		_, _ = lzA, lzB

		lzExprFunc =
			MakeExprFunc(fields, types, exprA, outTypes, path, "A") // <== notLz
		lzA = lzExprFunc
		base.TakeLastType(outTypes) // <== notLz

		lzExprFunc =
			MakeExprFunc(fields, types, exprB, outTypes, path, "B") // <== notLz
		lzB = lzExprFunc
		base.TakeLastType(outTypes) // <== notLz

		lzExprFunc = func(lzVals base.Vals) (lzVal base.Val) {
			lzVal =
				biExprFunc(lzA, lzB, lzVals) // <== notLz

			return lzVal
		}
	}

	base.SetLastType(outTypes, outType)

	return lzExprFunc
}

// -----------------------------------------------------

func ExprEq(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lzExprFunc base.ExprFunc) {
	var biExprFunc base.BiExprFunc

	biExprFunc = func(lzA, lzB base.ExprFunc, lzVals base.Vals) (lzVal base.Val) { // <== notLz
		if LzScope {
			lzVal = lzA(lzVals) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals) // <== emitCaptured: path "B"
			lzValB := lzVal

			lzVal = base.ValEqual(lzValA, lzValB)
		}

		return lzVal
	} // <== notLz

	lzExprFunc =
		MakeBiExprFunc(fields, types, params, outTypes, path, biExprFunc, "") // <== notLz

	return lzExprFunc
}

// -----------------------------------------------------

func ExprOr(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lzExprFunc base.ExprFunc) {
	var biExprFunc base.BiExprFunc

	biExprFunc = func(lzA, lzB base.ExprFunc, lzVals base.Vals) (lzVal base.Val) { // <== notLz
		// Implemented this way since compiler only allows return on last line.
		// TODO: This might not match N1QL logical OR semantics.
		lzVal = lzA(lzVals) // <== emitCaptured: path "A"
		if !base.ValEqualTrue(lzVal) {
			lzVal = lzB(lzVals) // <== emitCaptured: path "B"
		}

		return lzVal
	} // <== notLz

	lzExprFunc =
		MakeBiExprFunc(fields, types, params, outTypes, path, biExprFunc, "") // <== notLz

	return lzExprFunc
}

// -----------------------------------------------------

func ExprAnd(fields base.Fields, types base.Types, params []interface{},
	outTypes base.Types, path string) (lzExprFunc base.ExprFunc) {
	var biExprFunc base.BiExprFunc

	biExprFunc = func(lzA, lzB base.ExprFunc, lzVals base.Vals) (lzVal base.Val) { // <== notLz
		// Implemented this way since compiler only allows return on last line.
		// TODO: This might not match N1QL logical AND semantics.
		lzVal = lzA(lzVals) // <== emitCaptured: path "A"
		if base.ValEqualTrue(lzVal) {
			lzVal = lzB(lzVals) // <== emitCaptured: path "B"
		}

		return lzVal
	} // <== notLz

	lzExprFunc =
		MakeBiExprFunc(fields, types, params, outTypes, path, biExprFunc, "") // <== notLz

	return lzExprFunc
}
