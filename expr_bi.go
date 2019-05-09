package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func init() {
	ExprCatalog["eq"] = ExprEq

	ExprCatalog["or"] = ExprOr
	ExprCatalog["and"] = ExprAnd

	ExprCatalog["lt"] = ExprLT
	ExprCatalog["gt"] = ExprGT
}

// MakeBiExprFunc is for two-argument or "binary" expressions.
func MakeBiExprFunc(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, biExprFunc base.BiExprFunc) (
	lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})
	exprB := params[1].([]interface{})

	var lzA base.ExprFunc        // !lz
	var lzB base.ExprFunc        // !lz
	var lzVals base.Vals         // !lz
	var lzYieldErr base.YieldErr // !lz

	_, _, _, _ = lzA, lzB, lzVals, lzYieldErr // !lz

	if LzScope {
		lzExprFunc =
			MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
		lzA := lzExprFunc

		lzExprFunc =
			MakeExprFunc(lzVars, labels, exprB, path, "B") // !lz
		lzB := lzExprFunc

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal =
				biExprFunc(lzA, lzB, lzVals, lzYieldErr) // !lz

			return lzVal
		}
	}

	return lzExprFunc
}

// -----------------------------------------------------

func ExprEq(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValB := lzVal

			lzVal = base.ValEqual(lzValA, lzValB, lzVars.Ctx.ValComparer)
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// -----------------------------------------------------

func ExprOr(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		// TODO: This might not match N1QL logical OR semantics.
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
		if !base.ValEqualTrue(lzVal) {
			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// -----------------------------------------------------

func ExprAnd(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		// TODO: This might not match N1QL logical AND semantics.
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
		if base.ValEqualTrue(lzVal) {
			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// -----------------------------------------------------

func ExprLT(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	// Optimize if either param expression is a JSON static.
	for parami, param := range params {
		expr := param.([]interface{})
		if expr[0].(string) == "json" {
			return ExprLTStatic(lzVars, labels, params, path, parami)
		}
	}

	return ExprLTDynamic(lzVars, labels, params, path)
}

func ExprLTStatic(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, parami int) (lzExprFunc base.ExprFunc) {
	json := params[parami].([]interface{})[1].(string)

	staticVal, staticType := base.Parse([]byte(json))

	staticTypeHasValue := base.ParseTypeHasValue(staticType)

	exprX := params[(parami+1)%2].([]interface{})

	cmpLT, cmpGT := base.ValTrue, base.ValFalse
	if parami == 1 {
		cmpLT, cmpGT = base.ValFalse, base.ValTrue
	}

	if LzScope {
		var lzCmpLT base.Val = cmpLT // <== varLift: lzCmpLT by path
		var lzCmpGT base.Val = cmpGT // <== varLift: lzCmpGT by path

		lzExprFunc =
			MakeExprFunc(lzVars, labels, exprX, path, "X") // !lz
		lzX := lzExprFunc

		var lzValStatic base.Val = base.Val(staticVal) // <== varLift: lzValStatic by path

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			if LzScope {
				if !staticTypeHasValue { // !lz
					lzVal = lzValStatic
				} else { // !lz
					lzVal = lzX(lzVals, lzYieldErr) // <== emitCaptured: path "X"

					lzValX, lzTypeX := base.Parse(lzVal)
					if base.ParseTypeHasValue(lzTypeX) {
						lzCmp := lzVars.Ctx.ValComparer.CompareDeepType(lzValStatic, lzValX, staticType, lzTypeX, 0)
						if lzCmp < 0 {
							lzVal = lzCmpLT
						} else if lzCmp > 0 {
							lzVal = lzCmpGT
						} else {
							lzVal = base.ValFalse
						}
					}
				} // !lz
			}

			return lzVal
		}
	}

	return lzExprFunc
}

// Expressions A & B need to be runtime evaluated.
func ExprLTDynamic(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

			lzValA, lzTypeA := base.Parse(lzVal)
			if base.ParseTypeHasValue(lzTypeA) {
				lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"

				lzValB, lzTypeB := base.Parse(lzVal)
				if base.ParseTypeHasValue(lzTypeB) {
					lzCmp := lzVars.Ctx.ValComparer.CompareDeepType(lzValA, lzValB, lzTypeA, lzTypeB, 0)
					if lzCmp < 0 {
						lzVal = base.ValTrue
					} else {
						lzVal = base.ValFalse
					}
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// -----------------------------------------------------

func ExprGT(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprLT(lzVars, labels, []interface{}{params[1], params[0]}, path) // !lz
}
