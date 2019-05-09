package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func init() {
	ExprCatalog["lt"] = ExprLT
	ExprCatalog["le"] = ExprLE
	ExprCatalog["gt"] = ExprGT
	ExprCatalog["ge"] = ExprGE
}

// -----------------------------------------------------

func ExprLT(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprCmp(lzVars, labels, params, path, false)
}

func ExprLE(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprCmp(lzVars, labels, params, path, true)
}

func ExprGT(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprLT(lzVars, labels, []interface{}{params[1], params[0]}, path)
}

func ExprGE(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprLE(lzVars, labels, []interface{}{params[1], params[0]}, path)
}

// -----------------------------------------------------

func ExprCmp(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, eq bool) (lzExprFunc base.ExprFunc) {
	for parami, param := range params {
		expr := param.([]interface{})
		if expr[0].(string) == "json" {	// Optimize when param is static JSON.
			return ExprCmpStatic(lzVars, labels, params, path, parami, eq)
		}
	}

	return ExprCmpDynamic(lzVars, labels, params, path, eq)
}

// -----------------------------------------------------

// ExprCmpStatic optimizes when params[parami] is static.
func ExprCmpStatic(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, parami int, eq bool) (
	lzExprFunc base.ExprFunc) {
	json := params[parami].([]interface{})[1].(string)

	staticVal, staticType := base.Parse([]byte(json))

	staticTypeHasValue := base.ParseTypeHasValue(staticType)

	var staticF64 float64 // Optimize further when static is number.
	var staticF64Ok bool

	if base.ParseTypeToValType[staticType] == base.ValTypeNumber {
		var err error

		staticF64, err = base.ParseFloat64(staticVal)
		if err == nil {
			staticF64Ok = true
		}
	}

	exprX := params[(parami+1)%2].([]interface{})

	cmpLT, cmpGT := base.ValTrue, base.ValFalse // Ex: static < expr.
	if parami == 1 {
		cmpLT, cmpGT = base.ValFalse, base.ValTrue // Ex: expr < static.
	}

	cmpEQ := base.ValFalse
	if eq {
		cmpEQ = base.ValTrue
	}

	if LzScope {
		var lzCmpLT base.Val = cmpLT // <== varLift: lzCmpLT by path
		var lzCmpEQ base.Val = cmpEQ // <== varLift: lzCmpEQ by path
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
						lzCmpNeeded := true

						if staticF64Ok { // !lz
							if base.ParseTypeToValType[lzTypeX] == base.ValTypeNumber {
								lzF64, lzErr := base.ParseFloat64(lzValX)
								if lzErr == nil {
									lzCmpNeeded = false

									if staticF64 < lzF64 {
										lzVal = lzCmpLT
									} else if staticF64 == lzF64 {
										lzVal = lzCmpEQ
									} else {
										lzVal = lzCmpGT
									}
								}
							}
						} // !lz

						if lzCmpNeeded {
							lzCmp := lzVars.Ctx.ValComparer.CompareWithType(lzValStatic, lzValX, staticType, lzTypeX, 0)
							if lzCmp < 0 {
								lzVal = lzCmpLT
							} else if lzCmp == 0 {
								lzVal = lzCmpEQ
							} else {
								lzVal = lzCmpGT
							}
						}
					}
				} // !lz
			}

			return lzVal
		}
	}

	return lzExprFunc
}

// -----------------------------------------------------

// Expressions A & B need to be runtime evaluated.
func ExprCmpDynamic(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, eq bool) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

			lzValA, lzTypeA := base.Parse(lzVal)
			if base.ParseTypeHasValue(lzTypeA) {
				lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"

				lzValB, lzTypeB := base.Parse(lzVal)
				if base.ParseTypeHasValue(lzTypeB) {
					lzCmp := lzVars.Ctx.ValComparer.CompareWithType(lzValA, lzValB, lzTypeA, lzTypeB, 0)
					if lzCmp < 0 {
						lzVal = base.ValTrue
					} else if lzCmp == 0 {
						if eq { // !lz
							lzVal = base.ValTrue
						} else { // !lz
							lzVal = base.ValFalse
						} // !lz
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
