package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func init() {
	ExprCatalog["lt"] = ExprLT
	ExprCatalog["gt"] = ExprGT
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

// -----------------------------------------------------

func ExprLTStatic(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, parami int) (lzExprFunc base.ExprFunc) {
	json := params[parami].([]interface{})[1].(string)

	staticVal, staticType := base.Parse([]byte(json))

	staticTypeHasValue := base.ParseTypeHasValue(staticType)

	var staticF64 float64 // Optimize when static is number.
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
						lzCmpNeeded := true

						if staticF64Ok { // !lz
							if base.ParseTypeToValType[lzTypeX] == base.ValTypeNumber {
								lzF64, lzErr := base.ParseFloat64(lzValX)
								if lzErr == nil {
									lzCmpNeeded = false

									if staticF64 < lzF64 {
										lzVal = lzCmpLT
									} else if staticF64 > lzF64 {
										lzVal = lzCmpGT
									} else {
										lzVal = base.ValFalse
									}
								}
							}
						} // !lz

						if lzCmpNeeded {
							lzCmp := lzVars.Ctx.ValComparer.CompareWithType(lzValStatic, lzValX, staticType, lzTypeX, 0)
							if lzCmp < 0 {
								lzVal = lzCmpLT
							} else if lzCmp > 0 {
								lzVal = lzCmpGT
							} else {
								lzVal = base.ValFalse
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
					lzCmp := lzVars.Ctx.ValComparer.CompareWithType(lzValA, lzValB, lzTypeA, lzTypeB, 0)
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
