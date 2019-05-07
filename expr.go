package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

// LzScope is used to mark variable scopes as lz (ex: IF block).
const LzScope = true

// -----------------------------------------------------

// ExprCatalog is the default registry of known expression functions.
var ExprCatalog = map[string]base.ExprCatalogFunc{
	"json":      ExprJson,
	"labelPath": ExprLabelPath,
}

// -----------------------------------------------------

func MakeExprFunc(lzVars *base.Vars, labels base.Labels,
	expr []interface{}, path, pathItem string) (lzExprFunc base.ExprFunc) {
	pathNext := EmitPush(path, pathItem)

	lzExprFunc =
		lzVars.Ctx.ExprCatalog[expr[0].(string)](lzVars, labels, expr[1:], pathNext) // !lz

	EmitPop(path, pathItem)

	return lzExprFunc
}

// -----------------------------------------------------

func ExprJson(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	json := []byte(params[0].(string))

	var lzValJson base.Val = base.Val(json) // <== varLift: lzValJson by path

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzValJson
		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------

func ExprLabelPath(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	idx := labels.IndexOf(params[0].(string))
	if idx >= 0 {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal = lzVals[idx]

			if len(params) > 1 { // !lz
				var parts []string                 // !lz
				for _, param := range params[1:] { // !lz
					parts = append(parts, param.(string)) // !lz
				} // !lz

				lzVal = base.ValPathGet(lzVal, parts)
			} // !lz

			return lzVal
		}
	} else {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal = base.ValMissing
			return lzVal
		}
	}

	return lzExprFunc
}
