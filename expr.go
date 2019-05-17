package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

// ExprCatalog is the default registry of known expression functions.
var ExprCatalog = map[string]base.ExprCatalogFunc{
	"json":          ExprJson,
	"labelPath":     ExprLabelPath,
	"valsCanonical": ExprValsCanonical,
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
		var valPath []string
		for _, param := range params[1:] {
			valPath = append(valPath, param.(string))
		}

		var lzValPath []string = valPath // <== varLift: lzValPath by path

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal = lzVals[idx]

			if len(params) > 1 { // !lz
				lzVal = base.ValPathGet(lzVal, lzValPath)
			} else { // !lz
				_ = lzValPath
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

// -----------------------------------------------------

// ExprValsCanonical canonicalizes the vals and joins their bytes
// together using base.ValsEncodeCanonical(). The result is non-JSON /
// BINARY and can be parsed using base.ValsSplit().
func ExprValsCanonical(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzJoined []byte // <== varLift: lzJoined by path

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		if LzScope {
			var lzErr error

			lzBytes := lzJoined[:0]

			lzJoined, lzErr = base.ValsEncodeCanonical(lzVals, lzBytes, lzVars.Ctx.ValComparer)
			if lzErr == nil {
				lzVal = base.Val(lzJoined)
			}
		}

		return lzVal
	}

	return lzExprFunc
}
