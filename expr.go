package n1k1

import (
	"encoding/json"
)

func MakeExprFunc(fields Fields, types Types, expr []interface{},
	outTypes Types) (lazyExprFunc LazyExprFunc) {
	f := ExprCatalog[expr[0].(string)]
	lazyExprFunc =
		f(fields, types, expr[1:], outTypes) // <== inline-ok.
	return lazyExprFunc
}

// -----------------------------------------------------

type LazyExprFunc func(lazyVals LazyVals) (lazyAny interface{})

type ExprCatalogFunc func(fields Fields, types Types, params []interface{},
	outTypes Types) (lazyExprFunc LazyExprFunc)

var ExprCatalog = map[string]ExprCatalogFunc{}

func init() {
	ExprCatalog["bool"] = ExprConstant("bool")
	ExprCatalog["null"] = ExprConstant("null")
	ExprCatalog["number"] = ExprConstant("number")
	ExprCatalog["string"] = ExprConstant("string")
	ExprCatalog["array"] = ExprConstant("array")
	ExprCatalog["object"] = ExprConstant("object")

	ExprCatalog["eq"] = ExprEq

	ExprCatalog["field"] = ExprField
}

// -----------------------------------------------------

func ExprConstant(t string) ExprCatalogFunc {
	return func(fields Fields, types Types, params []interface{},
		outTypes Types) (lazyExprFunc LazyExprFunc) {
		SetLastType(outTypes, t)

		lazyExprFunc = func(lazyVals LazyVals) (lazyAny interface{}) {
			if len(params) > 0 {
				lazyAny = params[0]
			}
			return lazyAny
		}

		return lazyExprFunc
	}
}

// -----------------------------------------------------

func ExprEq(fields Fields, types Types, params []interface{},
	outTypes Types) (lazyExprFunc LazyExprFunc) {
	exprA := params[0].([]interface{})
	lazyExprFunc =
		MakeExprFunc(fields, types, exprA, outTypes) // <== inline-ok.
	lazyA := lazyExprFunc
	typeA := TakeLastType(outTypes)

	exprB := params[1].([]interface{})
	lazyExprFunc =
		MakeExprFunc(fields, types, exprB, outTypes) // <== inline-ok.
	lazyB := lazyExprFunc
	typeB := TakeLastType(outTypes)

	SetLastType(outTypes, "bool")

	lazyExprFunc = func(lazyVals LazyVals) (lazyAny interface{}) {
		lazyAny =
			lazyA(lazyVals) // <== inline-ok.
		lazyAnyA := lazyAny

		lazyAny =
			lazyB(lazyVals) // <== inline-ok.
		lazyAnyB := lazyAny

		if typeA == typeB && TypeCatalog[typeA] == "scalar" {
			lazyAny = lazyAnyA == lazyAnyB
		} else {
			lazyAny = DeepEqual(lazyAnyA, lazyAnyB)
		}

		return lazyAny
	}

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprField(fields Fields, types Types, params []interface{},
	outTypes Types) (lazyExprFunc LazyExprFunc) {
	idx := fields.IndexOf(params[0].(string))
	if idx < 0 {
		SetLastType(outTypes, "")
	} else {
		SetLastType(outTypes, types[idx])
	}

	lazyExprFunc = func(lazyVals LazyVals) (lazyAny interface{}) {
		if idx < 0 {
			lazyAny = ErrMissing
		} else {
			lazyValBytes := []byte(lazyVals[idx])
			lazyErr := json.Unmarshal(lazyValBytes, &lazyAny)
			if lazyErr != nil {
				lazyAny = lazyErr
			}
		}

		return lazyAny
	}

	return lazyExprFunc
}
