package n1k1

func MakeExprFunc(fields Fields, types Types, expr []interface{},
	outTypes Types) (lazyExprFunc LazyExprFunc) {
	f := ExprCatalog[expr[0].(string)]
	lazyExprFunc =
		f(fields, types, expr[1:], outTypes) // <== inline-ok.
	return lazyExprFunc
}

// -----------------------------------------------------

type LazyExprFunc func(lazyVals LazyVals) LazyVal

type ExprCatalogFunc func(fields Fields, types Types, params []interface{},
	outTypes Types) (lazyExprFunc LazyExprFunc)

var ExprCatalog = map[string]ExprCatalogFunc{}

func init() {
	ExprCatalog["eq"] = ExprEq
	ExprCatalog["json"] = ExprJson
	ExprCatalog["field"] = ExprField
}

// -----------------------------------------------------

func ExprJson(fields Fields, types Types, params []interface{},
	outTypes Types) (lazyExprFunc LazyExprFunc) {
	json := []byte(params[0].(string))
	jsonType := JsonTypes[json[0]] // Might be "".

	SetLastType(outTypes, jsonType)

	lazyValJson := LazyVal(json)

	lazyExprFunc = func(lazyVals LazyVals) (lazyVal LazyVal) {
		lazyVal = lazyValJson

		return lazyVal
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

	lazyExprFunc = func(lazyVals LazyVals) (lazyVal LazyVal) {
		if idx < 0 {
			lazyVal = LazyValMissing
		} else {
			lazyVal = lazyVals[idx]
		}

		return lazyVal
	}

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprEq(fields Fields, types Types, params []interface{},
	outTypes Types) (lazyExprFunc LazyExprFunc) {
	exprA := params[0].([]interface{})
	lazyExprFunc =
		MakeExprFunc(fields, types, exprA, outTypes) // <== inline-ok.
	lazyA := lazyExprFunc
	TakeLastType(outTypes)

	exprB := params[1].([]interface{})
	lazyExprFunc =
		MakeExprFunc(fields, types, exprB, outTypes) // <== inline-ok.
	lazyB := lazyExprFunc
	TakeLastType(outTypes)

	SetLastType(outTypes, "bool")

	lazyExprFunc = func(lazyVals LazyVals) (lazyVal LazyVal) {
		lazyVal =
			lazyA(lazyVals) // <== inline-ok.
		lazyValA := lazyVal

		lazyVal =
			lazyB(lazyVals) // <== inline-ok.
		lazyValB := lazyVal

		lazyVal =
			LazyValEqual(lazyValA, lazyValB) // <== inline-ok.

		return lazyVal
	}

	return lazyExprFunc
}
