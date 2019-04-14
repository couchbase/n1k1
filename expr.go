package n1k1

type Fields []string

func (a Fields) IndexOf(s string) int {
	for i, v := range a {
		if v == s {
			return i
		}
	}

	return -1
}

// -----------------------------------------------------

func MakeExprFunc(fields Fields, types Types, expr []interface{},
	outTypes Types, path string) (lazyExprFunc LazyExprFunc) {
	// <== varLiftTop: when path == ""

	ecf := ExprCatalog[expr[0].(string)]

	lazyExprFunc =
		ecf(fields, types, expr[1:], outTypes, path) // <== inlineOk

	return lazyExprFunc
}

// -----------------------------------------------------

type LazyExprFunc func(lazyVals LazyVals) LazyVal

type ExprCatalogFunc func(fields Fields, types Types, params []interface{},
	outTypes Types, path string) (lazyExprFunc LazyExprFunc)

var ExprCatalog = map[string]ExprCatalogFunc{}

func init() {
	ExprCatalog["eq"] = ExprEq
	ExprCatalog["json"] = ExprJson
	ExprCatalog["field"] = ExprField
}

// -----------------------------------------------------

func ExprJson(fields Fields, types Types, params []interface{},
	outTypes Types, path string) (lazyExprFunc LazyExprFunc) {
	json := []byte(params[0].(string))
	jsonType := JsonTypes[json[0]] // Might be "".

	SetLastType(outTypes, jsonType)

	var lazyValJson LazyVal // <== varLift: lazyValJson by path

	lazyValJson = LazyVal(json) // <== varLift: lazyValJson by path

	lazyExprFunc = func(lazyVals LazyVals) (lazyVal LazyVal) {
		lazyVal = lazyValJson // <== varLift: lazyValJson by path

		return lazyVal
	}

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprField(fields Fields, types Types, params []interface{},
	outTypes Types, path string) (lazyExprFunc LazyExprFunc) {
	idx := fields.IndexOf(params[0].(string))
	if idx < 0 {
		SetLastType(outTypes, "")
	} else {
		SetLastType(outTypes, types[idx])
	}

	lazyExprFunc = func(lazyVals LazyVals) (lazyVal LazyVal) {
		if idx >= 0 {
			lazyVal = lazyVals[idx]
		} else {
			lazyVal = LazyValMissing
		}

		return lazyVal
	}

	return lazyExprFunc
}

// -----------------------------------------------------

func ExprEq(fields Fields, types Types, params []interface{},
	outTypes Types, path string) (lazyExprFunc LazyExprFunc) {
	exprA := params[0].([]interface{})
	lazyExprFunc =
		MakeExprFunc(fields, types, exprA, outTypes, path+"_1")
	lazyA := lazyExprFunc
	TakeLastType(outTypes)

	exprB := params[1].([]interface{})
	lazyExprFunc =
		MakeExprFunc(fields, types, exprB, outTypes, path+"_2")
	lazyB := lazyExprFunc
	TakeLastType(outTypes)

	SetLastType(outTypes, "bool")

	lazyExprFunc = func(lazyVals LazyVals) (lazyVal LazyVal) {
		lazyVal =
			lazyA(lazyVals)
		lazyValA := lazyVal

		lazyVal =
			lazyB(lazyVals)
		lazyValB := lazyVal

		lazyVal =
			LazyValEqual(lazyValA, lazyValB)

		return lazyVal
	}

	return lazyExprFunc
}
