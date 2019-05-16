package test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/expr_glue"
)

func TestCasesSimpleWithInterp(t *testing.T) {
	for testi, test := range TestCasesSimple {
		vars, yieldVals, yieldErr, returnYields :=
			MakeYieldCaptureFuncs(t, testi, test.expectErr)

		n1k1.ExecOp(&test.o, vars, yieldVals, yieldErr, "", "")

		yields := returnYields()

		if len(yields) != len(test.expectYields) ||
			!reflect.DeepEqual(yields, test.expectYields) {
			t.Fatalf("testi: %d, test: %+v,\n"+
				" len(yields): %d,\n"+
				" len(test.expectYields): %d,\n"+
				" expectYields: %+v,\n"+
				" got yields: %+v",
				testi, test,
				len(yields), len(test.expectYields),
				test.expectYields, yields)
		}
	}
}

// --------------------------------------------------------

func BenchmarkInterpExprEq_1Docs(b *testing.B) {
	benchmarkInterpNDocs(b,
		[]interface{}{
			"eq",
			[]interface{}{"labelPath", ".", "b"},
			[]interface{}{"json", `10`},
		},
		1)
}

func BenchmarkInterpExprStr_1Docs(b *testing.B) {
	if n1k1.ExprCatalog["exprStr"] == nil {
		n1k1.ExprCatalog["exprStr"] = expr_glue.ExprStr
	}

	benchmarkInterpNDocs(b,
		[]interface{}{
			"exprStr",
			"b = 10",
		},
		1)
}

// --------------------------------------------------------

func BenchmarkInterpExprEq_1000Docs(b *testing.B) {
	benchmarkInterpNDocs(b,
		[]interface{}{
			"eq",
			[]interface{}{"labelPath", ".", "b"},
			[]interface{}{"json", `10`},
		},
		1000)
}

func BenchmarkInterpExprStr_1000Docs(b *testing.B) {
	if n1k1.ExprCatalog["exprStr"] == nil {
		n1k1.ExprCatalog["exprStr"] = expr_glue.ExprStr
	}

	benchmarkInterpNDocs(b,
		[]interface{}{
			"exprStr",
			"b = 10",
		},
		1000)
}

// --------------------------------------------------------

func BenchmarkInterpExprEq_100000Docs(b *testing.B) {
	benchmarkInterpNDocs(b,
		[]interface{}{
			"eq",
			[]interface{}{"labelPath", ".", "b"},
			[]interface{}{"json", `10`},
		},
		100000)
}

func BenchmarkInterpExprStr_100000Docs(b *testing.B) {
	if n1k1.ExprCatalog["exprStr"] == nil {
		n1k1.ExprCatalog["exprStr"] = expr_glue.ExprStr
	}

	benchmarkInterpNDocs(b,
		[]interface{}{
			"exprStr",
			"b = 10",
		},
		100000)
}

// --------------------------------------------------------

func benchmarkInterpNDocs(b *testing.B,
	filterParams []interface{}, nDocs int) {
	vars := &base.Vars{
		Ctx: &base.Ctx{
			ValComparer: base.NewValComparer(),
			ExprCatalog: n1k1.ExprCatalog,
			YieldStats:  func(stats *base.Stats) error { return nil },
		},
	}

	json := `{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}`
	jsonb := []byte(json)

	yieldValsCount := 0

	yieldVals := func(vals base.Vals) {
		yieldValsCount++

		if len(vals) != 1 && !bytes.Equal(vals[0], jsonb) {
			b.Fatalf("yieldVals: %+v", vals)
		}
	}

	yieldErr := func(err error) {
		if err != nil {
			b.Fatalf("yieldErr: %v", err)
		}
	}

	o := base.Op{
		Kind:   "filter",
		Labels: base.Labels{"."},
		Params: filterParams,
		Children: []*base.Op{&base.Op{
			Kind:   "scan",
			Labels: base.Labels{"."},
			Params: []interface{}{
				"jsonsData",
				json,
				nDocs,
			},
		}},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		yieldValsCount = 0

		n1k1.ExecOp(&o, vars, yieldVals, yieldErr, "", "")

		if yieldValsCount != nDocs {
			b.Fatalf("yieldValsCount: %d != nDocs: %d",
				yieldValsCount, nDocs)
		}
	}
}

// --------------------------------------------------------

func BenchmarkInterpGroupBy_1Docs(b *testing.B) {
	benchmarkInterpGroupBy(b, 1)
}

func BenchmarkInterpGroupBy_100Docs(b *testing.B) {
	benchmarkInterpGroupBy(b, 100)
}

func BenchmarkInterpGroupBy_10000Docs(b *testing.B) {
	benchmarkInterpGroupBy(b, 10000)
}

func benchmarkInterpGroupBy(b *testing.B, nDocs int) {
	vars := MakeVars()

	// TODO: Try object JSON once jsonparser.ObjectEach memory
	// allocations is fixed.
	json := `1234`

	yieldValsCount := 0

	yieldVals := func(vals base.Vals) {
		yieldValsCount++

		if len(vals) != 2 {
			b.Fatalf("yieldVals: %+v", vals)
		}
	}

	yieldErr := func(err error) {
		if err != nil {
			b.Fatalf("yieldErr: %v", err)
		}
	}

	o := base.Op{
		Kind:   "group",
		Labels: base.Labels{".", "count-."},
		Params: []interface{}{
			[]interface{}{
				[]interface{}{"labelPath", "."},
			},
			[]interface{}{
				[]interface{}{"labelPath", "."},
			},
			[]interface{}{
				[]interface{}{"count"},
			},
		},
		Children: []*base.Op{&base.Op{
			Kind:   "scan",
			Labels: base.Labels{"."},
			Params: []interface{}{
				"jsonsData",
				json,
				nDocs,
			},
		}},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		yieldValsCount = 0

		n1k1.ExecOp(&o, vars, yieldVals, yieldErr, "", "")

		if yieldValsCount != 1 {
			b.Fatalf("yieldValsCount: %d != 1", yieldValsCount)
		}
	}
}
