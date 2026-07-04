//go:build n1ql

package test

import (
	"encoding/json"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
	"github.com/couchbase/n1k1/glue"
)

func MakeYieldCaptureFuncs(t *testing.T, testi int, expectErr string) (
	string, *base.Vars, base.YieldVals, base.YieldErr,
	func() []base.Vals) {
	if engine.ExprCatalog["exprStr"] == nil {
		engine.ExprCatalog["exprStr"] = glue.ExprStr
	}

	if engine.ExprCatalog["exprTree"] == nil {
		engine.ExprCatalog["exprTree"] = glue.ExprTree
	}

	var yields []base.Vals

	yieldVals := func(lzVals base.Vals) {
		var lzValsCopy base.Vals
		for _, v := range lzVals {
			lzValsCopy = append(lzValsCopy, append(base.Val(nil), v...))
		}

		yields = append(yields, lzValsCopy)
	}

	yieldErr := func(err error) {
		if (expectErr != "") != (err != nil) {
			t.Fatalf("testi: %d, mismatched err: %+v, expectErr: %s",
				testi, err, expectErr)
		}
	}

	returnYields := func() []base.Vals {
		return yields
	}

	tmpDir, vars := glue.MakeVars("", "n1k1TmpDir")

	return tmpDir, vars, yieldVals, yieldErr, returnYields
}

func StringsToVals(a []string, valsPre base.Vals) base.Vals {
	vals := valsPre
	for _, v := range a {
		if v != "" {
			vals = append(vals, base.Val([]byte(v)))
		} else {
			vals = append(vals, base.ValMissing)
		}
	}
	return vals
}

type TestCaseSimple struct {
	about        string
	o            base.Op
	expectYields []base.Vals
	expectErr    string
}

var TestCasesSimple = []TestCaseSimple{
	{
		about: "test nil operator",
	},
	{
		about: "test empty csv-data scan",
		o: base.Op{
			Kind:   "scan",
			Labels: base.Labels(nil),
			Params: []interface{}{
				"csvData",
				"",
			},
		},
	},
	{
		about: "test empty csv-data scan with some labels",
		o: base.Op{
			Kind:   "scan",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"csvData",
				"",
			},
		},
	},
	{
		about: "test csv-data scan with 1 record",
		o: base.Op{
			Kind:   "scan",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"csvData",
				"1,2,3",
			},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("1"), []byte("2"), []byte("3")},
		},
	},
	{
		about: "test csv-data scan with 2 records",
		o: base.Op{
			Kind:   "scan",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"csvData",
				`
10,20,30
11,21,31
`,
			},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter with labelB == 21",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", "b"},
				[]interface{}{"json", `21`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter with labelB = 66",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", "b"},
				[]interface{}{"json", `66`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->filter on const == const",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"July"`},
				[]interface{}{"json", `"July"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter on constX == constY",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"July"`},
				[]interface{}{"json", `"June"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->filter with labelB == labelB",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", "b"},
				[]interface{}{"labelPath", "b"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter with labelA == labelB",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelPath", "b"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->filter more than 1 match",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", "c"},
				[]interface{}{"json", `3000`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
00,00,0000
10,20,3000
11,21,3000
12,22,1000
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("3000")},
			base.Vals{[]byte("11"), []byte("21"), []byte("3000")},
		},
	},
	{
		about: "test csv-data scan->filter->project",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "c"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelPath", "c"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "filter",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"labelPath", "c"},
					[]interface{}{"json", `3000`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
00,00,0000
10,20,3000
11,21,3000
12,22,1000
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("3000")},
			base.Vals{[]byte("11"), []byte("3000")},
		},
	},
	{
		about: "test csv-data scan->project",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "c"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelPath", "c"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
00,00,0000
10,20,3000
11,21,3000
12,22,1000
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("00"), []byte("0000")},
			base.Vals{[]byte("10"), []byte("3000")},
			base.Vals{[]byte("11"), []byte("3000")},
			base.Vals{[]byte("12"), []byte("1000")},
		},
	},
	{
		about: "test csv-data scan->project deeper labelPath",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"city"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a", "addr", "city"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a"},
				Params: []interface{}{
					"csvData",
					`
{"addr": {"city": "sf"}}
{"addr": {"city": "sj"}}
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(`"sf"`)},
			base.Vals{[]byte(`"sj"`)},
		},
	}, {
		about: "test csv-data scan->project deeper labelPath",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"city"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a", "addr"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a"},
				Params: []interface{}{
					"csvData",
					`
{"addr": {"city": "sf"}}
{"addr": {"city": "sj"}}
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(`{"city": "sf"}`)},
			base.Vals{[]byte(`{"city": "sj"}`)},
		},
	},
	{
		about: "test csv-data scan->filter->project nothing",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{},
			Params: []interface{}{},
			Children: []*base.Op{&base.Op{
				Kind:   "filter",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"labelPath", "c"},
					[]interface{}{"json", `3000`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
00,00,0000
10,20,3000
11,21,3000
12,22,1000
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals(nil),
			base.Vals(nil),
		},
	},
	{
		about: "test csv-data scan->filter->project unknown label",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "xxx"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelPath", "xxx"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "filter",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"labelPath", "c"},
					[]interface{}{"json", `3000`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
00,00,0000
10,20,3000
11,21,3000
12,22,1000
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), nil},
			base.Vals{[]byte("11"), nil},
		},
	},
	{
		about: "test csv-data scan->joinNL-inner",
		o: base.Op{
			Kind:   "joinNL-inner",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", "dept"},
				[]interface{}{"labelPath", "empDept"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
"sales","san diego"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
"mary","marketing"
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test csv-data scan->joinNL-inner but false join condition",
		o: base.Op{
			Kind:   "joinNL-inner",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", "dept"},
				[]interface{}{"json", `"NOT-MATCHING"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test inner join via always-true join condition",
		o: base.Op{
			Kind:   "joinNL-inner",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{"json", `true`},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"fred"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test inner join via always-matching join condition",
		o: base.Op{
			Kind:   "joinNL-inner",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"Hello"`},
				[]interface{}{"json", `"Hello"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"fred"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test left outer joinNL on dept",
		o: base.Op{
			Kind:   "joinNL-leftOuter",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", `dept`},
				[]interface{}{"labelPath", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
"sales","san diego"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
"mary","marketing"
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),

			StringsToVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),

			StringsToVals([]string{`"sales"`, `"san diego"`, ``, ``}, nil),
		},
	},
	{
		about: "test left outer join on dept with empty RHS",
		o: base.Op{
			Kind:   "joinNL-leftOuter",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", `dept`},
				[]interface{}{"labelPath", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
		},
	},
	{
		about: "test inner join on dept with empty LHS",
		o: base.Op{
			Kind:   "joinNL-inner",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", `dept`},
				[]interface{}{"labelPath", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test left outer join on dept with empty LHS",
		o: base.Op{
			Kind:   "joinNL-leftOuter",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", `dept`},
				[]interface{}{"labelPath", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test left outer join on never matching condition",
		o: base.Op{
			Kind:   "joinNL-leftOuter",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", `dept`},
				[]interface{}{"labelPath", `someFakeLabel`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
		},
	},
	{
		about: "test csv-data scan->filter on false OR true",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{"json", `false`},
				[]interface{}{"json", `true`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter on true OR false",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{"json", `true`},
				[]interface{}{"json", `false`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter on false OR false",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{"json", `false`},
				[]interface{}{"json", `false`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->filter on a=10 OR c=31",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{
					"eq",
					[]interface{}{"labelPath", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"eq",
					[]interface{}{"labelPath", `c`},
					[]interface{}{"json", `31`},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
12,22,32
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter on a=10 AND c=30",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"and",
				[]interface{}{
					"eq",
					[]interface{}{"labelPath", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"eq",
					[]interface{}{"labelPath", `c`},
					[]interface{}{"json", `30`},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
12,22,32
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
		},
	},
	{
		about: "test csv-data scan->filter on a=11 AND c=31",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"and",
				[]interface{}{
					"eq",
					[]interface{}{"labelPath", `a`},
					[]interface{}{"json", `11`},
				},
				[]interface{}{
					"eq",
					[]interface{}{"labelPath", `c`},
					[]interface{}{"json", `31`},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
12,22,32
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter on a=10 AND (c=30 AND b=20)",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"and",
				[]interface{}{
					"eq",
					[]interface{}{"labelPath", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"and",
					[]interface{}{
						"eq",
						[]interface{}{"labelPath", `c`},
						[]interface{}{"json", `30`},
					},
					[]interface{}{
						"eq",
						[]interface{}{"labelPath", `b`},
						[]interface{}{"json", `20`},
					},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
12,22,32
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
		},
	},
	{
		about: "test csv-data scan->filter on a=10 OR (c=31 AND b=21)",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{
					"eq",
					[]interface{}{"labelPath", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"and",
					[]interface{}{
						"eq",
						[]interface{}{"labelPath", `c`},
						[]interface{}{"json", `31`},
					},
					[]interface{}{
						"eq",
						[]interface{}{"labelPath", `b`},
						[]interface{}{"json", `21`},
					},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
12,22,32
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter on a=10 AND (c=4444 OR b=20)",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"and",
				[]interface{}{
					"eq",
					[]interface{}{"labelPath", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"or",
					[]interface{}{
						"eq",
						[]interface{}{"labelPath", `c`},
						[]interface{}{"json", `4444`},
					},
					[]interface{}{
						"eq",
						[]interface{}{"labelPath", `b`},
						[]interface{}{"json", `20`},
					},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
12,22,32
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
		},
	},
	{
		about: "test csv-data scan->joinNL-inner->project",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{"labelPath", "city"},
				[]interface{}{"labelPath", "emp"},
				[]interface{}{"labelPath", "empDept"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "joinNL-inner",
				Labels: base.Labels{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"labelPath", "dept"},
					[]interface{}{"labelPath", "empDept"},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Labels: base.Labels{"emp", "empDept"},
					Params: []interface{}{
						"csvData",
						`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"london"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"london"`, `"fred"`, `"finance"`}, nil),
		},
	},

	{
		about: "test csv-data scan->joinNL-inner->filter->project",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{"labelPath", "city"},
				[]interface{}{"labelPath", "emp"},
				[]interface{}{"labelPath", "empDept"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "filter",
				Labels: base.Labels{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"json", `"london"`},
					[]interface{}{"labelPath", `city`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "joinNL-inner",
					Labels: base.Labels{"dept", "city", "emp", "empDept"},
					Params: []interface{}{
						"eq",
						[]interface{}{"labelPath", "dept"},
						[]interface{}{"labelPath", "empDept"},
					},
					Children: []*base.Op{&base.Op{
						Kind:   "scan",
						Labels: base.Labels{"dept", "city"},
						Params: []interface{}{
							"csvData",
							`
"dev","paris"
"finance","london"
`,
						},
					}, &base.Op{
						Kind:   "scan",
						Labels: base.Labels{"emp", "empDept"},
						Params: []interface{}{
							"csvData",
							`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
						},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"london"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test csv-data scan->order-by",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
11,21
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20")},
			base.Vals{[]byte("11"), []byte("21")},
		},
	},
	{
		about: "test csv-data scan->order-by reverse-input",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
12,22
11,21
10,20
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20")},
			base.Vals{[]byte("11"), []byte("21")},
			base.Vals{[]byte("12"), []byte("22")},
		},
	},
	{
		about: "test csv-data scan->order-by 1 record",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20")},
		},
	},
	{
		about: "test csv-data scan->order-by DESC",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"desc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
11,21
12,22
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("12"), []byte("22")},
			base.Vals{[]byte("11"), []byte("21")},
			base.Vals{[]byte("10"), []byte("20")},
		},
	},
	{
		about: "test csv-data scan->order-by two-label",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
12,22
10,21
10,20
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20")},
			base.Vals{[]byte("10"), []byte("21")},
			base.Vals{[]byte("12"), []byte("22")},
		},
	},
	{
		about: "test csv-data scan->order-by two-label, DESC, ASC",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"desc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
12,22
10,21
10,20
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("12"), []byte("22")},
			base.Vals{[]byte("10"), []byte("20")},
			base.Vals{[]byte("10"), []byte("21")},
		},
	},
	{
		about: "test csv-data scan->order-by two-label, ASC, DESC",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
12,2200
10,210
10,90
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("210")},
			base.Vals{[]byte("10"), []byte("90")},
			base.Vals{[]byte("12"), []byte("2200")},
		},
	},
	{
		about: "test csv-data scan->order-by two-label, ASC, DESC, str+int",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
12,"a22"
10,"a21"
10,20
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte(`"a21"`)},
			base.Vals{[]byte("10"), []byte("20")},
			base.Vals{[]byte("12"), []byte(`"a22"`)},
		},
	},
	{
		about: "test csv-data scan->order-by two-label, ASC, DESC, bool+int",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
12,"a22"
10,false
10,20
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20")},
			base.Vals{[]byte("10"), []byte(`false`)},
			base.Vals{[]byte("12"), []byte(`"a22"`)},
		},
	},
	{
		about: "test csv-data scan->order-by OFFSET 0 LIMIT 1",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
				int64(0),
				int64(1),
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
11,21
12,22
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20")},
		},
	},
	{
		about: "test csv-data scan->order-by OFFSET 0 LIMIT 100",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
				int64(0),
				int64(100),
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
11,21
12,22
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20")},
			base.Vals{[]byte("11"), []byte("21")},
			base.Vals{[]byte("12"), []byte("22")},
		},
	},
	{
		about: "test csv-data scan->order-by OFFSET 100 LIMIT 100",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
				int64(100),
				int64(100),
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
11,21
12,22
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->order-by OFFSET 1 LIMIT 0",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
				int64(1),
				int64(0),
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
11,21
12,22
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->order-by OFFSET 1 LIMIT 1",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
				int64(1),
				int64(1),
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
11,21
12,22
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("11"), []byte("21")},
		},
	},
	{
		about: "test csv-data scan->NIL-order-by OFFSET 1 LIMIT 1",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{},
				[]interface{}{},
				int64(1),
				int64(1),
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
11,21
12,22
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("11"), []byte("21")},
		},
	},
	{
		about: "test csv-data scan->joinNL-inner->order-by",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "dept"},
					[]interface{}{"labelPath", "emp"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
				int64(0),
				int64(10),
			},
			Children: []*base.Op{&base.Op{
				Kind:   "joinNL-inner",
				Labels: base.Labels{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"labelPath", "dept"},
					[]interface{}{"labelPath", "empDept"},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
"sales","san diego"
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Labels: base.Labels{"emp", "empDept"},
					Params: []interface{}{
						"csvData",
						`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
"mary","marketing"
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
		},
	},
	{
		about: "test csv-data scan->union-all->order-by",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "union-all",
				Labels: base.Labels{"a", "b", "c"},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Labels: base.Labels{"b"},
					Params: []interface{}{
						"csvData",
						`
9
55
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(nil), []byte("9"), []byte(nil)},
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
			base.Vals{[]byte(nil), []byte("55"), []byte(nil)},
		},
	},
	{
		about: "test csv-data scan->union-all->order-by just 1 scan",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "union-all",
				Labels: base.Labels{"a", "b", "c"},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
11,21,31
10,20,30
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->union-all->order-by more complex",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "union-all",
				Labels: base.Labels{"a", "b", "c"},
				Children: []*base.Op{&base.Op{
					Kind:   "project",
					Labels: base.Labels{"b", "c"},
					Params: []interface{}{
						[]interface{}{"labelPath", "b"},
						[]interface{}{"labelPath", "c"},
					},
					Children: []*base.Op{&base.Op{
						Kind:   "filter",
						Labels: base.Labels{"a", "b", "c"},
						Params: []interface{}{
							"eq",
							[]interface{}{"labelPath", "c"},
							[]interface{}{"json", `3000`},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b", "c"},
							Params: []interface{}{
								"csvData",
								`
00,00,0000
10,20,3000
11,21,3000
12,22,1000
`,
							},
						}},
					}},
				}, &base.Op{
					Kind:   "project",
					Labels: base.Labels{"b", "a"},
					Params: []interface{}{
						[]interface{}{"labelPath", "b"},
						[]interface{}{"labelPath", "a"},
					},
					Children: []*base.Op{&base.Op{
						Kind:   "filter",
						Labels: base.Labels{"a", "b", "c"},
						Params: []interface{}{
							"eq",
							[]interface{}{"labelPath", "a"},
							[]interface{}{"json", `10`},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b", "c"},
							Params: []interface{}{
								"csvData",
								`
00,00,0000
10,80,3000
10,81,3000
12,20,1000
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(nil), []byte("20"), []byte("3000")},
			base.Vals{[]byte(nil), []byte("21"), []byte("3000")},
			base.Vals{[]byte("10"), []byte("80"), []byte(nil)},
			base.Vals{[]byte("10"), []byte("81"), []byte(nil)},
		},
	},
	{
		about: "test csv-data scan->filter exprStr TRUE",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"."},
			Params: []interface{}{
				"exprStr",
				"TRUE",
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"."},
				Params: []interface{}{
					"jsonsData",
					`
{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}
{"a":2,"b":20,"c":[2,3],"d":{"x":"a","y":"B"}}
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(`{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}`)},
			base.Vals{[]byte(`{"a":2,"b":20,"c":[2,3],"d":{"x":"a","y":"B"}}`)},
		},
	},
	{
		about: "test csv-data scan->filter exprStr FALSE",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"."},
			Params: []interface{}{
				"exprStr",
				"FALSE",
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"."},
				Params: []interface{}{
					"jsonsData",
					`
{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}
{"a":2,"b":20,"c":[2,3],"d":{"x":"a","y":"B"}}
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->filter exprStr a=2",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"."},
			Params: []interface{}{
				"exprStr",
				"a = 2",
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"."},
				Params: []interface{}{
					"jsonsData",
					`
{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}
{"a":2,"b":20,"c":[2,3],"d":{"x":"a","y":"B"}}
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(`{"a":2,"b":20,"c":[2,3],"d":{"x":"a","y":"B"}}`)},
		},
	},
	{
		about: `test csv-data scan->filter exprStr a = 999 or b = 10`,
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"."},
			Params: []interface{}{
				"exprStr",
				`a = 999 or b = 10`,
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"."},
				Params: []interface{}{
					"jsonsData",
					`
{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}
{"a":2,"b":20,"c":[2,3],"d":{"x":"a","y":"B"}}
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(`{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}`)},
		},
	},
	{
		about: `test csv-data scan->filter exprStr d.y = "b"`,
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"."},
			Params: []interface{}{
				"exprStr",
				`d.y = "b"`,
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"."},
				Params: []interface{}{
					"jsonsData",
					`
{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}
{"a":2,"b":20,"c":[2,3],"d":{"x":"a","y":"B"}}
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(`{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}`)},
		},
	},
	{
		about: `test csv-data scan->filter->project exprStr d.y = "b"`,
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a"},
			Params: []interface{}{
				[]interface{}{"exprStr", "a * 1000"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "filter",
				Labels: base.Labels{"."},
				Params: []interface{}{
					"exprStr",
					`d.y = "B"`,
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"."},
					Params: []interface{}{
						"jsonsData",
						`
{"a":1,"b":10,"c":[1,2],"d":{"x":"a","y":"b"}}
{"a":2,"b":20,"c":[2,3],"d":{"x":"a","y":"B"}}
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(`2000`)},
		},
	},
	{
		about: "test csv-data scan->filter with b < 21",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"lt",
				[]interface{}{"labelPath", "b"},
				[]interface{}{"json", `21`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
		},
	},
	{
		about: "test csv-data scan->filter with b <= 21",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"le",
				[]interface{}{"labelPath", "b"},
				[]interface{}{"json", `21`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter with 21 >= b",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"ge",
				[]interface{}{"json", `21`},
				[]interface{}{"labelPath", "b"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter with b > 20",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"gt",
				[]interface{}{"labelPath", "b"},
				[]interface{}{"json", `20`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->filter with 20 < b",
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"lt",
				[]interface{}{"json", `20`},
				[]interface{}{"labelPath", "b"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: `test csv-data scan->filter with b > "hello"`,
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"gt",
				[]interface{}{"labelPath", "b"},
				[]interface{}{"json", `"hello"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: `test csv-data scan->filter with b < "hello"`,
		o: base.Op{
			Kind:   "filter",
			Labels: base.Labels{"a", "b", "c"},
			Params: []interface{}{
				"lt",
				[]interface{}{"labelPath", "b"},
				[]interface{}{"json", `"hello"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("20"), []byte("30")},
			base.Vals{[]byte("11"), []byte("21"), []byte("31")},
		},
	},
	{
		about: "test csv-data scan->distinct",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "distinct",
				Labels: base.Labels{"a"},
				Params: []interface{}{
					[]interface{}{
						[]interface{}{"labelPath", "a"},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a"},
					Params: []interface{}{
						"csvData",
						`
10
11
12
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10")},
			base.Vals{[]byte("11")},
			base.Vals{[]byte("12")},
		},
	},
	{
		about: "test csv-data scan->distinct with duplicate tuples",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "distinct",
				Labels: base.Labels{"a"},
				Params: []interface{}{
					[]interface{}{
						[]interface{}{"labelPath", "a"},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a"},
					Params: []interface{}{
						"csvData",
						`
10
11
12
10
11
12
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10")},
			base.Vals{[]byte("11")},
			base.Vals{[]byte("12")},
		},
	},
	{
		about: "test csv-data scan->distinct with empty tuples",
		o: base.Op{
			Kind:   "distinct",
			Labels: base.Labels{"a"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a"},
				Params: []interface{}{
					"csvData",
					``,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->distinct on 1 label of 2",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "distinct",
				Labels: base.Labels{"a"},
				Params: []interface{}{
					[]interface{}{
						[]interface{}{"labelPath", "a"},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
10,11
10,12
20,20
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10")},
			base.Vals{[]byte("20")},
		},
	},
	{
		about: "test csv-data scan->distinct->order-by",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "distinct",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					[]interface{}{
						[]interface{}{"labelPath", "a"},
						[]interface{}{"labelPath", "b"},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
10,11
10,12
20,20
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("11")},
			base.Vals{[]byte("10"), []byte("12")},
			base.Vals{[]byte("20"), []byte("20")},
		},
	},
	{
		about: "test csv-data scan->group-by count",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "count-a"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "count-a"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "group",
				Labels: base.Labels{"a", "count-a"},
				Params: []interface{}{
					[]interface{}{
						[]interface{}{"labelPath", "a"},
					},
					[]interface{}{
						[]interface{}{"labelPath", "a"},
					},
					[]interface{}{
						[]interface{}{"count"},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
10,11
10,12
20,20
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("2")},
			base.Vals{[]byte("20"), []byte("1")},
		},
	},
	{
		about: "test csv-data scan->joinHash-inner",
		o: base.Op{
			Kind:   "joinHash-inner",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{"labelPath", "dept"},
				[]interface{}{"labelPath", "empDept"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
"sales","san diego"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
"mary","marketing"
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test csv-data scan->joinHash-inner but false join condition",
		o: base.Op{
			Kind:   "joinHash-inner",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{"labelPath", "dept"},
				[]interface{}{"json", `"NOT-MATCHING"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test inner joinHash via always true=true join condition",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "dept"},
					[]interface{}{"labelPath", "city"},
					[]interface{}{"labelPath", "emp"},
					[]interface{}{"labelPath", "empDept"},
				},
				[]interface{}{
					"asc",
					"asc",
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "joinHash-inner",
				Labels: base.Labels{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					[]interface{}{"json", `true`},
					[]interface{}{"json", `true`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Labels: base.Labels{"emp", "empDept"},
					Params: []interface{}{
						"csvData",
						`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"fred"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test inner joinHash on dept with empty LHS",
		o: base.Op{
			Kind:   "joinHash-inner",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{"labelPath", `dept`},
				[]interface{}{"labelPath", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->joinHash-inner->order-by",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "dept"},
					[]interface{}{"labelPath", "emp"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
				int64(0),
				int64(10),
			},
			Children: []*base.Op{&base.Op{
				Kind:   "joinHash-inner",
				Labels: base.Labels{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					[]interface{}{"labelPath", "dept"},
					[]interface{}{"labelPath", "empDept"},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
"sales","san diego"
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Labels: base.Labels{"emp", "empDept"},
					Params: []interface{}{
						"csvData",
						`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
"mary","marketing"
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
		},
	},
	{
		about: "test left outer joinHash on dept",
		o: base.Op{
			Kind:   "joinHash-leftOuter",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{"labelPath", `dept`},
				[]interface{}{"labelPath", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
"sales","san diego"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
"mary","marketing"
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),

			StringsToVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),

			StringsToVals([]string{`"sales"`, `"san diego"`, ``, ``}, nil),
		},
	},
	{
		about: "test left outer joinHash on dept with empty RHS",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "dept"},
					[]interface{}{"labelPath", "city"},
					[]interface{}{"labelPath", "emp"},
					[]interface{}{"labelPath", "empDept"},
				},
				[]interface{}{
					"asc",
					"asc",
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "joinHash-leftOuter",
				Labels: base.Labels{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					[]interface{}{"labelPath", `dept`},
					[]interface{}{"labelPath", `empDept`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Labels: base.Labels{"emp", "empDept"},
					Params: []interface{}{
						"csvData",
						`
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
		},
	},
	{
		about: "test left outer joinHash on dept with empty LHS",
		o: base.Op{
			Kind:   "joinHash-leftOuter",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{"labelPath", `dept`},
				[]interface{}{"labelPath", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test left outer joinHash on never matching condition",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "dept"},
					[]interface{}{"labelPath", "city"},
					[]interface{}{"labelPath", "emp"},
					[]interface{}{"labelPath", "empDept"},
				},
				[]interface{}{
					"asc",
					"asc",
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "joinHash-leftOuter",
				Labels: base.Labels{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					[]interface{}{"labelPath", `dept`},
					[]interface{}{"labelPath", `someFakeLabel`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Labels: base.Labels{"emp", "empDept"},
					Params: []interface{}{
						"csvData",
						`
"dan","dev"
"doug","dev"
"frank","finance"
"fred","finance"
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
		},
	},
	{
		about: "test csv-data scan->project",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"x"},
			Params: []interface{}{
				[]interface{}{"valsEncodeCanonical", "a"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
00,00,0.000
1.200,-22,-0.0
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("\x03\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x000\x01\x00\x00\x00\x00\x00\x00\x000\x01\x00\x00\x00\x00\x00\x00\x000")},
			base.Vals{[]byte("\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x001.2\x03\x00\x00\x00\x00\x00\x00\x00-22\x02\x00\x00\x00\x00\x00\x00\x00-0")},
		},
	},
	{
		about: "test csv-data scan->intersect-distinct",
		o: base.Op{
			Kind:   "intersect-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
20,21
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`20`, `21`}, nil),
		},
	},
	{
		about: "test csv-data scan->intersect-distinct of empty left",
		o: base.Op{
			Kind:   "intersect-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->intersect-distinct of empty right",
		o: base.Op{
			Kind:   "intersect-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
20,21
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->intersect-distinct of repeating left",
		o: base.Op{
			Kind:   "intersect-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
10,11
20,21
30,31
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->intersect-distinct of repeating right",
		o: base.Op{
			Kind:   "intersect-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,11
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->intersect-distinct of repeating",
		o: base.Op{
			Kind:   "intersect-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
10,11
20,21
10,11
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,11
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`20`, `21`}, nil),
		},
	},
	{
		about: "test csv-data scan->intersect-all",
		o: base.Op{
			Kind:   "intersect-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
20,21
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`20`, `21`}, nil),
		},
	},
	{
		about: "test csv-data scan->intersect-all of empty left",
		o: base.Op{
			Kind:   "intersect-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->intersect-all of empty right",
		o: base.Op{
			Kind:   "intersect-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
20,21
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->intersect-all of repeating left",
		o: base.Op{
			Kind:   "intersect-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
10,11
20,21
30,31
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->intersect-all of repeating right",
		o: base.Op{
			Kind:   "intersect-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,11
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->intersect-all of repeating",
		o: base.Op{
			Kind:   "intersect-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
20,21
10,11
20,21
10,11
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,11
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`20`, `21`}, nil),
			StringsToVals([]string{`20`, `21`}, nil),
		},
	},
	{
		about: "test csv-data scan->except-distinct",
		o: base.Op{
			Kind:   "except-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
20,21
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`10`, `11`}, nil),
		},
	},
	{
		about: "test csv-data scan->except-distinct of empty left",
		o: base.Op{
			Kind:   "except-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->except-distinct of empty right",
		o: base.Op{
			Kind:   "except-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
20,21
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`20`, `21`}, nil),
			StringsToVals([]string{`10`, `11`}, nil),
		},
	},
	{
		about: "test csv-data scan->except-distinct of repeating left",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "except-distinct",
				Labels: base.Labels{"a", "b"},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
20,21
10,11
20,21
30,31
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`10`, `11`}, nil),
			StringsToVals([]string{`20`, `21`}, nil),
			StringsToVals([]string{`30`, `31`}, nil),
		},
	},
	{
		about: "test csv-data scan->except-distinct of repeating right",
		o: base.Op{
			Kind:   "except-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,11
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->except-distinct of repeating",
		o: base.Op{
			Kind:   "except-distinct",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
20,21
10,11
20,21
10,11
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,11
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`10`, `11`}, nil),
		},
	},
	{
		about: "test csv-data scan->except-all",
		o: base.Op{
			Kind:   "except-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
20,21
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`10`, `11`}, nil),
		},
	},
	{
		about: "test csv-data scan->except-all of empty left",
		o: base.Op{
			Kind:   "except-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->except-all of empty right",
		o: base.Op{
			Kind:   "except-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
20,21
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`20`, `21`}, nil),
			StringsToVals([]string{`10`, `11`}, nil),
		},
	},
	{
		about: "test csv-data scan->except-all of repeating left",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "except-all",
				Labels: base.Labels{"a", "b"},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
20,21
10,11
20,21
30,31
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`10`, `11`}, nil),
			StringsToVals([]string{`20`, `21`}, nil),
			StringsToVals([]string{`20`, `21`}, nil),
			StringsToVals([]string{`30`, `31`}, nil),
		},
	},
	{
		about: "test csv-data scan->except-all of repeating right",
		o: base.Op{
			Kind:   "except-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,11
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data scan->except-all of repeating",
		o: base.Op{
			Kind:   "except-all",
			Labels: base.Labels{"a", "b"},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
20,21
10,11
20,21
10,11
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
20,21
30,11
20,21
30,31
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`10`, `11`}, nil),
			StringsToVals([]string{`10`, `11`}, nil),
		},
	},
	{
		about: "test csv-data scan->group-by a then sum(b)",
		o: base.Op{
			Kind:   "group",
			Labels: base.Labels{"a", "sum-b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					[]interface{}{"sum"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
10,12
20,20
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("20"), []byte("20")},
			base.Vals{[]byte("10"), []byte("23")},
		},
	},
	{
		about: "test csv-data scan->group-by a then sum(a)",
		o: base.Op{
			Kind:   "group",
			Labels: base.Labels{"a", "sum-b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					[]interface{}{"sum"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
10,12
20,20
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("20"), []byte("20")},
			base.Vals{[]byte("10"), []byte("20")},
		},
	},
	{
		about: "test csv-data scan->group-by a then sum(b), count(b)",
		o: base.Op{
			Kind:   "group",
			Labels: base.Labels{"a", "sum-b", "count-b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
				},
				[]interface{}{
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					[]interface{}{"sum", "count"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,11
10,12
20,20
`,
				},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("20"), []byte("20"), []byte("1")},
			base.Vals{[]byte("10"), []byte("23"), []byte("2")},
		},
	},
	{
		about: "test csv-data scan->group-by min(b)",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "min-b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "min-b"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "group",
				Labels: base.Labels{"a", "min-b"},
				Params: []interface{}{
					[]interface{}{
						[]interface{}{"labelPath", "a"},
					},
					[]interface{}{
						[]interface{}{"labelPath", "b"},
					},
					[]interface{}{
						[]interface{}{"min"},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
10,11
10,12
20,20
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("11")},
			base.Vals{[]byte("20"), []byte("20")},
		},
	},
	{
		about: "test csv-data scan->group-by max(b)",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "max-b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "max-b"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "group",
				Labels: base.Labels{"a", "max-b"},
				Params: []interface{}{
					[]interface{}{
						[]interface{}{"labelPath", "a"},
					},
					[]interface{}{
						[]interface{}{"labelPath", "b"},
					},
					[]interface{}{
						[]interface{}{"max"},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
10,11
10,12
20,20
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("12")},
			base.Vals{[]byte("20"), []byte("20")},
		},
	},
	{
		about: "test csv-data scan->group-by avg(b)",
		o: base.Op{
			Kind:   "order-offset-limit",
			Labels: base.Labels{"a", "avg-b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"labelPath", "a"},
					[]interface{}{"labelPath", "avg-b"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "group",
				Labels: base.Labels{"a", "avg-b"},
				Params: []interface{}{
					[]interface{}{
						[]interface{}{"labelPath", "a"},
					},
					[]interface{}{
						[]interface{}{"labelPath", "b"},
					},
					[]interface{}{
						[]interface{}{"avg"},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						"csvData",
						`
10,11
10,12
20,20
`,
					},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("11.5")},
			base.Vals{[]byte("20"), []byte("20")},
		},
	},
	{
		about: "test csv-data scan->unnest-inner",
		o: base.Op{
			Kind:   "unnest-inner",
			Labels: base.Labels{"."},
			Params: []interface{}{
				"labelPath", ".", "a",
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"."},
				Params: []interface{}{
					"jsonsData",
					`
{"a":[1,2]}
{"a":[3]}
{"a":[]}
{"a":123}
`,
				},
			}, &base.Op{
				Kind:   "noop",
				Labels: base.Labels{"child"},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(`{"a":[1,2]}`), []byte("1")},
			base.Vals{[]byte(`{"a":[1,2]}`), []byte("2")},
			base.Vals{[]byte(`{"a":[3]}`), []byte("3")},
		},
	},
	{
		about: "test csv-data scan->unnest-leftOuter",
		o: base.Op{
			Kind:   "unnest-leftOuter",
			Labels: base.Labels{"."},
			Params: []interface{}{
				"labelPath", ".", "a",
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"."},
				Params: []interface{}{
					"jsonsData",
					`
{"a":[1,2]}
{"a":[3]}
{"a":[]}
{"a":123}
`,
				},
			}, &base.Op{
				Kind:   "noop",
				Labels: base.Labels{"child"},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte(`{"a":[1,2]}`), []byte("1")},
			base.Vals{[]byte(`{"a":[1,2]}`), []byte("2")},
			base.Vals{[]byte(`{"a":[3]}`), []byte("3")},
			base.Vals{[]byte(`{"a":[]}`), []byte(nil)},
			base.Vals{[]byte(`{"a":123}`), []byte(nil)},
		},
	},
	{
		about: "test csv-data scan->nestNL-inner",
		o: base.Op{
			Kind:   "nestNL-inner",
			Labels: base.Labels{"dept", "city", "emp"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", "dept"},
				[]interface{}{"labelPath", "empDept"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
"sales","san diego"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"empDept", "emp"},
				Params: []interface{}{
					"csvData",
					`
"dev","dan"
"dev","doug"
"finance","frank"
"finance","fred"
"marketing","mary"
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `["dan","doug"]`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `["frank","fred"]`}, nil),
		},
	},
	{
		about: "test csv-data scan->nestNL-leftOuter",
		o: base.Op{
			Kind:   "nestNL-leftOuter",
			Labels: base.Labels{"dept", "city", "emp"},
			Params: []interface{}{
				"eq",
				[]interface{}{"labelPath", "dept"},
				[]interface{}{"labelPath", "empDept"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Labels: base.Labels{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
"sales","san diego"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Labels: base.Labels{"empDept", "emp"},
				Params: []interface{}{
					"csvData",
					`
"dev","dan"
"dev","doug"
"finance","frank"
"finance","fred"
"marketing","mary"
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToVals([]string{`"dev"`, `"paris"`, `["dan","doug"]`}, nil),
			StringsToVals([]string{`"finance"`, `"london"`, `["frank","fred"]`}, nil),
			StringsToVals([]string{`"sales"`, `"san diego"`, `[]`}, nil),
		},
	},
	{
		about: "test csv-data sequence->[scan->filter->project->temp-capture]",
		o: base.Op{
			Kind:   "sequence",
			Labels: base.Labels{"a", "c"},
			Children: []*base.Op{&base.Op{
				Kind:   "temp-capture",
				Labels: base.Labels{"a", "c"},
				Params: []interface{}{0},
				Children: []*base.Op{&base.Op{
					Kind:   "project",
					Labels: base.Labels{"a", "c"},
					Params: []interface{}{
						[]interface{}{"labelPath", "a"},
						[]interface{}{"labelPath", "c"},
					},
					Children: []*base.Op{&base.Op{
						Kind:   "filter",
						Labels: base.Labels{"a", "b", "c"},
						Params: []interface{}{
							"eq",
							[]interface{}{"labelPath", "c"},
							[]interface{}{"json", `3000`},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b", "c"},
							Params: []interface{}{
								"csvData",
								`
00,00,0000
10,20,3000
11,21,3000
12,22,1000
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test csv-data sequence->[scan->filter->project->temp-capture, temp-yield]",
		o: base.Op{
			Kind:   "sequence",
			Labels: base.Labels{"a", "c"},
			Children: []*base.Op{&base.Op{
				Kind:   "temp-capture",
				Labels: base.Labels{"a", "c"},
				Params: []interface{}{0},
				Children: []*base.Op{&base.Op{
					Kind:   "project",
					Labels: base.Labels{"a", "c"},
					Params: []interface{}{
						[]interface{}{"labelPath", "a"},
						[]interface{}{"labelPath", "c"},
					},
					Children: []*base.Op{&base.Op{
						Kind:   "filter",
						Labels: base.Labels{"a", "b", "c"},
						Params: []interface{}{
							"eq",
							[]interface{}{"labelPath", "c"},
							[]interface{}{"json", `3000`},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b", "c"},
							Params: []interface{}{
								"csvData",
								`
00,00,0000
10,20,3000
11,21,3000
12,22,1000
`,
							},
						}},
					}},
				}},
			}, &base.Op{
				Kind:   "temp-yield",
				Labels: base.Labels{"a", "c"},
				Params: []interface{}{0},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("3000")},
			base.Vals{[]byte("11"), []byte("3000")},
		},
	},
	{
		about: "test csv-data scan->order->window-partition->window-frame->project window-frame-count",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "count-a"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-frame-count",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"num", -1, // Preceding.
							"num", 1, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("2")},
			base.Vals{[]byte("10"), []byte("3")},
			base.Vals{[]byte("10"), []byte("2")},
			base.Vals{[]byte("20"), []byte("2")},
			base.Vals{[]byte("20"), []byte("2")},
			base.Vals{[]byte("30"), []byte("1")},
		},
	},
	{
		about: "test csv-data scan->order->window-partition->window-frame-exclude-current-row->project window-frame-count",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "count-a"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-frame-count",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"num", -1, // Preceding.
							"num", 1, // Following.
							"current-row", // Exclude.
							0,             // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("1")},
			base.Vals{[]byte("10"), []byte("2")},
			base.Vals{[]byte("10"), []byte("1")},
			base.Vals{[]byte("20"), []byte("1")},
			base.Vals{[]byte("20"), []byte("1")},
			base.Vals{[]byte("30"), []byte("0")},
		},
	},
	{
		about: "test csv-data scan->order->window-partition->window-frame current-row to unbounded ->project window-frame-count",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "count-a"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-frame-count",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"num", 0, // Preceding.
							"unbounded", 1, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("3")},
			base.Vals{[]byte("10"), []byte("2")},
			base.Vals{[]byte("10"), []byte("1")},
			base.Vals{[]byte("20"), []byte("2")},
			base.Vals{[]byte("20"), []byte("1")},
			base.Vals{[]byte("30"), []byte("1")},
		},
	},
	{
		about: "test csv-data scan->order->window-partition->window-frame unbounded to current-row-minus-1 ->project window-frame-count",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "count-a"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-frame-count",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"unbounded", 0, // Preceding.
							"num", -1, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("0")},
			base.Vals{[]byte("10"), []byte("1")},
			base.Vals{[]byte("10"), []byte("2")},
			base.Vals{[]byte("20"), []byte("0")},
			base.Vals{[]byte("20"), []byte("1")},
			base.Vals{[]byte("30"), []byte("0")},
		},
	},
	{
		about: "test csv-data window-partition->project window-frame FIRST_VALUE",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "rowNumber", "firstValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-partition-row-number",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					-1,        // Initial starting position is -1.
					true,      // Step is ascending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "b"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"num", -1, // Preceding.
							"num", 0, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("1"), []byte("11")},
			base.Vals{[]byte("10"), []byte("2"), []byte("11")},
			base.Vals{[]byte("10"), []byte("3"), []byte("12")},
			base.Vals{[]byte("20"), []byte("1"), []byte("20")},
			base.Vals{[]byte("20"), []byte("2"), []byte("20")},
			base.Vals{[]byte("30"), []byte("1"), []byte("30")},
		},
	},
	{
		about: "test csv-data window-partition->project window-frame LAST_VALUE",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "rowNumber", "lastValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-partition-row-number",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					1,         // Initial starting position is MaxInt64.
					false,     // Step is descending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "b"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"num", -1, // Preceding.
							"num", 1, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("1"), []byte("12")},
			base.Vals{[]byte("10"), []byte("2"), []byte("13")},
			base.Vals{[]byte("10"), []byte("3"), []byte("13")},
			base.Vals{[]byte("20"), []byte("1"), []byte("21")},
			base.Vals{[]byte("20"), []byte("2"), []byte("21")},
			base.Vals{[]byte("30"), []byte("1"), []byte("30")},
		},
	},
	{
		about: "test csv-data window-partition->project window-frame NTH_VALUE(b, 2)",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "rowNumber", "firstValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-partition-row-number",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					-1,        // Initial starting position is -1.
					true,      // Step is ascending.
					uint64(2), // Number of steps to take.
					[]interface{}{"labelPath", "b"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"unbounded", 0, // Preceding.
							"unbounded", 0, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("1"), []byte("12")},
			base.Vals{[]byte("10"), []byte("2"), []byte("12")},
			base.Vals{[]byte("10"), []byte("3"), []byte("12")},
			base.Vals{[]byte("20"), []byte("1"), []byte("21")},
			base.Vals{[]byte("20"), []byte("2"), []byte("21")},
			base.Vals{[]byte("30"), []byte("1"), []byte(nil)},
		},
	},
	{
		about: "test csv-data window-partition->project window-frame LEAD(b, 1)",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "rowNumber", "firstValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-partition-row-number",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					0,         // Initial starting position is current-row.
					true,      // Step is ascending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "b"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"unbounded", 0, // Preceding.
							"unbounded", 0, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("1"), []byte("12")},
			base.Vals{[]byte("10"), []byte("2"), []byte("13")},
			base.Vals{[]byte("10"), []byte("3"), []byte(nil)},
			base.Vals{[]byte("20"), []byte("1"), []byte("21")},
			base.Vals{[]byte("20"), []byte("2"), []byte(nil)},
			base.Vals{[]byte("30"), []byte("1"), []byte(nil)},
		},
	},
	{
		about: "test csv-data window-partition->project window-frame LEAD(b, 2)",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "rowNumber", "firstValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-partition-row-number",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					0,         // Initial starting position is current-row.
					true,      // Step is ascending.
					uint64(2), // Number of steps to take.
					[]interface{}{"labelPath", "b"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"unbounded", 0, // Preceding.
							"unbounded", 0, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("1"), []byte("13")},
			base.Vals{[]byte("10"), []byte("2"), []byte(nil)},
			base.Vals{[]byte("10"), []byte("3"), []byte(nil)},
			base.Vals{[]byte("20"), []byte("1"), []byte(nil)},
			base.Vals{[]byte("20"), []byte("2"), []byte(nil)},
			base.Vals{[]byte("30"), []byte("1"), []byte(nil)},
		},
	},
	{
		about: "test csv-data window-partition->project window-frame LAG(b, 1)",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "rowNumber", "firstValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-partition-row-number",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					0,         // Initial starting position is current-row.
					false,     // Step is descending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "b"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"unbounded", 0, // Preceding.
							"unbounded", 0, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("1"), []byte(nil)},
			base.Vals{[]byte("10"), []byte("2"), []byte("11")},
			base.Vals{[]byte("10"), []byte("3"), []byte("12")},
			base.Vals{[]byte("20"), []byte("1"), []byte(nil)},
			base.Vals{[]byte("20"), []byte("2"), []byte("20")},
			base.Vals{[]byte("30"), []byte("1"), []byte(nil)},
		},
	},
	{
		about: "test csv-data window-partition->project window-frame LAG(b, 2)",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "rowNumber", "firstValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{
					"window-partition-row-number",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					0,         // Initial starting position is current-row.
					false,     // Step is descending.
					uint64(2), // Number of steps to take.
					[]interface{}{"labelPath", "b"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"unbounded", 0, // Preceding.
							"unbounded", 0, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
						},
						1,  // # of the partitioning exprs for PARTITION-BY.
						"", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,13
20,20
20,21
30,30
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("1"), []byte(nil)},
			base.Vals{[]byte("10"), []byte("2"), []byte(nil)},
			base.Vals{[]byte("10"), []byte("3"), []byte("11")},
			base.Vals{[]byte("20"), []byte("1"), []byte(nil)},
			base.Vals{[]byte("20"), []byte("2"), []byte(nil)},
			base.Vals{[]byte("30"), []byte("1"), []byte(nil)},
		},
	},
	{
		about: "test csv-data scan->order->window-partition->window-frame->project RANK, DENSE_RANK",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "b", "rowNumber", "result-rank", "result-denseRank"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelPath", "b"},
				[]interface{}{
					"window-partition-row-number",
					1, // Slot for window frames.
					0, // Idx for window frame.
				},
				[]interface{}{"labelUint64", "myRank"},
				[]interface{}{"labelUint64", "myDenseRank"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b", "myRank", "myDenseRank"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"unbounded", 0, // Preceding.
							"unbounded", 0, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b", "myRank", "myDenseRank"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
							[]interface{}{"labelPath", "b"},
						},
						1,                // # of the partitioning exprs for PARTITION-BY.
						"rank,denseRank", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,12
10,13
10,13
10,14
20,20
20,21
20,21
30,30
30,30
30,31
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("11"), []byte("1"), []byte("1"), []byte("1")},
			base.Vals{[]byte("10"), []byte("12"), []byte("2"), []byte("2"), []byte("2")},
			base.Vals{[]byte("10"), []byte("12"), []byte("3"), []byte("2"), []byte("2")},
			base.Vals{[]byte("10"), []byte("13"), []byte("4"), []byte("4"), []byte("3")},
			base.Vals{[]byte("10"), []byte("13"), []byte("5"), []byte("4"), []byte("3")},
			base.Vals{[]byte("10"), []byte("14"), []byte("6"), []byte("6"), []byte("4")},
			base.Vals{[]byte("20"), []byte("20"), []byte("1"), []byte("1"), []byte("1")},
			base.Vals{[]byte("20"), []byte("21"), []byte("2"), []byte("2"), []byte("2")},
			base.Vals{[]byte("20"), []byte("21"), []byte("3"), []byte("2"), []byte("2")},
			base.Vals{[]byte("30"), []byte("30"), []byte("1"), []byte("1"), []byte("1")},
			base.Vals{[]byte("30"), []byte("30"), []byte("2"), []byte("1"), []byte("1")},
			base.Vals{[]byte("30"), []byte("31"), []byte("3"), []byte("3"), []byte("2")},
		},
	},
	{
		about: "test csv-data window-partition->ROWS window-frame [-1...1], project FIRST_VALUE, LAST_VALUE",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "denseRank", "firstValue", "lastValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelUint64", "myDenseRank"},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					-1,        // Initial starting position is -1.
					true,      // Step is ascending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "b"},
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					1,         // Initial starting position is end.
					false,     // Step is descending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "b"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b", "myDenseRank"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"rows",
							"num", -1, // Preceding.
							"num", 1, // Following.
							"no-others", // Exclude.
							0,           // ValIdx, unused with ROWS.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b", "myDenseRank"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
							[]interface{}{"labelPath", "b"},
						},
						1,           // # of the partitioning exprs for PARTITION-BY.
						"denseRank", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
							},
							[]interface{}{
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b"},
							Params: []interface{}{
								"csvData",
								`
10,11
10,12
10,12
10,12
10,13
20,20
20,20
20,21
30,30
30,31
30,31
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("1"), []byte("11"), []byte("12")},
			base.Vals{[]byte("10"), []byte("2"), []byte("11"), []byte("12")},
			base.Vals{[]byte("10"), []byte("2"), []byte("12"), []byte("12")},
			base.Vals{[]byte("10"), []byte("2"), []byte("12"), []byte("13")},
			base.Vals{[]byte("10"), []byte("3"), []byte("12"), []byte("13")},
			base.Vals{[]byte("20"), []byte("1"), []byte("20"), []byte("20")},
			base.Vals{[]byte("20"), []byte("1"), []byte("20"), []byte("21")},
			base.Vals{[]byte("20"), []byte("2"), []byte("20"), []byte("21")},
			base.Vals{[]byte("30"), []byte("1"), []byte("30"), []byte("31")},
			base.Vals{[]byte("30"), []byte("2"), []byte("30"), []byte("31")},
			base.Vals{[]byte("30"), []byte("2"), []byte("31"), []byte("31")},
		},
	},
	{
		about: "test csv-data window-partition->GROUPS window-frame [-1...1], project FIRST_VALUE, LAST_VALUE",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "c", "denseRank", "firstValue", "lastValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelPath", "c"},
				[]interface{}{"labelUint64", "myDenseRank"},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					-1,        // Initial starting position is -1.
					true,      // Step is ascending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "c"},
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					1,         // Initial starting position is end.
					false,     // Step is descending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "c"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b", "c", "myDenseRank"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"groups",
							"num", -1, // Preceding.
							"num", 1, // Following.
							"no-others", // Exclude.
							3,           // ValIdx to the denseRank val.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b", "c", "myDenseRank"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
							[]interface{}{"labelPath", "b"},
						},
						1,           // # of the partitioning exprs for PARTITION-BY.
						"denseRank", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b", "c"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
								[]interface{}{"labelPath", "c"},
							},
							[]interface{}{
								"asc",
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b", "c"},
							Params: []interface{}{
								"csvData",
								`
10,11,100
10,12,101
10,12,102
10,12,103
10,13,104
20,20,200
20,20,201
20,21,202
30,30,300
30,31,301
30,31,302
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("100"), []byte("1"), []byte("100"), []byte("103")},
			base.Vals{[]byte("10"), []byte("101"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("102"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("103"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("104"), []byte("3"), []byte("101"), []byte("104")},
			base.Vals{[]byte("20"), []byte("200"), []byte("1"), []byte("200"), []byte("202")},
			base.Vals{[]byte("20"), []byte("201"), []byte("1"), []byte("200"), []byte("202")},
			base.Vals{[]byte("20"), []byte("202"), []byte("2"), []byte("200"), []byte("202")},
			base.Vals{[]byte("30"), []byte("300"), []byte("1"), []byte("300"), []byte("302")},
			base.Vals{[]byte("30"), []byte("301"), []byte("2"), []byte("300"), []byte("302")},
			base.Vals{[]byte("30"), []byte("302"), []byte("2"), []byte("300"), []byte("302")},
		},
	},
	{
		about: "test csv-data window-partition->RANGE window-frame [-1...1], project FIRST_VALUE, LAST_VALUE",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "c", "denseRank", "firstValue", "lastValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelPath", "c"},
				[]interface{}{"labelUint64", "myDenseRank"},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					-1,        // Initial starting position is -1.
					true,      // Step is ascending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "c"},
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					1,         // Initial starting position is end.
					false,     // Step is descending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "c"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b", "c", "myDenseRank"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"range",
							"num", float64(-1.0), // Preceding.
							"num", float64(1.0), // Following.
							"no-others", // Exclude.
							1,           // ValIdx, for RANGE type.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b", "c", "myDenseRank"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
							[]interface{}{"labelPath", "b"},
						},
						1,           // # of the partitioning exprs for PARTITION-BY.
						"denseRank", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b", "c"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
								[]interface{}{"labelPath", "c"},
							},
							[]interface{}{
								"asc",
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b", "c"},
							Params: []interface{}{
								"csvData",
								`
10,11,100
10,12,101
10,12,102
10,12,103
10,13,104
20,20,200
20,20,201
20,21,202
30,30,300
30,31,301
30,31,302
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("100"), []byte("1"), []byte("100"), []byte("103")},
			base.Vals{[]byte("10"), []byte("101"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("102"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("103"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("104"), []byte("3"), []byte("101"), []byte("104")},
			base.Vals{[]byte("20"), []byte("200"), []byte("1"), []byte("200"), []byte("202")},
			base.Vals{[]byte("20"), []byte("201"), []byte("1"), []byte("200"), []byte("202")},
			base.Vals{[]byte("20"), []byte("202"), []byte("2"), []byte("200"), []byte("202")},
			base.Vals{[]byte("30"), []byte("300"), []byte("1"), []byte("300"), []byte("302")},
			base.Vals{[]byte("30"), []byte("301"), []byte("2"), []byte("300"), []byte("302")},
			base.Vals{[]byte("30"), []byte("302"), []byte("2"), []byte("300"), []byte("302")},
		},
	},
	{
		about: "test csv-data window-partition->RANGE window-frame [unbounded...unbounded] EXCLUDE GROUP, project FIRST_VALUE, LAST_VALUE",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "c", "denseRank", "firstValue", "lastValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelPath", "c"},
				[]interface{}{"labelUint64", "myDenseRank"},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					-1,        // Initial starting position is -1.
					true,      // Step is ascending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "c"},
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					1,         // Initial starting position is end.
					false,     // Step is descending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "c"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b", "c", "myDenseRank"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"range",
							"unbounded", 0, // Preceding.
							"unbounded", 0, // Following.
							"group", // Exclude.
							1,       // ValIdx, for RANGE type.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b", "c", "myDenseRank"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
							[]interface{}{"labelPath", "b"},
						},
						1,           // # of the partitioning exprs for PARTITION-BY.
						"denseRank", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b", "c"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
								[]interface{}{"labelPath", "c"},
							},
							[]interface{}{
								"asc",
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b", "c"},
							Params: []interface{}{
								"csvData",
								`
10,11,100
10,12,101
10,12,102
10,12,103
10,13,104
20,20,200
20,20,201
20,21,202
30,30,300
30,31,301
30,31,302
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("100"), []byte("1"), []byte("101"), []byte("104")},
			base.Vals{[]byte("10"), []byte("101"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("102"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("103"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("104"), []byte("3"), []byte("100"), []byte("103")},
			base.Vals{[]byte("20"), []byte("200"), []byte("1"), []byte("202"), []byte("202")},
			base.Vals{[]byte("20"), []byte("201"), []byte("1"), []byte("202"), []byte("202")},
			base.Vals{[]byte("20"), []byte("202"), []byte("2"), []byte("200"), []byte("201")},
			base.Vals{[]byte("30"), []byte("300"), []byte("1"), []byte("301"), []byte("302")},
			base.Vals{[]byte("30"), []byte("301"), []byte("2"), []byte("300"), []byte("300")},
			base.Vals{[]byte("30"), []byte("302"), []byte("2"), []byte("300"), []byte("300")},
		},
	},
	{
		about: "test csv-data window-partition->RANGE window-frame [unbounded...unbounded] EXCLUDE TIES, project FIRST_VALUE, LAST_VALUE",
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"a", "c", "denseRank", "firstValue", "lastValue"},
			Params: []interface{}{
				[]interface{}{"labelPath", "a"},
				[]interface{}{"labelPath", "c"},
				[]interface{}{"labelUint64", "myDenseRank"},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					-1,        // Initial starting position is -1.
					true,      // Step is ascending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "c"},
				},
				[]interface{}{
					"window-frame-step-value",
					1,         // Slot for window frames.
					0,         // Idx for window frame.
					1,         // Initial starting position is end.
					false,     // Step is descending.
					uint64(1), // Number of steps to take.
					[]interface{}{"labelPath", "c"},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "window-frames",
				Labels: base.Labels{"a", "b", "c", "myDenseRank"},
				Params: []interface{}{
					0, // Slot for window partition.
					1, // Slot for window frames.
					[]interface{}{ // Window frames cfg.
						[]interface{}{
							"range",
							"unbounded", 0, // Preceding.
							"unbounded", 0, // Following.
							"ties", // Exclude.
							1,      // ValIdx, for RANGE type.
						},
					},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "window-partition",
					Labels: base.Labels{"a", "b", "c", "myDenseRank"},
					Params: []interface{}{
						0, // Slot for window partition.
						[]interface{}{
							// Partitioning exprs...
							[]interface{}{"labelPath", "a"},
							[]interface{}{"labelPath", "b"},
						},
						1,           // # of the partitioning exprs for PARTITION-BY.
						"denseRank", // Additional tracking info.
					},
					Children: []*base.Op{&base.Op{
						Kind:   "order-offset-limit",
						Labels: base.Labels{"a", "b", "c"},
						Params: []interface{}{
							[]interface{}{
								[]interface{}{"labelPath", "a"},
								[]interface{}{"labelPath", "b"},
								[]interface{}{"labelPath", "c"},
							},
							[]interface{}{
								"asc",
								"asc",
								"asc",
							},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Labels: base.Labels{"a", "b", "c"},
							Params: []interface{}{
								"csvData",
								`
10,11,100
10,12,101
10,12,102
10,12,103
10,13,104
20,20,200
20,20,201
20,21,202
30,30,300
30,31,301
30,31,302
`,
							},
						}},
					}},
				}},
			}},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("10"), []byte("100"), []byte("1"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("101"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("102"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("103"), []byte("2"), []byte("100"), []byte("104")},
			base.Vals{[]byte("10"), []byte("104"), []byte("3"), []byte("100"), []byte("104")},
			base.Vals{[]byte("20"), []byte("200"), []byte("1"), []byte("200"), []byte("202")},
			base.Vals{[]byte("20"), []byte("201"), []byte("1"), []byte("201"), []byte("202")},
			base.Vals{[]byte("20"), []byte("202"), []byte("2"), []byte("200"), []byte("202")},
			base.Vals{[]byte("30"), []byte("300"), []byte("1"), []byte("300"), []byte("302")},
			base.Vals{[]byte("30"), []byte("301"), []byte("2"), []byte("300"), []byte("301")},
			base.Vals{[]byte("30"), []byte("302"), []byte("2"), []byte("300"), []byte("302")},
		},
	},

	// ---- Native n-ary / combining-op COMPILED-PATH coverage ----------------
	// These project each native op's param tree DIRECTLY (bypassing the glue
	// optimizer), so both the interpreter (TestCasesSimple) and -- crucially --
	// the compiler (TestCasesSimpleWithCompiler -> `go test ./test/tmp`) exercise
	// every op's generated code. The n-ary harness (MakeNaryExprFunc) and the
	// base-combining bi-ops (and/or/nullif) had compiled-path codegen bugs that
	// stayed latent because no compiled case reached them; these lock that down.
	// One 1-row scan feeds a single-label project of the computed value.
	naryProjectCase("and-2", []interface{}{"and",
		[]interface{}{"json", `true`}, []interface{}{"json", `false`}}, `false`),
	naryProjectCase("and-3-nested", []interface{}{"and",
		[]interface{}{"json", `true`},
		[]interface{}{"and", []interface{}{"json", `true`}, []interface{}{"json", `true`}}}, `true`),
	naryProjectCase("or-2", []interface{}{"or",
		[]interface{}{"json", `false`}, []interface{}{"json", `true`}}, `true`),
	naryProjectCase("or-null", []interface{}{"or",
		[]interface{}{"json", `false`}, []interface{}{"json", `null`}}, `null`),
	naryProjectCase("nullif-eq", []interface{}{"nullif",
		[]interface{}{"json", `5`}, []interface{}{"json", `5`}}, `null`),
	naryProjectCase("missingif-ne", []interface{}{"missingif",
		[]interface{}{"json", `5`}, []interface{}{"json", `6`}}, `5`),

	// Native arithmetic (value-producing) in the COMPILED path: exercises the
	// int-op-code dispatch (base.Num.Arith, not a func value) and the reused
	// lzBufPre buffer. `add` reads a scanned field (a=1) to also cover a labelPath
	// operand; the rest use constants for deterministic results.
	naryProjectCase("add", []interface{}{"add",
		[]interface{}{"labelPath", "a"}, []interface{}{"json", `1`}}, `2`),
	naryProjectCase("sub", []interface{}{"sub",
		[]interface{}{"json", `1`}, []interface{}{"json", `3`}}, `-2`),
	naryProjectCase("mult", []interface{}{"mult",
		[]interface{}{"json", `6`}, []interface{}{"json", `7`}}, `42`),
	naryProjectCase("div", []interface{}{"div",
		[]interface{}{"json", `7`}, []interface{}{"json", `2`}}, `3.5`),
	naryProjectCase("div-zero", []interface{}{"div",
		[]interface{}{"json", `1`}, []interface{}{"json", `0`}}, `null`),
	naryProjectCase("mod", []interface{}{"mod",
		[]interface{}{"json", `7`}, []interface{}{"json", `3`}}, `1`),
	naryProjectCase("idiv", []interface{}{"idiv",
		[]interface{}{"json", `7`}, []interface{}{"json", `2`}}, `3`),
	naryProjectCase("imod", []interface{}{"imod",
		[]interface{}{"json", `7`}, []interface{}{"json", `2`}}, `1`),
	naryProjectCase("neg", []interface{}{"neg",
		[]interface{}{"labelPath", "a"}}, `-1`),
	naryProjectCase("add-null", []interface{}{"add",
		[]interface{}{"json", `null`}, []interface{}{"json", `1`}}, `null`),

	// Unary numeric math functions (value-producing) in the COMPILED path.
	naryProjectCase("abs", []interface{}{"abs", []interface{}{"json", `-7`}}, `7`),
	naryProjectCase("ceil", []interface{}{"ceil", []interface{}{"json", `2.1`}}, `3`),
	naryProjectCase("floor", []interface{}{"floor", []interface{}{"json", `2.9`}}, `2`),
	naryProjectCase("sqrt", []interface{}{"sqrt", []interface{}{"json", `9`}}, `3`),
	naryProjectCase("sqrt-neg", []interface{}{"sqrt", []interface{}{"json", `-1`}}, `"NaN"`),
	naryProjectCase("sign-neg", []interface{}{"sign", []interface{}{"json", `-42`}}, `-1`),
	naryProjectCase("abs-field", []interface{}{"abs", []interface{}{"labelPath", "a"}}, `1`),
	naryProjectCase("abs-null", []interface{}{"abs", []interface{}{"json", `null`}}, `null`),
	naryProjectCase("abs-str", []interface{}{"abs", []interface{}{"json", `"x"`}}, `null`),

	// Unary string functions (value-producing) in the COMPILED path.
	naryProjectCase("upper", []interface{}{"upper", []interface{}{"json", `"aBc"`}}, `"ABC"`),
	naryProjectCase("lower", []interface{}{"lower", []interface{}{"json", `"aBc"`}}, `"abc"`),
	naryProjectCase("length", []interface{}{"length", []interface{}{"json", `"hello"`}}, `5`),
	naryProjectCase("upper-esc", []interface{}{"upper", []interface{}{"json", `"a\"b"`}}, `"A\"B"`),
	naryProjectCase("length-null", []interface{}{"length", []interface{}{"json", `null`}}, `null`),
	naryProjectCase("upper-num", []interface{}{"upper", []interface{}{"json", `5`}}, `null`),
	naryProjectCase("title", []interface{}{"title", []interface{}{"json", `"hELLO wORLD"`}}, `"Hello World"`),
	naryProjectCase("sin-zero", []interface{}{"sin", []interface{}{"json", `0`}}, `0`),
	naryProjectCase("cos-zero", []interface{}{"cos", []interface{}{"json", `0`}}, `1`),
	naryProjectCase("atan-zero", []interface{}{"atan", []interface{}{"json", `0`}}, `0`),

	// Binary math (value-producing, two operands) in the COMPILED path.
	naryProjectCase("power", []interface{}{"power",
		[]interface{}{"json", `2`}, []interface{}{"json", `10`}}, `1024`),
	naryProjectCase("power-frac", []interface{}{"power",
		[]interface{}{"json", `9`}, []interface{}{"json", `0.5`}}, `3`),
	naryProjectCase("power-field", []interface{}{"power",
		[]interface{}{"labelPath", "a"}, []interface{}{"json", `3`}}, `1`),
	naryProjectCase("atan2-zero", []interface{}{"atan2",
		[]interface{}{"json", `0`}, []interface{}{"json", `1`}}, `0`),
	naryProjectCase("power-null", []interface{}{"power",
		[]interface{}{"json", `null`}, []interface{}{"json", `2`}}, `null`),
	naryProjectCase("power-str", []interface{}{"power",
		[]interface{}{"json", `2`}, []interface{}{"json", `"x"`}}, `null`),

	// Binary string (value-producing) in the COMPILED path.
	naryProjectCase("contains-yes", []interface{}{"contains",
		[]interface{}{"json", `"hello"`}, []interface{}{"json", `"ell"`}}, `true`),
	naryProjectCase("contains-no", []interface{}{"contains",
		[]interface{}{"json", `"hello"`}, []interface{}{"json", `"z"`}}, `false`),
	naryProjectCase("position0-found", []interface{}{"position0",
		[]interface{}{"json", `"hello"`}, []interface{}{"json", `"ll"`}}, `2`),
	naryProjectCase("position0-absent", []interface{}{"position0",
		[]interface{}{"json", `"hello"`}, []interface{}{"json", `"z"`}}, `-1`),
	naryProjectCase("position1-found", []interface{}{"position1",
		[]interface{}{"json", `"hello"`}, []interface{}{"json", `"ll"`}}, `3`),
	naryProjectCase("contains-null", []interface{}{"contains",
		[]interface{}{"json", `null`}, []interface{}{"json", `"x"`}}, `null`),

	// Type conversions (value-producing) in the COMPILED path.
	naryProjectCase("to_bool-num", []interface{}{"to_boolean", []interface{}{"json", `5`}}, `true`),
	naryProjectCase("to_bool-zero", []interface{}{"to_boolean", []interface{}{"json", `0`}}, `false`),
	naryProjectCase("to_str-num", []interface{}{"to_string", []interface{}{"json", `42`}}, `"42"`),
	naryProjectCase("to_str-bool", []interface{}{"to_string", []interface{}{"json", `true`}}, `"true"`),
	naryProjectCase("to_num-str", []interface{}{"to_number", []interface{}{"json", `"42"`}}, `42`),
	naryProjectCase("to_num-bool", []interface{}{"to_number", []interface{}{"json", `true`}}, `1`),
	naryProjectCase("to_num-junk", []interface{}{"to_number", []interface{}{"json", `"abc"`}}, `null`),
}

// naryProjectCase builds a TestCaseSimple that projects a single native
// expression param tree over one scanned row, expecting one result row holding
// wantJSON. Used for the native-op compiled-path coverage block above.
func naryProjectCase(about string, expr []interface{}, wantJSON string) TestCaseSimple {
	return TestCaseSimple{
		about: "compiled-path native op: " + about,
		o: base.Op{
			Kind:   "project",
			Labels: base.Labels{"r"},
			Params: []interface{}{expr},
			Children: []*base.Op{{
				Kind:   "scan",
				Labels: base.Labels{"a"},
				Params: []interface{}{"csvData", "\n1\n"},
			}},
		},
		expectYields: []base.Vals{{[]byte(wantJSON)}},
	}
}

// -------------------------------------------------------------------
// Data-driven local SQL++ feature cases (LET / WITH / LETTING / subqueries),
// the SQL++-string sibling of TestCasesSimple above. Each runs a real query
// against the test/data: file store. Kept here (a non-_test file) so both the
// interpreter test (TestQueryCases, cases_test.go) and the compiler
// differential (TestQueryCasesWithCompiler, query_compiler_test.go) share them.
// As more features land, append cases here. (These are NOT the suite corpus,
// which is seeded from couchbase/query; see suite_test.go.)
//
// The data: store has 4 "orders" docs: id 1200/abc, 1234/bbb, 1235/ccc,
// 1236/ccc, each with an orderlines array of {qty, productId}.

// queryCase is one case: run stmt, optionally check the row count (rows >= 0),
// and optionally run finer assertions via check. base.Vals is a row's per-label
// raw values; a single-label JSON-object row can be decoded with rowObj.
type queryCase struct {
	name  string
	stmt  string
	rows  int // expected result row count, or -1 to skip the count check
	check func(t *testing.T, rows []base.Vals)
}

var queryCases = []queryCase{
	// ---- LET ----------------------------------------------------------
	{
		name: "LetConst", // a constant binding, projected by name
		stmt: `SELECT foo FROM data:orders AS a LET foo = 1`,
		rows: 4,
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if len(row) != 1 || string(row[0]) != "1" {
					t.Fatalf("expected foo==1, got %+v", row)
				}
			}
		},
	},
	{
		name: "LetField", // binding derived from a doc field
		stmt: `SELECT a.id, c FROM data:orders AS a LET c = a.custId`,
		rows: 4,
		check: func(t *testing.T, rows []base.Vals) {
			want := map[string]string{"1200": "abc", "1234": "bbb", "1235": "ccc", "1236": "ccc"}
			for _, row := range rows {
				if len(row) != 2 {
					t.Fatalf("expected 2 labels, got %+v", row)
				}
				id, c := trimQ(string(row[0])), trimQ(string(row[1]))
				if want[id] != c {
					t.Fatalf("id %s: expected c=%s, got %s", id, want[id], c)
				}
			}
		},
	},
	{
		name: "LetInWhere", // binding referenced by WHERE
		stmt: `SELECT a.id FROM data:orders AS a LET c = a.custId WHERE c = "ccc"`,
		rows: 2,
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if id := trimQ(string(row[0])); id != "1235" && id != "1236" {
					t.Fatalf("unexpected id %s", id)
				}
			}
		},
	},
	{
		name: "LetInOrderBy", // binding referenced by ORDER BY
		stmt: `SELECT a.id FROM data:orders AS a LET c = a.custId ORDER BY c DESC, a.id`,
		rows: 4,
		check: func(t *testing.T, rows []base.Vals) {
			// custId DESC: ccc (1235, 1236), bbb (1234), abc (1200).
			want := []string{"1235", "1236", "1234", "1200"}
			for i, row := range rows {
				if got := trimQ(string(row[0])); got != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, got, want[i])
				}
			}
		},
	},
	{
		name: "LetMultiple", // a later binding references an earlier one
		stmt: `SELECT x, y FROM data:orders AS a LET x = 10, y = x + 5 WHERE a.id = "1200"`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "10" || string(rows[0][1]) != "15" {
				t.Fatalf("expected x=10,y=15, got %+v", rows[0])
			}
		},
	},
	{
		name: "LetStarNoLeak", // SELECT * must not spread LET vars
		stmt: `SELECT * FROM data:orders AS a LET foo = 999 WHERE a.id = "1200"`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			m := rowObj(t, rows[0])
			if _, ok := m["foo"]; ok {
				t.Fatalf("LET var foo leaked into SELECT *: %+v", m)
			}
			if len(m) != 1 {
				t.Fatalf("expected only the 'a' key, got %+v", m)
			}
			if a, ok := m["a"].(map[string]interface{}); !ok || a["id"] != "1200" {
				t.Fatalf("expected a.id==1200, got %+v", m)
			}
		},
	},

	// ---- WITH (non-recursive CTE) -------------------------------------
	{
		name: "WithUnreferenced", // an unreferenced CTE must not change results
		stmt: `WITH w AS ({"k": 1}) SELECT a.id FROM data:orders AS a WHERE a.id = "1234"`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if id := trimQ(string(rows[0][0])); id != "1234" {
				t.Fatalf("expected id 1234, got %s", id)
			}
		},
	},
	{
		name: "WithStarNoLeak", // SELECT * must not spread a WITH binding name
		stmt: `WITH w AS (123) SELECT * FROM data:orders AS a WHERE a.id = "1200"`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			m := rowObj(t, rows[0])
			if _, ok := m["w"]; ok {
				t.Fatalf("WITH var w leaked into SELECT *: %+v", m)
			}
			if len(m) != 1 {
				t.Fatalf("expected only the 'a' key, got %+v", m)
			}
		},
	},

	// ---- LETTING (LET in the GROUP BY scope, over aggregates) ---------
	{
		name: "LettingSum", // a post-group aggregate binding, projected by name
		stmt: `SELECT a.custId, total FROM data:orders AS a UNNEST a.orderlines AS ol ` +
			`GROUP BY a.custId LETTING total = SUM(ol.qty)`,
		rows: 3,
		check: func(t *testing.T, rows []base.Vals) {
			// abc: 1+1=2; bbb: 2+1=3; ccc: (1+1)+(1+1)=4.
			want := map[string]string{"abc": "2", "bbb": "3", "ccc": "4"}
			for _, row := range rows {
				if len(row) != 2 {
					t.Fatalf("expected 2 labels, got %+v", row)
				}
				cust := trimQ(string(row[0]))
				if want[cust] != string(row[1]) {
					t.Fatalf("custId %s: expected total=%s, got %s", cust, want[cust], row[1])
				}
			}
		},
	},
	{
		name: "LettingHaving", // a LETTING var referenced by HAVING
		stmt: `SELECT a.custId, total FROM data:orders AS a UNNEST a.orderlines AS ol ` +
			`GROUP BY a.custId LETTING total = SUM(ol.qty) HAVING total > 2`,
		rows: 2,
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if cust := trimQ(string(row[0])); cust != "bbb" && cust != "ccc" {
					t.Fatalf("unexpected custId %s (total should be >2)", cust)
				}
			}
		},
	},

	// ---- Subqueries (uncorrelated) ------------------------------------
	{
		name: "SubqueryConstIn", // constant membership in a subquery result
		stmt: `SELECT 5 IN (SELECT RAW 5) AS hit`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "true" {
				t.Fatalf("expected hit=true, got %s", rows[0][0])
			}
		},
	},
	{
		name: "SubqueryArrayLength", // ARRAY_LENGTH over a subquery's rows
		stmt: `SELECT ARRAY_LENGTH((SELECT RAW o.id FROM data:orders o)) AS n`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "4" { // 4 orders
				t.Fatalf("expected n=4, got %s", rows[0][0])
			}
		},
	},
	{
		name: "SubqueryInProjection", // a subquery's array as a projected column
		stmt: `SELECT (SELECT RAW o.id FROM data:orders o WHERE o.custId = "abc") AS ids`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != `["1200"]` { // only order 1200 has custId abc
				t.Fatalf("expected ids=[\"1200\"], got %s", rows[0][0])
			}
		},
	},
	{
		name: "SubqueryWhereIn", // WHERE ... IN (uncorrelated subquery)
		stmt: `SELECT o.id FROM data:orders o ` +
			`WHERE o.custId IN (SELECT RAW o2.custId FROM data:orders o2 WHERE o2.id = "1235")`,
		rows: 2, // subquery -> ["ccc"]; orders with custId ccc = 1235, 1236
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if id := trimQ(string(row[0])); id != "1235" && id != "1236" {
					t.Fatalf("unexpected id %s", id)
				}
			}
		},
	},

	// ---- Subqueries (correlated: sub-op references an outer field) --------
	{
		name: "SubqueryCorrelatedShared",
		// orders whose custId is shared by >1 order. custIds: abc,bbb,ccc,ccc ->
		// only the two ccc orders (1235, 1236) qualify.
		stmt: `SELECT o.id FROM data:orders o WHERE ` +
			`ARRAY_LENGTH((SELECT RAW o2.id FROM data:orders o2 WHERE o2.custId = o.custId)) > 1`,
		rows: 2,
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if id := trimQ(string(row[0])); id != "1235" && id != "1236" {
					t.Fatalf("unexpected id %s", id)
				}
			}
		},
	},
	{
		name: "SubqueryCorrelatedSelf",
		// every order matches itself -> all 4 orders.
		stmt: `SELECT o.id FROM data:orders o WHERE ` +
			`1 IN (SELECT RAW 1 FROM data:orders o2 WHERE o2.id = o.id)`,
		rows: 4,
	},

	// ---- WITH CTE used as a FROM data source -------------------------
	{
		name: "WithCteFromConstArray", // WITH r AS (<array>) ... FROM r
		stmt: `WITH r AS ([{"n":1},{"n":2}]) SELECT x.n FROM r AS x ORDER BY x.n`,
		rows: 2,
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"1", "2"} // ordered by x.n
			for i, row := range rows {
				if string(row[0]) != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, row[0], want[i])
				}
			}
		},
	},
	{
		name: "WithCteFromSubquery", // WITH r AS (SELECT ...) ... FROM r (binding runs on the engine)
		stmt: `WITH r AS (SELECT o.id, o.custId FROM data:orders o) ` +
			`SELECT x.id FROM r AS x WHERE x.custId = "ccc" ORDER BY x.id`,
		rows: 2, // orders with custId ccc: 1235, 1236
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"1235", "1236"}
			for i, row := range rows {
				if got := trimQ(string(row[0])); got != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, got, want[i])
				}
			}
		},
	},

	// ---- WITH RECURSIVE (the fixpoint: anchor + repeated step) --------
	{
		name: "RecursiveCount", // 1..5 via UNION recursion
		stmt: `WITH RECURSIVE r AS (SELECT 1 AS n ` +
			`UNION SELECT r.n + 1 AS n FROM r WHERE r.n < 5) ` +
			`SELECT x.n FROM r AS x ORDER BY x.n`,
		rows: 5,
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"1", "2", "3", "4", "5"}
			for i, row := range rows {
				if string(row[0]) != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, row[0], want[i])
				}
			}
		},
	},
	{
		name: "RecursiveSum", // downstream aggregate over the recursive result: 1+2+3+4+5
		stmt: `WITH RECURSIVE r AS (SELECT 1 AS n ` +
			`UNION SELECT r.n + 1 AS n FROM r WHERE r.n < 5) ` +
			`SELECT SUM(x.n) AS total FROM r AS x`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "15" {
				t.Fatalf("expected total=15, got %s", rows[0][0])
			}
		},
	},

	// ---- more subqueries (stretch) ------------------------------------
	{
		name: "SubqueryCorrelatedInProjection",
		// a correlated subquery as a projected array: each order's peers (orders
		// sharing its custId). abc/bbb -> 1 peer; ccc -> 2 peers.
		stmt: `SELECT o.id, (SELECT RAW o2.id FROM data:orders o2 WHERE o2.custId = o.custId) AS peers ` +
			`FROM data:orders o ORDER BY o.id`,
		rows: 4,
		check: func(t *testing.T, rows []base.Vals) {
			wantIDs := []string{"1200", "1234", "1235", "1236"}
			wantLen := []int{1, 1, 2, 2}
			for i, row := range rows {
				if id := trimQ(string(row[0])); id != wantIDs[i] {
					t.Fatalf("row %d id: got %s, want %s", i, id, wantIDs[i])
				}
				var peers []interface{}
				if err := json.Unmarshal(row[1], &peers); err != nil {
					t.Fatalf("row %d peers unmarshal: %v", i, err)
				}
				if len(peers) != wantLen[i] {
					t.Fatalf("row %d peers: got %d, want %d (%s)", i, len(peers), wantLen[i], row[1])
				}
			}
		},
	},
	{
		name: "SubqueryNestedIn",
		// a subquery inside a subquery inside a WHERE IN.
		stmt: `SELECT o.id FROM data:orders o WHERE o.custId IN ` +
			`(SELECT RAW o2.custId FROM data:orders o2 WHERE o2.custId IN ` +
			`(SELECT RAW o3.custId FROM data:orders o3 WHERE o3.id = "1235")) ORDER BY o.id`,
		rows: 2, // -> custId ccc -> orders 1235, 1236
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if id := trimQ(string(row[0])); id != "1235" && id != "1236" {
					t.Fatalf("unexpected id %s", id)
				}
			}
		},
	},

	// ---- direct FROM-(subquery) AS x (plan.Alias) ---------------------
	{
		name: "FromSubqueryWhere",
		stmt: `SELECT x.id FROM (SELECT o.id, o.custId FROM data:orders o WHERE o.custId = "ccc") AS x ` +
			`ORDER BY x.id`,
		rows: 2,
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"1235", "1236"}
			for i, row := range rows {
				if got := trimQ(string(row[0])); got != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, got, want[i])
				}
			}
		},
	},
	{
		name: "FromSubqueryGroupBy",
		stmt: `SELECT x.custId, COUNT(*) AS c FROM (SELECT o.custId FROM data:orders o) AS x ` +
			`GROUP BY x.custId ORDER BY x.custId`,
		rows: 3,
		check: func(t *testing.T, rows []base.Vals) {
			want := map[string]string{"abc": "1", "bbb": "1", "ccc": "2"}
			for _, row := range rows {
				cust := trimQ(string(row[0]))
				if want[cust] != string(row[1]) {
					t.Fatalf("custId %s: got c=%s, want %s", cust, row[1], want[cust])
				}
			}
		},
	},

	// ---- more WITH RECURSIVE (stretch) --------------------------------
	{
		name: "RecursivePowers", // v *= 2 while v < 16 -> 1,2,4,8,16
		stmt: `WITH RECURSIVE p AS (SELECT 1 AS v ` +
			`UNION SELECT p.v * 2 AS v FROM p WHERE p.v < 16) ` +
			`SELECT x.v FROM p AS x ORDER BY x.v`,
		rows: 5,
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"1", "2", "4", "8", "16"}
			for i, row := range rows {
				if string(row[0]) != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, row[0], want[i])
				}
			}
		},
	},
	{
		name: "RecursiveFibonacci", // two-column recursion; a = 0,1,1,2,3,5,8,13
		stmt: `WITH RECURSIVE fib AS (SELECT 0 AS a, 1 AS b ` +
			`UNION SELECT fib.b AS a, fib.a + fib.b AS b FROM fib WHERE fib.b < 20) ` +
			`SELECT x.a FROM fib AS x ORDER BY x.a`,
		rows: 8,
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"0", "1", "1", "2", "3", "5", "8", "13"}
			for i, row := range rows {
				if string(row[0]) != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, row[0], want[i])
				}
			}
		},
	},
	{
		name: "RecursiveOuterWhere", // 1..10, keep evens downstream -> 2,4,6,8,10
		stmt: `WITH RECURSIVE r AS (SELECT 1 AS n ` +
			`UNION SELECT r.n + 1 AS n FROM r WHERE r.n < 10) ` +
			`SELECT x.n FROM r AS x WHERE x.n % 2 = 0 ORDER BY x.n`,
		rows: 5,
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"2", "4", "6", "8", "10"}
			for i, row := range rows {
				if string(row[0]) != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, row[0], want[i])
				}
			}
		},
	},
	{
		name: "RecursiveOptionsLevels", // OPTIONS caps recursion depth: anchor + 2 steps
		stmt: `WITH RECURSIVE r AS (SELECT 1 AS n ` +
			`UNION SELECT r.n + 1 AS n FROM r WHERE r.n < 100) OPTIONS {"levels": 2} ` +
			`SELECT x.n FROM r AS x ORDER BY x.n`,
		rows: 3,
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"1", "2", "3"}
			for i, row := range rows {
				if string(row[0]) != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, row[0], want[i])
				}
			}
		},
	},
	{
		name: "RecursiveCycleUnionAll",
		// UNION ALL (no dedup) with a cyclic step (1->2->3->1...); the CYCLE clause
		// detects the repeat on n and stops -> 1,2,3.
		stmt: `WITH RECURSIVE r AS (SELECT 1 AS n ` +
			`UNION ALL SELECT CASE WHEN r.n >= 3 THEN 1 ELSE r.n + 1 END AS n FROM r) ` +
			`CYCLE n RESTRICT SELECT x.n FROM r AS x ORDER BY x.n`,
		rows: 3,
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"1", "2", "3"}
			for i, row := range rows {
				if string(row[0]) != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, row[0], want[i])
				}
			}
		},
	},
	{
		name: "RecursiveMax", // downstream MAX over 1..10
		stmt: `WITH RECURSIVE r AS (SELECT 1 AS n ` +
			`UNION SELECT r.n + 1 AS n FROM r WHERE r.n < 10) ` +
			`SELECT MAX(x.n) AS m FROM r AS x`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "10" {
				t.Fatalf("expected m=10, got %s", rows[0][0])
			}
		},
	},

	// ---- CTE referencing another CTE (chained WITH) ------------------
	{
		name: "CteRefCte", // b's binding scans CTE a
		stmt: `WITH a AS (SELECT o.custId FROM data:orders o), ` +
			`b AS (SELECT DISTINCT x.custId FROM a AS x) ` +
			`SELECT y.custId FROM b AS y ORDER BY y.custId`,
		rows: 3, // distinct custIds
		check: func(t *testing.T, rows []base.Vals) {
			want := []string{"abc", "bbb", "ccc"}
			for i, row := range rows {
				if got := trimQ(string(row[0])); got != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, got, want[i])
				}
			}
		},
	},
	{
		name: "CteChain3", // a -> b (a filtered) -> c (b filtered)
		stmt: `WITH a AS ([{"n":1},{"n":2},{"n":3}]), ` +
			`b AS (SELECT x.n FROM a AS x WHERE x.n > 1), ` +
			`c AS (SELECT y.n FROM b AS y WHERE y.n < 3) ` +
			`SELECT z.n FROM c AS z`,
		rows: 1, // {n:2}
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "2" {
				t.Fatalf("expected n=2, got %s", rows[0][0])
			}
		},
	},
	// --- records decoders through BOTH interpreter and compiler. These use
	// test/data keyspaces backed by JSONL / CSV / gzip (not one-doc-per-file) so
	// TestQueryCasesWithCompiler proves datastore-scan-records + the decoders bake
	// and run in the compiled path, not just the interpreter.
	{
		name: "RecScanJSONL", // multi-file JSONL union
		stmt: `SELECT e.act AS act FROM data:events AS e`,
		rows: 5,
	},
	{
		name: "RecScanCountJSONL", // COUNT(*) over JSONL (records, not files)
		stmt: `SELECT COUNT(*) AS n FROM data:events`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "5" {
				t.Fatalf("expected n=5 records, got %s", rows[0][0])
			}
		},
	},
	{
		name: "RecScanCSV", // CSV rows -> JSON objects, filter + aggregate
		stmt: `SELECT SUM(t.amount) AS tot FROM data:txns AS t WHERE t.currency = "USD"`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "30.5" {
				t.Fatalf("expected USD tot=30.5, got %s", rows[0][0])
			}
		},
	},
	{
		name: "RecScanGzip", // transparent gzip
		stmt: `SELECT COUNT(*) AS n FROM data:gzipped`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "3" {
				t.Fatalf("expected n=3, got %s", rows[0][0])
			}
		},
	},
	{
		name: "RecScanExtract", // PDF/DOCX/XLSX text extraction + full-text-ish filter
		stmt: `SELECT d.filename AS filename FROM data:docs AS d ` +
			`WHERE d.text LIKE "%vacation%"`,
		rows: 2, // handbook.pdf + q1-report.docx mention "vacation"
	},
}

// rowObj unmarshals a single-label result row (a JSON object) into a map.
func rowObj(t *testing.T, row base.Vals) map[string]interface{} {
	t.Helper()
	if len(row) != 1 {
		t.Fatalf("expected row with 1 label, got %d: %+v", len(row), row)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(row[0], &m); err != nil {
		t.Fatalf("unmarshal %s: %v", row[0], err)
	}
	return m
}

// trimQ strips one leading and trailing double-quote from a JSON string token.
func trimQ(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
