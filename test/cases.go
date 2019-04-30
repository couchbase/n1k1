package test

import (
	"testing"

	"github.com/couchbase/n1k1/base"
)

func MakeYieldCaptureFuncs(t *testing.T, testi int, expectErr string) (
	base.YieldVals, base.YieldStats, base.YieldErr, func() []base.Vals) {
	var yields []base.Vals

	yieldVals := func(lzVals base.Vals) {
		var lzValsCopy base.Vals
		for _, v := range lzVals {
			lzValsCopy = append(lzValsCopy,
				append(base.Val(nil), v...))
		}

		yields = append(yields, lzValsCopy)
	}

	yieldStats := func(stats *base.Stats) error {
		return nil
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

	return yieldVals, yieldStats, yieldErr, returnYields
}

func StringsToLzVals(a []string, lzValsPre base.Vals) base.Vals {
	lzVals := lzValsPre
	for _, v := range a {
		if v != "" {
			lzVals = append(lzVals, base.Val([]byte(v)))
		} else {
			lzVals = append(lzVals, base.ValMissing)
		}
	}
	return lzVals
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
			Fields: base.Fields(nil),
			Params: []interface{}{
				"csvData",
				"",
			},
		},
	},
	{
		about: "test empty csv-data scan with some fields",
		o: base.Op{
			Kind:   "scan",
			Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->filter on const == const",
		o: base.Op{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"July"`},
				[]interface{}{"json", `"July"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"July"`},
				[]interface{}{"json", `"June"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->filter with fieldB == fieldB",
		o: base.Op{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", "b"},
				[]interface{}{"identifier", "b"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->filter with fieldA == fieldB",
		o: base.Op{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", "a"},
				[]interface{}{"identifier", "b"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->filter with fieldB = 66",
		o: base.Op{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", "b"},
				[]interface{}{"json", `66`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->filter with fieldB == 21",
		o: base.Op{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", "b"},
				[]interface{}{"json", `21`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->filter more than 1 match",
		o: base.Op{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", "c"},
				[]interface{}{"json", `3000`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "c"},
			Params: []interface{}{
				[]interface{}{"identifier", "a"},
				[]interface{}{"identifier", "c"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "filter",
				Fields: base.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"identifier", "c"},
					[]interface{}{"json", `3000`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "c"},
			Params: []interface{}{
				[]interface{}{"identifier", "a"},
				[]interface{}{"identifier", "c"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->project deeper identifier",
		o: base.Op{
			Kind:   "project",
			Fields: base.Fields{"city"},
			Params: []interface{}{
				[]interface{}{"identifier", "a", "addr", "city"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a"},
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
			base.Vals{[]byte("sf")},
			base.Vals{[]byte("sj")},
		},
	}, {
		about: "test csv-data scan->project deeper identifier",
		o: base.Op{
			Kind:   "project",
			Fields: base.Fields{"city"},
			Params: []interface{}{
				[]interface{}{"identifier", "a", "addr"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a"},
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
			Fields: base.Fields{},
			Params: []interface{}{},
			Children: []*base.Op{&base.Op{
				Kind:   "filter",
				Fields: base.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"identifier", "c"},
					[]interface{}{"json", `3000`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->filter->project unknown field",
		o: base.Op{
			Kind:   "project",
			Fields: base.Fields{"a", "xxx"},
			Params: []interface{}{
				[]interface{}{"identifier", "a"},
				[]interface{}{"identifier", "xxx"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "filter",
				Fields: base.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"identifier", "c"},
					[]interface{}{"json", `3000`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->join-nl-inner",
		o: base.Op{
			Kind:   "join-nl-inner",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", "dept"},
				[]interface{}{"identifier", "empDept"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
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
				Fields: base.Fields{"emp", "empDept"},
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
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test csv-data scan->join-nl-inner but false join condition",
		o: base.Op{
			Kind:   "join-nl-inner",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", "dept"},
				[]interface{}{"json", `"NOT-MATCHING"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"emp", "empDept"},
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
			Kind:   "join-nl-inner",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{"json", `true`},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"emp", "empDept"},
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
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"frank"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"fred"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"dan"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"doug"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test inner join via always-matching join condition",
		o: base.Op{
			Kind:   "join-nl-inner",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"Hello"`},
				[]interface{}{"json", `"Hello"`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"emp", "empDept"},
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
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"frank"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"fred"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"dan"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"doug"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test left outer join on dept",
		o: base.Op{
			Kind:   "join-nl-outerLeft",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", `dept`},
				[]interface{}{"identifier", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
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
				Fields: base.Fields{"emp", "empDept"},
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
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),

			StringsToLzVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, ``, ``}, nil),

			StringsToLzVals([]string{`"sales"`, `"san diego"`, ``, ``}, nil),
			StringsToLzVals([]string{`"sales"`, `"san diego"`, ``, ``}, nil),
			StringsToLzVals([]string{`"sales"`, `"san diego"`, ``, ``}, nil),
			StringsToLzVals([]string{`"sales"`, `"san diego"`, ``, ``}, nil),
			StringsToLzVals([]string{`"sales"`, `"san diego"`, ``, ``}, nil),
		},
	},
	{
		about: "test left outer join on dept with empty RHS",
		o: base.Op{
			Kind:   "join-nl-outerLeft",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", `dept`},
				[]interface{}{"identifier", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}},
		},
		expectYields: []base.Vals{
			StringsToLzVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
		},
	},
	{
		about: "test inner join on dept with empty LHS",
		o: base.Op{
			Kind:   "join-nl-inner",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", `dept`},
				[]interface{}{"identifier", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"emp", "empDept"},
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
			Kind:   "join-nl-outerLeft",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", `dept`},
				[]interface{}{"identifier", `empDept`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"emp", "empDept"},
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
			Kind:   "join-nl-outerLeft",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"identifier", `dept`},
				[]interface{}{"identifier", `someFakeField`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			}, &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"emp", "empDept"},
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
			StringsToLzVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
		},
	},
	{
		about: "test csv-data scan->filter on false OR true",
		o: base.Op{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{"json", `false`},
				[]interface{}{"json", `true`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{"json", `true`},
				[]interface{}{"json", `false`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{"json", `false`},
				[]interface{}{"json", `false`},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{
					"eq",
					[]interface{}{"identifier", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"eq",
					[]interface{}{"identifier", `c`},
					[]interface{}{"json", `31`},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"and",
				[]interface{}{
					"eq",
					[]interface{}{"identifier", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"eq",
					[]interface{}{"identifier", `c`},
					[]interface{}{"json", `30`},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"and",
				[]interface{}{
					"eq",
					[]interface{}{"identifier", `a`},
					[]interface{}{"json", `11`},
				},
				[]interface{}{
					"eq",
					[]interface{}{"identifier", `c`},
					[]interface{}{"json", `31`},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"and",
				[]interface{}{
					"eq",
					[]interface{}{"identifier", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"and",
					[]interface{}{
						"eq",
						[]interface{}{"identifier", `c`},
						[]interface{}{"json", `30`},
					},
					[]interface{}{
						"eq",
						[]interface{}{"identifier", `b`},
						[]interface{}{"json", `20`},
					},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"or",
				[]interface{}{
					"eq",
					[]interface{}{"identifier", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"and",
					[]interface{}{
						"eq",
						[]interface{}{"identifier", `c`},
						[]interface{}{"json", `31`},
					},
					[]interface{}{
						"eq",
						[]interface{}{"identifier", `b`},
						[]interface{}{"json", `21`},
					},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"and",
				[]interface{}{
					"eq",
					[]interface{}{"identifier", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"or",
					[]interface{}{
						"eq",
						[]interface{}{"identifier", `c`},
						[]interface{}{"json", `4444`},
					},
					[]interface{}{
						"eq",
						[]interface{}{"identifier", `b`},
						[]interface{}{"json", `20`},
					},
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b", "c"},
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
		about: "test csv-data scan->join-nl-inner->project",
		o: base.Op{
			Kind:   "project",
			Fields: base.Fields{"city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{"identifier", "city"},
				[]interface{}{"identifier", "emp"},
				[]interface{}{"identifier", "empDept"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "join-nl-inner",
				Fields: base.Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"identifier", "dept"},
					[]interface{}{"identifier", "empDept"},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Fields: base.Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Fields: base.Fields{"emp", "empDept"},
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
			StringsToLzVals([]string{`"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"london"`, `"fred"`, `"finance"`}, nil),
		},
	},

	{
		about: "test csv-data scan->join-nl-inner->filter->project",
		o: base.Op{
			Kind:   "project",
			Fields: base.Fields{"city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{"identifier", "city"},
				[]interface{}{"identifier", "emp"},
				[]interface{}{"identifier", "empDept"},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "filter",
				Fields: base.Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"json", `"london"`},
					[]interface{}{"identifier", `city`},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "join-nl-inner",
					Fields: base.Fields{"dept", "city", "emp", "empDept"},
					Params: []interface{}{
						"eq",
						[]interface{}{"identifier", "dept"},
						[]interface{}{"identifier", "empDept"},
					},
					Children: []*base.Op{&base.Op{
						Kind:   "scan",
						Fields: base.Fields{"dept", "city"},
						Params: []interface{}{
							"csvData",
							`
"dev","paris"
"finance","london"
`,
						},
					}, &base.Op{
						Kind:   "scan",
						Fields: base.Fields{"emp", "empDept"},
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
			StringsToLzVals([]string{`"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test csv-data scan->order-by",
		o: base.Op{
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "b"},
				},
				[]interface{}{
					"desc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
		about: "test csv-data scan->order-by two-field",
		o: base.Op{
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
					[]interface{}{"identifier", "b"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
		about: "test csv-data scan->order-by two-field, DESC, ASC",
		o: base.Op{
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
					[]interface{}{"identifier", "b"},
				},
				[]interface{}{
					"desc",
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
		about: "test csv-data scan->order-by two-field, ASC, DESC",
		o: base.Op{
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
					[]interface{}{"identifier", "b"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
		about: "test csv-data scan->order-by two-field, ASC, DESC, str+int",
		o: base.Op{
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
					[]interface{}{"identifier", "b"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
		about: "test csv-data scan->order-by two-field, ASC, DESC, bool+int",
		o: base.Op{
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
					[]interface{}{"identifier", "b"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
				},
				[]interface{}{
					"asc",
				},
				0,
				1,
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
				},
				[]interface{}{
					"asc",
				},
				0,
				100,
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
				},
				[]interface{}{
					"asc",
				},
				100,
				100,
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
				},
				[]interface{}{
					"asc",
				},
				1,
				0,
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "a"},
				},
				[]interface{}{
					"asc",
				},
				1,
				1,
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b"},
			Params: []interface{}{
				[]interface{}{},
				[]interface{}{},
				1,
				1,
			},
			Children: []*base.Op{&base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
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
		about: "test csv-data scan->join-nl-inner->order-by",
		o: base.Op{
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "dept"},
					[]interface{}{"identifier", "emp"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
				0,
				10,
			},
			Children: []*base.Op{&base.Op{
				Kind:   "join-nl-inner",
				Fields: base.Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"identifier", "dept"},
					[]interface{}{"identifier", "empDept"},
				},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Fields: base.Fields{"dept", "city"},
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
					Fields: base.Fields{"emp", "empDept"},
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
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
			StringsToLzVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
		},
	},
	{
		about: "test csv-data scan->union-all->order-by",
		o: base.Op{
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "b"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "union-all",
				Fields: base.Fields{"a", "b", "c"},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Fields: base.Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				}, &base.Op{
					Kind:   "scan",
					Fields: base.Fields{"b"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "b"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "union-all",
				Fields: base.Fields{"a", "b", "c"},
				Children: []*base.Op{&base.Op{
					Kind:   "scan",
					Fields: base.Fields{"a", "b", "c"},
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
			Kind:   "order-by-offset-limit",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				[]interface{}{
					[]interface{}{"identifier", "b"},
				},
				[]interface{}{
					"asc",
				},
			},
			Children: []*base.Op{&base.Op{
				Kind:   "union-all",
				Fields: base.Fields{"a", "b", "c"},
				Children: []*base.Op{&base.Op{
					Kind:   "project",
					Fields: base.Fields{"b", "c"},
					Params: []interface{}{
						[]interface{}{"identifier", "b"},
						[]interface{}{"identifier", "c"},
					},
					Children: []*base.Op{&base.Op{
						Kind:   "filter",
						Fields: base.Fields{"a", "b", "c"},
						Params: []interface{}{
							"eq",
							[]interface{}{"identifier", "c"},
							[]interface{}{"json", `3000`},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Fields: base.Fields{"a", "b", "c"},
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
					Fields: base.Fields{"b", "a"},
					Params: []interface{}{
						[]interface{}{"identifier", "b"},
						[]interface{}{"identifier", "a"},
					},
					Children: []*base.Op{&base.Op{
						Kind:   "filter",
						Fields: base.Fields{"a", "b", "c"},
						Params: []interface{}{
							"eq",
							[]interface{}{"identifier", "a"},
							[]interface{}{"json", `10`},
						},
						Children: []*base.Op{&base.Op{
							Kind:   "scan",
							Fields: base.Fields{"a", "b", "c"},
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
}
