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
			ParentA: &base.Op{
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
			ParentA: &base.Op{
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
				[]interface{}{"field", "b"},
				[]interface{}{"field", "b"},
			},
			ParentA: &base.Op{
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
				[]interface{}{"field", "a"},
				[]interface{}{"field", "b"},
			},
			ParentA: &base.Op{
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
				[]interface{}{"field", "b"},
				[]interface{}{"json", `66`},
			},
			ParentA: &base.Op{
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
				[]interface{}{"field", "b"},
				[]interface{}{"json", `21`},
			},
			ParentA: &base.Op{
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
				[]interface{}{"field", "c"},
				[]interface{}{"json", `3000`},
			},
			ParentA: &base.Op{
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
			},
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
				[]interface{}{"field", "a"},
				[]interface{}{"field", "c"},
			},
			ParentA: &base.Op{
				Kind:   "filter",
				Fields: base.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "c"},
					[]interface{}{"json", `3000`},
				},
				ParentA: &base.Op{
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
				},
			},
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
				[]interface{}{"field", "a"},
				[]interface{}{"field", "c"},
			},
			ParentA: &base.Op{
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
			},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("00"), []byte("0000")},
			base.Vals{[]byte("10"), []byte("3000")},
			base.Vals{[]byte("11"), []byte("3000")},
			base.Vals{[]byte("12"), []byte("1000")},
		},
	},
	{
		about: "test csv-data scan->filter->project nothing",
		o: base.Op{
			Kind:   "project",
			Fields: base.Fields{},
			Params: []interface{}{},
			ParentA: &base.Op{
				Kind:   "filter",
				Fields: base.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "c"},
					[]interface{}{"json", `3000`},
				},
				ParentA: &base.Op{
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
				},
			},
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
				[]interface{}{"field", "a"},
				[]interface{}{"field", "xxx"},
			},
			ParentA: &base.Op{
				Kind:   "filter",
				Fields: base.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "c"},
					[]interface{}{"json", `3000`},
				},
				ParentA: &base.Op{
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
				},
			},
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
				[]interface{}{"field", "dept"},
				[]interface{}{"field", "empDept"},
			},
			ParentA: &base.Op{
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
			},
			ParentB: &base.Op{
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
			},
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
				[]interface{}{"field", "dept"},
				[]interface{}{"json", `"NOT-MATCHING"`},
			},
			ParentA: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			},
			ParentB: &base.Op{
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
			},
		},
		expectYields: []base.Vals(nil),
	},
	{
		about: "test inner join via always-true join condition",
		o: base.Op{
			Kind:   "join-nl-inner",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{"json", `true`},
			ParentA: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			},
			ParentB: &base.Op{
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
			},
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
			ParentA: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			},
			ParentB: &base.Op{
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
			},
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
				[]interface{}{"field", `dept`},
				[]interface{}{"field", `empDept`},
			},
			ParentA: &base.Op{
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
			},
			ParentB: &base.Op{
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
			},
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
				[]interface{}{"field", `dept`},
				[]interface{}{"field", `empDept`},
			},
			ParentA: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			},
			ParentB: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"emp", "empDept"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			},
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
				[]interface{}{"field", `dept`},
				[]interface{}{"field", `empDept`},
			},
			ParentA: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			},
			ParentB: &base.Op{
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
			},
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
				[]interface{}{"field", `dept`},
				[]interface{}{"field", `empDept`},
			},
			ParentA: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
`,
				},
			},
			ParentB: &base.Op{
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
			},
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
				[]interface{}{"field", `dept`},
				[]interface{}{"field", `someFakeField`},
			},
			ParentA: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"dept", "city"},
				Params: []interface{}{
					"csvData",
					`
"dev","paris"
"finance","london"
`,
				},
			},
			ParentB: &base.Op{
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
			},
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
			ParentA: &base.Op{
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
			ParentA: &base.Op{
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
			ParentA: &base.Op{
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
					[]interface{}{"field", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"eq",
					[]interface{}{"field", `c`},
					[]interface{}{"json", `31`},
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"eq",
					[]interface{}{"field", `c`},
					[]interface{}{"json", `30`},
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", `a`},
					[]interface{}{"json", `11`},
				},
				[]interface{}{
					"eq",
					[]interface{}{"field", `c`},
					[]interface{}{"json", `31`},
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"and",
					[]interface{}{
						"eq",
						[]interface{}{"field", `c`},
						[]interface{}{"json", `30`},
					},
					[]interface{}{
						"eq",
						[]interface{}{"field", `b`},
						[]interface{}{"json", `20`},
					},
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"and",
					[]interface{}{
						"eq",
						[]interface{}{"field", `c`},
						[]interface{}{"json", `31`},
					},
					[]interface{}{
						"eq",
						[]interface{}{"field", `b`},
						[]interface{}{"json", `21`},
					},
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", `a`},
					[]interface{}{"json", `10`},
				},
				[]interface{}{
					"or",
					[]interface{}{
						"eq",
						[]interface{}{"field", `c`},
						[]interface{}{"json", `4444`},
					},
					[]interface{}{
						"eq",
						[]interface{}{"field", `b`},
						[]interface{}{"json", `20`},
					},
				},
			},
			ParentA: &base.Op{
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
			},
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
				[]interface{}{"field", "city"},
				[]interface{}{"field", "emp"},
				[]interface{}{"field", "empDept"},
			},
			ParentA: &base.Op{
				Kind:   "join-nl-inner",
				Fields: base.Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "dept"},
					[]interface{}{"field", "empDept"},
				},
				ParentA: &base.Op{
					Kind:   "scan",
					Fields: base.Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				},
				ParentB: &base.Op{
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
				},
			},
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
				[]interface{}{"field", "city"},
				[]interface{}{"field", "emp"},
				[]interface{}{"field", "empDept"},
			},
			ParentA: &base.Op{
				Kind:   "filter",
				Fields: base.Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"json", `"london"`},
					[]interface{}{"field", `city`},
				},
				ParentA: &base.Op{
					Kind:   "join-nl-inner",
					Fields: base.Fields{"dept", "city", "emp", "empDept"},
					Params: []interface{}{
						"eq",
						[]interface{}{"field", "dept"},
						[]interface{}{"field", "empDept"},
					},
					ParentA: &base.Op{
						Kind:   "scan",
						Fields: base.Fields{"dept", "city"},
						Params: []interface{}{
							"csvData",
							`
"dev","paris"
"finance","london"
`,
						},
					},
					ParentB: &base.Op{
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
					},
				},
			},
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
					[]interface{}{"field", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			ParentA: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
11,21
`,
				},
			},
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
					[]interface{}{"field", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
				},
				[]interface{}{
					"asc",
				},
			},
			ParentA: &base.Op{
				Kind:   "scan",
				Fields: base.Fields{"a", "b"},
				Params: []interface{}{
					"csvData",
					`
10,20
`,
				},
			},
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
					[]interface{}{"field", "b"},
				},
				[]interface{}{
					"desc",
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
					[]interface{}{"field", "b"},
				},
				[]interface{}{
					"asc",
					"asc",
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
					[]interface{}{"field", "b"},
				},
				[]interface{}{
					"desc",
					"asc",
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
					[]interface{}{"field", "b"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
					[]interface{}{"field", "b"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
					[]interface{}{"field", "b"},
				},
				[]interface{}{
					"asc",
					"desc",
				},
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
				},
				[]interface{}{
					"asc",
				},
				0,
				1,
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
				},
				[]interface{}{
					"asc",
				},
				0,
				100,
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
				},
				[]interface{}{
					"asc",
				},
				100,
				100,
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
				},
				[]interface{}{
					"asc",
				},
				1,
				0,
			},
			ParentA: &base.Op{
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
			},
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
					[]interface{}{"field", "a"},
				},
				[]interface{}{
					"asc",
				},
				1,
				1,
			},
			ParentA: &base.Op{
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
			},
		},
		expectYields: []base.Vals{
			base.Vals{[]byte("11"), []byte("21")},
		},
	},
}
