package test

import (
	"testing"

	"github.com/couchbase/n1k1/base"
)

func MakeYieldCaptureFuncs(t *testing.T, testi int, expectErr string) (
	base.YieldVals, base.YieldErr, func() []base.Vals) {
	var yields []base.Vals

	yieldVals := func(lazyVals base.Vals) {
		var lazyValsCopy base.Vals
		for _, v := range lazyVals {
			lazyValsCopy = append(lazyValsCopy,
				append(base.Val(nil), v...))
		}

		yields = append(yields, lazyValsCopy)
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

	return yieldVals, yieldErr, returnYields
}

func StringsToLazyVals(a []string, lazyValsPre base.Vals) base.Vals {
	lazyVals := lazyValsPre
	for _, v := range a {
		if v != "" {
			lazyVals = append(lazyVals, base.Val([]byte(v)))
		} else {
			lazyVals = append(lazyVals, base.ValMissing)
		}
	}
	return lazyVals
}

type TestCaseSimple struct {
	about        string
	o            base.Operator
	expectYields []base.Vals
	expectErr    string
}

var TestCasesSimple = []TestCaseSimple{
	{
		about: "test nil operator",
	},
	{
		about: "test empty csv-data scan",
		o: base.Operator{
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
		o: base.Operator{
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
		o: base.Operator{
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
		o: base.Operator{
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
		o: base.Operator{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"July"`},
				[]interface{}{"json", `"July"`},
			},
			ParentA: &base.Operator{
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
		o: base.Operator{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"July"`},
				[]interface{}{"json", `"June"`},
			},
			ParentA: &base.Operator{
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
		o: base.Operator{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", "b"},
				[]interface{}{"field", "b"},
			},
			ParentA: &base.Operator{
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
		o: base.Operator{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", "a"},
				[]interface{}{"field", "b"},
			},
			ParentA: &base.Operator{
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
		o: base.Operator{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", "b"},
				[]interface{}{"json", `66`},
			},
			ParentA: &base.Operator{
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
		o: base.Operator{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", "b"},
				[]interface{}{"json", `21`},
			},
			ParentA: &base.Operator{
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
		o: base.Operator{
			Kind:   "filter",
			Fields: base.Fields{"a", "b", "c"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", "c"},
				[]interface{}{"json", `3000`},
			},
			ParentA: &base.Operator{
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
		o: base.Operator{
			Kind:   "project",
			Fields: base.Fields{"a", "c"},
			Params: []interface{}{
				[]interface{}{"field", "a"},
				[]interface{}{"field", "c"},
			},
			ParentA: &base.Operator{
				Kind:   "filter",
				Fields: base.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "c"},
					[]interface{}{"json", `3000`},
				},
				ParentA: &base.Operator{
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
		about: "test csv-data scan->filter->project nothing",
		o: base.Operator{
			Kind:   "project",
			Fields: base.Fields{},
			Params: []interface{}{},
			ParentA: &base.Operator{
				Kind:   "filter",
				Fields: base.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "c"},
					[]interface{}{"json", `3000`},
				},
				ParentA: &base.Operator{
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
		o: base.Operator{
			Kind:   "project",
			Fields: base.Fields{"a", "xxx"},
			Params: []interface{}{
				[]interface{}{"field", "a"},
				[]interface{}{"field", "xxx"},
			},
			ParentA: &base.Operator{
				Kind:   "filter",
				Fields: base.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "c"},
					[]interface{}{"json", `3000`},
				},
				ParentA: &base.Operator{
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
		about: "test csv-data scan->join-inner-nl",
		o: base.Operator{
			Kind:   "join-inner-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", "dept"},
				[]interface{}{"field", "empDept"},
			},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test csv-data scan->join-inner-nl but false join condition",
		o: base.Operator{
			Kind:   "join-inner-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", "dept"},
				[]interface{}{"json", `"NOT-MATCHING"`},
			},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
		o: base.Operator{
			Kind:   "join-inner-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{"json", `true`},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"fred"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test inner join via always-matching join condition",
		o: base.Operator{
			Kind:   "join-inner-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"Hello"`},
				[]interface{}{"json", `"Hello"`},
			},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"fred"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test full outer join via never-matching join condition",
		o: base.Operator{
			Kind:   "join-outerFull-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"json", `"Hello"`},
				[]interface{}{"json", `"Goodbye"`},
			},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"fred"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test full outer join via sometimes-matching join condition",
		o: base.Operator{
			Kind:   "join-outerFull-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", `dept`},
				[]interface{}{"field", `empDept`},
			},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"fred"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test left outer join on dept",
		o: base.Operator{
			Kind:   "join-outerLeft-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", `dept`},
				[]interface{}{"field", `empDept`},
			},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test left outer join on never matching condition",
		o: base.Operator{
			Kind:   "join-outerLeft-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", `dept`},
				[]interface{}{"field", `someFakeField`},
			},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
			StringsToLazyVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, ``, ``}, nil),
		},
	},
	{
		about: "test right outer join on dept",
		o: base.Operator{
			Kind:   "join-outerRight-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", `dept`},
				[]interface{}{"field", `empDept`},
			},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{``, ``, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{``, ``, `"fred"`, `"finance"`}, nil),
			StringsToLazyVals([]string{``, ``, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{``, ``, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
		},
	},
	{
		about: "test right outer join on never-matching condition",
		o: base.Operator{
			Kind:   "join-outerRight-nl",
			Fields: base.Fields{"dept", "city", "emp", "empDept"},
			Params: []interface{}{
				"eq",
				[]interface{}{"field", `someFakeField`},
				[]interface{}{"field", `empDept`},
			},
			ParentA: &base.Operator{
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
			ParentB: &base.Operator{
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
			StringsToLazyVals([]string{``, ``, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{``, ``, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{``, ``, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{``, ``, `"fred"`, `"finance"`}, nil),
			StringsToLazyVals([]string{``, ``, `"dan"`, `"dev"`}, nil),
			StringsToLazyVals([]string{``, ``, `"doug"`, `"dev"`}, nil),
			StringsToLazyVals([]string{``, ``, `"frank"`, `"finance"`}, nil),
			StringsToLazyVals([]string{``, ``, `"fred"`, `"finance"`}, nil),
		},
	},
}
