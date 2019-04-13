package n1k1

import (
	"reflect"
	"testing"
)

func StringsToLazyVals(a []string, lazyValsPre LazyVals) LazyVals {
	lazyVals := lazyValsPre
	for _, v := range a {
		lazyVals = append(lazyVals, LazyVal([]byte(v)))
	}
	return lazyVals
}

func TestFieldsIndexOf(t *testing.T) {
	fs := Fields{"a", "b"}
	if fs.IndexOf("a") != 0 {
		t.Fatal("should have worked")
	}
	if fs.IndexOf("b") != 1 {
		t.Fatal("should have worked")
	}
	if fs.IndexOf("c") != -1 {
		t.Fatal("should have worked")
	}

	fs = Fields{}
	if fs.IndexOf("c") != -1 {
		t.Fatal("should have worked")
	}
}

func TestExecOperator(t *testing.T) {
	tests := []struct {
		about        string
		o            Operator
		expectYields []LazyVals
		expectErr    string
	}{
		{
			about: "test nil operator",
		},
		{
			about: "test empty csv-data scan",
			o: Operator{
				Kind:   "scan",
				Fields: Fields(nil),
				Params: []interface{}{
					"csvData",
					"",
				},
			},
		},
		{
			about: "test empty csv-data scan with some fields",
			o: Operator{
				Kind:   "scan",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					"",
				},
			},
		},
		{
			about: "test csv-data scan with 1 record",
			o: Operator{
				Kind:   "scan",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					"1,2,3",
				},
			},
			expectYields: []LazyVals{
				LazyVals{[]byte("1"), []byte("2"), []byte("3")},
			},
		},
		{
			about: "test csv-data scan with 2 records",
			o: Operator{
				Kind:   "scan",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			},
			expectYields: []LazyVals{
				LazyVals{[]byte("10"), []byte("20"), []byte("30")},
				LazyVals{[]byte("11"), []byte("21"), []byte("31")},
			},
		},
		{
			about: "test csv-data scan->filter on const == const",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"json", `"July"`},
					[]interface{}{"json", `"July"`},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []LazyVals{
				LazyVals{[]byte("10"), []byte("20"), []byte("30")},
				LazyVals{[]byte("11"), []byte("21"), []byte("31")},
			},
		},
		{
			about: "test csv-data scan->filter on constX == constY",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"json", `"July"`},
					[]interface{}{"json", `"June"`},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []LazyVals(nil),
		},
		{
			about: "test csv-data scan->filter with fieldB == fieldB",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "b"},
					[]interface{}{"field", "b"},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []LazyVals{
				LazyVals{[]byte("10"), []byte("20"), []byte("30")},
				LazyVals{[]byte("11"), []byte("21"), []byte("31")},
			},
		},
		{
			about: "test csv-data scan->filter with fieldA == fieldB",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "a"},
					[]interface{}{"field", "b"},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []LazyVals(nil),
		},
		{
			about: "test csv-data scan->filter with fieldB = 66",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "b"},
					[]interface{}{"json", `66`},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []LazyVals(nil),
		},
		{
			about: "test csv-data scan->filter with fieldB == 21",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "b"},
					[]interface{}{"json", `21`},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []LazyVals{
				LazyVals{[]byte("11"), []byte("21"), []byte("31")},
			},
		},
		{
			about: "test csv-data scan->filter more than 1 match",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "c"},
					[]interface{}{"json", `3000`},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"a", "b", "c"},
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
			expectYields: []LazyVals{
				LazyVals{[]byte("10"), []byte("20"), []byte("3000")},
				LazyVals{[]byte("11"), []byte("21"), []byte("3000")},
			},
		},
		{
			about: "test csv-data scan->filter->project",
			o: Operator{
				Kind:   "project",
				Fields: Fields{"a", "c"},
				Params: []interface{}{
					[]interface{}{"field", "a"},
					[]interface{}{"field", "c"},
				},
				ParentA: &Operator{
					Kind:   "filter",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{
						"eq",
						[]interface{}{"field", "c"},
						[]interface{}{"json", `3000`},
					},
					ParentA: &Operator{
						Kind:   "scan",
						Fields: Fields{"a", "b", "c"},
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
			expectYields: []LazyVals{
				LazyVals{[]byte("10"), []byte("3000")},
				LazyVals{[]byte("11"), []byte("3000")},
			},
		},
		{
			about: "test csv-data scan->filter->project nothing",
			o: Operator{
				Kind:   "project",
				Fields: Fields{},
				Params: []interface{}{},
				ParentA: &Operator{
					Kind:   "filter",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{
						"eq",
						[]interface{}{"field", "c"},
						[]interface{}{"json", `3000`},
					},
					ParentA: &Operator{
						Kind:   "scan",
						Fields: Fields{"a", "b", "c"},
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
			expectYields: []LazyVals{
				LazyVals(nil),
				LazyVals(nil),
			},
		},
		{
			about: "test csv-data scan->filter->project unknown field",
			o: Operator{
				Kind:   "project",
				Fields: Fields{"a", "xxx"},
				Params: []interface{}{
					[]interface{}{"field", "a"},
					[]interface{}{"field", "xxx"},
				},
				ParentA: &Operator{
					Kind:   "filter",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{
						"eq",
						[]interface{}{"field", "c"},
						[]interface{}{"json", `3000`},
					},
					ParentA: &Operator{
						Kind:   "scan",
						Fields: Fields{"a", "b", "c"},
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
			expectYields: []LazyVals{
				LazyVals{[]byte("10"), nil},
				LazyVals{[]byte("11"), nil},
			},
		},
		{
			about: "test csv-data scan->join-nl",
			o: Operator{
				Kind:   "join-nl",
				Fields: Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "dept"},
					[]interface{}{"field", "empDept"},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				},
				ParentB: &Operator{
					Kind:   "scan",
					Fields: Fields{"emp", "empDept"},
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
			expectYields: []LazyVals{
				StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
				StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
				StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
				StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
			},
		},
		{
			about: "test csv-data scan->join-nl but false join condition",
			o: Operator{
				Kind:   "join-nl",
				Fields: Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "dept"},
					[]interface{}{"json", `"NOT-MATCHING"`},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				},
				ParentB: &Operator{
					Kind:   "scan",
					Fields: Fields{"emp", "empDept"},
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
			expectYields: []LazyVals(nil),
		},
		{
			about: "test full join via always-true join condition",
			o: Operator{
				Kind:   "join-nl",
				Fields: Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{"json", `true`},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				},
				ParentB: &Operator{
					Kind:   "scan",
					Fields: Fields{"emp", "empDept"},
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
			expectYields: []LazyVals{
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
			about: "test full join via always-matching join condition",
			o: Operator{
				Kind:   "join-nl",
				Fields: Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"json", `"Hello"`},
					[]interface{}{"json", `"Hello"`},
				},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				},
				ParentB: &Operator{
					Kind:   "scan",
					Fields: Fields{"emp", "empDept"},
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
			expectYields: []LazyVals{
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
	}

	for testi, test := range tests {
		var yields []LazyVals

		lazyYield := func(lazyVals LazyVals) {
			var lazyValsCopy LazyVals
			for _, v := range lazyVals {
				lazyValsCopy = append(lazyValsCopy, append(LazyVal(nil), v...))
			}

			yields = append(yields, lazyValsCopy)
		}

		lazyYieldErr := func(err error) {
			if (test.expectErr != "") != (err != nil) {
				t.Fatalf("testi: %d, test: %+v,\n"+
					" got err: %v",
					testi, test, err)
			}
		}

		ExecOperator(&test.o, lazyYield, lazyYieldErr)

		if len(yields) != len(test.expectYields) ||
			!reflect.DeepEqual(yields, test.expectYields) {
			t.Fatalf("testi: %d, test: %+v,\n"+
				" len(yields): %d,\n"+
				" len(test.expectYields): %d,\n"+
				" expectYields: %+v,\n"+
				" got yields: %+v",
				testi, test,
				len(yields), len(test.expectYields), test.expectYields, yields)
		}
	}
}
