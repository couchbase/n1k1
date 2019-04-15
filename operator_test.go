package n1k1

import (
	"reflect"
	"testing"

	"github.com/couchbase/n1k1/base"
)

func StringsToLazyVals(a []string, lazyValsPre base.LazyVals) base.LazyVals {
	lazyVals := lazyValsPre
	for _, v := range a {
		lazyVals = append(lazyVals, base.LazyVal([]byte(v)))
	}
	return lazyVals
}

func TestFieldsIndexOf(t *testing.T) {
	fs := base.Fields{"a", "b"}
	if fs.IndexOf("a") != 0 {
		t.Fatal("should have worked")
	}
	if fs.IndexOf("b") != 1 {
		t.Fatal("should have worked")
	}
	if fs.IndexOf("c") != -1 {
		t.Fatal("should have worked")
	}

	fs = base.Fields{}
	if fs.IndexOf("c") != -1 {
		t.Fatal("should have worked")
	}
}

func TestExecOperator(t *testing.T) {
	tests := []struct {
		about        string
		o            base.Operator
		expectYields []base.LazyVals
		expectErr    string
	}{
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
			expectYields: []base.LazyVals{
				base.LazyVals{[]byte("1"), []byte("2"), []byte("3")},
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
			expectYields: []base.LazyVals{
				base.LazyVals{[]byte("10"), []byte("20"), []byte("30")},
				base.LazyVals{[]byte("11"), []byte("21"), []byte("31")},
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
			expectYields: []base.LazyVals{
				base.LazyVals{[]byte("10"), []byte("20"), []byte("30")},
				base.LazyVals{[]byte("11"), []byte("21"), []byte("31")},
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
			expectYields: []base.LazyVals(nil),
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
			expectYields: []base.LazyVals{
				base.LazyVals{[]byte("10"), []byte("20"), []byte("30")},
				base.LazyVals{[]byte("11"), []byte("21"), []byte("31")},
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
			expectYields: []base.LazyVals(nil),
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
			expectYields: []base.LazyVals(nil),
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
			expectYields: []base.LazyVals{
				base.LazyVals{[]byte("11"), []byte("21"), []byte("31")},
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
			expectYields: []base.LazyVals{
				base.LazyVals{[]byte("10"), []byte("20"), []byte("3000")},
				base.LazyVals{[]byte("11"), []byte("21"), []byte("3000")},
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
			expectYields: []base.LazyVals{
				base.LazyVals{[]byte("10"), []byte("3000")},
				base.LazyVals{[]byte("11"), []byte("3000")},
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
			expectYields: []base.LazyVals{
				base.LazyVals(nil),
				base.LazyVals(nil),
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
			expectYields: []base.LazyVals{
				base.LazyVals{[]byte("10"), nil},
				base.LazyVals{[]byte("11"), nil},
			},
		},
		{
			about: "test csv-data scan->join-nl",
			o: base.Operator{
				Kind:   "join-nl",
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
			expectYields: []base.LazyVals{
				StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
				StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
				StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
				StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
			},
		},
		{
			about: "test csv-data scan->join-nl but false join condition",
			o: base.Operator{
				Kind:   "join-nl",
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
			expectYields: []base.LazyVals(nil),
		},
		{
			about: "test full join via always-true join condition",
			o: base.Operator{
				Kind:   "join-nl",
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
			expectYields: []base.LazyVals{
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
			o: base.Operator{
				Kind:   "join-nl",
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
			expectYields: []base.LazyVals{
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
		var yields []base.LazyVals

		lazyYield := func(lazyVals base.LazyVals) {
			var lazyValsCopy base.LazyVals
			for _, v := range lazyVals {
				lazyValsCopy = append(lazyValsCopy,
					append(base.LazyVal(nil), v...))
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
