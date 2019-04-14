package test

import (
	"encoding/json"
	"fmt"
	"log"

	"reflect"
	"strings"
	"testing"

	n1k1 "github.com/couchbase/n1k1/n1k1_compiler"
)

func StringsToLazyVals(a []string, lazyValsPre n1k1.LazyVals) n1k1.LazyVals {
	lazyVals := lazyValsPre
	for _, v := range a {
		lazyVals = append(lazyVals, n1k1.LazyVal([]byte(v)))
	}
	return lazyVals
}

func TestIt(t *testing.T) {
	tests := []struct {
		about        string
		o            n1k1.Operator
		expectYields []n1k1.LazyVals
		expectErr    string
	}{
		{
			about: "test nil operator",
		},
		{
			about: "test empty csv-data scan",
			o: n1k1.Operator{
				Kind:   "scan",
				Fields: n1k1.Fields(nil),
				Params: []interface{}{
					"csvData",
					"",
				},
			},
		},
		{
			about: "test empty csv-data scan with some fields",
			o: n1k1.Operator{
				Kind:   "scan",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					"",
				},
			},
		},
		{
			about: "test csv-data scan with 1 record",
			o: n1k1.Operator{
				Kind:   "scan",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					"1,2,3",
				},
			},
			expectYields: []n1k1.LazyVals{
				n1k1.LazyVals{[]byte("1"), []byte("2"), []byte("3")},
			},
		},
		{
			about: "test csv-data scan with 2 records",
			o: n1k1.Operator{
				Kind:   "scan",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"csvData",
					`
10,20,30
11,21,31
`,
				},
			},
			expectYields: []n1k1.LazyVals{
				n1k1.LazyVals{[]byte("10"), []byte("20"), []byte("30")},
				n1k1.LazyVals{[]byte("11"), []byte("21"), []byte("31")},
			},
		},
		{
			about: "test csv-data scan->filter on const == const",
			o: n1k1.Operator{
				Kind:   "filter",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"json", `"July"`},
					[]interface{}{"json", `"July"`},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []n1k1.LazyVals{
				n1k1.LazyVals{[]byte("10"), []byte("20"), []byte("30")},
				n1k1.LazyVals{[]byte("11"), []byte("21"), []byte("31")},
			},
		},
		{
			about: "test csv-data scan->filter on constX == constY",
			o: n1k1.Operator{
				Kind:   "filter",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"json", `"July"`},
					[]interface{}{"json", `"June"`},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []n1k1.LazyVals(nil),
		},
		{
			about: "test csv-data scan->filter with fieldB == fieldB",
			o: n1k1.Operator{
				Kind:   "filter",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "b"},
					[]interface{}{"field", "b"},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []n1k1.LazyVals{
				n1k1.LazyVals{[]byte("10"), []byte("20"), []byte("30")},
				n1k1.LazyVals{[]byte("11"), []byte("21"), []byte("31")},
			},
		},
		{
			about: "test csv-data scan->filter with fieldA == fieldB",
			o: n1k1.Operator{
				Kind:   "filter",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "a"},
					[]interface{}{"field", "b"},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []n1k1.LazyVals(nil),
		},
		{
			about: "test csv-data scan->filter with fieldB = 66",
			o: n1k1.Operator{
				Kind:   "filter",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "b"},
					[]interface{}{"json", `66`},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []n1k1.LazyVals(nil),
		},
		{
			about: "test csv-data scan->filter with fieldB == 21",
			o: n1k1.Operator{
				Kind:   "filter",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "b"},
					[]interface{}{"json", `21`},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"a", "b", "c"},
					Params: []interface{}{
						"csvData",
						`
10,20,30
11,21,31
`,
					},
				},
			},
			expectYields: []n1k1.LazyVals{
				n1k1.LazyVals{[]byte("11"), []byte("21"), []byte("31")},
			},
		},
		{
			about: "test csv-data scan->filter more than 1 match",
			o: n1k1.Operator{
				Kind:   "filter",
				Fields: n1k1.Fields{"a", "b", "c"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "c"},
					[]interface{}{"json", `3000`},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"a", "b", "c"},
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
			expectYields: []n1k1.LazyVals{
				n1k1.LazyVals{[]byte("10"), []byte("20"), []byte("3000")},
				n1k1.LazyVals{[]byte("11"), []byte("21"), []byte("3000")},
			},
		},
		{
			about: "test csv-data scan->filter->project",
			o: n1k1.Operator{
				Kind:   "project",
				Fields: n1k1.Fields{"a", "c"},
				Params: []interface{}{
					[]interface{}{"field", "a"},
					[]interface{}{"field", "c"},
				},
				ParentA: &n1k1.Operator{
					Kind:   "filter",
					Fields: n1k1.Fields{"a", "b", "c"},
					Params: []interface{}{
						"eq",
						[]interface{}{"field", "c"},
						[]interface{}{"json", `3000`},
					},
					ParentA: &n1k1.Operator{
						Kind:   "scan",
						Fields: n1k1.Fields{"a", "b", "c"},
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
			expectYields: []n1k1.LazyVals{
				n1k1.LazyVals{[]byte("10"), []byte("3000")},
				n1k1.LazyVals{[]byte("11"), []byte("3000")},
			},
		},
		{
			about: "test csv-data scan->filter->project nothing",
			o: n1k1.Operator{
				Kind:   "project",
				Fields: n1k1.Fields{},
				Params: []interface{}{},
				ParentA: &n1k1.Operator{
					Kind:   "filter",
					Fields: n1k1.Fields{"a", "b", "c"},
					Params: []interface{}{
						"eq",
						[]interface{}{"field", "c"},
						[]interface{}{"json", `3000`},
					},
					ParentA: &n1k1.Operator{
						Kind:   "scan",
						Fields: n1k1.Fields{"a", "b", "c"},
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
			expectYields: []n1k1.LazyVals{
				n1k1.LazyVals(nil),
				n1k1.LazyVals(nil),
			},
		},
		{
			about: "test csv-data scan->filter->project unknown field",
			o: n1k1.Operator{
				Kind:   "project",
				Fields: n1k1.Fields{"a", "xxx"},
				Params: []interface{}{
					[]interface{}{"field", "a"},
					[]interface{}{"field", "xxx"},
				},
				ParentA: &n1k1.Operator{
					Kind:   "filter",
					Fields: n1k1.Fields{"a", "b", "c"},
					Params: []interface{}{
						"eq",
						[]interface{}{"field", "c"},
						[]interface{}{"json", `3000`},
					},
					ParentA: &n1k1.Operator{
						Kind:   "scan",
						Fields: n1k1.Fields{"a", "b", "c"},
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
			expectYields: []n1k1.LazyVals{
				n1k1.LazyVals{[]byte("10"), nil},
				n1k1.LazyVals{[]byte("11"), nil},
			},
		},
		{
			about: "test csv-data scan->join-nl",
			o: n1k1.Operator{
				Kind:   "join-nl",
				Fields: n1k1.Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "dept"},
					[]interface{}{"field", "empDept"},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				},
				ParentB: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"emp", "empDept"},
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
			expectYields: []n1k1.LazyVals{
				StringsToLazyVals([]string{`"dev"`, `"paris"`, `"dan"`, `"dev"`}, nil),
				StringsToLazyVals([]string{`"dev"`, `"paris"`, `"doug"`, `"dev"`}, nil),
				StringsToLazyVals([]string{`"finance"`, `"london"`, `"frank"`, `"finance"`}, nil),
				StringsToLazyVals([]string{`"finance"`, `"london"`, `"fred"`, `"finance"`}, nil),
			},
		},
		{
			about: "test csv-data scan->join-nl but false join condition",
			o: n1k1.Operator{
				Kind:   "join-nl",
				Fields: n1k1.Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"field", "dept"},
					[]interface{}{"json", `"NOT-MATCHING"`},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				},
				ParentB: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"emp", "empDept"},
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
			expectYields: []n1k1.LazyVals(nil),
		},
		{
			about: "test full join via always-true join condition",
			o: n1k1.Operator{
				Kind:   "join-nl",
				Fields: n1k1.Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{"json", `true`},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				},
				ParentB: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"emp", "empDept"},
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
			expectYields: []n1k1.LazyVals{
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
			o: n1k1.Operator{
				Kind:   "join-nl",
				Fields: n1k1.Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{
					"eq",
					[]interface{}{"json", `"Hello"`},
					[]interface{}{"json", `"Hello"`},
				},
				ParentA: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
"dev","paris"
"finance","london"
`,
					},
				},
				ParentB: &n1k1.Operator{
					Kind:   "scan",
					Fields: n1k1.Fields{"emp", "empDept"},
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
			expectYields: []n1k1.LazyVals{
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
		var yields []n1k1.LazyVals

		lazyYield := func(lazyVals n1k1.LazyVals) {
			var lazyValsCopy n1k1.LazyVals
			for _, v := range lazyVals {
				lazyValsCopy = append(lazyValsCopy, append(n1k1.LazyVal(nil), v...))
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

		_ = lazyYield
		_ = lazyYieldErr

		var out []string

		n1k1.Emit = func(format string, a ...interface{}) (n int, err error) {
			s := fmt.Sprintf(format, a...)
			out = append(out, s)
			return len(s), nil
		}

		n1k1.ExecOperator(&test.o, nil, nil)

		oj, _ := json.Marshal(test.o)

		log.Printf("-------------\n")
		log.Printf("testi: %d", testi)
		log.Printf("test.about: %s", test.about)
		log.Printf("test.o: %s", oj)
		log.Printf("\n%s", strings.Join(out, ""))

		continue

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
