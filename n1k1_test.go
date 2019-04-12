package n1k1

import (
	"reflect"
	"testing"
)

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
				LazyVals{"1", "2", "3"},
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
				LazyVals{"10", "20", "30"},
				LazyVals{"11", "21", "31"},
			},
		},
		{
			about: "test csv-data scan->filter on const == const",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{"eq", "sJuly", "sJuly"},
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
				LazyVals{"10", "20", "30"},
				LazyVals{"11", "21", "31"},
			},
		},
		{
			about: "test csv-data scan->filter on constX == constY",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{"eq", "sJuly", "sJune"},
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
				Params: []interface{}{"eq", "fb", "fb"},
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
				LazyVals{"10", "20", "30"},
				LazyVals{"11", "21", "31"},
			},
		},
		{
			about: "test csv-data scan->filter with fieldA == fieldB",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{"eq", "fa", "fb"},
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
			about: "test csv-data scan->filter with fieldB = '66'",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{"eq", "fb", "s66"},
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
			about: "test csv-data scan->filter with fieldB == '21'",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{"eq", "fb", "s21"},
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
				LazyVals{"11", "21", "31"},
			},
		},
		{
			about: "test csv-data scan->filter more than 1 match",
			o: Operator{
				Kind:   "filter",
				Fields: Fields{"a", "b", "c"},
				Params: []interface{}{"eq", "fc", "s3000"},
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
				LazyVals{"10", "20", "3000"},
				LazyVals{"11", "21", "3000"},
			},
		},
		{
			about: "test csv-data scan->filter->project",
			o: Operator{
				Kind:   "project",
				Fields: Fields{"a", "c"},
				Params: []interface{}{"a", "c"},
				ParentA: &Operator{
					Kind:   "filter",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{"eq", "fc", "s3000"},
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
				LazyVals{"10", "3000"},
				LazyVals{"11", "3000"},
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
					Params: []interface{}{"eq", "fc", "s3000"},
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
				Params: []interface{}{"a", "xxx"},
				ParentA: &Operator{
					Kind:   "filter",
					Fields: Fields{"a", "b", "c"},
					Params: []interface{}{"eq", "fc", "s3000"},
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
				LazyVals{"10", ""},
				LazyVals{"11", ""},
			},
		},
		{
			about: "test csv-data scan->join-nl",
			o: Operator{
				Kind:   "join-nl",
				Fields: Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{"eq", "fdept", "fempDept"},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
dev,paris
finance,london
`,
					},
				},
				ParentB: &Operator{
					Kind:   "scan",
					Fields: Fields{"emp", "empDept"},
					Params: []interface{}{
						"csvData",
						`
dan,dev
doug,dev
frank,finance
fred,finance
`,
					},
				},
			},
			expectYields: []LazyVals{
				LazyVals{"dev", "paris", "dan", "dev"},
				LazyVals{"dev", "paris", "doug", "dev"},
				LazyVals{"finance", "london", "frank", "finance"},
				LazyVals{"finance", "london", "fred", "finance"},
			},
		},
		{
			about: "test csv-data scan->join-nl but never-matching join condition",
			o: Operator{
				Kind:   "join-nl",
				Fields: Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{"eq", "fdept", "sNOT-MATCHING"},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
dev,paris
finance,london
`,
					},
				},
				ParentB: &Operator{
					Kind:   "scan",
					Fields: Fields{"emp", "empDept"},
					Params: []interface{}{
						"csvData",
						`
dan,dev
doug,dev
frank,finance
fred,finance
`,
					},
				},
			},
			expectYields: []LazyVals(nil),
		},
		{
			about: "test full join via always-matching join condition",
			o: Operator{
				Kind:   "join-nl",
				Fields: Fields{"dept", "city", "emp", "empDept"},
				Params: []interface{}{"eq", "sHello", "sHello"},
				ParentA: &Operator{
					Kind:   "scan",
					Fields: Fields{"dept", "city"},
					Params: []interface{}{
						"csvData",
						`
dev,paris
finance,london
`,
					},
				},
				ParentB: &Operator{
					Kind:   "scan",
					Fields: Fields{"emp", "empDept"},
					Params: []interface{}{
						"csvData",
						`
dan,dev
doug,dev
frank,finance
fred,finance
`,
					},
				},
			},
			expectYields: []LazyVals{
				LazyVals{"dev", "paris", "dan", "dev"},
				LazyVals{"dev", "paris", "doug", "dev"},
				LazyVals{"dev", "paris", "frank", "finance"},
				LazyVals{"dev", "paris", "fred", "finance"},
				LazyVals{"finance", "london", "dan", "dev"},
				LazyVals{"finance", "london", "doug", "dev"},
				LazyVals{"finance", "london", "frank", "finance"},
				LazyVals{"finance", "london", "fred", "finance"},
			},
		},
	}

	for testi, test := range tests {
		var yields []LazyVals

		lazyYield := func(lazyVals LazyVals) {
			yields = append(yields,
				append(LazyVals(nil), lazyVals...))
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
				" got yields: %+v",
				testi, test,
				len(yields), len(test.expectYields), yields)
		}
	}
}
