package test

import (
	"fmt"
	"io/ioutil"
	"sync/atomic"
	"testing"

	"github.com/couchbase/rhmap/store"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/expr_glue"
)

func MakeYieldCaptureFuncs(t *testing.T, testi int, expectErr string) (
	*base.Vars, base.YieldVals, base.YieldErr,
	func() []base.Vals) {
	if n1k1.ExprCatalog["exprStr"] == nil {
		n1k1.ExprCatalog["exprStr"] = expr_glue.ExprStr
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

	return MakeVars(), yieldVals, yieldErr, returnYields
}

func MakeVars() *base.Vars {
	tmpDir, _ := ioutil.TempDir("", "n1k1TmpDir")

	var counter uint64

	return &base.Vars{
		Ctx: &base.Ctx{
			ValComparer: base.NewValComparer(),
			ExprCatalog: n1k1.ExprCatalog,
			YieldStats:  func(stats *base.Stats) error { return nil },
			AllocMap: func() (*store.RHStore, error) {
				options := store.DefaultRHStoreFileOptions

				counterMine := atomic.AddUint64(&counter, 1)

				pathPrefix := fmt.Sprintf("%s/%d", tmpDir, counterMine)

				sf, err := store.CreateRHStoreFile(pathPrefix, options)
				if err != nil {
					return nil, err
				}

				return &sf.RHStore, nil
			},
			AllocChunks: func() (*store.Chunks, error) {
				options := store.DefaultRHStoreFileOptions

				counterMine := atomic.AddUint64(&counter, 1)

				pathPrefix := fmt.Sprintf("%s/%d", tmpDir, counterMine)

				return &store.Chunks{
					PathPrefix:     pathPrefix,
					FileSuffix:     ".rhchunk,",
					ChunkSizeBytes: options.ChunkSizeBytes,
				}, nil
			},
		},
	}
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
			base.Vals{[]byte("sf")},
			base.Vals{[]byte("sj")},
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
			Kind:   "joinNL-outerLeft",
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
			Kind:   "joinNL-outerLeft",
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
			Kind:   "joinNL-outerLeft",
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
			Kind:   "joinNL-outerLeft",
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
				0,
				1,
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
				0,
				100,
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
				100,
				100,
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
				1,
				0,
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
				1,
				1,
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
				1,
				1,
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
				0,
				10,
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
				0,
				10,
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
			Kind:   "joinHash-outerLeft",
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
				Kind:   "joinHash-outerLeft",
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
			Kind:   "joinHash-outerLeft",
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
				Kind:   "joinHash-outerLeft",
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
				[]interface{}{"valsCanonical", "a"},
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
			base.Vals{[]byte("0\n0\n0")},
			base.Vals{[]byte("1.2\n-22\n-0")},
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
			StringsToVals([]string{`10`, `11`}, nil),
			StringsToVals([]string{`20`, `21`}, nil),
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
			StringsToVals([]string{`10`, `11`}, nil),
			StringsToVals([]string{`20`, `21`}, nil),
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
			base.Vals{[]byte("10"), []byte("23")},
			base.Vals{[]byte("20"), []byte("20")},
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
			base.Vals{[]byte("10"), []byte("20")},
			base.Vals{[]byte("20"), []byte("20")},
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
			base.Vals{[]byte("10"), []byte("23"), []byte("2")},
			base.Vals{[]byte("20"), []byte("20"), []byte("1")},
		},
	},
}
