//go:build n1ql

package test

import (
	"reflect"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestUnion covers UNION (set union, deduped) and UNION ALL (concatenation),
// wired via VisitUnionAll -> OpUnionAll. Uses inline arrays so it needs no
// corpus data. gsi select_functions[27] covers the UNION-distinct path too.
func TestUnion(t *testing.T) {
	store, err := glue.FileStore(gsiSuiteRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		q    string
		want []string
	}{
		{ // UNION dedups (3 appears in both inputs)
			`(SELECT x.a FROM [{"a":1},{"a":3}] x) UNION (SELECT y.a FROM [{"a":2},{"a":3}] y) ORDER BY a`,
			[]string{`{"a":1}`, `{"a":2}`, `{"a":3}`},
		},
		{ // UNION ALL keeps duplicates
			`(SELECT x.a FROM [{"a":1},{"a":3}] x) UNION ALL (SELECT y.a FROM [{"a":2},{"a":3}] y) ORDER BY a`,
			[]string{`{"a":1}`, `{"a":2}`, `{"a":3}`, `{"a":3}`},
		},
	}
	for _, c := range cases {
		rows, err := n1k1RunStatement(store, c.q)
		if err != nil {
			t.Errorf("%s: %v", c.q, err)
			continue
		}
		if !reflect.DeepEqual(rows, c.want) {
			t.Errorf("%s\n got=%v\nwant=%v", c.q, rows, c.want)
		}
	}
}
