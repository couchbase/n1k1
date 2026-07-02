//go:build n1ql

package test

import (
	"reflect"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestAnsiHashJoin exercises the hash-join wiring (VisitHashJoin ->
// OpJoinHash's probe-map equijoin). The planner only emits a HashJoin under a
// USE HASH hint, so force one both ways (build/probe) and require the result to
// equal the nested-loop forms (no hint / USE NL) -- same rows, so the hash path
// is correct. Uses the aggregate_functions agg_func docs (orders + the four
// customer docs) already in the gsi corpus.
func TestAnsiHashJoin(t *testing.T) {
	store, err := glue.FileStore(gsiSuiteRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}

	q := func(hint string) string {
		return "SELECT o.custId oc, c.firstName fn FROM orders o INNER JOIN customer c " +
			hint + " ON o.custId=c.custId AND o.test_id=\"agg_func\" AND c.test_id=\"agg_func\" ORDER BY oc"
	}

	run := func(hint string) []string {
		rows, err := n1k1RunStatement(store, q(hint))
		if err != nil {
			t.Fatalf("hint %q: %v", hint, err)
		}
		return rows
	}

	want := run("USE NL")
	if len(want) != 4 { // sanity: the four agg_func customers with matching orders
		t.Fatalf("USE NL baseline = %d rows, want 4: %v", len(want), want)
	}
	for _, hint := range []string{"", "USE HASH(build)", "USE HASH(probe)"} {
		if got := run(hint); !reflect.DeepEqual(got, want) {
			t.Errorf("join hint %q = %v, want %v", hint, got, want)
		}
	}
}
