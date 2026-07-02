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

// TestAnsiHashLeftJoin exercises a LEFT OUTER JOIN under a USE HASH hint
// end-to-end. The four agg_func customers each have exactly one matching order,
// so an ON clause that excludes customer38's match (o.custId != "customer38")
// leaves customer38 unmatched: LEFT OUTER preserves it (4 rows) where INNER drops
// it (3 rows) -- a format-agnostic proof that outer semantics actually fire. All
// hint variants (none / USE HASH build / probe) must equal the USE NL baseline.
//
// NOTE: VisitHashJoin only routes the *inner* single-key equijoin to the real
// hash op; LEFT OUTER (and residual-filter) shapes fall back to the nested-loop
// join (glue/conv.go). So this locks the end-to-end correctness of USE HASH on a
// LEFT JOIN -- currently via that NL fallback -- not the joinHash-leftOuter op
// itself (which is covered at the op level by test/cases.go).
func TestAnsiHashLeftJoin(t *testing.T) {
	store, err := glue.FileStore(gsiSuiteRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}

	// Preserved (left) side = customer; the ON clause excludes customer38's order,
	// so customer38 becomes an unmatched outer row (its oc is MISSING).
	on := ` ON o.custId=c.custId AND o.test_id="agg_func" AND o.custId != "customer38"`
	where := ` WHERE c.test_id="agg_func" ORDER BY cc`
	sel := `SELECT c.custId cc, o.custId oc FROM customer c `

	q := func(join, hint string) string {
		return sel + join + " orders o " + hint + on + where
	}
	run := func(join, hint string) []string {
		rows, err := n1k1RunStatement(store, q(join, hint))
		if err != nil {
			t.Fatalf("%s %q: %v", join, hint, err)
		}
		return rows
	}

	want := run("LEFT JOIN", "USE NL")
	inner := run("INNER JOIN", "")
	if len(want) != len(inner)+1 { // the LEFT side preserves the unmatched customer38
		t.Fatalf("LEFT OUTER should preserve one unmatched row: left=%d inner=%d\nleft=%v",
			len(want), len(inner), want)
	}
	for _, hint := range []string{"", "USE HASH(build)", "USE HASH(probe)"} {
		if got := run("LEFT JOIN", hint); !reflect.DeepEqual(got, want) {
			t.Errorf("LEFT JOIN hint %q = %v, want %v", hint, got, want)
		}
	}
}
