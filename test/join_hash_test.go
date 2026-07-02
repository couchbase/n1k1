//go:build n1ql

package test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

// planHasKind reports whether op or any descendant has the given Kind.
func planHasKind(op *base.Op, kind string) bool {
	if op == nil {
		return false
	}
	if op.Kind == kind {
		return true
	}
	for _, ch := range op.Children {
		if planHasKind(ch, kind) {
			return true
		}
	}
	return false
}

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
// NOTE: this query's ON clause has residual predicates (test_id, !=), which for a
// LEFT JOIN can't be pushed off the preserved side, so VisitHashJoin falls back to
// the nested-loop join (glue/conv.go). It therefore locks USE HASH-on-LEFT-JOIN
// correctness via that fallback; TestAnsiHashLeftJoinEquijoin below covers a pure
// equijoin that actually runs on the joinHash-leftOuter op.
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

// TestAnsiHashLeftJoinEquijoin exercises the real joinHash-leftOuter op for a pure
// equijoin LEFT JOIN (VisitHashJoin builds the map from the outer/left side). It
// asserts the plan actually uses joinHash-leftOuter (not the NL fallback) and that
// its result equals USE NL -- including the two outer rows that stress the op: an
// unmatched left row (rome) and, critically, a left row whose join key is NULL
// (void), which OpJoinHash must preserve rather than drop.
func TestAnsiHashLeftJoinEquijoin(t *testing.T) {
	root := t.TempDir()
	write := func(ks, key, doc string) {
		t.Helper()
		dir := filepath.Join(root, "default", ks)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, key+".json"), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("dept", "d1", `{"id":"d1","city":"paris"}`)
	write("dept", "d2", `{"id":"d2","city":"london"}`)
	write("dept", "d3", `{"id":"d3","city":"rome"}`) // unmatched -> outer
	write("dept", "d4", `{"id":null,"city":"void"}`) // NULL key -> outer, must survive
	write("emp", "e1", `{"nm":"al","dep":"d1"}`)
	write("emp", "e2", `{"nm":"bo","dep":"d1"}`)
	write("emp", "e3", `{"nm":"cy","dep":"d2"}`)

	store, err := glue.FileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}

	q := func(hint string) string {
		return "SELECT d.city ct, e.nm nm FROM dept d LEFT JOIN emp e " + hint +
			" ON e.dep = d.id ORDER BY ct, nm"
	}

	want, _, err := n1k1RunStatementCtx(store, q("USE NL"))
	if err != nil {
		t.Fatalf("USE NL: %v", err)
	}
	if len(want) != 5 { // paris×2 (d1), london (d2), rome (unmatched), void (null key)
		t.Fatalf("USE NL baseline = %d rows, want 5: %v", len(want), want)
	}

	got, res, err := n1k1RunStatementCtx(store, q("USE HASH(build)"))
	if err != nil {
		t.Fatalf("USE HASH(build): %v", err)
	}
	if !planHasKind(res.Plan, "joinHash-leftOuter") {
		t.Fatalf("USE HASH(build) LEFT JOIN should run on joinHash-leftOuter, plan was:\n%v", res.Plan)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("joinHash-leftOuter result = %v, want (USE NL) %v", got, want)
	}
}
