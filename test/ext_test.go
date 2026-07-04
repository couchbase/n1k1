//go:build n1ql

//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/couchbase/n1k1/glue"
)

// extSession opens a self-contained temp datastore (so these tests don't depend
// on the shared corpus) with a single empty keyspace, enough to Run() no-FROM
// and FROM-array queries.
func extSession(t *testing.T) *glue.Session {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "default", "k"), 0o755); err != nil {
		t.Fatal(err)
	}
	// one throwaway doc so the keyspace is non-empty if ever scanned
	if err := os.WriteFile(filepath.Join(root, "default", "k", "d1.json"), []byte(`{"x":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := glue.FileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}
	return &glue.Session{Store: store, Namespace: "default"}
}

func extRawRows(t *testing.T, sess *glue.Session, stmt string) []string {
	t.Helper()
	res, err := sess.Run(stmt)
	if err != nil {
		t.Fatalf("Run(%q): %v", stmt, err)
	}
	out := make([]string, len(res.Rows))
	for i, r := range res.Rows {
		out[i] = string(r)
	}
	return out
}

// TestExtJSUDFScalar exercises a Tier-2 goja JS scalar UDF end-to-end: register
// it, then have the cbq parser resolve NAME(args) and evaluate through the
// ExprTree fallback lane.
func TestExtJSUDFScalar(t *testing.T) {
	if err := glue.RegisterJSFunc("addtwonumbers",
		`function addtwonumbers(a, b) { return a + b; }`); err != nil {
		t.Fatalf("RegisterJSFunc: %v", err)
	}
	// A UDF that uses JS string builtins and returns a string.
	if err := glue.RegisterJSFunc("shout",
		`function shout(s) { return String(s).toUpperCase() + "!"; }`); err != nil {
		t.Fatalf("RegisterJSFunc shout: %v", err)
	}
	// A UDF returning a composite (object) value.
	if err := glue.RegisterJSFunc("mkpair",
		`function mkpair(a, b) { return {lo: Math.min(a,b), hi: Math.max(a,b)}; }`); err != nil {
		t.Fatalf("RegisterJSFunc mkpair: %v", err)
	}

	sess := extSession(t)

	cases := []struct{ stmt, want string }{
		{`SELECT RAW addtwonumbers(3, 4)`, `7`},
		{`SELECT RAW addtwonumbers(1.5, 2.25)`, `3.75`},
		{`SELECT RAW shout("hi")`, `"HI!"`},
		{`SELECT RAW mkpair(9, 2)`, `{"hi":9,"lo":2}`},
		{`SELECT RAW addtwonumbers(10, 20) + 100`, `130`}, // composes with native arithmetic
	}
	for _, c := range cases {
		got := extRawRows(t, sess, c.stmt)
		if len(got) != 1 || got[0] != c.want {
			t.Fatalf("%s => %v, want [%s]", c.stmt, got, c.want)
		}
	}
}

// TestExtJSUDFGuards covers the safety fixes on the goja path: a UDF that
// mutates an object argument must NOT corrupt the source row (deep-copy in), a
// runaway UDF is interrupted by the timeout guard, and a UDF name that would
// shadow a stock builtin/aggregate is refused.
func TestExtJSUDFGuards(t *testing.T) {
	// (a) argument isolation: evil() mutates its object arg; the row's own field
	// must be unaffected regardless of projection evaluation order.
	if err := glue.RegisterJSFunc("evil",
		`function evil(o) { o.x = 999; return o.x; }`); err != nil {
		t.Fatalf("RegisterJSFunc evil: %v", err)
	}
	sess := extSession(t)
	got := extRawRows(t, sess, `SELECT RAW [evil(d), d.x] FROM [{"x":1}] AS d`)
	if len(got) != 1 || got[0] != `[999,1]` {
		t.Fatalf(`arg-isolation: [evil(d), d.x] = %v, want [[999,1]] (source not mutated)`, got)
	}

	// (b) timeout guard: an infinite loop is interrupted, surfacing an error
	// rather than hanging forever.
	if err := glue.RegisterJSFunc("spin",
		`function spin() { while (true) {} }`); err != nil {
		t.Fatalf("RegisterJSFunc spin: %v", err)
	}
	saved := glue.JSCallTimeout
	glue.JSCallTimeout = 150 * time.Millisecond
	defer func() { glue.JSCallTimeout = saved }()
	if _, err := sess.Run(`SELECT RAW spin()`); err == nil {
		t.Fatalf("spin(): expected a timeout error, got nil")
	}

	// (c) shadowing guard: refuse a first-time name that collides with a builtin
	// (UPPER) or an aggregate (COUNT).
	if err := glue.RegisterJSFunc("upper",
		`function upper(s) { return s; }`); err == nil {
		t.Fatalf("registering UDF named 'upper' should be refused (builtin collision)")
	}
	if err := glue.RegisterJSFunc("count",
		`function count(x) { return x; }`); err == nil {
		t.Fatalf("registering UDF named 'count' should be refused (aggregate collision)")
	}
}

// TestExtAggregatesSparklineHistogram exercises the native, zero-garbage
// extension aggregates over a deterministic FROM-array source (so accumulation
// order is fixed).
func TestExtAggregatesSparklineHistogram(t *testing.T) {
	sess := extSession(t)

	// sparkline over an exact ramp 1..8 -> the eight distinct block levels.
	got := extRawRows(t, sess, `SELECT RAW sparkline(v) FROM [1,2,3,4,5,6,7,8] AS v`)
	if len(got) != 1 {
		t.Fatalf("sparkline ramp: expected 1 row, got %v", got)
	}
	if want := `"▁▂▃▄▅▆▇█"`; got[0] != want {
		t.Fatalf("sparkline([1..8]) = %s, want %s", got[0], want)
	}

	// A flat series (all equal) renders the baseline block for every point.
	got = extRawRows(t, sess, `SELECT RAW sparkline(v) FROM [5,5,5,5] AS v`)
	if want := `"▁▁▁▁"`; got[0] != want {
		t.Fatalf("sparkline([5,5,5,5]) = %s, want %s", got[0], want)
	}

	// histogram renders exactly histogramBuckets (20) block runes.
	got = extRawRows(t, sess, `SELECT RAW histogram(v) FROM [1,2,3,4,5,6,7,8] AS v`)
	inner := strings.Trim(got[0], `"`)
	if n := utf8.RuneCountInString(inner); n != 20 {
		t.Fatalf("histogram rune count = %d, want 20 (%s)", n, got[0])
	}
	for _, r := range inner {
		if r < '▁' || r > '█' {
			t.Fatalf("histogram contains non-block rune %q in %s", r, got[0])
		}
	}

	// A group with no NUMBER values (the aggregate ignores non-numbers, like the
	// stddev/median family) renders NULL, not an empty string.
	got = extRawRows(t, sess, `SELECT RAW sparkline(v) FROM ["a","b","c"] AS v`)
	if len(got) != 1 || got[0] != `null` {
		t.Fatalf(`sparkline(["a","b","c"]) = %v, want [null]`, got)
	}
}

// TestExtRegisterJSDir exercises the directory-as-catalog registry.
func TestExtRegisterJSDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "triple.js"),
		[]byte(`function triple(x) { return x * 3; }`), 0o644); err != nil {
		t.Fatal(err)
	}
	names, err := glue.RegisterJSDir(dir)
	if err != nil {
		t.Fatalf("RegisterJSDir: %v", err)
	}
	if len(names) != 1 || names[0] != "triple" {
		t.Fatalf("RegisterJSDir names = %v, want [triple]", names)
	}

	sess := extSession(t)
	got := extRawRows(t, sess, `SELECT RAW triple(14)`)
	if len(got) != 1 || got[0] != `42` {
		t.Fatalf("triple(14) = %v, want [42]", got)
	}
}

// TestExtShippedJSExamples loads the example UDFs shipped in
// extensions/functions/js and confirms they resolve and run, so the docs'
// examples can't silently rot.
func TestExtShippedJSExamples(t *testing.T) {
	names, err := glue.RegisterJSDir("../extensions/functions/js")
	if err != nil {
		t.Fatalf("RegisterJSDir(shipped): %v", err)
	}
	want := []string{"add_two_numbers", "celsius_to_fahrenheit", "slugify"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("shipped UDF names = %v, want %v", names, want)
	}

	sess := extSession(t)
	cases := []struct{ stmt, want string }{
		{`SELECT RAW add_two_numbers(2, 5)`, `7`},
		{`SELECT RAW celsius_to_fahrenheit(100)`, `212`},
		{`SELECT RAW slugify("Hello, World!")`, `"hello-world"`},
	}
	for _, c := range cases {
		got := extRawRows(t, sess, c.stmt)
		if len(got) != 1 || got[0] != c.want {
			t.Fatalf("%s => %v, want [%s]", c.stmt, got, c.want)
		}
	}
}
