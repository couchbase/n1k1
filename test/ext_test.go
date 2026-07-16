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
	"bytes"
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

	// (d) async / Promise: unsupported (no event loop). A UDF that returns a
	// Promise must fail with a clear message, not an opaque panic or a hang.
	if err := glue.RegisterJSFunc("asyncudf", `async function asyncudf(x){ return x+1; }`); err != nil {
		t.Fatalf("RegisterJSFunc asyncudf: %v", err)
	}
	_, err := sess.Run(`SELECT RAW asyncudf(1)`)
	if err == nil || !strings.Contains(err.Error(), "Promise") {
		t.Fatalf("async UDF: want a clear Promise-unsupported error, got %v", err)
	}
}

// TestExtJSDirLoadOrder confirms directory loads are sorted by filename, so a
// shared top-level helper defined in two files resolves to the alphabetically
// LAST file's version (deterministic collision control by naming).
func TestExtJSDirLoadOrder(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a_first.js"),
		[]byte("function ldhelper(){return \"from-a\";}\nfunction a_first(x){return ldhelper();}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "z_last.js"),
		[]byte("function ldhelper(){return \"from-z\";}\nfunction z_last(x){return ldhelper();}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := glue.RegisterExtensionDir(dir); err != nil {
		t.Fatalf("RegisterExtensionDir: %v", err)
	}
	sess := extSession(t)
	// z_last.js loads last, so its ldhelper() wins for BOTH callers.
	for _, fn := range []string{"a_first", "z_last"} {
		got := extRawRows(t, sess, `SELECT RAW `+fn+`(0)`)
		if len(got) != 1 || got[0] != `"from-z"` {
			t.Fatalf("%s() = %v, want [\"from-z\"] (last-loaded helper wins)", fn, got)
		}
	}
}

// TestExtJSRuntimeModel covers the per-query/per-actor shared-runtime model:
// (a) a UDF can call another loaded UDF (shared JS scope); (b) console.log is
// available and writes to glue.JSConsoleWriter; (c) module-scope globals reset
// on each query (fresh runtime per Session.Run).
func TestExtJSRuntimeModel(t *testing.T) {
	// (a) cross-UDF call: outer() calls inner().
	if err := glue.RegisterJSFunc("rt_inner", `function rt_inner(x){ return x*10; }`); err != nil {
		t.Fatalf("register inner: %v", err)
	}
	if err := glue.RegisterJSFunc("rt_outer", `function rt_outer(x){ return rt_inner(x) + 1; }`); err != nil {
		t.Fatalf("register outer: %v", err)
	}
	sess := extSession(t)
	if got := extRawRows(t, sess, `SELECT RAW rt_outer(5)`); len(got) != 1 || got[0] != `51` {
		t.Fatalf("cross-UDF rt_outer(5) = %v, want [51]", got)
	}

	// (b) console.log writes to JSConsoleWriter.
	var logbuf bytes.Buffer
	saved := glue.JSConsoleWriter
	glue.JSConsoleWriter = &logbuf
	defer func() { glue.JSConsoleWriter = saved }()
	if err := glue.RegisterJSFunc("rt_logger", `function rt_logger(x){ console.log("saw", x); return x; }`); err != nil {
		t.Fatalf("register logger: %v", err)
	}
	if got := extRawRows(t, sess, `SELECT RAW rt_logger(7)`); len(got) != 1 || got[0] != `7` {
		t.Fatalf("rt_logger(7) = %v, want [7]", got)
	}
	if !strings.Contains(logbuf.String(), "saw 7") {
		t.Fatalf("console.log output = %q, want it to contain %q", logbuf.String(), "saw 7")
	}

	// (c) module-scope globals reset per query: a counter restarts at 1 each Run.
	if err := glue.RegisterJSFunc("rt_counter", `var n=0; function rt_counter(x){ return ++n; }`); err != nil {
		t.Fatalf("register counter: %v", err)
	}
	q := `SELECT RAW rt_counter(v) FROM [10,20,30] AS v`
	first := extRawRows(t, sess, q)
	second := extRawRows(t, sess, q)
	want := []string{"1", "2", "3"}
	if strings.Join(first, ",") != strings.Join(want, ",") {
		t.Fatalf("counter query 1 = %v, want %v", first, want)
	}
	if strings.Join(second, ",") != strings.Join(want, ",") {
		t.Fatalf("counter query 2 = %v, want %v (globals should reset per query)", second, want)
	}
}

// TestExtJSAggregate exercises a JS aggregate written to the 3-callback protocol
// (NAME_init / NAME_update / NAME_final), driven by the base.Agg bridge.
func TestExtJSAggregate(t *testing.T) {
	// product(): scalar accumulator state (a running product).
	if err := glue.RegisterJSAggregate("product", `
		function product_init()          { return 1; }
		function product_update(s, v)    { return s * v; }
		function product_final(s)        { return s; }
	`); err != nil {
		t.Fatalf("RegisterJSAggregate product: %v", err)
	}
	// jstats(): object-valued accumulator state {n,min,max} -> an object result.
	if err := glue.RegisterJSAggregate("jstats", `
		function jstats_init()        { return {n:0, min:null, max:null}; }
		function jstats_update(s, v)  {
			s.n++;
			if (s.min === null || v < s.min) s.min = v;
			if (s.max === null || v > s.max) s.max = v;
			return s;
		}
		function jstats_final(s)      { return s; }
	`); err != nil {
		t.Fatalf("RegisterJSAggregate jstats: %v", err)
	}

	sess := extSession(t)

	// Single group: product of 1..4 = 24.
	if got := extRawRows(t, sess, `SELECT RAW product(v) FROM [1,2,3,4] AS v`); len(got) != 1 || got[0] != `24` {
		t.Fatalf("product([1..4]) = %v, want [24]", got)
	}
	// Object-valued result.
	if got := extRawRows(t, sess, `SELECT RAW jstats(v) FROM [3,1,4,1,5,9,2,6] AS v`); len(got) != 1 || got[0] != `{"max":9,"min":1,"n":8}` {
		t.Fatalf("jstats(...) = %v, want [{\"max\":9,\"min\":1,\"n\":8}]", got)
	}
	// GROUP BY: product per group.
	got := extRawRows(t, sess, `SELECT x.g AS g, product(x.v) AS p
		FROM [{"g":"a","v":2},{"g":"a","v":3},{"g":"a","v":4},{"g":"b","v":5},{"g":"b","v":6}] AS x
		GROUP BY x.g ORDER BY x.g`)
	want := []string{`{"g":"a","p":24}`, `{"g":"b","p":30}`}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("grouped product = %v, want %v", got, want)
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

// TestExtRegisterExtensionDir exercises the directory-as-catalog registry and
// the single-file loader, with kind auto-detected from the file extension. A
// non-extension file (README.md) in the dir is skipped.
func TestExtRegisterExtensionDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "triple.js"),
		[]byte(`function triple(x) { return x * 3; }`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# not an extension"), 0o644); err != nil {
		t.Fatal(err)
	}
	names, err := glue.RegisterExtensionDir(dir)
	if err != nil {
		t.Fatalf("RegisterExtensionDir: %v", err)
	}
	if len(names) != 1 || names[0] != "triple" {
		t.Fatalf("RegisterExtensionDir names = %v, want [triple] (README.md skipped)", names)
	}

	// Single-file loader, kind auto-detected from ".js".
	f := filepath.Join(dir, "quadruple.js")
	if err := os.WriteFile(f, []byte(`function quadruple(x) { return x * 4; }`), 0o644); err != nil {
		t.Fatal(err)
	}
	name, err := glue.RegisterExtensionFile(f)
	if err != nil || name != "quadruple" {
		t.Fatalf("RegisterExtensionFile = (%q, %v), want (quadruple, nil)", name, err)
	}

	sess := extSession(t)
	if got := extRawRows(t, sess, `SELECT RAW triple(14)`); len(got) != 1 || got[0] != `42` {
		t.Fatalf("triple(14) = %v, want [42]", got)
	}
	if got := extRawRows(t, sess, `SELECT RAW quadruple(3)`); len(got) != 1 || got[0] != `12` {
		t.Fatalf("quadruple(3) = %v, want [12]", got)
	}

	// An unrecognized extension is a clean error, not a silent skip.
	bad := filepath.Join(dir, "thing.xyz")
	if err := os.WriteFile(bad, []byte("noop"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := glue.RegisterExtensionFile(bad); err == nil {
		t.Fatalf("RegisterExtensionFile(%q): expected unsupported-extension error", bad)
	}
}

// TestExtListUnloadReload exercises the extension registry: list reflects loads,
// unload disables (a call then errors) and drops it from the list, and reload
// re-enables. Uses uniquely-named UDFs so it doesn't depend on what other tests
// registered in the shared process.
func TestExtListUnloadReload(t *testing.T) {
	const fn = "extlisttest_dbl"
	loaded := func() bool {
		for _, e := range glue.ListExtensions() {
			if e.Name == fn {
				return true
			}
		}
		return false
	}

	if err := glue.RegisterJSFunc(fn, `function `+fn+`(x){return x*2;}`); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !loaded() {
		t.Fatalf("ListExtensions missing %q after load", fn)
	}
	sess := extSession(t)
	if got := extRawRows(t, sess, `SELECT RAW `+fn+`(21)`); len(got) != 1 || got[0] != `42` {
		t.Fatalf("%s(21) = %v, want [42]", fn, got)
	}

	// Unload: dropped from the list, and calling it now errors.
	if err := glue.UnloadExtension(fn); err != nil {
		t.Fatalf("unload: %v", err)
	}
	if loaded() {
		t.Fatalf("ListExtensions still has %q after unload", fn)
	}
	if _, err := sess.Run(`SELECT RAW ` + fn + `(21)`); err == nil {
		t.Fatalf("%s after unload: expected an error", fn)
	}
	// Unloading again is a clean error, not a panic.
	if err := glue.UnloadExtension(fn); err == nil {
		t.Fatalf("double-unload: expected 'not loaded' error")
	}

	// Reload re-enables (and does not trip the builtin-shadow guard).
	if err := glue.RegisterJSFunc(fn, `function `+fn+`(x){return x*3;}`); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := extRawRows(t, sess, `SELECT RAW `+fn+`(21)`); len(got) != 1 || got[0] != `63` {
		t.Fatalf("%s(21) after reload = %v, want [63]", fn, got)
	}
}

// TestExtShippedJSExamples loads the example UDFs shipped in
// extensions/functions/js and confirms they resolve and run, so the docs'
// examples can't silently rot.
func TestExtShippedJSExamples(t *testing.T) {
	names, err := glue.RegisterExtensionDir("../extensions/functions/js")
	if err != nil {
		t.Fatalf("RegisterExtensionDir(shipped): %v", err)
	}
	// Scalar UDFs (*.js), the decimal.js multi-export MODULE (a whole DECIMAL_* family
	// in one file), the geomean aggregate (*.agg.js), the series streaming source
	// (*.stream.js), sorted by filename stem.
	want := []string{"add_two_numbers", "celsius_to_fahrenheit", "decimal", "geomean", "series", "slugify"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("shipped extension names = %v, want %v", names, want)
	}

	sess := extSession(t)
	cases := []struct{ stmt, want string }{
		{`SELECT RAW add_two_numbers(2, 5)`, `7`},
		{`SELECT RAW celsius_to_fahrenheit(100)`, `212`},
		{`SELECT RAW slugify("Hello, World!")`, `"hello-world"`},
		// decimal.js multi-export module: exact decimal, loaded via the dir catalog.
		{`SELECT RAW DECIMAL_ADD("0.1", "0.2")`, `{"$numberDecimal":"0.3"}`},
		{`SELECT RAW ROUND(geomean(v), 4) FROM [1,2,4,8] AS v`, `2.8284`}, // 64^(1/4)
		{`SELECT RAW SUM(x.n) FROM series(1, 5) AS x`, `15`},              // streaming source
	}
	for _, c := range cases {
		got := extRawRows(t, sess, c.stmt)
		if len(got) != 1 || got[0] != c.want {
			t.Fatalf("%s => %v, want [%s]", c.stmt, got, c.want)
		}
	}
}

// TestExtJSStream exercises a JS streaming table-valued source (*.stream.js): a
// function that emits rows used in a FROM clause, composing with WHERE/aggregates,
// the batch (multi-arg emit) form, and a clear error when misused outside FROM.
func TestExtJSStream(t *testing.T) {
	if err := glue.RegisterJSStream("streamgen",
		`function streamgen(emit, n){ for (var i=1;i<=n;i++) emit({i:i, sq:i*i}); }`); err != nil {
		t.Fatalf("RegisterJSStream: %v", err)
	}
	if err := glue.RegisterJSStream("streamtwins",
		`function streamtwins(emit, n){ for (var i=1;i<=n;i++) emit({a:i},{a:i*10}); }`); err != nil {
		t.Fatalf("RegisterJSStream twins: %v", err)
	}
	sess := extSession(t)

	// Basic table-valued source in FROM.
	if got := extRawRows(t, sess, `SELECT x.i, x.sq FROM streamgen(4) AS x`); len(got) != 4 ||
		got[0] != `{"i":1,"sq":1}` || got[3] != `{"i":4,"sq":16}` {
		t.Fatalf("streamgen(4) = %v", got)
	}
	// Composes with WHERE + aggregate.
	if got := extRawRows(t, sess, `SELECT RAW SUM(x.sq) FROM streamgen(4) AS x WHERE x.i > 1`); len(got) != 1 || got[0] != `29` {
		t.Fatalf("SUM(sq) i>1 over 1..4 = %v, want [29]", got) // 4+9+16
	}
	// Batch form: emit(a, b) yields two rows per call.
	if got := extRawRows(t, sess, `SELECT RAW x.a FROM streamtwins(2) AS x`); len(got) != 4 ||
		got[0] != `1` || got[1] != `10` || got[2] != `2` || got[3] != `20` {
		t.Fatalf("streamtwins(2) = %v, want [1 10 2 20]", got)
	}
	// LIMIT yields the right rows (dropped downstream; source still finite here).
	if got := extRawRows(t, sess, `SELECT RAW x.i FROM streamgen(1000) AS x LIMIT 3`); len(got) != 3 {
		t.Fatalf("streamgen(1000) LIMIT 3 = %d rows, want 3", len(got))
	}
	// Misuse outside FROM -> clear error, not a crash.
	if _, err := sess.Run(`SELECT streamgen(3)`); err == nil || !strings.Contains(err.Error(), "FROM clause") {
		t.Fatalf("streamgen() outside FROM: want a 'FROM clause' error, got %v", err)
	}
}

// TestExtJSDecimalModule exercises a multi-export JS MODULE (DESIGN-extensions.md "JS
// modules"): one decimal.js file exports the whole DECIMAL_* family, and the functions
// do EXACT base-10 arithmetic (via BigInt) that float64 can't — DECIMAL_ADD(0.1, 0.2) is
// exactly 0.3, not 0.30000000000000004. Loads the shipped extensions/functions/js/
// decimal.js so the test also proves the module auto-detection + loader routing.
func TestExtJSDecimalModule(t *testing.T) {
	src, err := os.ReadFile("../extensions/functions/js/decimal.js")
	if err != nil {
		t.Fatal(err)
	}
	if err := glue.RegisterJSModule("decimal", string(src)); err != nil {
		t.Fatalf("RegisterJSModule: %v", err)
	}
	sess := extSession(t)

	cases := []struct{ stmt, want string }{
		// Exact — a plain SQL `0.1 + 0.2` would drift to 0.30000000000000004.
		{`SELECT RAW DECIMAL_ADD("0.1", "0.2")`, `{"$numberDecimal":"0.3"}`},
		{`SELECT RAW DECIMAL_ADD(0.1, 0.2)`, `{"$numberDecimal":"0.3"}`}, // number args stringify cleanly
		{`SELECT RAW DECIMAL_SUB("1", "0.9")`, `{"$numberDecimal":"0.1"}`},
		{`SELECT RAW DECIMAL_MUL("1.5", "1.5")`, `{"$numberDecimal":"2.25"}`},
		{`SELECT RAW DECIMAL_MUL("0.1", "0.1")`, `{"$numberDecimal":"0.01"}`},
		// Exact beyond float64's 2^53 integer precision.
		{`SELECT RAW DECIMAL_ADD("123456789012345678", "1")`, `{"$numberDecimal":"123456789012345679"}`},
		// Nesting round-trips through the EJSON-tagged form.
		{`SELECT RAW DECIMAL_ADD(DECIMAL_MUL("0.1","0.1"), "0.99")`, `{"$numberDecimal":"1"}`},
		// Comparison returns a plain -1/0/1 (marshal:"json"). 0.10 == 0.1.
		{`SELECT RAW DECIMAL_CMP("0.10", "0.1")`, `0`},
		{`SELECT RAW DECIMAL_CMP("0.2", "0.1")`, `1`},
		{`SELECT RAW DECIMAL_CMP("0.1", "0.2")`, `-1`},
	}
	for _, c := range cases {
		got := extRawRows(t, sess, c.stmt)
		if len(got) != 1 || got[0] != c.want {
			t.Fatalf("%s => %v, want [%s]", c.stmt, got, c.want)
		}
	}
}
