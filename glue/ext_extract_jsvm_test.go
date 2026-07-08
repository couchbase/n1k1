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

package glue

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/n1k1/records"
)

// nsLogKeyspaceFixture is a small ns_server-style multiline log (lead line +
// continuation lines), claimed by the BUILT-IN records ns_server_log recipe.
const nsLogKeyspaceFixture = `[ns_server:info,2026-05-17T15:36:11.198+02:00,n1@host:normal]started rebalance
  moving vbucket 42 to node n2@host
[stats:warn,2026-05-17T15:36:12.500+02:00,n2@host:normal]slow operation detected
[couch_log:error,2026-05-17T15:36:10.000+02:00,n1@host:default]connection failure
`

// TestExtractRecipeNativeDifferential proves recipes are wired into the real FROM
// scan path end-to-end AND that the result is identical through the interpreter and
// the standalone compiled child (interp == compiler). A keyspace whose only file is
// an ns_server-style .log is scanned via the built-in native recipe: describe() once
// per file, then records.SpecApply per record -- yielding typed ts/level/node/msg
// rows with the timestamp normalized to int64 epoch-nanos.
func TestExtractRecipeNativeDifferential(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "logs")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	// The name matches the built-in recipe's `ns_server\..*\.log$` claim.
	if err := os.WriteFile(filepath.Join(ks, "ns_server.debug.log"),
		[]byte(nsLogKeyspaceFixture), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()

	// The proof query: typed columns off the recipe-extracted records. No ORDER BY --
	// runRows sorts the row strings for comparison, and the ORDER-BY (heap) op has an
	// unrelated compiled-child codegen gap (Track C), which this data-layer test must
	// not depend on. The normalized int64 ts is still exercised as a projected column.
	const q = "SELECT l.node, l.`level`, l.ts, l.msg FROM logs l"

	interp := runRows(t, sess, q)
	want := []string{
		`{"node":"n1@host","level":"error","ts":1779024970000000000,"msg":"connection failure"}`,
		`{"node":"n1@host","level":"info","ts":1779024971198000000,"msg":"started rebalance\n  moving vbucket 42 to node n2@host"}`,
		`{"node":"n2@host","level":"warn","ts":1779024972500000000,"msg":"slow operation detected"}`,
	}
	sort.Strings(want)
	if len(interp) != len(want) {
		t.Fatalf("interp rows = %v, want %v", interp, want)
	}
	for i := range want {
		if interp[i] != want[i] {
			t.Fatalf("interp row %d = %s, want %s", i, interp[i], want[i])
		}
	}

	// interp == compiler: rerun the same query through the standalone compiled child.
	if !GoToolchainDetect().Available {
		t.Skip("no go toolchain: compiled EXECUTE degrades to the interpreter (interp path already checked)")
	}
	repo, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("N1K1_SRC", repo)
	sess.PrepareLevel = PrepareCompiledFull

	if _, err := sess.Run("PREPARE plogs AS " + q); err != nil {
		t.Fatalf("PREPARE: %v", err)
	}
	compiled := runRows(t, sess, "EXECUTE plogs")
	if len(compiled) != len(interp) {
		t.Fatalf("compiled rows = %v, want (interp) %v", compiled, interp)
	}
	for i := range interp {
		if compiled[i] != interp[i] {
			t.Fatalf("compiled != interp at row %d:\n compiled=%s\n interp  =%s", i, compiled[i], interp[i])
		}
	}
	if sess.prepareds["plogs"].compiledBin == "" {
		t.Error("recipe scan of SELECT ... FROM logs should have compiled to a standalone child")
	}
}

// appLogFixture is a simple single-line-per-record app log the JS extract recipe
// below parses. Distinct from ns_server format so the JS-produced spec is what does
// the work (the built-in recipe does not claim these files).
const appLogFixture = `2026-05-17T15:36:11.198Z INFO node-a starting up
2026-05-17T15:36:12.500Z WARN node-b slow query
2026-05-17T15:36:10.000Z ERROR node-a disk full
`

// jsAppLogRecipe is an inline *.extract.js recipe: module-scope `match` claims
// `myapp.*.log` files, and describe(file) returns a line-framed ExtractSpec with
// named-capture fields and an RFC3339 timestamp. describe runs once per file in
// goja; per-row extraction runs natively via records.SpecApply.
const jsAppLogRecipe = `
var match = { exts: [".log"], names: ["myapp\\..*\\.log$"], priority: 20 };

function describe(file) {
  // Prove describe actually runs in JS (once per file) by peeking at the head.
  if (file.head.indexOf("INFO") < 0 && file.head.indexOf("ERROR") < 0) {
    throw "unexpected app-log head: " + file.head.slice(0, 20);
  }
  return {
    format: "myapp_log_js",
    framing: { kind: "line" },
    fields:  { pattern: "^(?P<ts>\\S+) (?P<level>\\S+) (?P<node>\\S+) (?P<msg>.*)" },
    time:    { field: "ts", layout: "RFC3339" },
    order:   { by: "ts", sorted: "near" }
  };
}
`

// TestJSExtractRecipeEndToEnd registers a JS *.extract.js recipe and proves that a
// real SQL FROM over a matching log keyspace returns typed rows produced by the
// JS-authored describe() + native SpecApply. (The JS recipe lives in the parent's
// records registry, so this is the interpreter path; the standalone compiled child
// is a separate process without it -- see the native-recipe differential above.)
func TestJSExtractRecipeEndToEnd(t *testing.T) {
	if err := RegisterJSExtractRecipe("myapp_log", jsAppLogRecipe); err != nil {
		t.Fatalf("RegisterJSExtractRecipe: %v", err)
	}

	// The recipe is registered in the records registry and claims myapp.*.log files.
	rp := records.RecipeFor("myapp.debug.log")
	if rp == nil || rp.Name != "myapp_log" {
		t.Fatalf("RecipeFor(myapp.debug.log) = %v, want the myapp_log recipe", rp)
	}
	if records.RecipeFor("server.log") != nil {
		t.Errorf("myapp_log recipe should not claim a bare server.log")
	}

	// describe() (run in JS) produces the expected declarative spec + measured meta.
	spec, meta, err := rp.Describe(writeAppLog(t))
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if spec.Format != "myapp_log_js" || spec.Framing.Kind != records.FramingLine ||
		spec.Time == nil || spec.Time.Field != "ts" {
		t.Fatalf("JS-produced spec unexpected: %+v", spec)
	}
	if meta.RecordCount != 3 || meta.SortKeyLabel != "ts" {
		t.Errorf("measured meta = %+v, want 3 records sorted by ts", meta)
	}

	// End-to-end: a keyspace whose file is claimed by the JS recipe.
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "app")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "myapp.debug.log"),
		[]byte(appLogFixture), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()

	got := runRows(t, sess,
		"SELECT a.node, a.`level`, a.ts, a.msg FROM app a ORDER BY a.ts")
	want := []string{
		`{"node":"node-a","level":"ERROR","ts":1779032170000000000,"msg":"disk full"}`,
		`{"node":"node-a","level":"INFO","ts":1779032171198000000,"msg":"starting up"}`,
		`{"node":"node-b","level":"WARN","ts":1779032172500000000,"msg":"slow query"}`,
	}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %s, want %s", i, got[i], want[i])
		}
	}

	// The normalized ts is an int64 epoch-nanos NUMBER (not a string), as the extract
	// layer promises -- so it sorts and compares as time.
	var row map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got[0]), &row); err != nil {
		t.Fatal(err)
	}
	if _, err := row["ts"].MarshalJSON(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(row["ts"]), `"`) {
		t.Errorf("ts should be a JSON number, got %s", row["ts"])
	}

	// A WHERE on an extracted field flows through the native scan.
	one := runRows(t, sess, "SELECT a.node FROM app a WHERE a.`level` = \"ERROR\"")
	if len(one) != 1 || one[0] != `{"node":"node-a"}` {
		t.Errorf(`WHERE level='ERROR' = %v, want [{"node":"node-a"}]`, one)
	}

	// A recipe with a `match` that claims nothing is rejected at registration.
	if err := RegisterJSExtractRecipe("bad", `var match = {}; function describe(f){return {};}`); err == nil {
		t.Error("expected an error registering a recipe whose match claims nothing")
	}
	// A recipe with no describe() is rejected.
	if err := RegisterJSExtractRecipe("nodesc", `var match = {exts:[".zq"]};`); err == nil {
		t.Error("expected an error registering a recipe with no describe()")
	}
}

// TestRegisterExtractRecipeFile proves the ext.go file-loader branch: a "*.extract.js"
// file is routed to RegisterJSExtractRecipe (not the generic scalar-UDF loader) and
// registers a records.Recipe. It loads the shipped example so the example stays valid.
func TestRegisterExtractRecipeFile(t *testing.T) {
	repo, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(repo, "extensions", "extract_recipes", "apache_access.extract.js")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("example recipe not present: %v", err)
	}
	name, err := RegisterExtensionFile(path)
	if err != nil {
		t.Fatalf("RegisterExtensionFile(%s): %v", path, err)
	}
	if name != "apache_access" {
		t.Errorf("registered name = %q, want apache_access", name)
	}
	// It went to the extract-recipe registry, not the SQL-function registry.
	found := false
	for _, r := range ListExtractRecipes() {
		if r.Name == "apache_access" && r.Source == path {
			found = true
		}
	}
	if !found {
		t.Errorf("apache_access not in ListExtractRecipes(): %+v", ListExtractRecipes())
	}
	if records.RecipeFor("access.log") == nil {
		t.Errorf("apache_access recipe should claim access.log")
	}
}

func writeAppLog(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "myapp.debug.log")
	if err := os.WriteFile(p, []byte(appLogFixture), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// nsLogKeyspaceFixture ts values normalized to int64 epoch-nanos (see the
// differential test above): error@15:36:10.000 is the min, warn@15:36:12.500 the max,
// and the file order (info, warn, error) puts the min LAST -- so the source is
// near-sorted with a ~2.5s disorder window.
const (
	nsMinKeyNanos = int64(1779024970000000000) // 2026-05-17T15:36:10.000+02:00
	nsMaxKeyNanos = int64(1779024972500000000) // 2026-05-17T15:36:12.500+02:00
)

// writeNSLogKeyspace lays out a datastore root with one ns_server-style .log keyspace
// (default/logs) claimed by the built-in records recipe, and registers the root so
// describe results memoize under <root>/.n1k1/. Returns the root and the log path.
func writeNSLogKeyspace(t *testing.T) (root, logPath string) {
	t.Helper()
	root = t.TempDir()
	ks := filepath.Join(root, "default", "logs")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath = filepath.Join(ks, "ns_server.debug.log")
	if err := os.WriteFile(logPath, []byte(nsLogKeyspaceFixture), 0o644); err != nil {
		t.Fatal(err)
	}
	registerDataRoot(root) // OpenSession would do this; do it directly for the unit path.
	return root, logPath
}

// extractCacheFile is the sidecar JSON path a file's memoized describe lands at.
func extractCacheFile(t *testing.T, root, logPath string) string {
	t.Helper()
	absRoot, _ := filepath.Abs(root)
	absLog, _ := filepath.Abs(logPath)
	rel, err := filepath.Rel(absRoot, absLog)
	if err != nil {
		t.Fatalf("Rel(%s,%s): %v", absRoot, absLog, err)
	}
	return extractCachePath(absRoot, rel)
}

func readExtractCacheEntry(t *testing.T, path string) extractCacheEntry {
	t.Helper()
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache %s: %v", path, err)
	}
	var ent extractCacheEntry
	if err := json.Unmarshal(blob, &ent); err != nil {
		t.Fatalf("decode cache %s: %v", path, err)
	}
	return ent
}

// TestExtractCacheMemoizesDescribe proves the .n1k1 sidecar makes describe() run
// ONCE per file: the first open runs describe (a cache miss, cache file written); a
// second open of the UNCHANGED file reads the cache and skips describe; and a CHANGED
// file (size/mtime bumped) re-describes and rewrites the cache with the new
// fingerprint. Uses a counting DescribeFunc so the run/skip is observed directly,
// independent of the global counters.
func TestExtractCacheMemoizesDescribe(t *testing.T) {
	root, logPath := writeNSLogKeyspace(t)

	var calls int32
	counting := func(path string) (records.ExtractSpec, records.SortedSourceMeta, error) {
		atomic.AddInt32(&calls, 1)
		return records.NSLogDescribe(path)
	}

	hits0 := atomic.LoadInt64(&ExtractCacheHits)
	cacheFile := extractCacheFile(t, root, logPath)

	// First open: cache miss -> describe runs once, cache file written.
	if _, _, err := describeMemoized(logPath, counting, "fp-A"); err != nil {
		t.Fatalf("describeMemoized #1: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("after first open, describe calls = %d, want 1", got)
	}
	ent1 := readExtractCacheEntry(t, cacheFile)
	if ent1.Relpath != filepath.Join("default", "logs", "ns_server.debug.log") {
		t.Errorf("cached relpath = %q", ent1.Relpath)
	}
	if ent1.Spec.Format != "ns_server_log" || ent1.Meta.SortKeyLabel != "ts" {
		t.Errorf("cached spec/meta unexpected: spec=%+v meta=%+v", ent1.Spec, ent1.Meta)
	}

	// Second open of the UNCHANGED file: cache hit -> describe NOT re-run.
	if _, _, err := describeMemoized(logPath, counting, "fp-A"); err != nil {
		t.Fatalf("describeMemoized #2: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("after second (unchanged) open, describe calls = %d, want still 1", got)
	}
	if hits := atomic.LoadInt64(&ExtractCacheHits); hits <= hits0 {
		t.Errorf("ExtractCacheHits did not advance on the unchanged re-open (%d -> %d)", hits0, hits)
	}

	// Change the file (append a record -> size AND mtime differ): fingerprint mismatch
	// forces a re-describe and a cache rewrite.
	changed := nsLogKeyspaceFixture +
		"[ns_server:info,2026-05-17T15:36:13.000+02:00,n3@host:normal]added record\n"
	if err := os.WriteFile(logPath, []byte(changed), 0o644); err != nil {
		t.Fatal(err)
	}
	// Bump mtime explicitly too, so the mtime half of the fingerprint is exercised
	// even on a filesystem with coarse mtime resolution.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(logPath, future, future); err != nil {
		t.Fatal(err)
	}

	if _, _, err := describeMemoized(logPath, counting, "fp-A"); err != nil {
		t.Fatalf("describeMemoized #3 (changed): %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("after changed open, describe calls = %d, want 2 (re-described)", got)
	}
	ent2 := readExtractCacheEntry(t, cacheFile)
	if ent2.Size == ent1.Size {
		t.Errorf("cache size not rewritten: still %d", ent2.Size)
	}
	if ent2.MtimeNanos == ent1.MtimeNanos {
		t.Errorf("cache mtime not rewritten: still %d", ent2.MtimeNanos)
	}
	if ent2.Meta.RecordCount != 4 {
		t.Errorf("re-described record count = %d, want 4", ent2.Meta.RecordCount)
	}
}

// TestExtractCacheInvalidatesOnProducerChange proves the config_fingerprint guard
// (DESIGN-data.md §5): an UNCHANGED file (same size/mtime) whose CLAIMING RECIPE
// changed -- a different recipe fingerprint, or equivalently a bumped engine producer
// version -- re-describes rather than serving the stale spec. This is what caught the
// framing:json staleness: identical (relpath,size,mtime) but describe now produces a
// different spec, so the fingerprint alone must not be trusted.
func TestExtractCacheInvalidatesOnProducerChange(t *testing.T) {
	root, logPath := writeNSLogKeyspace(t)

	var calls int32
	counting := func(path string) (records.ExtractSpec, records.SortedSourceMeta, error) {
		atomic.AddInt32(&calls, 1)
		return records.NSLogDescribe(path)
	}
	cacheFile := extractCacheFile(t, root, logPath)

	// First open with recipe fingerprint "fp-old": describe runs, cache written with
	// its producer fingerprint.
	if _, _, err := describeMemoized(logPath, counting, "fp-old"); err != nil {
		t.Fatalf("describeMemoized #1: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("after first open, describe calls = %d, want 1", got)
	}
	ent1 := readExtractCacheEntry(t, cacheFile)
	if ent1.Producer != producerFingerprint("fp-old") {
		t.Errorf("cached producer = %q, want %q", ent1.Producer, producerFingerprint("fp-old"))
	}

	// Re-open the UNCHANGED file but with a DIFFERENT recipe fingerprint (a recipe
	// edit): the fingerprint (relpath,size,mtime) still matches, but the producer does
	// not -> re-describe, and the cache is rewritten with the new producer.
	if _, _, err := describeMemoized(logPath, counting, "fp-new"); err != nil {
		t.Fatalf("describeMemoized #2 (changed recipe): %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("changed recipe fingerprint should re-describe; calls = %d, want 2", got)
	}
	ent2 := readExtractCacheEntry(t, cacheFile)
	if ent2.Producer != producerFingerprint("fp-new") {
		t.Errorf("rewritten producer = %q, want %q", ent2.Producer, producerFingerprint("fp-new"))
	}

	// Re-open again with the new fingerprint: now it is a hit (describe not re-run).
	if _, _, err := describeMemoized(logPath, counting, "fp-new"); err != nil {
		t.Fatalf("describeMemoized #3: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("stable recipe fingerprint should hit the cache; calls = %d, want still 2", got)
	}

	// A pre-invalidation entry (no producer field) must not be trusted: simulate one by
	// writing an entry with a matching fingerprint but an empty producer.
	stale := ent2
	stale.Producer = ""
	writeExtractCache(cacheFile, stale)
	if _, _, err := describeMemoized(logPath, counting, "fp-new"); err != nil {
		t.Fatalf("describeMemoized #4 (legacy entry): %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("a producer-less legacy entry should re-describe; calls = %d, want 3", got)
	}
}

// TestSortedSourceMetaFor proves the Track-B lookup seam returns the normalized
// sorted-source contract for a recipe-matched file: the int64 epoch-nanos min/max key
// zone map, the sort-key label, and the measured near-sorted classification. It also
// checks the keyspace-level helper and that a non-recipe file yields ok=false.
func TestSortedSourceMetaFor(t *testing.T) {
	root, logPath := writeNSLogKeyspace(t)

	meta, ok, err := SortedSourceMetaFor(logPath)
	if err != nil {
		t.Fatalf("SortedSourceMetaFor: %v", err)
	}
	if !ok {
		t.Fatal("SortedSourceMetaFor: recipe-matched .log reported ok=false")
	}
	if meta.SortKeyLabel != "ts" {
		t.Errorf("SortKeyLabel = %q, want ts", meta.SortKeyLabel)
	}
	if meta.MinKey != nsMinKeyNanos || meta.MaxKey != nsMaxKeyNanos {
		t.Errorf("zone map = [%d,%d], want [%d,%d]",
			meta.MinKey, meta.MaxKey, nsMinKeyNanos, nsMaxKeyNanos)
	}
	if meta.RecordCount != 3 {
		t.Errorf("RecordCount = %d, want 3", meta.RecordCount)
	}
	// File order (info, warn, error) puts the min-key record last -> near-sorted with a
	// ~2.5s disorder window (the max lateness measured off the sample).
	if meta.Sortedness != records.SortedNear {
		t.Errorf("Sortedness = %q, want %q", meta.Sortedness, records.SortedNear)
	}
	if meta.Disorder.WindowNanos != nsMaxKeyNanos-nsMinKeyNanos {
		t.Errorf("Disorder.WindowNanos = %d, want %d",
			meta.Disorder.WindowNanos, nsMaxKeyNanos-nsMinKeyNanos)
	}

	// A file no recipe claims (plain JSON) has no sorted-source contract.
	plain := filepath.Join(filepath.Dir(logPath), "plain.json")
	if err := os.WriteFile(plain, []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := SortedSourceMetaFor(plain); err != nil || ok {
		t.Errorf("SortedSourceMetaFor(plain.json) = ok %v err %v, want ok=false", ok, err)
	}

	// The keyspace-level helper resolves the same file + meta from the store keyspace.
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()
	ns, err := sess.Store.Datastore.NamespaceByName("default")
	if err != nil {
		t.Fatalf("NamespaceByName: %v", err)
	}
	logsKS, err := ns.KeyspaceByName("logs")
	if err != nil {
		t.Fatalf("KeyspaceByName(logs): %v", err)
	}
	metas, err := SortedSourceMetasForKeyspace(logsKS, nil)
	if err != nil {
		t.Fatalf("SortedSourceMetasForKeyspace: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("keyspace metas = %d, want 1", len(metas))
	}
	if metas[0].Meta.MinKey != nsMinKeyNanos || metas[0].Meta.MaxKey != nsMaxKeyNanos {
		t.Errorf("keyspace meta zone map = [%d,%d], want [%d,%d]",
			metas[0].Meta.MinKey, metas[0].Meta.MaxKey, nsMinKeyNanos, nsMaxKeyNanos)
	}
	if metas[0].Relpath != filepath.Join("default", "logs", "ns_server.debug.log") {
		t.Errorf("keyspace meta relpath = %q", metas[0].Relpath)
	}
}
