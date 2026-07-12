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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// writeBundleFile writes body to <root>/<rel>, creating parent dirs.
func writeBundleFile(t *testing.T, root, rel, body string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The equivalent record contents both bundles carry, laid out under DIFFERENTLY-named
// files. `indexer` records (a level field), `orders` records (a total field).
const (
	bundleIndexerBody = `{"id":"i1","sev":"ERROR","msg":"index build failed"}` + "\n" +
		`{"id":"i2","sev":"INFO","msg":"index ready"}` + "\n" +
		`{"id":"i3","sev":"ERROR","msg":"scan timeout"}` + "\n"
	bundleOrdersBody = `{"id":"o1","total":150,"cust":"a"}` + "\n" +
		`{"id":"o2","total":50,"cust":"b"}` + "\n" +
		`{"id":"o3","total":300,"cust":"c"}` + "\n"
)

// theManifest is the ONE bundle-independent manifest both bundles resolve through:
// the stable logical vocabulary the detector corpus is authored against. Bare
// (root-relative) `**` globs, so each bundle's differently-named files are matched.
func theManifest() Binding {
	return Binding{
		"indexer_log": "**/indexer*.jsonl",
		"orders":      "**/orders*.jsonl",
	}
}

// theCorpus is the bundle-independent detector corpus. TWO detectors on the logical
// keyspace `indexer_log` (must FUSE into one shared scan) plus one on `orders`.
func theCorpus() []CorpusDetector {
	return []CorpusDetector{
		{Label: "idx_error", Stmt: `SELECT * FROM indexer_log l WHERE l.sev = "ERROR"`},
		{Label: "idx_timeout", Stmt: `SELECT * FROM indexer_log l WHERE l.msg = "scan timeout"`},
		{Label: "big_order", Stmt: `SELECT * FROM orders o WHERE o.total > 100`},
	}
}

func findingTags(fs []Finding) map[string]int {
	m := map[string]int{}
	for _, f := range fs {
		m[f.Label]++
	}
	return m
}

// TestBindingTwoBundles is THE payoff: the SAME detector corpus + SAME manifest,
// run against TWO differently-named bundles, produces the expected findings from
// each -- with NO detector edits, only a re-bind to the new bundle root. It also
// asserts the two `FROM indexer_log` detectors FUSE into a single shared scan (the
// union-all has exactly TWO children -- one per logical keyspace -- not three).
func TestBindingTwoBundles(t *testing.T) {
	// Bundle A: files named indexer.jsonl / orders.jsonl, under subdirs.
	rootA := t.TempDir()
	writeBundleFile(t, rootA, "logs/indexer.jsonl", bundleIndexerBody)
	writeBundleFile(t, rootA, "data/orders.jsonl", bundleOrdersBody)

	// Bundle B: SAME contents, DIFFERENTLY-named files (indexer_2024.jsonl /
	// orders_2024Q4.jsonl), in different subdirs.
	rootB := t.TempDir()
	writeBundleFile(t, rootB, "node1/indexer_2024.jsonl", bundleIndexerBody)
	writeBundleFile(t, rootB, "misc/orders_2024Q4.jsonl", bundleOrdersBody)

	for _, bundle := range []struct {
		name string
		root string
	}{{"A", rootA}, {"B", rootB}} {
		t.Run("bundle_"+bundle.name, func(t *testing.T) {
			sess, err := OpenSessionBound(bundle.root, "default", theManifest())
			if err != nil {
				t.Fatalf("OpenSessionBound: %v", err)
			}
			defer sess.Close()

			cc, err := sess.CorpusCompile(theCorpus())
			if err != nil {
				t.Fatalf("CorpusCompile: %v", err)
			}
			if len(cc.Rejected) != 0 {
				t.Fatalf("unexpected rejected detectors: %+v", cc.Rejected)
			}
			if len(cc.Standalone) != 0 {
				t.Fatalf("unexpected standalone detectors: %+v", cc.Standalone)
			}

			// FUSION: two logical keyspaces (indexer_log, orders) -> union-all of TWO
			// broadcast-indexed fan-outs. If the two `FROM indexer_log` detectors had
			// NOT fused (distinct QualifiedName each), there would be THREE children.
			if cc.Plan == nil || cc.Plan.Kind != "union-all" || len(cc.Plan.Children) != 2 {
				t.Fatalf("plan = %s; want union-all of 2 (indexer_log fused + orders)", dumpPlan(cc.Plan))
			}
			// Each per-keyspace op scans its keyspace exactly ONCE (one
			// datastore-scan-records leaf under each broadcast).
			for _, bc := range cc.Plan.Children {
				if bc.Kind != "broadcast-indexed" {
					t.Fatalf("per-keyspace op = %q, want broadcast-indexed", bc.Kind)
				}
				if n := countScans(bc); n != 1 {
					t.Fatalf("per-keyspace op has %d datastore-scan-records leaves, want 1 (shared scan)", n)
				}
			}

			findings, err := cc.Run()
			if err != nil {
				t.Fatalf("Run: %v", err)
			}

			// Expected findings, identical for both bundles (equivalent contents):
			//   idx_error   -> 2 ERROR rows (i1, i3)
			//   idx_timeout -> 1 row (i3)
			//   big_order   -> 2 rows (o1 total 150, o3 total 300)
			got := findingTags(findings)
			want := map[string]int{"idx_error": 2, "idx_timeout": 1, "big_order": 2}
			for label, n := range want {
				if got[label] != n {
					t.Errorf("bundle %s: label %q findings = %d, want %d (all=%v)",
						bundle.name, label, got[label], n, got)
				}
			}
			for label := range got {
				if _, ok := want[label]; !ok {
					t.Errorf("bundle %s: unexpected finding label %q", bundle.name, label)
				}
			}
			// t.Logf("bundle %s findings: %v", bundle.name, got)
		})
	}
}

// countScans counts datastore-scan-records leaves in an op subtree -- a scan-once
// check for the fused per-keyspace plan.
func countScans(op *base.Op) int {
	if op == nil {
		return 0
	}
	n := 0
	if op.Kind == "datastore-scan-records" {
		n++
	}
	for _, c := range op.Children {
		n += countScans(c)
	}
	return n
}

// TestBindingFailLoudEmptyGlob: a manifest entry whose glob matches NOTHING must be a
// HARD ERROR at resolution -- never a silently empty (falsely "clean") result.
func TestBindingFailLoudEmptyGlob(t *testing.T) {
	root := t.TempDir()
	writeBundleFile(t, root, "logs/indexer.jsonl", bundleIndexerBody)

	// `orders` is bound but its pattern matches no file in this bundle.
	b := Binding{
		"indexer_log": "**/indexer*.jsonl",
		"orders":      "**/orders*.jsonl", // no such file here.
	}
	sess, err := OpenSessionBound(root, "default", b)
	if err != nil {
		t.Fatalf("OpenSessionBound: %v", err)
	}
	defer sess.Close()

	_, err = sess.Run(`SELECT * FROM orders o WHERE o.total > 100`)
	if err == nil {
		t.Fatal("expected a hard error for an empty-glob logical keyspace, got nil (silently clean!)")
	}
	if !strings.Contains(err.Error(), "orders") {
		t.Errorf("empty-glob error should name the logical keyspace; got: %v", err)
	}
	// t.Logf("fail-loud (empty glob): %v", err)

	// The other bound keyspace still resolves fine (the empty one didn't poison it).
	if _, err := sess.Run(`SELECT * FROM indexer_log l WHERE l.sev = "ERROR"`); err != nil {
		t.Fatalf("bound indexer_log should still resolve: %v", err)
	}
}

// TestBindingFailLoudUnbound: a name that is neither in the manifest nor a real
// keyspace hits the normal loud "no keyspace" error -- the binding wrapper delegates
// it down and does not swallow it.
func TestBindingFailLoudUnbound(t *testing.T) {
	root := t.TempDir()
	writeBundleFile(t, root, "logs/indexer.jsonl", bundleIndexerBody)

	sess, err := OpenSessionBound(root, "default", Binding{"indexer_log": "**/indexer*.jsonl"})
	if err != nil {
		t.Fatalf("OpenSessionBound: %v", err)
	}
	defer sess.Close()

	_, err = sess.Run(`SELECT * FROM unbound_name x`)
	if err == nil {
		t.Fatal("expected an error for an unbound, non-existent keyspace, got nil")
	}
	// t.Logf("fail-loud (unbound): %v", err)
}

// TestBindingDelegationIntact: with a binding installed, NON-bound names still work
// exactly as before -- a real (classic <ns>/<keyspace>) keyspace, and an inline glob
// keyspace, both resolve through the delegated chain.
func TestBindingDelegationIntact(t *testing.T) {
	root := t.TempDir()
	// A classic default:<keyspace> layout keyspace (real, not bound).
	writeBundleFile(t, root, "default/events/e.jsonl",
		`{"id":"e1","act":"login"}`+"\n"+`{"id":"e2","act":"logout"}`+"\n")
	// A file for the inline-glob path.
	writeBundleFile(t, root, "raw/a.json", `{"v":42}`)

	// Bind only indexer_log (which doesn't even exist here -- unused in this test).
	sess, err := OpenSessionBound(root, "default", Binding{"indexer_log": "**/indexer*.jsonl"})
	if err != nil {
		t.Fatalf("OpenSessionBound: %v", err)
	}
	defer sess.Close()

	// (1) The real keyspace resolves via delegation.
	res, err := sess.Run(`SELECT RAW e.act FROM events e ORDER BY e.act`)
	if err != nil {
		t.Fatalf("real keyspace Run: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("real keyspace rows = %d, want 2 (%v)", len(res.Rows), rowsAsStrings(res.Rows))
	}

	// (2) An inline glob keyspace (a glob-shaped name, NOT in the manifest) resolves
	// via delegation down to the glob wrapper.
	res, err = sess.Run("SELECT RAW x.v FROM `raw/*.json` AS x")
	if err != nil {
		t.Fatalf("inline glob Run: %v", err)
	}
	if len(res.Rows) != 1 || strings.TrimSpace(string(res.Rows[0])) != "42" {
		t.Fatalf("inline glob rows = %v, want [42]", rowsAsStrings(res.Rows))
	}
}

// TestBindingSameLogicalFusesScanOnce is a focused fusion check: several detectors
// all on ONE logical keyspace collapse to a single-keyspace broadcast with exactly
// one shared scan (no union-all wrapper).
func TestBindingSameLogicalFusesScanOnce(t *testing.T) {
	root := t.TempDir()
	writeBundleFile(t, root, "n/indexer_A.jsonl", bundleIndexerBody)

	sess, err := OpenSessionBound(root, "default", Binding{"indexer_log": "**/indexer*.jsonl"})
	if err != nil {
		t.Fatalf("OpenSessionBound: %v", err)
	}
	defer sess.Close()

	cc, err := sess.CorpusCompile([]CorpusDetector{
		{Label: "a", Stmt: `SELECT * FROM indexer_log l WHERE l.sev = "ERROR"`},
		{Label: "b", Stmt: `SELECT * FROM indexer_log l WHERE l.msg = "scan timeout"`},
		{Label: "c", Stmt: `SELECT * FROM indexer_log l`},
	})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	if len(cc.Standalone) != 0 || len(cc.Rejected) != 0 {
		t.Fatalf("unexpected non-fused: standalone=%+v rejected=%+v", cc.Standalone, cc.Rejected)
	}
	// One logical keyspace -> a bare broadcast-indexed (no union-all), one shared scan.
	if cc.Plan == nil || cc.Plan.Kind != "broadcast-indexed" {
		t.Fatalf("plan = %s; want a single broadcast-indexed (all detectors share one scan)", dumpPlan(cc.Plan))
	}
	if n := countScans(cc.Plan); n != 1 {
		t.Fatalf("shared plan has %d scans, want exactly 1", n)
	}

	findings, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := findingTags(findings)
	// a: 2 ERROR, b: 1 timeout, c: all 3 rows.
	want := map[string]int{"a": 2, "b": 1, "c": 3}
	for label, n := range want {
		if got[label] != n {
			t.Errorf("label %q = %d, want %d (all=%v)", label, got[label], n, got)
		}
	}
}
