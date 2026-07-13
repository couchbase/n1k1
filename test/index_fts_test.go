//go:build n1ql && !wasm

package test

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// writeJSONLKeyspace writes a single multi-record .jsonl file (like a real bundle
// keyspace, e.g. master_events) -- records get framing-generated CONTAINER ids
// (`<relpath>#<line>@<offset>`), NOT the doc's own "id", unlike the one-file-per-doc
// writeKeyspaceDocs. This is the shape that exposed the FTS fetch bug (IDEA-0030).
func writeJSONLKeyspace(t *testing.T, keyspace string, lines []string, catalog string) string {
	t.Helper()
	root := t.TempDir()
	ksDir := filepath.Join(root, "default", keyspace)
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := ""
	for _, ln := range lines {
		body += ln + "\n"
	}
	if err := os.WriteFile(filepath.Join(ksDir, keyspace+".jsonl"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	sc := filepath.Join(root, ".n1k1")
	if err := os.MkdirAll(sc, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sc, "catalog.json"), []byte(catalog), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

// TestFTSMultiRecordFetch (IDEA-0030): an FTS index over a MULTI-RECORD file keyspace
// must return its hits. The hit ids are container ids that cbq's Keyspace.Fetch can't
// resolve, so DatastoreScanFTS must read them via n1k1's byte-path reader. Regression:
// before the fix, every SEARCH over such a keyspace returned zero rows, and an FTS
// index silently turned an equality predicate (served via the flex path) into empty
// results. Covers identifier-style tokens (camelCase), field-scoped SEARCH, and the
// implicit-predicate flex path.
func TestFTSMultiRecordFetch(t *testing.T) {
	lines := []string{
		`{"id":"e1","etype":"seqnoWaitingStarted","node":"n1"}`,
		`{"id":"e2","etype":"seqnoWaitingStarted","node":"n2"}`,
		`{"id":"e3","etype":"autoFailoverNodeStateChange","node":"n1"}`,
		`{"id":"e4","etype":"rebalanceStart","node":"n3"}`,
	}
	catalog := `{"indexes":[{"name":"ft_events","keyspace":"events","kind":"fts"}]}`
	root := writeJSONLKeyspace(t, "events", lines, catalog)

	run := func(stmt string) []string {
		store, conv := flatRootConv(t, root, stmt)
		if !hasKind(conv.TopOp, "datastore-scan-fts") {
			t.Fatalf("%q: expected an FTS scan, got %v", stmt, opKinds(conv.TopOp))
		}
		got := idJSONs(flatRootRows(t, conv, testGlueExec(t, false, store, conv)))
		sort.Strings(got)
		return got
	}
	both := []string{`{"id":"e1"}`, `{"id":"e2"}`}

	cases := []struct {
		name, stmt string
		want       []string
	}{
		{"whole-doc-lowercased", `SELECT d.id AS id FROM events d WHERE SEARCH(d, "seqnowaitingstarted")`, both},
		{"whole-doc-camel", `SELECT d.id AS id FROM events d WHERE SEARCH(d, "seqnoWaitingStarted")`, both},
		{"field-scoped-camel", `SELECT d.id AS id FROM events d WHERE SEARCH(d.etype, "seqnoWaitingStarted")`, both},
		{"other-term", `SELECT d.id AS id FROM events d WHERE SEARCH(d, "rebalanceStart")`, []string{`{"id":"e4"}`}},
		// The flex path: a plain equality predicate served by the FTS index (bleve
		// superset + residual filter). Must return the exact matches, not empty.
		{"equality-via-flex", `SELECT d.id AS id FROM events d WHERE d.etype = "seqnoWaitingStarted"`, both},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := run(tc.stmt); !equalStrs(got, tc.want) {
				t.Errorf("%q: want %v, got %v", tc.stmt, tc.want, got)
			}
		})
	}
}

// TestFTSSearchByKeyspaceName (IDEA-0033): SEARCH() naming the KEYSPACE -- as support
// engineers naturally write it (`SEARCH(events, "q")`, vs the FROM alias `SEARCH(e,"q")`)
// -- must RETRIEVE rows, not just COUNT them. cbq hands the keyspace name to the fts scan
// as a bleve FIELD (field="events"); no document has such a field, so a match query found
// nothing and every row-emitting projection silently returned 0, while COUNT(*) (a covered
// path) still worked -- the confusing "counts but can't fetch" split. The keyspace-name
// form must behave like the alias form (a whole-keyspace search); a genuinely field-scoped
// SEARCH(alias.field) must still work (not be broadened).
func TestFTSSearchByKeyspaceName(t *testing.T) {
	lines := []string{
		`{"id":"e1","etype":"seqnoWaitingStarted","node":"n1"}`,
		`{"id":"e2","etype":"rebalanceStart","node":"n2"}`,
		`{"id":"e3","etype":"seqnoWaitingStarted","node":"n3"}`,
	}
	catalog := `{"indexes":[{"name":"ft_events","keyspace":"events","kind":"fts"}]}`
	root := writeJSONLKeyspace(t, "events", lines, catalog)

	ids := func(stmt string) []string {
		store, conv := flatRootConv(t, root, stmt)
		if !hasKind(conv.TopOp, "datastore-scan-fts") {
			t.Fatalf("%q: expected an FTS scan, got %v", stmt, opKinds(conv.TopOp))
		}
		got := idJSONs(flatRootRows(t, conv, testGlueExec(t, false, store, conv)))
		sort.Strings(got)
		return got
	}
	want := []string{`{"id":"e1"}`, `{"id":"e3"}`}

	cases := []struct{ name, stmt string }{
		{"by-keyspace-name", `SELECT e.id AS id FROM events e WHERE SEARCH(events,"seqnowaitingstarted")`},
		{"by-alias", `SELECT e.id AS id FROM events e WHERE SEARCH(e,"seqnowaitingstarted")`},
		{"by-keyspace-name-unaliased", `SELECT id FROM events WHERE SEARCH(events,"seqnowaitingstarted")`},
		// A real field-scoped search must NOT be broadened to whole-doc by the fix.
		{"field-scoped", `SELECT e.id AS id FROM events e WHERE SEARCH(e.etype,"seqnoWaitingStarted")`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ids(tc.stmt); !equalStrs(got, want) {
				t.Errorf("%q: want %v, got %v", tc.stmt, want, got)
			}
		})
	}
}
