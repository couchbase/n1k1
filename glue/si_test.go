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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/records"
)

// jsonlDocs builds n JSONL lines, each with a unique high-cardinality "sku" and a
// low-cardinality "kind" -- so the advisor should suggest "sku" but never "kind".
func jsonlDocs(n int) []string {
	lines := make([]string, n)
	for i := 0; i < n; i++ {
		kind := "a"
		if i%2 == 1 {
			kind = "b"
		}
		lines[i] = fmt.Sprintf(`{"sku":"SKU-%04d","kind":%q}`, i, kind)
	}
	return lines
}

func hasField(sugg []IndexSuggestion, field string) *IndexSuggestion {
	for i := range sugg {
		if sugg[i].Field == field {
			return &sugg[i]
		}
	}
	return nil
}

func drainCount(t *testing.T, src records.Source) int {
	t.Helper()
	n := 0
	var rec records.Record
	for {
		ok, err := src.Next(&rec)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		n++
	}
	return n
}

// TestSuggestIndexesSingleFile: .index suggest samples a single multi-record file
// (the layout the old .schema mis-handled). If sampling regressed to one-doc-per-
// *.json it would see 0 docs and suggest nothing.
func TestSuggestIndexesSingleFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "products.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(jsonlDocs(12), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := FileStore(path)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}

	sugg, _, err := SuggestIndexes(store, "default", "", 0)
	if err != nil {
		t.Fatalf("SuggestIndexes: %v", err)
	}
	sku := hasField(sugg, "sku")
	if sku == nil {
		t.Fatalf("expected a suggestion for high-cardinality 'sku', got %+v", sugg)
	}
	if sku.Sampled != 12 || sku.Keyspace != "products" {
		t.Errorf("sku suggestion sampled=%d keyspace=%q, want 12/products", sku.Sampled, sku.Keyspace)
	}
	if hasField(sugg, "kind") != nil {
		t.Errorf("low-cardinality 'kind' should not be suggested: %+v", sugg)
	}
}

// TestSuggestIndexesFlatRoot: same, but the keyspace is a flat-root dir whose
// records are spread across multiple files (must union them).
func TestSuggestIndexesFlatRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "inv")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := jsonlDocs(12)
	if err := os.WriteFile(filepath.Join(root, "a.jsonl"), []byte(strings.Join(docs[:6], "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.jsonl"), []byte(strings.Join(docs[6:], "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := FileStore(root)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}

	sugg, _, err := SuggestIndexes(store, "default", "", 0)
	if err != nil {
		t.Fatalf("SuggestIndexes: %v", err)
	}
	sku := hasField(sugg, "sku")
	if sku == nil || sku.Sampled != 12 {
		t.Fatalf("expected 'sku' suggestion sampled across both files (12), got %+v", sugg)
	}
	if sku.Keyspace != "inv" {
		t.Errorf("keyspace = %q, want inv", sku.Keyspace)
	}
}

// TestSuggestIndexesSmallSampleNote: a tiny sample (fewer than suggestMinDistinct
// docs) yields no suggestions but an explanatory note -- this is the examples/
// archive case (5 gzip'd docs) that read as "broken" without the diagnostic.
func TestSuggestIndexesSmallSampleNote(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orders.jsonl")
	// 5 docs, every field unique -> selective, but 5 < suggestMinDistinct(8).
	if err := os.WriteFile(path, []byte(strings.Join(jsonlDocs(5), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := FileStore(path)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}

	sugg, note, err := SuggestIndexes(store, "default", "", 0)
	if err != nil {
		t.Fatalf("SuggestIndexes: %v", err)
	}
	if len(sugg) != 0 {
		t.Fatalf("expected no suggestions from a 5-doc sample, got %+v", sugg)
	}
	if !strings.Contains(note, "5") || !strings.Contains(note, "too few") {
		t.Errorf("note should flag the tiny sample, got %q", note)
	}
}

// TestOpenKeyspaceRecords: the shared resolver opens the right records for both a
// single-file and a directory keyspace -- the one seam both the scan op and the
// suggest sampler depend on.
func TestOpenKeyspaceRecords(t *testing.T) {
	// Single file.
	fdir := t.TempDir()
	fpath := filepath.Join(fdir, "events.jsonl")
	if err := os.WriteFile(fpath, []byte(strings.Join(jsonlDocs(5), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Flat-root dir.
	ddir := filepath.Join(t.TempDir(), "many")
	if err := os.MkdirAll(ddir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ddir, "x.jsonl"), []byte(strings.Join(jsonlDocs(7), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		path string
		want int
	}{{fpath, 5}, {ddir, 7}} {
		store, err := FileStore(tc.path)
		if err != nil {
			t.Fatalf("FileStore(%q): %v", tc.path, err)
		}
		ns, err := store.Datastore.NamespaceByName("default")
		if err != nil {
			t.Fatalf("NamespaceByName: %v", err)
		}
		names, err := ns.KeyspaceNames()
		if err != nil || len(names) != 1 {
			t.Fatalf("KeyspaceNames: %v names=%v", err, names)
		}
		ks, err := ns.KeyspaceByName(names[0])
		if err != nil {
			t.Fatalf("KeyspaceByName: %v", err)
		}
		src, err := openKeyspaceRecords(ks, records.AllModes())
		if err != nil {
			t.Fatalf("openKeyspaceRecords(%q): %v", tc.path, err)
		}
		got := drainCount(t, src)
		src.Close()
		if got != tc.want {
			t.Errorf("%q: opened %d records, want %d", tc.path, got, tc.want)
		}
	}
}
