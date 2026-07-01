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

package recordsource

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// collect drains a Source into copied (id, doc) pairs, validating that each Doc
// is well-formed JSON. It copies because the slices are borrowed until Next.
func collect(t *testing.T, s Source) (ids []string, docs []string) {
	t.Helper()
	var rec Record
	for {
		ok, err := s.Next(&rec)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		var probe interface{}
		if err := json.Unmarshal(rec.Doc, &probe); err != nil {
			t.Fatalf("record %q not valid JSON: %v\n  %s", rec.ID, err, rec.Doc)
		}
		ids = append(ids, string(rec.ID))
		docs = append(docs, string(rec.Doc))
	}
	s.Close()
	return ids, docs
}

func ex(rel string) string { return filepath.Join("..", "examples", rel) }

func TestWalkMultiFileJSONL(t *testing.T) { // scenario C
	s, err := Walk(ex("logs/default/events"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 8 {
		t.Fatalf("want 8 event records, got %d (%v)", len(docs), ids)
	}
	// IDs are dir-relative path + record index within file.
	want0 := "2026-01-01.jsonl#0"
	if ids[0] != want0 {
		t.Errorf("first id = %q, want %q", ids[0], want0)
	}
}

func TestWalkRecursiveUnion(t *testing.T) { // scenario E
	s, err := Walk(ex("metrics/default/cpu"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 5 {
		t.Fatalf("want 5 cpu samples across nested dirs, got %d (%v)", len(docs), ids)
	}
	// A nested file's ID keeps its dir-relative path.
	found := false
	for _, id := range ids {
		if id == "hostA/2026/01/data-0001.jsonl#0" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a nested relative id, got %v", ids)
	}
}

func TestWalkGzip(t *testing.T) { // scenario H
	s, err := Walk(ex("archive/default/orders"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	_, docs := collect(t, s)
	if len(docs) != 5 {
		t.Fatalf("want 5 orders from gzip'd JSONL, got %d", len(docs))
	}
}

func TestWalkOneDocPerFile(t *testing.T) { // scenario A
	s, err := Walk(ex("shop/default/orders"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 3 {
		t.Fatalf("want 3 orders, got %d", len(docs))
	}
	// Single-doc files keep today's convention: META().id == file stem.
	for _, id := range ids {
		if id[:6] != "order-" {
			t.Errorf("one-doc-per-file id should be the stem, got %q", id)
		}
	}
}

func TestOpenFileSingleJSON(t *testing.T) {
	s, err := OpenFile(ex("shop/default/orders/order-1001.json"), "orders/order-1001.json")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 1 || ids[0] != "order-1001" {
		t.Fatalf("single-doc: got ids=%v docs=%d", ids, len(docs))
	}
}

func TestJSONArrayAndValueStream(t *testing.T) {
	dir := t.TempDir()
	arr := filepath.Join(dir, "a.json")
	if err := os.WriteFile(arr, []byte(`[{"x":1},{"x":2},{"x":3}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	stream := filepath.Join(dir, "b.json")
	if err := os.WriteFile(stream, []byte("{\"y\":1}\n{\"y\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		n    int
	}{{arr, 3}, {stream, 2}} {
		s, err := OpenFile(tc.path, "t.json")
		if err != nil {
			t.Fatal(err)
		}
		ids, docs := collect(t, s)
		if len(docs) != tc.n {
			t.Fatalf("%s: want %d docs, got %d", tc.path, tc.n, len(docs))
		}
		// Multi-record .json uses prefix#i IDs (not the stem).
		if ids[0] != "t.json#0" {
			t.Errorf("%s: first id = %q, want t.json#0", tc.path, ids[0])
		}
	}
}

// TestBorrowedSliceStableAfterCopy documents the borrow contract: the returned
// slices are only valid until the next Next, but a copy survives.
func TestBorrowedSliceStableAfterCopy(t *testing.T) {
	s, err := Walk(ex("logs/default/events"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var rec Record
	ok, err := s.Next(&rec)
	if err != nil || !ok {
		t.Fatalf("first Next: ok=%v err=%v", ok, err)
	}
	saved := append([]byte(nil), rec.Doc...)
	// Advance; the borrowed rec.Doc may now be overwritten, but saved is stable.
	s.Next(&rec)
	var probe interface{}
	if err := json.Unmarshal(saved, &probe); err != nil {
		t.Fatalf("copied doc corrupted after advance: %v", err)
	}
}

func TestParseModesRestriction(t *testing.T) {
	// Empty => flexible default.
	if o, err := ParseModes(""); err != nil || !o.Recurse || o.Formats != nil || !o.AllowGzip {
		t.Fatalf("empty modes should be AllModes, got %+v err=%v", o, err)
	}
	// A locked-down set: only JSONL, no gzip, no recurse.
	o, err := ParseModes("jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if o.Recurse || o.AllowGzip || o.Formats == nil {
		t.Fatalf("restricted modes leaked flexibility: %+v", o)
	}
	// Under this restriction, the gzip archive (needs gzip) yields nothing, and a
	// flat non-recursive walk of the events dir still finds the top-level .jsonl.
	if !o.eligible("events/2026-01-01.jsonl") || o.eligible("orders/2025.jsonl.gz") ||
		o.eligible("orders/order.json") {
		t.Errorf("eligibility wrong under jsonl-only restriction: %+v", o)
	}
	if _, err := ParseModes("json,bogus"); err == nil {
		t.Errorf("unknown mode token should error")
	}
}

// TestWalkAllocsFlat guards the allocation model: allocs/op must not scale with
// record count (borrowed slices, reused ID buffer). JSONL is the streaming path.
func TestWalkAllocsFlat(t *testing.T) {
	run := func() int {
		s, _ := Walk(ex("logs/default/events"), AllModes())
		var rec Record
		n := 0
		for {
			ok, _ := s.Next(&rec)
			if !ok {
				break
			}
			n++
		}
		s.Close()
		return n
	}
	// Warm any one-time cost, then measure per-record allocs across the JSONL walk.
	run()
	avg := testing.AllocsPerRun(20, func() { run() })
	// 8 records across 3 files; opening files + scanners dominates. Assert the
	// per-run allocs stay small and file-bounded (not per-record growth).
	if avg > 120 {
		t.Errorf("allocs/run = %.0f, higher than expected for a small walk", avg)
	}
}
