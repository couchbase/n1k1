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
	"testing"
)

// TestCensus pins the schema census: per-type grouping, depth-2 paths, a born field
// (coverage + first_seen), a polymorphic field surfaced as two rows with disjoint
// windows, and the coverage denominator.
func TestCensus(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "e.jsonl"), []byte(joinLines([]string{
		`{"type":"assistant","timestamp":"2026-06-21T00:00:00Z","model":"opus","usage":{"in":10}}`,
		`{"type":"assistant","timestamp":"2026-06-24T00:00:00Z","model":"opus","usage":{"in":20},"effort":"high"}`,
		`{"type":"assistant","timestamp":"2026-07-01T00:00:00Z","model":"opus","usage":"BROKEN"}`,
		`{"type":"state","key":"mode","val":"auto"}`,
	})), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	res, err := sess.Census("events", CensusOptions{})
	if err != nil {
		t.Fatalf("Census: %v", err)
	}
	if res.Records != 4 {
		t.Fatalf("records: want 4, got %d", res.Records)
	}
	if res.TypeTotals["assistant"] != 3 || res.TypeTotals["state"] != 1 {
		t.Fatalf("type totals: %v", res.TypeTotals)
	}

	find := func(typ, path, vt string) *CensusRow {
		for i := range res.Rows {
			r := &res.Rows[i]
			if r.Type == typ && r.Path == path && r.ValType == vt {
				return r
			}
		}
		return nil
	}

	// `effort` was born on 06-24: 1/3 coverage, first_seen 06-24.
	if e := find("assistant", "effort", "string"); e == nil || e.Docs != 1 || e.FirstSeen != "2026-06-24T00:00:00Z" {
		t.Fatalf("effort (born field): %+v", e)
	}

	// `usage` is polymorphic — an object on 2 docs (until 06-24) and a string on 1
	// (from 07-01): TWO rows with disjoint windows, not one lossy summary.
	uo := find("assistant", "usage", "object")
	us := find("assistant", "usage", "string")
	if uo == nil || uo.Docs != 2 || uo.LastSeen != "2026-06-24T00:00:00Z" {
		t.Fatalf("usage:object: %+v", uo)
	}
	if us == nil || us.Docs != 1 || us.FirstSeen != "2026-07-01T00:00:00Z" {
		t.Fatalf("usage:string: %+v", us)
	}

	// depth-2 path.
	if find("assistant", "usage.in", "number") == nil {
		t.Fatal("missing depth-2 path usage.in")
	}
	// per-type isolation: state's key/val don't leak into assistant.
	if find("assistant", "key", "string") != nil {
		t.Fatal("state field leaked into assistant type")
	}
	if find("state", "key", "string") == nil {
		t.Fatal("missing state.key")
	}

	// --depth 1 suppresses the nested path.
	res1, err := sess.Census("events", CensusOptions{Depth: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res1.Rows {
		if r.Path == "usage.in" {
			t.Fatal("--depth 1 should not descend into usage")
		}
	}
}
