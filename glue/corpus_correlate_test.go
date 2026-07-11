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
	"sort"
	"testing"
)

// correlateSession writes two keyspaces (errors, state) with a ts field and opens it.
func correlateSession(t *testing.T) *Session {
	t.Helper()
	dir := t.TempDir()
	for _, ks := range []string{"errors", "state"} {
		d := filepath.Join(dir, "default", ks)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "a.jsonl"),
			[]byte(`{"ts":1,"msg":"a"}`+"\n"+`{"ts":2,"msg":"b"}`+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return sess
}

// TestCorpusCorrelationGrouping (Part B foundation): temporal-correlation detectors are
// recognized and grouped by their (left, right, key, direction) signature -- two
// nearest-PRECEDING errors->state-by-ts detectors share a group; a nearest-FOLLOWING one
// is a separate group. They still run standalone (this slice only surfaces the grouping).
func TestCorpusCorrelationGrouping(t *testing.T) {
	sess := correlateSession(t)

	preceding := func() string {
		return "SELECT e.ts AS ts, (SELECT r.msg FROM default:state r WHERE r.ts <= e.ts " +
			"ORDER BY r.ts DESC LIMIT 1) AS state_at FROM default:errors e"
	}
	following := "SELECT e.ts AS ts, (SELECT r.msg FROM default:state r WHERE r.ts >= e.ts " +
		"ORDER BY r.ts ASC LIMIT 1) AS next_state FROM default:errors e"

	dets := []CorpusDetector{
		{Tag: "p1", Stmt: preceding()},
		{Tag: "p2", Stmt: preceding() + " WHERE e.msg = \"x\""}, // same sig (outer WHERE doesn't change it)
		{Tag: "f1", Stmt: following},                            // different direction -> different sig
	}
	cc, err := sess.CorpusCompile(dets)
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}

	// All three are correlation detectors -> still standalone (no execution change here).
	if len(cc.Standalone) != 3 {
		t.Fatalf("expected 3 standalone correlation detectors, got %d", len(cc.Standalone))
	}

	// Two groups: {p1,p2} (preceding) and {f1} (following).
	if len(cc.CorrelationGroups) != 2 {
		t.Fatalf("expected 2 correlation groups, got %d: %v", len(cc.CorrelationGroups), cc.CorrelationGroups)
	}
	var shared []string
	for _, tags := range cc.CorrelationGroups {
		if len(tags) == 2 {
			shared = append(shared, tags...)
			sort.Strings(shared)
		}
	}
	if want := []string{"p1", "p2"}; len(shared) != 2 || shared[0] != want[0] || shared[1] != want[1] {
		t.Errorf("shared preceding group = %v, want [p1 p2]", shared)
	}
}
