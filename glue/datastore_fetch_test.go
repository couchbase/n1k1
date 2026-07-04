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

// TestDatastoreFetchModesDifferential: DatastoreFetch's inline and actor drivers
// must return identical results across query shapes (native fetch, container
// .jsonl, join, subquery, SELECT *). (runRows is defined in discard_elision_test.go.)
func TestDatastoreFetchModesDifferential(t *testing.T) {
	dir := t.TempDir()

	// A classic <key>.json keyspace (native path).
	ks := filepath.Join(dir, "default", "nums")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	for i, d := range []string{`{"a":1,"g":"x"}`, `{"a":2,"g":"x"}`, `{"a":3,"g":"y"}`} {
		if err := os.WriteFile(filepath.Join(ks, "n"+string(rune('0'+i))+".json"), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A multi-record .jsonl keyspace (container path).
	logs := filepath.Join(dir, "default", "evts")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logs, "e.jsonl"),
		[]byte(`{"t":"a"}`+"\n"+`{"t":"b"}`+"\n"+`{"t":"c"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	queries := []string{
		"SELECT COUNT(*) AS c FROM (SELECT 1 FROM nums a, nums b) t",                 // join over native fetch
		"SELECT a FROM nums WHERE a >= 2 ORDER BY a",                                 // native fetch + filter
		"SELECT * FROM nums n ORDER BY n.a",                                          // native SELECT *
		"SELECT COUNT(*) AS c FROM evts",                                             // container .jsonl scan
		"SELECT t FROM evts ORDER BY t",                                              // container fetch + project
		"SELECT COUNT(*) AS c FROM nums n WHERE n.a IN (SELECT RAW m.a FROM nums m)", // subquery
	}

	restore := DatastoreFetchActor
	defer func() { DatastoreFetchActor = restore }()

	for _, q := range queries {
		DatastoreFetchActor = false // inline
		inline := runRows(t, sess, q)
		DatastoreFetchActor = true // concurrent actor
		actor := runRows(t, sess, q)
		if len(inline) != len(actor) {
			t.Errorf("%q: inline %d rows vs actor %d\n inline=%v\n actor=%v", q, len(inline), len(actor), inline, actor)
			continue
		}
		for i := range inline {
			if inline[i] != actor[i] {
				t.Errorf("%q: mode changed results\n inline=%v\n actor=%v", q, inline, actor)
				break
			}
		}
	}
}
