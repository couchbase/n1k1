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
	"testing"

	"github.com/couchbase/n1k1/engine"
)

// TestSessionPipeInlineData runs a real SQL++ query whose data comes from an
// in-memory DatastorePipe rather than the files on disk: the plan is still built
// against the Store (schema resolution needs the keyspace), but at run time the
// scan reads the pipe's INLINE records. The inline data deliberately differs from
// the files, so a correct result proves the pipe overrode the datastore.
func TestSessionPipeInlineData(t *testing.T) {
	// A file datastore with keyspace "beers" (its docs have i = 0..2), used only so
	// the planner can resolve `FROM beers`.
	root := writePlainBeers(t, 3)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	// Point the session at inline data instead. The scan's alias is "b"
	// (FROM beers b), so the pipe keys its records under "b".
	s.Pipe = &engine.MemPipe{Data: map[string][]engine.MemRecord{
		"b": {
			{ID: "x1", Doc: []byte(`{"i":100,"name":"inline-a"}`)},
			{ID: "x2", Doc: []byte(`{"i":200,"name":"inline-b"}`)},
			{ID: "x3", Doc: []byte(`{"i":300,"name":"inline-c"}`)},
		},
	}}

	// A filter + projection over the inline data. Values (100/200/300) exist only
	// in the pipe, not in the files (which have 0/1/2) -- so matching them proves
	// the query read the pipe.
	res, err := s.Run(`SELECT b.i, b.name FROM beers b WHERE b.i >= 200 ORDER BY b.i`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`{"i":200,"name":"inline-b"}`, `{"i":300,"name":"inline-c"}`}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows %v, want %d", len(res.Rows), res.Rows, len(want))
	}
	for i, w := range want {
		if string(res.Rows[i]) != w {
			t.Errorf("row %d = %s, want %s", i, res.Rows[i], w)
		}
	}

	// META().id comes from the pipe's record IDs, confirming ^id flows through.
	res2, err := s.Run(`SELECT META(b).id AS id FROM beers b WHERE b.i = 100`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Rows) != 1 || string(res2.Rows[0]) != `{"id":"x1"}` {
		t.Errorf(`META().id rows = %v, want [{"id":"x1"}]`, res2.Rows)
	}
}
