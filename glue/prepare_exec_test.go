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
	"strings"
	"testing"

	"github.com/couchbase/query/value"
)

// TestPrepareExecute exercises the interpreter-level PREPARE/EXECUTE surface: a
// PREPARE caches an inner statement in the session; a later EXECUTE binds params
// (from a USING clause or request-level Session args) and runs it. Mirrors the
// cbq-derived case_prepare.json semantics. Beers are keyed by filename stem
// (b000, b001, ...) so USE KEYS $1 selects a known doc. See DESIGN-prepare.md.
func TestPrepareExecute(t *testing.T) {
	root := writePlainBeers(t, 5)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	run1 := func(stmt string) string { // run, require exactly one row, return it
		t.Helper()
		res, err := s.Run(stmt)
		if err != nil {
			t.Fatalf("Run(%q) err: %v", stmt, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("Run(%q) rows = %v, want exactly 1", stmt, res.Rows)
		}
		return string(res.Rows[0])
	}

	// PREPARE caches, returns no rows, and does not execute the inner statement.
	if res, err := s.Run(`PREPARE p1 FROM SELECT b.i FROM beers b USE KEYS $1`); err != nil {
		t.Fatalf("PREPARE p1: %v", err)
	} else if len(res.Rows) != 0 {
		t.Fatalf("PREPARE p1 returned rows %v, want none", res.Rows)
	}

	// EXECUTE with a USING positional arg ($1).
	if got := run1(`EXECUTE p1 USING ["b001"]`); got != `{"i":1}` {
		t.Errorf(`EXECUTE p1 USING ["b001"] = %s, want {"i":1}`, got)
	}

	// EXECUTE with a request-level positional arg (Session.PositionalArgs), no USING.
	s.PositionalArgs = value.Values{value.NewValue("b002")}
	if got := run1(`EXECUTE p1`); got != `{"i":2}` {
		t.Errorf(`EXECUTE p1 (request $1=b002) = %s, want {"i":2}`, got)
	}

	// USING and request-level params are mutually exclusive (cbq rejects both).
	if _, err := s.Run(`EXECUTE p1 USING ["b001"]`); err == nil {
		t.Error("EXECUTE with both USING and request params should error")
	} else if !strings.Contains(err.Error(), "cannot have both USING clause and request parameters") {
		t.Errorf("both-params error = %q, want the cbq 'cannot have both' message", err)
	}
	s.PositionalArgs = nil

	// EXECUTE with a USING named arg ($k).
	if _, err := s.Run(`PREPARE p2 FROM SELECT b.i FROM beers b USE KEYS $k`); err != nil {
		t.Fatalf("PREPARE p2: %v", err)
	}
	if got := run1(`EXECUTE p2 USING {"k": "b003"}`); got != `{"i":3}` {
		t.Errorf(`EXECUTE p2 USING {"k":"b003"} = %s, want {"i":3}`, got)
	}

	// A no-arg prepared statement round-trips too.
	if _, err := s.Run(`PREPARE p3 FROM SELECT 1 + 2 AS x`); err != nil {
		t.Fatalf("PREPARE p3: %v", err)
	}
	if got := run1(`EXECUTE p3`); got != `{"x":3}` {
		t.Errorf(`EXECUTE p3 = %s, want {"x":3}`, got)
	}

	// EXECUTE of an unknown name errors cleanly (not a panic).
	if _, err := s.Run(`EXECUTE nope`); err == nil {
		t.Error("EXECUTE of an unknown prepared statement should error")
	} else if !strings.Contains(err.Error(), "No such prepared statement") {
		t.Errorf("unknown-prepared error = %q, want 'No such prepared statement'", err)
	}
}

// TestPrepareAsSpelling checks that the `PREPARE <name> AS <stmt>` spelling parses
// and works, alongside the `FROM` spelling the cbq corpus uses -- confirming the
// cbq-compatible `AS` surface DESIGN-prepare.md advertises.
func TestPrepareAsSpelling(t *testing.T) {
	root := writePlainBeers(t, 3)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Run(`PREPARE q AS SELECT 6 * 7 AS x`); err != nil {
		t.Fatalf("PREPARE ... AS: %v", err)
	}
	res, err := s.Run(`EXECUTE q`)
	if err != nil {
		t.Fatalf("EXECUTE q: %v", err)
	}
	if len(res.Rows) != 1 || string(res.Rows[0]) != `{"x":42}` {
		t.Errorf(`EXECUTE q rows = %v, want [{"x":42}]`, res.Rows)
	}
}
