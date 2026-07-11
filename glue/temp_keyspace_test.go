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
	"reflect"
	"strings"
	"testing"
)

// (runRows -- run a statement, return its rows as a sorted []string -- lives in
// optimize_test.go and is reused here.)

// TestTempKeyspaceRoundTrip is the headline (IDEA-0027): CREATE TEMP KEYSPACE ... AS
// <select> materializes a query's rows in memory, and a LATER statement in the same
// session reads them back via `FROM <name>` -- no file, no re-parse across processes.
func TestTempKeyspaceRoundTrip(t *testing.T) {
	dir := insertTestDir(t) // `logs` keyspace: 4 rows, 2 sev=ERROR
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	res, err := sess.Run(`CREATE TEMP KEYSPACE errs AS ` +
		`SELECT l.id, l.code FROM logs l WHERE l.sev = "ERROR"`)
	if err != nil {
		t.Fatalf("CREATE TEMP KEYSPACE: %v", err)
	}
	if res.Count != 1 || len(res.Rows) != 1 {
		t.Fatalf("create summary rows=%d count=%d, want 1/1", len(res.Rows), res.Count)
	}

	// Read it back in the same session.
	if rows := runRows(t, sess, "SELECT COUNT(*) AS n FROM errs"); len(rows) != 1 ||
		rows[0] != `{"n":2}` {
		t.Fatalf("COUNT(*) FROM errs = %v, want [{\"n\":2}]", rows)
	}

	// Content round-trips: the two ERROR ids read back from the materialized rows.
	if rows := runRows(t, sess, "SELECT e.id FROM errs e"); !reflect.DeepEqual(rows,
		[]string{`{"id":"a"}`, `{"id":"b"}`}) {
		t.Fatalf("SELECT id FROM errs = %v, want [a b]", rows)
	}
}

// TestTempKeyspaceJoinAndChain covers the flagship staged use case: JOIN two
// materialized finding sets, and build a temp keyspace FROM other temp keyspaces.
func TestTempKeyspaceJoinAndChain(t *testing.T) {
	dir := insertTestDir(t)
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	if _, err := sess.Run(`CREATE TEMP KEYSPACE errs AS SELECT l.id, l.code FROM logs l WHERE l.sev="ERROR"`); err != nil {
		t.Fatalf("create errs: %v", err)
	}
	if _, err := sess.Run(`CREATE TEMP KEYSPACE lowcode AS SELECT l.id FROM logs l WHERE l.code < 3`); err != nil {
		t.Fatalf("create lowcode: %v", err)
	}

	// JOIN across the two in-memory keyspaces: ERROR rows (a:5, b:1) x code<3 (b:1, c:2)
	// -> only id "b" is in both.
	rows := runRows(t, sess, "SELECT e.id FROM errs e JOIN lowcode c ON e.id = c.id")
	if !reflect.DeepEqual(rows, []string{`{"id":"b"}`}) {
		t.Fatalf("temp JOIN temp = %v, want [b]", rows)
	}

	// Chain: a temp keyspace built FROM other temp keyspaces.
	if _, err := sess.Run("CREATE TEMP KEYSPACE both AS SELECT e.id FROM errs e JOIN lowcode c ON e.id=c.id"); err != nil {
		t.Fatalf("create both: %v", err)
	}
	if rows := runRows(t, sess, "SELECT COUNT(*) AS n FROM both"); !reflect.DeepEqual(rows, []string{`{"n":1}`}) {
		t.Fatalf("chained temp count = %v, want 1", rows)
	}
}

// TestTempKeyspaceReplaceDropGuards covers CREATE-exists / OR REPLACE / DROP / DROP
// IF EXISTS and that a dropped keyspace is gone.
func TestTempKeyspaceReplaceDropGuards(t *testing.T) {
	dir := insertTestDir(t)
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	if _, err := sess.Run(`CREATE TEMP KEYSPACE k AS SELECT l.id FROM logs l`); err != nil {
		t.Fatalf("create k: %v", err)
	}
	// CREATE on an existing name errors without OR REPLACE.
	if _, err := sess.Run(`CREATE TEMP KEYSPACE k AS SELECT 1 AS x`); err == nil ||
		!strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected already-exists error, got %v", err)
	}
	// OR REPLACE swaps it (now 1 row).
	if _, err := sess.Run(`CREATE OR REPLACE TEMP KEYSPACE k AS SELECT 1 AS x`); err != nil {
		t.Fatalf("create or replace: %v", err)
	}
	if rows := runRows(t, sess, "SELECT COUNT(*) AS n FROM k"); !reflect.DeepEqual(rows, []string{`{"n":1}`}) {
		t.Fatalf("after replace count = %v, want 1", rows)
	}

	// DROP a missing one errors; IF EXISTS is quiet.
	if _, err := sess.Run("DROP TEMP KEYSPACE missing"); err == nil {
		t.Fatal("expected error dropping missing temp keyspace")
	}
	if _, err := sess.Run("DROP TEMP KEYSPACE IF EXISTS missing"); err != nil {
		t.Fatalf("DROP IF EXISTS missing: %v", err)
	}

	// DROP the real one; it's then unresolvable.
	if _, err := sess.Run("DROP TEMP KEYSPACE k"); err != nil {
		t.Fatalf("drop k: %v", err)
	}
	if _, err := sess.Run("SELECT COUNT(*) FROM k"); err == nil {
		t.Fatal("expected error querying dropped temp keyspace")
	}
}

// TestParseTempKeyspaceStmt pins the recognizer: it claims exactly the CREATE/DROP
// TEMP KEYSPACE forms and leaves every ordinary statement for the real parser.
func TestParseTempKeyspaceStmt(t *testing.T) {
	claim := []struct {
		in                  string
		kind, name, inner   string
		orReplace, ifExists bool
	}{
		{in: "CREATE TEMP KEYSPACE foo AS SELECT 1", kind: "create", name: "foo", inner: "SELECT 1"},
		{in: "create temporary keyspace Bar as select * from x", kind: "create", name: "Bar", inner: "select * from x"},
		{in: "CREATE OR REPLACE TEMP KEYSPACE `a.b` AS SELECT 1;", kind: "create", name: "a.b", inner: "SELECT 1", orReplace: true},
		{in: "  DROP TEMP KEYSPACE foo ", kind: "drop", name: "foo"},
		{in: "DROP TEMPORARY KEYSPACE IF EXISTS `q` ;", kind: "drop", name: "q", ifExists: true},
	}
	for _, c := range claim {
		got, ok := parseTempKeyspaceStmt(c.in)
		if !ok {
			t.Errorf("%q: not recognized, want %s", c.in, c.kind)
			continue
		}
		if got.kind != c.kind || got.name != c.name || got.inner != c.inner ||
			got.orReplace != c.orReplace || got.ifExists != c.ifExists {
			t.Errorf("%q: got %+v", c.in, got)
		}
	}

	reject := []string{
		"SELECT 1",
		"CREATE INDEX i ON k(a)",
		"CREATE COLLECTION c",
		"CREATED TEMP KEYSPACE x AS SELECT 1", // CREATED, not CREATE
		"CREATE TEMP KEYSPACE foo",            // no AS
		"CREATE TEMP KEYSPACE foo AS ",        // empty inner
		"DROP TEMP KEYSPACE foo bar",          // trailing junk
		"CREATE TEMP TABLE foo AS SELECT 1",   // TABLE, not KEYSPACE
		"INSERT INTO k (KEY UUID(), VALUE self) SELECT * FROM x",
	}
	for _, in := range reject {
		if _, ok := parseTempKeyspaceStmt(in); ok {
			t.Errorf("%q: wrongly recognized as a temp-keyspace statement", in)
		}
	}
}
