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

package test

import (
	"encoding/json"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// Local unit tests for LET / WITH / LETTING over the test/data:orders keyspace
// (4 docs: id 1200/abc, 1234/bbb, 1235/ccc, 1236/ccc; each with orderlines).
// These don't use the suite corpus -- they exercise glue.VisitLet / VisitWith
// and the SELECT-* binding-name strip directly with explicit expected values.

// runQuery parses, plans, converts and executes stmt against the local
// data: file store, failing the test on any error, and returns the result rows.
func runQuery(t *testing.T, stmt string) []base.Vals {
	t.Helper()

	store, _, conv, err := testFileStoreSelect(t, stmt, false)
	if err != nil {
		t.Fatalf("stmt %q: convert err: %v", stmt, err)
	}
	if conv == nil || conv.TopOp == nil {
		t.Fatalf("stmt %q: nil TopOp (unsupported)", stmt)
	}

	return testGlueExec(t, false, store, conv)
}

// rowObj unmarshals a single-label result row (a JSON object) into a map.
func rowObj(t *testing.T, row base.Vals) map[string]interface{} {
	t.Helper()
	if len(row) != 1 {
		t.Fatalf("expected row with 1 label, got %d: %+v", len(row), row)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(row[0], &m); err != nil {
		t.Fatalf("unmarshal %s: %v", row[0], err)
	}
	return m
}

// -------------------------------------------------------------------
// LET

// LET binding of a constant, projected by name.
func TestLetConst(t *testing.T) {
	rows := runQuery(t, `SELECT foo FROM data:orders AS a LET foo = 1`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d: %+v", len(rows), rows)
	}
	for _, row := range rows {
		if len(row) != 1 || string(row[0]) != "1" {
			t.Fatalf("expected foo==1, got %+v", row)
		}
	}
}

// LET binding derived from a document field, projected alongside another field.
func TestLetField(t *testing.T) {
	rows := runQuery(t, `SELECT a.id, c FROM data:orders AS a LET c = a.custId`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// id -> expected custId.
	want := map[string]string{"1200": "abc", "1234": "bbb", "1235": "ccc", "1236": "ccc"}
	for _, row := range rows {
		if len(row) != 2 {
			t.Fatalf("expected 2 labels, got %+v", row)
		}
		id, c := trimQ(string(row[0])), trimQ(string(row[1]))
		if want[id] != c {
			t.Fatalf("id %s: expected c=%s, got %s", id, want[id], c)
		}
	}
}

// LET variable referenced by a WHERE predicate.
func TestLetInWhere(t *testing.T) {
	rows := runQuery(t, `SELECT a.id FROM data:orders AS a LET c = a.custId WHERE c = "ccc"`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (custId ccc), got %d: %+v", len(rows), rows)
	}
	for _, row := range rows {
		id := trimQ(string(row[0]))
		if id != "1235" && id != "1236" {
			t.Fatalf("unexpected id %s", id)
		}
	}
}

// LET variable referenced by ORDER BY.
func TestLetInOrderBy(t *testing.T) {
	rows := runQuery(t, `SELECT a.id FROM data:orders AS a LET c = a.custId ORDER BY c DESC, a.id`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// custId DESC: ccc (1235, 1236), bbb (1234), abc (1200).
	got := []string{}
	for _, row := range rows {
		got = append(got, trimQ(string(row[0])))
	}
	want := []string{"1235", "1236", "1234", "1200"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order mismatch: got %v, want %v", got, want)
		}
	}
}

// Multiple LET bindings in one clause.
func TestLetMultiple(t *testing.T) {
	rows := runQuery(t, `SELECT x, y FROM data:orders AS a LET x = 10, y = x + 5 WHERE a.id = "1200"`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if string(rows[0][0]) != "10" || string(rows[0][1]) != "15" {
		t.Fatalf("expected x=10,y=15, got %+v", rows[0])
	}
}

// Regression: SELECT * must NOT leak LET binding variables.
func TestLetStarNoLeak(t *testing.T) {
	rows := runQuery(t, `SELECT * FROM data:orders AS a LET foo = 999 WHERE a.id = "1200"`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	m := rowObj(t, rows[0])
	if _, ok := m["foo"]; ok {
		t.Fatalf("LET var foo leaked into SELECT *: %+v", m)
	}
	a, ok := m["a"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected row keyed by alias 'a', got %+v", m)
	}
	if a["id"] != "1200" {
		t.Fatalf("expected a.id==1200, got %+v", a["id"])
	}
	if len(m) != 1 {
		t.Fatalf("expected only the 'a' key, got %+v", m)
	}
}

// -------------------------------------------------------------------
// WITH (non-recursive CTE)

// An unreferenced WITH binding must not affect the result.
func TestWithUnreferenced(t *testing.T) {
	rows := runQuery(t, `WITH w AS ({"k": 1}) SELECT a.id FROM data:orders AS a WHERE a.id = "1234"`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %+v", len(rows), rows)
	}
	if trimQ(string(rows[0][0])) != "1234" {
		t.Fatalf("expected id 1234, got %s", rows[0][0])
	}
}

// Regression: SELECT * must NOT leak a WITH binding name.
func TestWithStarNoLeak(t *testing.T) {
	rows := runQuery(t, `WITH w AS (123) SELECT * FROM data:orders AS a WHERE a.id = "1200"`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	m := rowObj(t, rows[0])
	if _, ok := m["w"]; ok {
		t.Fatalf("WITH var w leaked into SELECT *: %+v", m)
	}
	if len(m) != 1 {
		t.Fatalf("expected only the 'a' key, got %+v", m)
	}
}

// -------------------------------------------------------------------
// LETTING (LET in the GROUP BY scope -- bindings over aggregates)

// LETTING binds a post-group aggregate, projected by name.
func TestLettingSum(t *testing.T) {
	rows := runQuery(t, `SELECT a.custId, total FROM data:orders AS a UNNEST a.orderlines AS ol `+
		`GROUP BY a.custId LETTING total = SUM(ol.qty)`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 groups, got %d: %+v", len(rows), rows)
	}
	// abc: 1+1=2; bbb: 2+1=3; ccc: (1+1)+(1+1)=4.
	want := map[string]string{"abc": "2", "bbb": "3", "ccc": "4"}
	for _, row := range rows {
		if len(row) != 2 {
			t.Fatalf("expected 2 labels, got %+v", row)
		}
		cust := trimQ(string(row[0]))
		if want[cust] != string(row[1]) {
			t.Fatalf("custId %s: expected total=%s, got %s", cust, want[cust], row[1])
		}
	}
}

// LETTING variable referenced by HAVING.
func TestLettingHaving(t *testing.T) {
	rows := runQuery(t, `SELECT a.custId, total FROM data:orders AS a UNNEST a.orderlines AS ol `+
		`GROUP BY a.custId LETTING total = SUM(ol.qty) HAVING total > 2`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups (total>2), got %d: %+v", len(rows), rows)
	}
	for _, row := range rows {
		cust := trimQ(string(row[0]))
		if cust != "bbb" && cust != "ccc" {
			t.Fatalf("unexpected custId %s (total should be >2)", cust)
		}
	}
}

// trimQ strips one leading and trailing double-quote from a JSON string token.
func trimQ(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
