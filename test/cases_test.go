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
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

// This is the data-driven home for local SQL++ feature unit tests -- run each
// query against the test/data: file store and assert on its rows, in the style
// of cases.go's TestCasesSimple. As more features land, append cases here rather
// than adding a new _test.go file per feature. (These are NOT the suite corpus,
// which is seeded from couchbase/query; see suite_test.go.)
//
// The data: store has 4 "orders" docs: id 1200/abc, 1234/bbb, 1235/ccc,
// 1236/ccc, each with an orderlines array of {qty, productId}.

// queryCase is one case: run stmt, optionally check the row count (rows >= 0),
// and optionally run finer assertions via check. base.Vals is a row's per-label
// raw values; a single-label JSON-object row can be decoded with rowObj.
type queryCase struct {
	name  string
	stmt  string
	rows  int // expected result row count, or -1 to skip the count check
	check func(t *testing.T, rows []base.Vals)
}

var queryCases = []queryCase{
	// ---- LET ----------------------------------------------------------
	{
		name: "LetConst", // a constant binding, projected by name
		stmt: `SELECT foo FROM data:orders AS a LET foo = 1`,
		rows: 4,
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if len(row) != 1 || string(row[0]) != "1" {
					t.Fatalf("expected foo==1, got %+v", row)
				}
			}
		},
	},
	{
		name: "LetField", // binding derived from a doc field
		stmt: `SELECT a.id, c FROM data:orders AS a LET c = a.custId`,
		rows: 4,
		check: func(t *testing.T, rows []base.Vals) {
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
		},
	},
	{
		name: "LetInWhere", // binding referenced by WHERE
		stmt: `SELECT a.id FROM data:orders AS a LET c = a.custId WHERE c = "ccc"`,
		rows: 2,
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if id := trimQ(string(row[0])); id != "1235" && id != "1236" {
					t.Fatalf("unexpected id %s", id)
				}
			}
		},
	},
	{
		name: "LetInOrderBy", // binding referenced by ORDER BY
		stmt: `SELECT a.id FROM data:orders AS a LET c = a.custId ORDER BY c DESC, a.id`,
		rows: 4,
		check: func(t *testing.T, rows []base.Vals) {
			// custId DESC: ccc (1235, 1236), bbb (1234), abc (1200).
			want := []string{"1235", "1236", "1234", "1200"}
			for i, row := range rows {
				if got := trimQ(string(row[0])); got != want[i] {
					t.Fatalf("order[%d]: got %s, want %s", i, got, want[i])
				}
			}
		},
	},
	{
		name: "LetMultiple", // a later binding references an earlier one
		stmt: `SELECT x, y FROM data:orders AS a LET x = 10, y = x + 5 WHERE a.id = "1200"`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "10" || string(rows[0][1]) != "15" {
				t.Fatalf("expected x=10,y=15, got %+v", rows[0])
			}
		},
	},
	{
		name: "LetStarNoLeak", // SELECT * must not spread LET vars
		stmt: `SELECT * FROM data:orders AS a LET foo = 999 WHERE a.id = "1200"`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			m := rowObj(t, rows[0])
			if _, ok := m["foo"]; ok {
				t.Fatalf("LET var foo leaked into SELECT *: %+v", m)
			}
			if len(m) != 1 {
				t.Fatalf("expected only the 'a' key, got %+v", m)
			}
			if a, ok := m["a"].(map[string]interface{}); !ok || a["id"] != "1200" {
				t.Fatalf("expected a.id==1200, got %+v", m)
			}
		},
	},

	// ---- WITH (non-recursive CTE) -------------------------------------
	{
		name: "WithUnreferenced", // an unreferenced CTE must not change results
		stmt: `WITH w AS ({"k": 1}) SELECT a.id FROM data:orders AS a WHERE a.id = "1234"`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if id := trimQ(string(rows[0][0])); id != "1234" {
				t.Fatalf("expected id 1234, got %s", id)
			}
		},
	},
	{
		name: "WithStarNoLeak", // SELECT * must not spread a WITH binding name
		stmt: `WITH w AS (123) SELECT * FROM data:orders AS a WHERE a.id = "1200"`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			m := rowObj(t, rows[0])
			if _, ok := m["w"]; ok {
				t.Fatalf("WITH var w leaked into SELECT *: %+v", m)
			}
			if len(m) != 1 {
				t.Fatalf("expected only the 'a' key, got %+v", m)
			}
		},
	},

	// ---- LETTING (LET in the GROUP BY scope, over aggregates) ---------
	{
		name: "LettingSum", // a post-group aggregate binding, projected by name
		stmt: `SELECT a.custId, total FROM data:orders AS a UNNEST a.orderlines AS ol ` +
			`GROUP BY a.custId LETTING total = SUM(ol.qty)`,
		rows: 3,
		check: func(t *testing.T, rows []base.Vals) {
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
		},
	},
	{
		name: "LettingHaving", // a LETTING var referenced by HAVING
		stmt: `SELECT a.custId, total FROM data:orders AS a UNNEST a.orderlines AS ol ` +
			`GROUP BY a.custId LETTING total = SUM(ol.qty) HAVING total > 2`,
		rows: 2,
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if cust := trimQ(string(row[0])); cust != "bbb" && cust != "ccc" {
					t.Fatalf("unexpected custId %s (total should be >2)", cust)
				}
			}
		},
	},

	// ---- Subqueries (uncorrelated) ------------------------------------
	{
		name: "SubqueryConstIn", // constant membership in a subquery result
		stmt: `SELECT 5 IN (SELECT RAW 5) AS hit`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "true" {
				t.Fatalf("expected hit=true, got %s", rows[0][0])
			}
		},
	},
	{
		name: "SubqueryArrayLength", // ARRAY_LENGTH over a subquery's rows
		stmt: `SELECT ARRAY_LENGTH((SELECT RAW o.id FROM data:orders o)) AS n`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != "4" { // 4 orders
				t.Fatalf("expected n=4, got %s", rows[0][0])
			}
		},
	},
	{
		name: "SubqueryInProjection", // a subquery's array as a projected column
		stmt: `SELECT (SELECT RAW o.id FROM data:orders o WHERE o.custId = "abc") AS ids`,
		rows: 1,
		check: func(t *testing.T, rows []base.Vals) {
			if string(rows[0][0]) != `["1200"]` { // only order 1200 has custId abc
				t.Fatalf("expected ids=[\"1200\"], got %s", rows[0][0])
			}
		},
	},
	{
		name: "SubqueryWhereIn", // WHERE ... IN (uncorrelated subquery)
		stmt: `SELECT o.id FROM data:orders o ` +
			`WHERE o.custId IN (SELECT RAW o2.custId FROM data:orders o2 WHERE o2.id = "1235")`,
		rows: 2, // subquery -> ["ccc"]; orders with custId ccc = 1235, 1236
		check: func(t *testing.T, rows []base.Vals) {
			for _, row := range rows {
				if id := trimQ(string(row[0])); id != "1235" && id != "1236" {
					t.Fatalf("unexpected id %s", id)
				}
			}
		},
	},
}

// TestQueryCases runs every queryCase as a subtest.
func TestQueryCases(t *testing.T) {
	for _, c := range queryCases {
		t.Run(c.name, func(t *testing.T) {
			rows := runQuery(t, c.stmt)
			if c.rows >= 0 && len(rows) != c.rows {
				t.Fatalf("expected %d rows, got %d: %+v", c.rows, len(rows), rows)
			}
			if c.check != nil {
				c.check(t, rows)
			}
		})
	}
}

// TestSubqueryCorrelatedUnsupported checks that a correlated subquery (one
// referencing an outer field) fails with a clear error rather than silently
// returning wrong results -- correlation isn't threaded into the sub-op scope
// yet. Run via glue.Session so the exec error surfaces as a return value.
func TestSubqueryCorrelatedUnsupported(t *testing.T) {
	sess, err := glue.OpenSession(".", "data")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	_, err = sess.Run(`SELECT o.id FROM data:orders o ` +
		`WHERE 1 IN (SELECT RAW 1 FROM data:orders o2 WHERE o2.id = o.id)`)
	if err == nil {
		t.Fatalf("expected an error for a correlated subquery")
	}
	if !strings.Contains(err.Error(), "correlated") {
		t.Fatalf("expected a 'correlated' error, got: %v", err)
	}
}

// -------------------------------------------------------------------
// Helpers shared by the cases above.

// runQuery parses, plans, converts and executes stmt against the local data:
// file store, failing the test on any error, and returns the result rows.
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

// trimQ strips one leading and trailing double-quote from a JSON string token.
func trimQ(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
