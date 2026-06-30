//go:build n1ql

//  Copyright (c) 2019 Couchbase, Inc.
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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/query/plan"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

// orderId pulls "id" out of a `SELECT * FROM data:orders` row. N1QL wraps a
// SELECT * under the keyspace's effective alias, so the row is
// {"orders": {..., "id": ...}} (the keyspace name "orders" is the alias).
func orderId(m map[string]interface{}) string {
	o, _ := m["orders"].(map[string]interface{})
	id, _ := o["id"].(string)
	return id
}

func TestFileStoreSelectStarUseKeys1(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders USE KEYS "1234"`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 1 {
		t.Fatalf("expected 1 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}

		var m map[string]interface{}
		err := json.Unmarshal(result[0], &m)
		if err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}

		if strings.Index("1234",
			orderId(m)) < 0 {
			t.Fatalf("unexpected id: %+v, m: %+v", result, m)
		}
	}
}

func TestFileStoreSelectStarUseKeys2(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders USE KEYS ["1234","9999","1200"]`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}

		var m map[string]interface{}
		err := json.Unmarshal(result[0], &m)
		if err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}

		if strings.Index("1200,1234",
			orderId(m)) < 0 {
			t.Fatalf("unexpected id: %+v, m: %+v", result, m)
		}
	}
}

func TestFileStoreSelectStar(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}

		var m map[string]interface{}
		err := json.Unmarshal(result[0], &m)
		if err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}

		if strings.Index("1200,1234,1235,1236",
			orderId(m)) < 0 {
			t.Fatalf("unexpected id: %+v, m: %+v", result, m)
		}
	}
}

func TestFileStoreSelectStar123(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT *, 100 + 23 FROM data:orders`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 2 {
			t.Fatalf("expected result has two labels, got: %+v", result)
		}

		var m map[string]interface{}
		err := json.Unmarshal(result[0], &m)
		if err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}

		if strings.Index("1200,1234,1235,1236",
			orderId(m)) < 0 {
			t.Fatalf("unexpected id: %+v, m: %+v", result, m)
		}

		if string(result[1]) != "123" {
			t.Fatalf("expected result[1] == '123', got: %+v", result)
		}
	}
}

func TestFileStoreInnerJoinOnKeys(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT a.id FROM data:orders AS a INNER JOIN data:orders AS b ON KEYS a.id`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}
	}
}

func TestFileStoreInnerJoinOnKeysOnMissingField(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT a.id FROM data:orders AS a INNER JOIN data:orders AS b ON KEYS a.notAField`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got: %+v", results)
	}
}

func TestFileStoreLeftOuterJoinOnKeys(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT a.id FROM data:orders AS a LEFT OUTER JOIN data:orders AS b ON KEYS a.notAField`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}
		if strings.Index(`"1200","1234","1235","1236"`, string(result[0])) < 0 {
			t.Fatalf("expected entry from array, got: %+v", result)
		}
	}
}

func TestFileStoreUnnest(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders AS a UNNEST orderlines AS ol`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 8 {
		t.Fatalf("expected 8 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}
	}
}

func TestFileStoreUnnestOnMissingField(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders AS a UNNEST notAField AS ol`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got: %+v", results)
	}
}

func TestFileStoreLeftOuterUnnest(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders AS a LEFT OUTER UNNEST notAField AS ol`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}
	}
}

func TestFileStoreConst(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT 1+2`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 1 {
		t.Fatalf("expected 1 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}
		if string(result[0]) != "3" {
			t.Fatalf("expected result[0] == 3, got: %+v", result)
		}
	}
}

func TestFileStoreFromConstArray(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM [1,2,{"x":[3]}] AS a`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}
		if strings.Index(`{"a":1},{"a":2},{"a":{"x":[3]}}`, string(result[0])) < 0 {
			t.Fatalf("expected entry from array, got: %+v", result)
		}
	}
}

func TestFileStoreFromConstArrayWhere(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM [1,2,{"x":[3]}] AS a WHERE a > 2`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 1 {
		t.Fatalf("expected 1 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}
		if strings.Index(`{"a":{"x":[3]}}`, string(result[0])) < 0 {
			t.Fatalf("expected entry from array, got: %+v", result)
		}
	}
}

func TestFileStoreOrderBy(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders AS a ORDER BY a.id DESC`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got: %+v", results)
	}

	lastId := "9999999"

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}

		var m map[string]interface{}
		err := json.Unmarshal(result[0], &m)
		if err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}

		currId := m["a"].(map[string]interface{})["id"].(string)
		if currId >= lastId {
			t.Fatalf("unexpected descending id: %+v, m: %+v", result, m)
		}

		lastId = currId
	}
}

func TestFileStoreOrderByOffset(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders AS a ORDER BY a.id DESC OFFSET 2`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got: %+v", results)
	}

	lastId := "9999999"

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}

		var m map[string]interface{}
		err := json.Unmarshal(result[0], &m)
		if err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}

		currId := m["a"].(map[string]interface{})["id"].(string)
		if currId >= lastId {
			t.Fatalf("unexpected descending id: %+v, m: %+v", result, m)
		}

		lastId = currId
	}
}

func TestFileStoreOrderByLimit(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders AS a ORDER BY a.id DESC LIMIT 2`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got: %+v", results)
	}

	lastId := "9999999"

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}

		var m map[string]interface{}
		err := json.Unmarshal(result[0], &m)
		if err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}

		currId := m["a"].(map[string]interface{})["id"].(string)
		if currId >= lastId {
			t.Fatalf("unexpected descending id: %+v, m: %+v", result, m)
		}

		lastId = currId
	}
}

func TestFileStoreOrderByOffsetLimit(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders AS a ORDER BY a.id DESC OFFSET 2 LIMIT 1`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 1 {
		t.Fatalf("expected 1 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has 1 labels, got: %+v", result)
		}

		var m map[string]interface{}
		err := json.Unmarshal(result[0], &m)
		if err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}

		currId := m["a"].(map[string]interface{})["id"].(string)
		if currId != "1234" {
			t.Fatalf("unexpected descending id: %+v, m: %+v, currId: %v", result, m, currId)
		}
	}
}

func TestFileStoreDistinct(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT DISTINCT a FROM [1,2,3,4,1,2,3,4] AS a`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got: %+v", results)
	}
}

func TestFileStoreGroupBy(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT custId FROM data:orders AS a GROUP BY custId`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 1 {
			t.Fatalf("expected result has one label, got: %+v", result)
		}

		if strings.Index(`"abc","bbb","ccc"`, string(result[0])) < 0 {
			t.Fatalf("unexpected id: %+v", result)
		}
	}
}

func TestFileStoreGroupByCount(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT custId, COUNT(custId) FROM data:orders AS a GROUP BY custId`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 2 {
			t.Fatalf("expected result has 2 labels, got: %+v", result)
		}

		if strings.Index(`"abc","bbb","ccc"`, string(result[0])) < 0 {
			t.Fatalf("unexpected id: %+v", result)
		}

		if strings.Index(`1,2`, string(result[1])) < 0 {
			t.Fatalf("unexpected count: %+v", result)
		}
	}
}

func TestFileStoreGroupBySum(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT o.custId, SUM(ol.qty) FROM data:orders AS o UNNEST o.orderlines AS ol GROUP BY o.custId`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 2 {
			t.Fatalf("expected result has one label, got: %+v", result)
		}

		if strings.Index(`"abc","bbb","ccc"`, string(result[0])) < 0 {
			t.Fatalf("unexpected id: %+v", result)
		}

		if strings.Index(`2,3,4`, string(result[1])) < 0 {
			t.Fatalf("unexpected sum: %+v", result)
		}
	}
}

func TestFileStoreGroupByCountSum(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `SELECT o.custId, COUNT(o.custId), SUM(ol.qty) FROM data:orders AS o UNNEST o.orderlines AS ol GROUP BY o.custId`, false)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, false, store, conv)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 3 {
			t.Fatalf("expected result has 3 labels, got: %+v", result)
		}

		if strings.Index(`"abc","bbb","ccc"`, string(result[0])) < 0 {
			t.Fatalf("unexpected id: %+v", result)
		}

		if strings.Index(`2,4`, string(result[1])) < 0 {
			t.Fatalf("unexpected count: %+v", result)
		}

		if strings.Index(`2,3,4`, string(result[2])) < 0 {
			t.Fatalf("unexpected sum: %+v", result)
		}
	}
}

// TODO: Need to rewrite the projection expression of...
//
//	"count((`o`.`custId`)) OVER (PARTITION BY (`o`.`custId`) ORDER BY (`o`.`id`) ROWS UNBOUNDED PRECEDING)"
//
// into a n1k1 expr_window function like "window-frame-count".
func DISABLED_TestFileStoreWindowOver(t *testing.T) {
	store, p, conv, err :=
		testFileStoreSelect(t, `
SELECT
 COUNT(o.custId) OVER (PARTITION BY o.custId ORDER BY o.id ROWS UNBOUNDED PRECEDING)
FROM data:orders AS o`,
			true)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || conv.TopOp == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	results := testGlueExec(t, true, store, conv)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got: %+v", results)
	}

	for _, result := range results {
		if len(result) != 3 {
			t.Fatalf("expected result has 3 labels, got: %+v", result)
		}

		if strings.Index(`"abc","bbb","ccc"`, string(result[0])) < 0 {
			t.Fatalf("unexpected id: %+v", result)
		}

		if strings.Index(`2,4`, string(result[1])) < 0 {
			t.Fatalf("unexpected count: %+v", result)
		}

		if strings.Index(`2,3,4`, string(result[2])) < 0 {
			t.Fatalf("unexpected sum: %+v", result)
		}
	}
}

// ---------------------------------------------------------------

func TestFileStoreSelectComplex(t *testing.T) {
	testFileStoreSelect(t,
		`WITH
           myCTE AS (
             SELECT * FROM data:orders
           ),
           myThree AS (
             SELECT 1+2
           )
         SELECT t.x,
                contact.name,
                SUM(DISTINCT t.x) AS s,
                COUNT(*) OVER(PARTITION BY t.x ORDER BY contact.name
                              ROWS UNBOUNDED PRECEDING) AS c`+
			" FROM data:`1doc` AS t UNNEST contacts AS contact"+
			"      JOIN myCTE ON t.xxx = myCTE.yyy"+
			` LET y = t.x
              WHERE y > 10
                AND t.z in myThree
                AND t.foo in (SELECT e.item from data:empty AS e WHERE e.x == t.x)
              GROUP BY t.x, contact.name
              HAVING t.x > 20
          EXCEPT
            SELECT * FROM data:empty
          INTERSECT
            SELECT * FROM data:orders`,
		false)
}

// ---------------------------------------------------------------

func testFileStoreSelect(t *testing.T, stmt string, emit bool) (
	*glue.Store, plan.Operator, *glue.Conv, error) {
	store, err := glue.FileStore("./")
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	store.InitParser()

	s, err := glue.ParseStatement(stmt, "", true)
	if err != nil {
		t.Fatalf("parse did not expect err: %v", err)
	}

	p, err := store.PlanStatement(s, "", nil, nil)
	if err != nil {
		t.Fatalf("plan did not expect err: %v, stmt: %s", err, stmt)
	}
	if p == nil {
		t.Fatalf("did not expect nil plan")
	}

	if emit {
		fmt.Printf("p: %+v\n", p)

		jp, _ := json.MarshalIndent(p, " ", " ")

		fmt.Printf("jp: %s\n", jp)
	}

	conv := &glue.Conv{Temps: []interface{}{nil}}

	_, err = p.Accept(conv)

	return store, p, conv, err
}

// -------------------------------------------------------------

func testGlueExec(t *testing.T, emit bool,
	store *glue.Store, conv *glue.Conv) []base.Vals {
	if emit {
		jop, _ := json.MarshalIndent(conv.TopOp, " ", " ")
		fmt.Printf("jop: %s\n", jop)
	}

	testi := 0

	testExpectErr := ""

	tmpDir, vars, yieldVals, yieldErr, returnYields :=
		MakeYieldCaptureFuncs(t, testi, testExpectErr)

	defer os.RemoveAll(tmpDir)

	// n1k1's own context, replacing query/execution.Context (which is cgo).
	context := glue.NewGlueContext(time.Now())

	vars.Temps = vars.Temps[:0]

	vars.Temps = append(vars.Temps, context)

	vars.Temps = append(vars.Temps, conv.Temps[1:]...)

	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	origExecOpEx := n1k1.ExecOpEx

	defer func() { n1k1.ExecOpEx = origExecOpEx }()

	n1k1.ExecOpEx = glue.DatastoreOp

	n1k1.ExecOp(conv.TopOp, vars, yieldVals, yieldErr, "", "")

	results := returnYields()

	if emit {
		fmt.Printf("vars.Temps: %#v\n", vars.Temps)

		fmt.Printf("results: %+v\n  context errs: %v\n", results, context.GetErrors())
	}

	return results
}

// -------------------------------------------------------------
