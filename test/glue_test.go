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
	"testing"

	"github.com/couchbase/query/plan"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

func TestFileStoreSelect1(t *testing.T) {
	p, conv, op, err :=
		testFileStoreSelect(t, `SELECT *, 123, name FROM data:orders`, true)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || op == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	jop, _ := json.MarshalIndent(op, " ", " ")

	fmt.Printf("jop: %s\n", jop)
}

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
	plan.Operator, *glue.Conv, *base.Op, error) {
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
		t.Fatalf("plan did not expect err: %v", err)
	}
	if p == nil {
		t.Fatalf("did not expect nil plan")
	}

	if emit {
		fmt.Printf("p: %+v\n", p)

		jp, _ := json.MarshalIndent(p, " ", " ")

		fmt.Printf("jp: %s\n", jp)
	}

	conv := &glue.Conv{Store: store, Aliases: map[string]string{}}

	v, err := p.Accept(conv)

	op, _ := v.(*base.Op)

	return p, conv, op, err
}
