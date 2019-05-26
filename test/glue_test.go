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

	"github.com/couchbase/n1k1/glue"
)

func TestFileStoreSelect(t *testing.T) {
	store, err := glue.FileStore("./")
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	store.InitParser()

	s, err := glue.ParseStatement(
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
              HAVING t.x > 20`,
		"", true)
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

	fmt.Printf("p: %+v\n", p)

	jp, _ := json.MarshalIndent(p, " ", " ")

	fmt.Printf("jp: %s\n", jp)
}
