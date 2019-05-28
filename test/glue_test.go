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
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/execution"
	"github.com/couchbase/query/plan"
	server_http "github.com/couchbase/query/server/http"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

func TestFileStoreSelectId(t *testing.T) {
	p, conv, op, err :=
		testFileStoreSelect(t, `SELECT * FROM data:orders`, true)
	if err != nil {
		t.Fatalf("expected no nil err, got: %v", err)
	}
	if p == nil || conv == nil || op == nil {
		t.Fatalf("expected p and conv an op, got nil")
	}

	jop, _ := json.MarshalIndent(op, " ", " ")

	fmt.Printf("jop: %s\n", jop)

	testi := 0

	testExpectErr := ""

	tmpDir, vars, yieldVals, yieldErr, returnYields :=
		MakeYieldCaptureFuncs(t, testi, testExpectErr)

	defer os.RemoveAll(tmpDir)

	namespace := ""
	readonly := true
	maxParallelism := 1

	requestId := "test-request"
	requestScanCap := int64(1000)
	requestPipelineCap := int64(1000)
	requestPipelineBatch := 100
	requestNamedArgs := map[string]value.Value(nil)
	requestPositionalArgs := value.Values(nil)
	requestCredentials := auth.Credentials(nil)
	requestScanConsistency := datastore.UNBOUNDED
	requestScanVectorSource := &server_http.ZeroScanVectorSource{}
	requestOutput := &Output{}

	var requestOriginalHttpRequest *http.Request

	var prepared *plan.Prepared

	context := execution.NewContext(requestId,
		conv.Store.Datastore, conv.Store.Systemstore, namespace,
		readonly, maxParallelism,
		requestScanCap, requestPipelineCap, requestPipelineBatch,
		requestNamedArgs, requestPositionalArgs,
		requestCredentials, requestScanConsistency, requestScanVectorSource,
		requestOutput, requestOriginalHttpRequest,
		prepared, conv.Store.IndexApiVersion, conv.Store.FeatureControls)

	vars.Temps = vars.Temps[:0]

	vars.Temps = append(vars.Temps, context)

	vars.Temps = append(vars.Temps, conv.Temps[1:]...)

	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	origExecOpEx := n1k1.ExecOpEx

	defer func() { n1k1.ExecOpEx = origExecOpEx }()

	n1k1.ExecOpEx = glue.DatastoreOp

	n1k1.ExecOp(op, vars, yieldVals, yieldErr, "", "")

	results := returnYields()

	fmt.Printf("vars.Temps: %#v\n", vars.Temps)

	fmt.Printf("results: %+v\n  output: %v\n", results, requestOutput)
}

func TestFileStoreSelectSimple(t *testing.T) {
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

	conv := &glue.Conv{
		Store:   store,
		Aliases: map[string]string{},
		Temps:   []interface{}{nil}, // Placeholder for execution.Context.
	}

	v, err := p.Accept(conv)

	op, _ := v.(*base.Op)

	return p, conv, op, err
}

// -------------------------------------------------------------

type Output struct {
	ErrAbort, ErrError, ErrFail, ErrFatal, ErrWarning errors.Error
}

func (this *Output) HasErr() bool {
	return this.ErrAbort != nil ||
		this.ErrError != nil ||
		this.ErrFail != nil ||
		this.ErrFatal != nil ||
		this.ErrWarning != nil
}

func (this *Output) SetUp()                                                      {}
func (this *Output) Result(item value.AnnotatedValue) bool                       { return true }
func (this *Output) CloseResults()                                               {}
func (this *Output) Abort(err errors.Error)                                      { this.ErrAbort = err }
func (this *Output) Error(err errors.Error)                                      { this.ErrError = err }
func (this *Output) Fail(err errors.Error)                                       { this.ErrFail = err }
func (this *Output) Fatal(err errors.Error)                                      { this.ErrFatal = err }
func (this *Output) Warning(wrn errors.Error)                                    { this.ErrWarning = wrn }
func (this *Output) AddMutationCount(uint64)                                     {}
func (this *Output) MutationCount() uint64                                       { return 0 }
func (this *Output) SortCount() uint64                                           { return 0 }
func (this *Output) SetSortCount(i uint64)                                       {}
func (this *Output) AddPhaseOperator(p execution.Phases)                         {}
func (this *Output) AddPhaseCount(p execution.Phases, c uint64)                  {}
func (this *Output) FmtPhaseCounts() map[string]interface{}                      { return nil }
func (this *Output) FmtPhaseOperators() map[string]interface{}                   { return nil }
func (this *Output) AddPhaseTime(phase execution.Phases, duration time.Duration) {}
func (this *Output) FmtPhaseTimes() map[string]interface{}                       { return nil }