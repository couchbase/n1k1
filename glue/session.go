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
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/plan"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

// Session is a reusable end-to-end driver: parse -> plan -> convert -> execute a
// SQL++ statement over a file datastore using n1k1's own operators. It is the
// single engine path shared by the conformance test harness and the cmd/n1k1
// CLI, so only one place knows the full pipeline.
type Session struct {
	Store     *Store
	Namespace string // e.g. "default"
}

// OpenSession opens a file-datastore directory and prepares it for queries.
func OpenSession(datastoreDir, namespace string) (*Session, error) {
	store, err := FileStore(datastoreDir)
	if err != nil {
		return nil, err
	}
	if err := store.InitParser(); err != nil {
		return nil, err
	}
	if namespace == "" {
		namespace = "default"
	}
	return &Session{Store: store, Namespace: namespace}, nil
}

// Result is the outcome of one Session.Run. Each Row is a canonical-JSON value:
// a JSON object keyed by the projection's column aliases, or a bare JSON value
// for SELECT RAW. Warnings are advisories the engine recorded during evaluation
// (e.g. divide-by-zero), distinct from a hard error.
type Result struct {
	Labels   base.Labels       // the converted op tree's top labels (internal syntax)
	Rows     []json.RawMessage // one canonical-JSON value per result row
	Warnings []errors.Error    // non-fatal advisories recorded during execution
	Elapsed  time.Duration     // wall-clock of the ExecOp run
	Plan     *base.Op          // the converted n1k1 op tree (for .explain / -v)
}

// findExplain returns the *plan.Explain node in a plan tree (it sits under the
// top-level Authorize the planner adds), or nil if the statement isn't EXPLAIN.
func findExplain(op plan.Operator) *plan.Explain {
	switch o := op.(type) {
	case *plan.Explain:
		return o
	case *plan.Authorize:
		return findExplain(o.Child())
	}
	return nil
}

// ErrUnsupported means n1k1 can't (yet) run this statement -- the plan converted
// to no op tree, a convert step failed, or execution panicked -- as opposed to
// the statement being genuinely invalid (a parse/plan error, returned verbatim).
type ErrUnsupported struct{ Reason string }

func (e *ErrUnsupported) Error() string { return "unsupported: " + e.Reason }

// Run parses, plans, converts and executes a single statement, returning its
// rows (plus any warnings, timing and converted plan). A parse or plan error is
// returned verbatim (the statement is wrong); an unconvertible plan or a panic
// is returned as *ErrUnsupported (n1k1 can't run it yet).
func (s *Session) Run(stmt string) (res *Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, &ErrUnsupported{Reason: fmt.Sprintf("panic: %v", r)}
		}
	}()

	parsed, err := ParseStatement(stmt, s.Namespace, true)
	if err != nil {
		return nil, err
	}

	p, err := s.Store.PlanStatement(parsed, s.Namespace, nil, nil)
	if err != nil {
		return nil, err
	}

	// EXPLAIN: the couchbase/query planner already built the plan (it's what conv
	// would otherwise convert). Rather than convert+execute, emit that plan as a
	// single JSON row, matching N1QL's EXPLAIN output. The Explain op sits under
	// the top Authorize wrapper, so unwrap to find it.
	if ex := findExplain(p); ex != nil {
		// plan.Explain marshals to {"text": <stmt>, "plan": <op>} -- N1QL's
		// EXPLAIN result shape.
		b, err := json.Marshal(ex)
		if err != nil {
			return nil, err
		}
		return &Result{Rows: []json.RawMessage{b}}, nil
	}

	conv := &Conv{Temps: []interface{}{nil}}
	if _, err = p.Accept(conv); err != nil {
		return nil, &ErrUnsupported{Reason: err.Error()}
	}
	if conv.TopOp == nil {
		return nil, &ErrUnsupported{Reason: "nil TopOp (unconverted plan)"}
	}

	cv, err := NewConvertVals(conv.TopOp.Labels)
	if err != nil {
		return nil, &ErrUnsupported{Reason: err.Error()}
	}

	if engine.ExprCatalog["exprStr"] == nil {
		engine.ExprCatalog["exprStr"] = ExprStr
	}
	if engine.ExprCatalog["exprTree"] == nil {
		engine.ExprCatalog["exprTree"] = ExprTree
	}

	tmpDir, vars := MakeVars("", "n1k1")
	defer os.RemoveAll(tmpDir)

	gctx := NewGlueContext(time.Now())

	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, gctx)
	vars.Temps = append(vars.Temps, conv.Temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	origExecOpEx := engine.ExecOpEx
	defer func() { engine.ExecOpEx = origExecOpEx }()
	engine.ExecOpEx = DatastoreOp

	var rows []json.RawMessage
	var execErr error

	yieldVals := func(vals base.Vals) {
		v, e := cv.Convert(vals)
		if e != nil {
			if execErr == nil {
				execErr = e
			}
			return
		}
		var b []byte
		if v != nil {
			b, _ = json.Marshal(v.Actual())
		} else {
			b = []byte("null")
		}
		rows = append(rows, json.RawMessage(b))
	}
	yieldErr := func(e error) {
		if e != nil && execErr == nil {
			execErr = e
		}
	}

	start := time.Now()
	engine.ExecOp(conv.TopOp, vars, yieldVals, yieldErr, "", "")
	elapsed := time.Since(start)

	if execErr != nil {
		return nil, execErr
	}

	return &Result{
		Labels:   conv.TopOp.Labels,
		Rows:     rows,
		Warnings: gctx.GetErrors(),
		Elapsed:  elapsed,
		Plan:     conv.TopOp,
	}, nil
}
