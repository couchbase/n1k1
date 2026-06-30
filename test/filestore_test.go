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

// Runs the upstream couchbase/query "filestore" test corpus (vendored under
// test/filestore/json/default) against n1k1. Each case is {statements, results}
// over the file datastore. n1k1 supports a subset of N1QL, so this reports
// pass / fail / unsupported counts rather than requiring 100%.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

const filestoreRoot = "filestore/json" // FileStore root; queries use default:<keyspace>.

// n1k1RunStatement parses, plans, converts and executes a single statement
// through n1k1's own operators, returning the result rows as canonical JSON
// strings. Any parse/plan/convert/exec error (or panic) is returned as err,
// which the harness treats as "unsupported".
func n1k1RunStatement(store *glue.Store, stmt string) (rows []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			rows, err = nil, fmt.Errorf("panic: %v", r)
		}
	}()

	s, err := glue.ParseStatement(stmt, "default", true)
	if err != nil {
		return nil, err
	}

	p, err := store.PlanStatement(s, "default", nil, nil)
	if err != nil {
		return nil, err
	}

	conv := &glue.Conv{Temps: []interface{}{nil}}
	if _, err = p.Accept(conv); err != nil {
		return nil, err
	}
	if conv.TopOp == nil {
		return nil, fmt.Errorf("nil TopOp (unsupported plan)")
	}

	cv, err := glue.NewConvertVals(conv.TopOp.Labels)
	if err != nil {
		return nil, err
	}

	if n1k1.ExprCatalog["exprStr"] == nil {
		n1k1.ExprCatalog["exprStr"] = glue.ExprStr
	}
	if n1k1.ExprCatalog["exprTree"] == nil {
		n1k1.ExprCatalog["exprTree"] = glue.ExprTree
	}

	tmpDir, vars := glue.MakeVars("", "n1k1fs")
	defer os.RemoveAll(tmpDir)

	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, glue.NewGlueContext(time.Now()))
	vars.Temps = append(vars.Temps, conv.Temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	origExecOpEx := n1k1.ExecOpEx
	defer func() { n1k1.ExecOpEx = origExecOpEx }()
	n1k1.ExecOpEx = glue.DatastoreOp

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
		rows = append(rows, string(b))
	}
	yieldErr := func(e error) {
		if e != nil && execErr == nil {
			execErr = e
		}
	}

	n1k1.ExecOp(conv.TopOp, vars, yieldVals, yieldErr, "", "")

	return rows, execErr
}

// canonStrings turns expected result objects into canonical JSON strings.
func canonStrings(results []interface{}) []string {
	out := make([]string, 0, len(results))
	for _, r := range results {
		b, _ := json.Marshal(r)
		out = append(out, string(b))
	}
	return out
}

// rowsMatch compares as multisets (order-insensitive) -- n1k1's row order can
// differ, and json.Marshal sorts object keys, so both sides are canonical.
func rowsMatch(got []string, expected []interface{}) bool {
	e := canonStrings(expected)
	if len(got) != len(e) {
		return false
	}
	g := append([]string(nil), got...)
	sort.Strings(g)
	sort.Strings(e)
	for i := range g {
		if g[i] != e[i] {
			return false
		}
	}
	return true
}

// caseRunnable reports whether a case is the simple {statements, results}
// shape n1k1 can attempt (not an error/match/resultset/pre-post case).
func caseRunnable(c map[string]interface{}) (stmt string, results []interface{}, ok bool) {
	for k := range c {
		switch k {
		case "statements", "results", "ordered", "description":
		default:
			return "", nil, false // exotic field -> skip
		}
	}
	s, hasStmt := c["statements"].(string)
	r, hasResults := c["results"].([]interface{})
	if !hasStmt || !hasResults {
		return "", nil, false
	}
	return s, r, true
}

func TestFilestoreCases(t *testing.T) {
	if _, err := os.Stat(filestoreRoot + "/default/cases"); err != nil {
		t.Skipf("filestore corpus not present: %v", err)
	}

	store, err := glue.FileStore(filestoreRoot)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}
	store.InitParser()

	files, _ := filepath.Glob(filestoreRoot + "/default/cases/case_*.json")
	sort.Strings(files)

	var pass, fail, unsupported, skipped int
	var failEx, unsupEx []string

	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Errorf("read %s: %v", f, err)
			continue
		}
		var cases []map[string]interface{}
		if json.Unmarshal(b, &cases) != nil {
			continue
		}

		for ci, c := range cases {
			stmt, results, ok := caseRunnable(c)
			if !ok {
				skipped++
				continue
			}

			got, err := n1k1RunStatement(store, stmt)
			loc := fmt.Sprintf("%s[%d]", filepath.Base(f), ci)
			if err != nil {
				unsupported++
				if len(unsupEx) < 12 {
					unsupEx = append(unsupEx, fmt.Sprintf("%s: %s -- %v", loc, stmt, err))
				}
				continue
			}
			if rowsMatch(got, results) {
				pass++
			} else {
				fail++
				if len(failEx) < 12 {
					failEx = append(failEx, fmt.Sprintf("%s: %s", loc, stmt))
				}
			}
		}
	}

	total := pass + fail + unsupported
	t.Logf("filestore cases over %d files: PASS=%d FAIL=%d UNSUPPORTED=%d (skipped exotic=%d), runnable=%d",
		len(files), pass, fail, unsupported, skipped, total)
	if len(failEx) > 0 {
		t.Logf("sample FAILs (ran, wrong results):\n  %v", joinLines(failEx))
	}
	if len(unsupEx) > 0 {
		t.Logf("sample UNSUPPORTED (parse/plan/exec error or panic):\n  %v", joinLines(unsupEx))
	}

	// Regression guard: n1k1 supports a subset of N1QL, so we don't require
	// 100% -- but the number of upstream cases that pass should never drop.
	// Ratchet this up as n1k1's coverage grows.
	const passFloor = 358
	if pass < passFloor {
		t.Errorf("filestore conformance regressed: PASS=%d < baseline %d", pass, passFloor)
	}
}

func joinLines(a []string) string {
	out := ""
	for _, s := range a {
		out += s + "\n  "
	}
	return out
}
