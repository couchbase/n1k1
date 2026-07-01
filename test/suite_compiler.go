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

// Result-comparison helpers shared by the suite interpreter test
// (suite_test.go) and the suite *compiler* differential test. These
// live in a regular (non-_test) file because CheckCompiledRows is called by the
// generated test/tmp package, whose import of "test" only sees the regular
// package API (symbols in _test.go files aren't exported across packages).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
	"github.com/couchbase/n1k1/glue"
)

// deepNormalize recursively sorts object keys (implicitly, via json.Marshal)
// and array elements. The harness treats result rows as an unordered multiset;
// array element order is likewise not meaningful for the comparison --
// ARRAY_AGG order is undefined in N1QL, and the upstream corpus's order
// reflects its own scan/aggregation order, which n1k1 needn't replicate. So we
// compare arrays as multisets too. (Sorting both sides identically can only
// turn a false failure into a pass; it never breaks a genuinely-equal pair.)
func deepNormalize(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		m := make(map[string]interface{}, len(x))
		for k, e := range x {
			m[k] = deepNormalize(e)
		}
		return m
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, e := range x {
			out[i] = deepNormalize(e)
		}
		sort.Slice(out, func(i, j int) bool {
			bi, _ := json.Marshal(out[i])
			bj, _ := json.Marshal(out[j])
			return string(bi) < string(bj)
		})
		return out
	default:
		return v
	}
}

// canonJSON returns a fully order-normalized JSON string for a value (an n1k1
// result row passed as a JSON string, or an expected result object).
func canonJSON(v interface{}) string {
	if s, ok := v.(string); ok { // n1k1 result row (JSON string)
		var parsed interface{}
		if json.Unmarshal([]byte(s), &parsed) != nil {
			return s
		}
		v = parsed
	}
	b, _ := json.Marshal(deepNormalize(v))
	return string(b)
}

// rowsMatch compares as multisets (order-insensitive across rows, and
// key-order-insensitive within each object). n1k1's row order can differ, and
// it doesn't sort object keys, so both sides are canonicalized.
func rowsMatch(got []string, expected []interface{}) bool {
	if len(got) != len(expected) {
		return false
	}
	g := make([]string, len(got))
	for i, s := range got {
		g[i] = canonJSON(s)
	}
	e := make([]string, len(expected))
	for i, r := range expected {
		e[i] = canonJSON(r)
	}
	sort.Strings(g)
	sort.Strings(e)
	for i := range g {
		if g[i] != e[i] {
			return false
		}
	}
	return true
}

// CheckCompiledRows is the assertion the generated suite compiler tests
// call: it merges each captured base.Vals row into a value.Value via the
// projection labels (the same conversion the interpreter driver uses), renders
// it to JSON, and multiset-compares against the corpus's expected results JSON.
// It is the differential oracle -- compiled output must match what the corpus
// expects (which, for the selected cases, the interpreter already matches).
func CheckCompiledRows(t *testing.T, labels base.Labels, yields []base.Vals,
	expectedJSON, about string) {
	cv, err := glue.NewConvertVals(labels)
	if err != nil {
		t.Fatalf("CheckCompiledRows NewConvertVals: %v\n about: %s", err, about)
	}

	got := make([]string, 0, len(yields))
	for _, vals := range yields {
		v, e := cv.Convert(vals)
		if e != nil {
			t.Fatalf("CheckCompiledRows Convert: %v\n about: %s", e, about)
		}
		var b []byte
		if v != nil {
			b, _ = json.Marshal(v.Actual())
		} else {
			b = []byte("null")
		}
		got = append(got, string(b))
	}

	var expected []interface{}
	if err := json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		t.Fatalf("CheckCompiledRows expected JSON: %v\n about: %s", err, about)
	}

	if !rowsMatch(got, expected) {
		t.Fatalf("compiled rows mismatch\n about: %s\n got:   %v\n want:  %s",
			about, got, expectedJSON)
	}
}

// --------------------------------------------------------
// Runtime support for the datastore-backed compiler tests.

var (
	suiteStoreOnce sync.Once
	suiteStore     *glue.Store
	suiteStoreErr  error

	dataStoreOnce sync.Once
	dataStore     *glue.Store
	dataStoreErr  error
)

// wireCompiledRuntime sets the global engine hooks the compiled-query islands
// depend on: ExecOpEx routes datastore ops to glue.DatastoreOp, and the expr
// catalog provides the interpreted exprStr/exprTree evaluators.
func wireCompiledRuntime() {
	engine.ExecOpEx = glue.DatastoreOp
	if engine.ExprCatalog["exprStr"] == nil {
		engine.ExprCatalog["exprStr"] = glue.ExprStr
	}
	if engine.ExprCatalog["exprTree"] == nil {
		engine.ExprCatalog["exprTree"] = glue.ExprTree
	}
}

// compiledSuiteStore opens the corpus FileStore once. The root is located
// relative to this source file (via runtime.Caller) so it resolves regardless
// of the test's working dir -- the generated tests run from test/tmp/, not test/.
func compiledSuiteStore() (*glue.Store, error) {
	suiteStoreOnce.Do(func() {
		_, file, _, _ := runtime.Caller(0)
		root := filepath.Join(filepath.Dir(file), "suite", "json")
		suiteStore, suiteStoreErr = glue.FileStore(root)
		if suiteStoreErr != nil {
			return
		}
		suiteStore.InitParser()
		wireCompiledRuntime()
	})
	return suiteStore, suiteStoreErr
}

// compiledDataStore opens the local test/ file store once (namespace "data" =
// test/data), for the queryCases compiler differential. Located relative to
// this source file so it resolves from test/tmp/.
func compiledDataStore() (*glue.Store, error) {
	dataStoreOnce.Do(func() {
		_, file, _, _ := runtime.Caller(0)
		root := filepath.Dir(file) // test/ ; "data:" -> test/data
		dataStore, dataStoreErr = glue.FileStore(root)
		if dataStoreErr != nil {
			return
		}
		dataStore.InitParser()
		wireCompiledRuntime()
	})
	return dataStore, dataStoreErr
}

// SetupCompiledSuite / SetupCompiledData are the runtime preamble for a generated
// datastore-backed compiler test, over the suite corpus / the local test data
// respectively. See setupCompiled.
func SetupCompiledSuite(t *testing.T, stmt string) (
	*base.Vars, base.YieldVals, base.YieldErr, func() []base.Vals, func()) {
	store, err := compiledSuiteStore()
	if err != nil {
		t.Fatalf("SetupCompiledSuite store: %v", err)
	}
	return setupCompiled(t, store, "default", stmt)
}

func SetupCompiledData(t *testing.T, stmt string) (
	*base.Vars, base.YieldVals, base.YieldErr, func() []base.Vals, func()) {
	store, err := compiledDataStore()
	if err != nil {
		t.Fatalf("SetupCompiledData store: %v", err)
	}
	return setupCompiled(t, store, "", stmt)
}

// setupCompiled re-parses/plans/converts the statement to obtain the live
// query-plan objects, and exposes them to the compiled operator code as runtime
// "parameters" via lzVars.Temps -- Temps[0] a GlueContext (with subqueries
// enabled), then the conv's plan objects at the same indices the baked datastore
// ops reference. (The compiled code carries only the SQL++ shape; the datastore
// arrives here.) Returns the vars + yield-capture funcs the generated code
// drives, plus a cleanup that removes the temp dir.
func setupCompiled(t *testing.T, store *glue.Store, namespace, stmt string) (
	lzVars *base.Vars, lzYieldVals base.YieldVals, lzYieldErr base.YieldErr,
	returnYields func() []base.Vals, cleanup func()) {
	s, err := glue.ParseStatement(stmt, namespace, true)
	if err != nil {
		t.Fatalf("setupCompiled parse: %v\n stmt: %s", err, stmt)
	}
	p, err := store.PlanStatement(s, namespace, nil, nil)
	if err != nil {
		t.Fatalf("setupCompiled plan: %v\n stmt: %s", err, stmt)
	}
	conv := &glue.Conv{Temps: []interface{}{nil}}
	if _, err = p.Accept(conv); err != nil {
		t.Fatalf("setupCompiled accept: %v\n stmt: %s", err, stmt)
	}

	tmpDir, vars := glue.MakeVars("", "n1k1fsc")

	// Build vars.Temps exactly as the interpreter driver does, so the int Temps
	// indices baked into the datastore ops line up with the live plan objects.
	gctx := glue.NewGlueContext(time.Now())
	gctx.InitSubqueries(store, namespace, conv.WithBindings()) // so compiled expression subqueries run
	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, gctx)
	vars.Temps = append(vars.Temps, conv.Temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	var yields []base.Vals
	yv := func(vals base.Vals) {
		var cp base.Vals
		for _, v := range vals {
			cp = append(cp, append(base.Val(nil), v...))
		}
		yields = append(yields, cp)
	}
	ye := func(err error) {
		if err != nil {
			t.Errorf("setupCompiled yieldErr: %v\n stmt: %s", err, stmt)
		}
	}

	return vars, yv, ye, func() []base.Vals { return yields }, func() { os.RemoveAll(tmpDir) }
}
