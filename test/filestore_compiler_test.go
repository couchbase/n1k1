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

// TestFilestoreWithCompiler is the compiler half of the filestore differential
// test. It reuses the same generate->compile->run->compare harness as
// TestCasesSimpleWithCompiler, but instead of hand-built Op trees it feeds the
// compiler the Op trees the glue layer derives from real SQL++ corpus queries.
//
// Scope (this milestone): only "datastore-free" cases -- those whose Op tree
// contains no datastore-scan-*/datastore-fetch leaf. Those are the FROM-less
// SELECT <expr> cases (constant/scalar-function queries), whose leaf is the
// "nil" single-empty-row op. They need no datastore wiring, so the compiler can
// emit fully self-contained Go today. (Cases with a FROM bottom out in a
// datastore scan whose Params is an index into a live query-plan object held in
// vars.Temps -- not yet expressible as generated source; that's the next step.)
//
// Of these, we only emit cases the *interpreter* already passes, so the
// generated TestGeneratedFS_N functions are a clean differential oracle:
// compiled output must match the corpus's expected results.

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"

	"github.com/couchbase/query/expression"
)

// stringifyExprTrees rewrites, in place, every ["exprTree", <expression>] param
// in an Op tree to ["exprStr", <expression>.String()]. The glue conv emits
// expressions as live query/expression.Expression objects (exprTree), which the
// compiler cannot bake into generated source. Serializing each back to its N1QL
// text lets the existing exprStr codegen path emit a glue.ExprStr(...) call that
// re-parses the text at the compiled program's runtime -- the same hybrid the
// hand-written TestCasesSimple cases already use. Returns false if any exprTree
// param can't be serialized (so the caller can skip the case).
func stringifyExprTrees(o *base.Op) (ok bool) {
	ok = true
	var rewrite func(v interface{}) interface{}
	rewrite = func(v interface{}) interface{} {
		arr, isArr := v.([]interface{})
		if !isArr {
			return v
		}
		if len(arr) >= 2 {
			if name, _ := arr[0].(string); name == "exprTree" {
				e, isExpr := arr[1].(expression.Expression)
				if !isExpr || e == nil {
					ok = false
					return v
				}
				rest := []interface{}{"exprStr", e.String()}
				return append(rest, arr[2:]...)
			}
		}
		out := make([]interface{}, len(arr))
		for i, e := range arr {
			out[i] = rewrite(e)
		}
		return out
	}
	var walk func(op *base.Op)
	walk = func(op *base.Op) {
		if op == nil {
			return
		}
		for i := range op.Params {
			op.Params[i] = rewrite(op.Params[i])
		}
		for _, c := range op.Children {
			walk(c)
		}
	}
	walk(o)
	return ok
}

// convOf parses, plans and converts a statement to a n1k1 Op tree, recovering
// from the panics some unsupported plans raise (mirrors n1k1RunStatement).
func convOf(store *glue.Store, stmt string) (cv *glue.Conv) {
	defer func() { recover() }()
	s, err := glue.ParseStatement(stmt, "default", true)
	if err != nil {
		return nil
	}
	p, err := store.PlanStatement(s, "default", nil, nil)
	if err != nil {
		return nil
	}
	c := &glue.Conv{Temps: []interface{}{nil}}
	if _, err = p.Accept(c); err != nil || c.TopOp == nil {
		return nil
	}
	return c
}

// needsRuntimeState reports whether an Op tree contains any op that depends on
// runtime state the standalone generated harness does not set up: datastore
// scans/fetches (live query-plan objects in vars.Temps), and the temp-*/
// sequence ops (subquery/LET machinery that populates vars.Temps[1+] at run
// time). Such a tree can't run as self-contained generated code yet -- the leaf
// would dereference a nil Temps slot. We restrict this milestone to trees free
// of them (in practice: FROM-less SELECT <expr> over the "nil" single-row leaf).
func needsRuntimeState(o *base.Op) bool {
	if o == nil {
		return false
	}
	k := o.Kind
	if strings.HasPrefix(k, "datastore") || strings.HasPrefix(k, "temp") || k == "sequence" {
		return true
	}
	for _, c := range o.Children {
		if needsRuntimeState(c) {
			return true
		}
	}
	return false
}

// nonDetTokens are N1QL builtins whose result depends on wall-clock time or
// randomness. A query using one isn't a stable compiler differential: the
// compiled program runs at a different instant than the interpreter oracle (and
// its glue context's "now" basis differs), so results legitimately diverge.
var nonDetTokens = []string{
	"now_", "clock_", "uuid(", "random(", "rand(",
}

func nonDeterministic(stmt string) bool {
	low := strings.ToLower(stmt)
	for _, tok := range nonDetTokens {
		if strings.Contains(low, tok) {
			return true
		}
	}
	return false
}

// optionalImports are imported by the generated file only if the emitted code
// actually references the package (Go errors on unused imports). The always-on
// imports (os, testing, base, glue, test) are emitted unconditionally.
var optionalImports = []struct{ qualifier, path string }{
	{"bufio.", "bufio"},
	{"bytes.", "bytes"},
	{"heap.", "container/heap"},
	{"binary.", "encoding/binary"},
	{"math.", "math"},
	{"strconv.", "strconv"},
	{"strings.", "strings"},
	{"reflect.", "reflect"},
	{"store.", "github.com/couchbase/rhmap/store"},
}

func TestFilestoreWithCompiler(t *testing.T) {
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

	type genCase struct {
		about    string
		labels   base.Labels
		expected string // expected results as JSON
		lines    []string
	}
	var gen []genCase
	var considered, datastoreFree int

	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var cases []map[string]interface{}
		if json.Unmarshal(b, &cases) != nil {
			continue
		}
		for ci, c := range cases {
			stmt, results, ok := caseRunnable(c)
			if !ok {
				continue
			}
			if nonDeterministic(stmt) {
				continue // wall-clock / random funcs aren't a stable differential
			}
			considered++

			conv := convOf(store, stmt)
			if conv == nil || needsRuntimeState(conv.TopOp) {
				continue
			}
			datastoreFree++

			// Only emit cases the interpreter already gets right, so the
			// generated test is a clean compiler-vs-oracle differential.
			got, runErr := n1k1RunStatement(store, stmt)
			if runErr != nil || !rowsMatch(got, results) {
				continue
			}

			// Rewrite live exprTree objects to exprStr text so the tree is
			// expressible as generated source; skip if any can't serialize.
			if !stringifyExprTrees(conv.TopOp) {
				continue
			}

			expectedJSON, _ := json.Marshal(results)

			gen = append(gen, genCase{
				about:    fmt.Sprintf("%s[%d] %s", filepath.Base(f), ci, oneLine(stmt)),
				labels:   conv.TopOp.Labels,
				expected: string(expectedJSON),
				lines:    emitOpToLines(conv.TopOp),
			})
		}
	}

	// Decide imports: always-on, plus any optional package the emitted code
	// references across all selected cases.
	var allLines strings.Builder
	for _, g := range gen {
		for _, ln := range g.lines {
			allLines.WriteString(ln)
		}
	}
	emitted := allLines.String()

	c := []string{
		"//go:build n1ql",
		``,
		"// Code generated by TestFilestoreWithCompiler. DO NOT EDIT.",
		``,
		"package tmp",
		``,
		`import "os"`,
		`import "testing"`,
		`import "github.com/couchbase/n1k1/base"`,
		`import "github.com/couchbase/n1k1/glue"`,
		`import "github.com/couchbase/n1k1/test"`,
	}
	for _, oi := range optionalImports {
		if strings.Contains(emitted, oi.qualifier) {
			c = append(c, fmt.Sprintf("import %q", oi.path))
		}
	}
	c = append(c, ``, "var _ = glue.ExprStr", "var _ = base.Labels(nil)", ``)

	for i, g := range gen {
		c = append(c, "// ------------------------------------------")
		c = append(c, "// "+g.about)
		c = append(c, fmt.Sprintf("func TestGeneratedFS_%d(t *testing.T) {", i))
		c = append(c, `  lzTmpDir, lzVars, lzYieldVals, lzYieldErr, returnYields :=`)
		c = append(c, fmt.Sprintf(`    test.MakeYieldCaptureFuncs(nil, %d, %q)`, i, ""))
		c = append(c, "  os.RemoveAll(lzTmpDir)")
		c = append(c, "  _ = lzVars")
		c = append(c, "  _ = lzYieldVals")
		c = append(c, "  _ = lzYieldErr")
		c = append(c, "")
		c = append(c, g.lines...)
		c = append(c, "")
		c = append(c, fmt.Sprintf("  test.CheckCompiledRows(t, %#v, returnYields(), %q, %q)",
			g.labels, g.expected, g.about))
		c = append(c, "}\n")
	}

	err = ioutil.WriteFile("./tmp/generated_by_filestore_compiler_test.go",
		[]byte(strings.Join(c, "\n")), 0644)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("filestore compiler: considered=%d datastore-free=%d emitted=%d",
		considered, datastoreFree, len(gen))
}
