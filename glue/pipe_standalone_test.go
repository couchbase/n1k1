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
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue/emit"
)

// TestPipeEmitCbqFree asserts (no toolchain needed) that a fully-native FROM query
// emits, in PipeMode, a cbq-free standalone file: the datastore leaf is a
// base.DatastorePipe call (not a glue.DatastoreOp island), the file imports base
// only (no glue import, no n1ql build tag), and it parses as valid Go.
func TestPipeEmitCbqFree(t *testing.T) {
	root := writePlainBeers(t, 3)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := ParseStatement(`SELECT * FROM beers b`, "default", true)
	qp, _ := s.Store.PlanStatementQP(parsed, "default", nil, nil)
	pp, err := PlanConvert(qp)
	if err != nil {
		t.Fatal(err)
	}
	stringifyExprTrees(pp.topOp)

	emit.PipeMode = true
	src := wrapPrepare(pp.topOp.Labels, emit.OpToLines(pp.topOp))
	emit.PipeMode = false

	if !strings.Contains(src, `lzVars.Ctx.Pipe.Op(&base.Op{Kind: "datastore-scan-records"`) {
		t.Errorf("expected a base.DatastorePipe scan call; src:\n%s", src)
	}
	for _, bad := range []string{"glue.DatastoreOp", `import "github.com/couchbase/n1k1/glue"`, "//go:build n1ql"} {
		if strings.Contains(src, bad) {
			t.Errorf("cbq-free pipe emit should not contain %q", bad)
		}
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "gen.go", src, parser.AllErrors); err != nil {
		t.Fatalf("emitted standalone Go doesn't parse: %v\n%s", err, src)
	}
}

// TestPipeStandaloneCompileRun is the phase-2 end-to-end proof: compile a query to
// standalone Go (datastore leaves emitted as base.DatastorePipe calls, no glue/cbq)
// and RUN it over an engine.MemPipe -- with the go toolchain, cbq-off. The emitted
// program links only n1k1/base + n1k1/engine, so `go build` needs no private fork.
func TestPipeStandaloneCompileRun(t *testing.T) {
	if !GoToolchainDetect().Available {
		t.Skip("no go toolchain: compiled EXECUTE degrades to the interpreter")
	}

	// 1. Convert `SELECT * FROM beers b` and emit its body in PipeMode. SELECT *'s
	// star projection is native (base.ValsSelfObject), so with the scan going
	// through the Pipe the whole body is cbq-free.
	root := writePlainBeers(t, 3)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseStatement(`SELECT * FROM beers b`, "default", true)
	if err != nil {
		t.Fatal(err)
	}
	qp, err := s.Store.PlanStatementQP(parsed, "default", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	pp, err := PlanConvert(qp)
	if err != nil {
		t.Fatal(err)
	}
	stringifyExprTrees(pp.topOp)

	emit.PipeMode = true
	body := strings.Join(emit.OpToLines(pp.topOp), "")
	emit.PipeMode = false

	if strings.Contains(body, "glue.") {
		t.Fatalf("emitted body is not cbq-free (references glue.):\n%s", body)
	}

	// 2. Wrap the body in a standalone `package main` that feeds the compiled Run an
	// engine.MemPipe of inline records and prints each yielded row.
	prog := "package main\n\n" +
		"import (\n\t\"fmt\"\n\n" +
		"\t\"github.com/couchbase/n1k1/base\"\n" +
		"\t\"github.com/couchbase/n1k1/engine\"\n)\n\n" +
		"var _ = base.Labels(nil)\n\n" +
		"func Run(lzVars *base.Vars, lzYieldVals base.YieldVals, lzYieldErr base.YieldErr) {\n" +
		"\t_ = lzVars\n\t_ = lzYieldVals\n\t_ = lzYieldErr\n\n" + body + "\n}\n\n" +
		"func main() {\n" +
		"\tpipe := &engine.MemPipe{Data: map[string][]engine.MemRecord{\n" +
		"\t\t\"b\": {{ID: \"k1\", Doc: []byte(`{\"i\":1}`)}, {ID: \"k2\", Doc: []byte(`{\"i\":2}`)}},\n" +
		"\t}}\n" +
		"\tvars := &base.Vars{Ctx: &base.Ctx{Pipe: pipe}}\n" +
		"\tRun(vars, func(vals base.Vals) { fmt.Println(string(vals[0])) },\n" +
		"\t\tfunc(err error) { if err != nil { panic(err) } })\n}\n"

	// 3. Write it into a temp package under the repo (so it resolves base/engine via
	// the repo go.mod, no separate module setup) and `go run` it cbq-off.
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(repoRoot, "zz_n1k1gen_standalone_test")
	os.RemoveAll(tmp)
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	if err := os.WriteFile(filepath.Join(tmp, "main.go"), []byte(prog), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "run", "./"+filepath.Base(tmp))
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOPRIVATE=github.com/couchbase/*")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go run standalone: %v\n%s\n--- program ---\n%s", err, out, prog)
	}

	// The compiled program read the pipe's inline records; SELECT * wraps each doc
	// under its keyspace alias "b".
	got := strings.TrimSpace(string(out))
	want := "{\"b\":{\"i\":1}}\n{\"b\":{\"i\":2}}"
	if got != want {
		t.Errorf("standalone program output:\n%s\nwant:\n%s", got, want)
	}
}

// TestExprTreesOptimizeInline asserts that after ExprTreesOptimize a fully-native
// FROM query (field access + filter + arithmetic) emits cbq-free native inline (no
// glue.ExprStr island), while a denylisted-cluster expr (CASE) stays boxed -- the
// safe-landing contract until the nary/case/json emitters are fixed.
func TestExprTreesOptimizeInline(t *testing.T) {
	root := writePlainBeers(t, 3)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	emitPipe := func(q string) string {
		parsed, _ := ParseStatement(q, "default", true)
		qp, _ := s.Store.PlanStatementQP(parsed, "default", nil, nil)
		pp, cerr := PlanConvert(qp)
		if cerr != nil {
			t.Fatalf("%s: %v", q, cerr)
		}
		ExprTreesOptimize(pp.topOp)
		emit.PipeMode = true
		defer func() { emit.PipeMode = false }()
		src := func() (s string) { defer func() { recover() }(); return strings.Join(emit.OpToLines(pp.topOp), "") }()
		return src
	}
	for _, q := range []string{
		`SELECT b.i FROM beers b`,
		`SELECT b.i FROM beers b WHERE b.i >= 1`,
		`SELECT b.i + 10 AS x FROM beers b`,
	} {
		if src := emitPipe(q); strings.Contains(src, "glue.") {
			t.Errorf("%q should emit cbq-free native inline; found a glue ref", q)
		}
	}
	// A CASE (denylisted cluster) stays boxed as a glue.ExprStr island -- correct,
	// just not standalone -- so nothing breaks pending the emitter fix.
	cq := `SELECT CASE WHEN b.i > 0 THEN "pos" ELSE "zero" END AS c FROM beers b`
	if src := emitPipe(cq); !strings.Contains(src, "glue.ExprStr") {
		t.Errorf("CASE should stay boxed (denylisted); expected a glue.ExprStr island")
	}
}
