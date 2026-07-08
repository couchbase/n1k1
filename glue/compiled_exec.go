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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
	"github.com/couchbase/n1k1/glue/emit"
)

// Close releases resources the session holds -- currently the temp dirs of any
// compiled EXECUTE child binaries built by executeCompiled. Safe to call multiple
// times; a session that never compiled needs no Close. A long-lived REPL/server
// should Close when done with a session.
func (s *Session) Close() {
	for _, ps := range s.prepareds {
		if ps.compiledCleanup != nil {
			ps.compiledCleanup()
			ps.compiledCleanup, ps.compiledBin = nil, ""
		}
	}
}

// executeCompiled runs a prepared statement as a COMPILED standalone program (the
// -prepare=full path). It emits the plan as cbq-free Go (datastore leaves ->
// base.DatastorePipe), go-builds it ONCE into a child binary cached on the
// preparedStmt, then per EXECUTE: scans the query's keyspaces in-process, ships the
// records to the child over stdin, and reads result rows back over stdout. The
// child links n1k1/base + n1k1/engine only -- no cbq -- so `go build` needs no
// private fork.
//
// Returns (result, true, nil) on success; (nil, false, nil) to fall back to the
// interpreter when the statement isn't standalone-compilable (a boxed expression),
// the go toolchain is absent, or the n1k1 source isn't locatable (N1K1_SRC) to
// build against -- the graceful-degradation contract.
func (s *Session) executeCompiled(ps *preparedStmt) (*Result, bool, error) {
	if !ps.compiledTried {
		ps.compiledTried = true // attempt the build at most once, whatever the outcome
		bin, cleanup, err := s.buildCompiled(ps)
		if err != nil {
			return nil, false, err
		}
		ps.compiledBin, ps.compiledCleanup = bin, cleanup
	}
	if ps.compiledBin == "" {
		return nil, false, nil // not compilable / no toolchain / no source -> interpret
	}

	inputs, err := s.scanCompiledInputs(ps.compiled)
	if err != nil {
		return nil, false, err
	}
	rows, err := runCompiledChild(ps.compiledBin, inputs)
	if err != nil {
		return nil, false, err
	}
	return &Result{Labels: ps.compiled.topOp.Labels, Rows: rows, Count: len(rows)}, true, nil
}

// buildCompiled emits + go-builds the standalone child binary for a prepared
// statement, returning ("", cleanup, nil) when it can't be compiled standalone
// (a boxed expr) or the toolchain/source is unavailable -- a clean fall-back, not
// an error. The build is done against the n1k1 source at ProvenanceSourceDir
// (N1K1_SRC); a deployed binary without a source checkout degrades to the
// interpreter (embedding the source is future work -- DESIGN-prepare.md).
func (s *Session) buildCompiled(ps *preparedStmt) (bin string, cleanup func(), err error) {
	if !GoToolchainDetect().Available {
		return "", func() {}, nil
	}
	srcDir := ProvenanceSourceDir()
	if srcDir == "" {
		return "", func() {}, nil // no n1k1 source to build against
	}

	// Re-convert the statement into a FRESH tree so stringifyExprTrees (which
	// mutates) doesn't disturb ps.compiled (reused by the interpreter/scan path).
	qp, err := s.Store.PlanStatementQP(ps.stmt, s.Namespace, nil, nil)
	if err != nil {
		return "", func() {}, err
	}
	pp, err := PlanConvert(qp)
	if err != nil {
		return "", func() {}, err
	}
	stringifyExprTrees(pp.topOp)

	emit.PipeMode = true
	body := strings.Join(emit.OpToLines(pp.topOp), "")
	emit.PipeMode = false
	if strings.Contains(body, "glue.") {
		return "", func() {}, nil // a boxed expr remains -> not cbq-free -> interpret
	}

	dir, err := os.MkdirTemp("", "n1k1compiled")
	if err != nil {
		return "", func() {}, err
	}
	cleanup = func() { os.RemoveAll(dir) }

	prov := ProvenanceCapture(srcDir)
	files := map[string]string{
		"go.mod":  compiledGoMod(srcDir),
		"main.go": compiledMain(body, prov.Stamp()),
	}
	for name, content := range files {
		if werr := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); werr != nil {
			cleanup()
			return "", func() {}, werr
		}
	}

	bin = filepath.Join(dir, "n1k1compiled")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOPRIVATE=github.com/couchbase/*", "GOFLAGS=-mod=mod")
	if out, berr := cmd.CombinedOutput(); berr != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("compiled EXECUTE: go build failed: %v\n%s", berr, out)
	}
	return bin, cleanup, nil
}

// compiledInput is one record the parent ships to the child: the scan alias it
// belongs to (the child keys its MemPipe by alias), the doc key, and the doc JSON.
type compiledInput struct {
	A   string          `json:"a"`
	ID  string          `json:"id"`
	Doc json.RawMessage `json:"doc"`
}

// scanCompiledInputs runs the plan's datastore-scan-records leaves in-process (the
// file datastore, via the interpreter) and collects each record under its scan
// alias, to feed the compiled child. This is the "thin child" split: the parent
// does the I/O (glue/records), the child does the compiled compute.
func (s *Session) scanCompiledInputs(pp *PreparedPlan) ([]compiledInput, error) {
	leaves := scanRecordLeaves(pp.topOp)
	if len(leaves) == 0 {
		return nil, nil
	}

	tmpDir, vars := MakeVars("", "n1k1cscan")
	defer os.RemoveAll(tmpDir)
	vars.Ctx.Pipe = nil // read the files, not a pipe

	gctx := NewGlueContext(time.Now())
	gctx.InitSubqueries(s.Store, s.Namespace, pp.withBindings, pp.subqueries)
	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, gctx)
	vars.Temps = append(vars.Temps, pp.temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	orig := engine.ExecOpEx
	defer func() { engine.ExecOpEx = orig }()
	engine.ExecOpEx = DatastoreOp

	var inputs []compiledInput
	var scanErr error
	for _, leaf := range leaves {
		alias := engine.MemPipeScanAlias(leaf)
		engine.ExecOp(leaf, vars, func(vals base.Vals) {
			if len(vals) < 2 {
				return
			}
			id, _ := strconv.Unquote(string(vals[1])) // ^id is a quoted JSON string
			inputs = append(inputs, compiledInput{
				A: alias, ID: id, Doc: append(json.RawMessage(nil), vals[0]...),
			})
		}, func(e error) {
			if e != nil && scanErr == nil {
				scanErr = e
			}
		}, "", "")
		if scanErr != nil {
			return nil, scanErr
		}
	}
	return inputs, nil
}

// scanRecordLeaves collects a plan's datastore-scan-records leaf ops (in pre-order).
func scanRecordLeaves(o *base.Op) []*base.Op {
	if o == nil {
		return nil
	}
	var out []*base.Op
	if o.Kind == "datastore-scan-records" {
		out = append(out, o)
	}
	for _, c := range o.Children {
		out = append(out, scanRecordLeaves(c)...)
	}
	return out
}

// runCompiledChild spawns the compiled child, streams the scanned records to its
// stdin (one JSON object per line), and reads the result rows from its stdout (one
// JSON row per line).
func runCompiledChild(bin string, inputs []compiledInput) ([]json.RawMessage, error) {
	cmd := exec.Command(bin)
	var stdin bytes.Buffer
	for i := range inputs {
		b, _ := json.Marshal(&inputs[i])
		stdin.Write(b)
		stdin.WriteByte('\n')
	}
	cmd.Stdin = &stdin
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("compiled EXECUTE: child failed: %v\n%s", err, stderr.String())
	}

	var rows []json.RawMessage
	sc := bufio.NewScanner(&stdout)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		rows = append(rows, append(json.RawMessage(nil), line...))
	}
	return rows, sc.Err()
}

// compiledGoMod is the child module's go.mod: a throwaway module that resolves
// n1k1/base + n1k1/engine (and their public deps, e.g. rhmap) from the local n1k1
// source at srcDir. No cbq is referenced by a cbq-free compiled query, so the build
// needs no private fork.
func compiledGoMod(srcDir string) string {
	gover := "1.21"
	if tc := GoToolchainDetect(); tc.Version != "" {
		if fs := strings.Fields(tc.Version); len(fs) >= 3 {
			gover = strings.TrimPrefix(fs[2], "go")
		}
	}
	return fmt.Sprintf("module n1k1compiled\n\ngo %s\n\nrequire github.com/couchbase/n1k1 v0.0.0\n\nreplace github.com/couchbase/n1k1 => %s\n",
		gover, srcDir)
}

// compiledMain wraps an emitted cbq-free query body in a runnable child: it reads
// the parent's records from stdin into an engine.MemPipe, runs the compiled Run,
// and writes each result row's JSON to stdout. (Result rows are single-value today
// -- SELECT * / RAW -- since a field-access projection still boxes; a multi-column
// native projection would need a base-level Vals->JSON assembler here.)
func compiledMain(body, provStamp string) string {
	return "// Code generated for a compiled n1k1 EXECUTE. DO NOT EDIT.\n" +
		"// " + provStamp + "\n" +
		"package main\n\n" +
		"import (\n\t\"bufio\"\n\t\"encoding/json\"\n\t\"fmt\"\n\t\"os\"\n\n" +
		"\t\"github.com/couchbase/n1k1/base\"\n\t\"github.com/couchbase/n1k1/engine\"\n)\n\n" +
		"var _ = base.Labels(nil)\n\n" +
		"func Run(lzVars *base.Vars, lzYieldVals base.YieldVals, lzYieldErr base.YieldErr) {\n" +
		"\t_ = lzVars\n\t_ = lzYieldVals\n\t_ = lzYieldErr\n\n" + body + "\n}\n\n" +
		"func main() {\n" +
		"\tdata := map[string][]engine.MemRecord{}\n" +
		"\tin := bufio.NewScanner(os.Stdin)\n" +
		"\tin.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)\n" +
		"\tfor in.Scan() {\n" +
		"\t\tvar r struct{ A, ID string; Doc json.RawMessage }\n" +
		"\t\tif json.Unmarshal(in.Bytes(), &r) == nil {\n" +
		"\t\t\tdata[r.A] = append(data[r.A], engine.MemRecord{ID: r.ID, Doc: r.Doc})\n" +
		"\t\t}\n\t}\n" +
		"\tvars := &base.Vars{Ctx: &base.Ctx{Pipe: &engine.MemPipe{Data: data}}}\n" +
		"\tout := bufio.NewWriter(os.Stdout)\n\tdefer out.Flush()\n" +
		"\tRun(vars, func(vals base.Vals) {\n" +
		"\t\tif len(vals) > 0 { out.Write(vals[0]); out.WriteByte('\\n') }\n" +
		"\t}, func(err error) {\n" +
		"\t\tif err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }\n" +
		"\t})\n}\n"
}
