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

// This file exposes n1k1's SQL++ -> Go source compiler as a production surface
// (the .prepare dot-command / Session.Prepare), reusing the same emitter the
// compiler differential tests use to build test/tmp. See
// DESIGN-extensions-prepare.md.
//
// It is emit-only + gated + with an interpreter fallback: Prepare decides, via
// Preparable, whether a converted base.Op tree can be lowered to standalone Go,
// and emits the .go source when so. The gate reuses the SAME two conditions the
// compiler suite (test/suite_compiler_test.go) already tests:
//
//   1. every datastore-scan/-fetch op is bakeable to a Go literal (emit.BakeOp),
//      the datastore-leaf bridge; and
//   2. no per-row expression is boxed -- i.e. every ["exprTree", <expr>] param is
//      recognized by the native optimizer (exprIsNative), and there is no
//      ["exprStr", ...] param (the cbq text fallback). A boxed expression needs
//      cbq's Evaluate() at run time, which the generated (engine+base only) code
//      cannot do -- so the caller must fall back to the interpreter.
//
// OUT OF SCOPE (documented future work in DESIGN-extensions-prepare.md): the pipe
// protocol / thin child, DatastorePipe/EvalExpr, WASM, const-folding, and
// actually `go build`-ing (or running) the emitted source.

import (
	"fmt"
	"strings"

	"github.com/couchbase/query/expression"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue/emit"
)

// Preparable reports whether a converted base.Op tree can be lowered to
// standalone Go source (emit-only). When it can't, ok is false and reason is a
// human-readable explanation (e.g. a boxed expression's SQL text, or a
// non-bakeable datastore op kind) suitable for showing the user before the
// caller falls back to the interpreter.
//
// It reuses the exact conditions the compiler suite gates on:
//   - allDatastoreOpsBakeable (here: datastoreOpsBakeable, via emit.BakeOp), and
//   - no boxed expression (here: exprIsNative over each ["exprTree", <expr>] param;
//     an ["exprStr", ...] param is itself the boxed fallback).
//
// It does NOT mutate op (unlike the suite's stringifyExprTrees), so a caller can
// gate-check first and only emit -- via Prepare -- when ok.
func Preparable(op *base.Op) (ok bool, reason string) {
	if op == nil {
		return false, "nil op tree (unconverted plan)"
	}
	if r, bad := firstUnbakeableDatastoreOp(op); bad {
		return false, r
	}
	if r, bad := firstBoxedExpr(op); bad {
		return false, r
	}
	return true, ""
}

// firstUnbakeableDatastoreOp walks the tree and returns a reason for the first
// datastore op whose params can't be rendered as a Go literal (emit.BakeOp),
// mirroring allDatastoreOpsBakeable in test/suite_compiler_test.go.
func firstUnbakeableDatastoreOp(op *base.Op) (reason string, bad bool) {
	if op == nil {
		return "", false
	}
	if strings.HasPrefix(op.Kind, "datastore") {
		if _, ok := emit.BakeOp(op); !ok {
			return "datastore op not bakeable: " + op.Kind, true
		}
	}
	for _, c := range op.Children {
		if r, b := firstUnbakeableDatastoreOp(c); b {
			return r, true
		}
	}
	return "", false
}

// firstBoxedExpr walks the tree's op Params and returns a reason for the first
// boxed per-row expression -- one that the native optimizer doesn't recognize
// (an ["exprTree", <expr>] exprIsNative rejects) or that is already the cbq text
// fallback (an ["exprStr", ...] param). A boxed expression is exactly the cbq
// fallback (see DESIGN-exprs.md / glue/expr.go): the generated engine+base code
// has no cbq, so it can't evaluate it.
//
// The verdict per op uses the op's input (child) labels -- what the engine passes
// to MakeExprFunc -- via the same inputLabels/exprIsNative used by FormatConvPlan
// (explain.go). This is the unscoped static view; see FormatConvPlan's caveat.
func firstBoxedExpr(op *base.Op) (reason string, bad bool) {
	if op == nil {
		return "", false
	}
	labels := inputLabels(op)
	if r, b := firstBoxedInParam(labels, op.Params); b {
		return r, true
	}
	for _, c := range op.Children {
		if r, b := firstBoxedExpr(c); b {
			return r, true
		}
	}
	return "", false
}

// firstBoxedInParam recurses through a param value (a []interface{} tree that may
// hold ["exprTree", <expr>, ...] / ["exprStr", <text>, ...] pairs nested at any
// depth -- e.g. a project's list-of-projections, or a join's key pair) and
// reports the first boxed one.
func firstBoxedInParam(labels base.Labels, v interface{}) (reason string, bad bool) {
	arr, ok := v.([]interface{})
	if !ok {
		return "", false
	}
	if len(arr) >= 2 {
		if name, _ := arr[0].(string); name == "exprStr" {
			// Already the cbq text fallback -- boxed by construction.
			txt, _ := arr[1].(string)
			return "boxed expression (needs cbq): " + txt, true
		}
	}
	if e, isTree := exprTreeParam(arr); isTree {
		if !exprIsNative(labels, e) {
			return "boxed expression (needs cbq): " + e.String(), true
		}
		// A native exprTree: its operands are already validated by
		// ExprTreeOptimize as a whole, so no need to recurse into it.
		return "", false
	}
	for _, e := range arr {
		if r, b := firstBoxedInParam(labels, e); b {
			return r, true
		}
	}
	return "", false
}

// stringifyExprTrees rewrites, in place, every ["exprTree", <expression>] param
// in an Op tree to ["exprStr", <expression>.String()]. The conv layer emits
// expressions as live query/expression.Expression objects (exprTree), which the
// compiler can't bake into generated source. Serializing each to its N1QL text
// lets the exprStr prepare path (emit.OpToLines' ExprCatalog["exprStr"]) emit a
// glue.ExprStr(...) call that re-parses at the generated program's runtime.
// Returns false if any exprTree param can't be serialized (so Prepare can bail).
//
// (This is the production twin of the identically-named helper the compiler
// suite uses in test/suite_compiler_test.go -- same rewrite, so the emitted
// source matches the compiler differential's.)
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
		if len(op.Params) > 0 {
			if rw, isArr := rewrite(op.Params).([]interface{}); isArr {
				op.Params = rw
			}
		}
		for _, c := range op.Children {
			walk(c)
		}
	}
	walk(o)
	return ok
}

// PrepareFuncName is the name of the entry-point function in the emitted .go
// source. It matches DESIGN-extensions-prepare.md's Run(vars, yield, yieldErr).
const PrepareFuncName = "Run"

// wrapPrepare assembles emit.OpToLines' body into a complete, parseable Go file:
// a `package n1k1gen` with the on-demand imports and a single
// Run(lzVars, lzYieldVals, lzYieldErr) whose body is the fused plan. The
// projection labels travel as a companion PrepareLabels var so a caller can
// interpret the base.Vals rows Run yields.
//
// This mirrors how the compiler differential wraps the same lines into
// test/tmp's TestGeneratedFS_N funcs (test/suite_compiler_test.go), minus the
// test scaffolding -- so the body is byte-for-byte the compiler's, just hosted in
// a callable Run instead of a *testing.T harness.
func wrapPrepare(labels base.Labels, lines []string) string {
	emitted := strings.Join(lines, "")

	var b strings.Builder
	b.WriteString("//go:build n1ql\n\n")
	b.WriteString("// Code generated by glue.Prepare (n1k1 .prepare). DO NOT EDIT.\n")
	b.WriteString("//\n")
	b.WriteString("// Run executes the fused query plan, yielding each result row's base.Vals\n")
	b.WriteString("// to lzYieldVals (columns ordered per PrepareLabels) and any error to\n")
	b.WriteString("// lzYieldErr. The caller supplies lzVars (see glue setup); datastore leaves\n")
	b.WriteString("// dispatch through glue.DatastoreOp islands baked inline.\n")
	b.WriteString("package n1k1gen\n\n")

	b.WriteString("import \"github.com/couchbase/n1k1/base\"\n")
	b.WriteString("import \"github.com/couchbase/n1k1/glue\"\n")
	for _, oi := range emit.OptionalImports {
		if strings.Contains(emitted, oi.Qualifier) {
			fmt.Fprintf(&b, "import %q\n", oi.Path)
		}
	}
	b.WriteString("\n")
	// Keep the always-on imports referenced even if an emitted body doesn't use
	// them directly (matches the suite's `var _ = glue.ExprStr` guard).
	b.WriteString("var _ = glue.ExprStr\n")
	b.WriteString("var _ = base.Labels(nil)\n\n")

	fmt.Fprintf(&b, "// PrepareLabels are the projection labels for the rows Run yields.\n")
	fmt.Fprintf(&b, "var PrepareLabels = %#v\n\n", labels)

	fmt.Fprintf(&b, "func %s(lzVars *base.Vars, lzYieldVals base.YieldVals, lzYieldErr base.YieldErr) {\n", PrepareFuncName)
	b.WriteString("\t_ = lzVars\n")
	b.WriteString("\t_ = lzYieldVals\n")
	b.WriteString("\t_ = lzYieldErr\n\n")
	for _, ln := range lines {
		b.WriteString(ln)
	}
	b.WriteString("}\n")

	return b.String()
}

// Prepare runs parse -> plan -> convert for a statement and, when the converted
// op tree is Preparable, emits standalone Go source for it (emit-only). On
// success it returns (goSource, true, "", nil). When the tree can't be compiled
// (a boxed expression that needs cbq, or a non-bakeable datastore op), it returns
// ("", false, reason, nil) WITHOUT emitting -- the caller falls back to running
// the statement through the interpreter (Session.Run), so a query needing cbq
// still produces results. A parse or plan error is returned verbatim (err != 0),
// as is an unconvertible plan (ok=false, reason set).
//
// This is the user-facing surface behind the CLI's .prepare / -prepare (see
// cmd/n1k1). It reuses the SAME emitter (emit.OpToLines) the compiler
// differential tests use to build test/tmp, so what it prints is exactly what the
// compiler would compile.
func (s *Session) Prepare(stmt string) (goSource string, ok bool, reason string, err error) {
	// Recover from the panics some unsupported plans raise (mirrors Run).
	defer func() {
		if r := recover(); r != nil {
			goSource, ok, reason, err = "", false, fmt.Sprintf("panic: %v", r), nil
		}
	}()

	parsed, err := ParseStatement(stmt, s.Namespace, true)
	if err != nil {
		return "", false, "", err
	}

	qp, err := s.Store.PlanStatementQP(parsed, s.Namespace, s.NamedArgs, nil)
	if err != nil {
		return "", false, "", err
	}
	p := qp.PlanOp()

	// EXPLAIN doesn't execute -- it has no runnable plan to compile.
	if ex := findExplain(p); ex != nil {
		return "", false, "EXPLAIN statement (nothing to compile)", nil
	}

	conv := &Conv{Temps: []interface{}{nil}}
	if _, err = p.Accept(conv); err != nil {
		return "", false, "unconvertible plan: " + err.Error(), nil
	}
	if conv.TopOp == nil {
		return "", false, "nil TopOp (unconverted plan)", nil
	}

	// Apply the same optimizations the execution path (Run) does, so the emitted
	// plan matches what the interpreter would run.
	if DiscardElision {
		elideDiscarded(conv.TopOp)
	}
	maybeColumnarOptimize(conv.TopOp, conv.Temps)

	// Gate on the UN-stringified tree: Preparable inspects ["exprTree", <expr>]
	// params to decide native vs boxed. (stringifyExprTrees below would erase that
	// distinction by turning every expr into text.)
	if ok, reason = Preparable(conv.TopOp); !ok {
		return "", false, reason, nil
	}

	// Rewrite live exprTree objects to exprStr text so the tree is expressible as
	// generated source. Preparable already proved every expr is native, so an
	// unserializable exprTree here is unexpected -- treat it as not-prepareable.
	if !stringifyExprTrees(conv.TopOp) {
		return "", false, "expression not serializable to text", nil
	}

	return wrapPrepare(conv.TopOp.Labels, emit.OpToLines(conv.TopOp)), true, "", nil
}
