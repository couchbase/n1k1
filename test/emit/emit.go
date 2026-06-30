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

// Package emit holds the n1k1-compiler codegen helpers shared by the compiler
// test/benchmark generators: OpToLines runs the compiler (intermed.ExecOp with
// the Emit hooks captured) over a base.Op tree and returns the generated Go
// source lines; BakeOp renders a base.Op subtree as a Go literal (for the
// datastore-scan "island"); OptionalImports is the import set the generated
// `package tmp` files pull in on demand. Used by test/ (the differential
// generators) and test/benchmark/ (the Phase-2 bench generator).
package emit

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/intermed"
)

// captured records where an emitted, captured chunk landed in the output stack.
type captured struct {
	At  int
	Pos int
	Out []string
}

// OptionalImports are imported by a generated file only if the emitted code
// actually references the qualifier -- so the generated `package tmp` compiles
// without unused-import errors regardless of which ops a query used.
var OptionalImports = []struct{ Qualifier, Path string }{
	{"os.", "os"},
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

// OpToLines runs the n1k1 *compiler* (intermed.ExecOp with the Emit hooks
// captured) over a single base.Op tree and returns the generated Go source
// lines for that query. This is the core codegen step shared by the
// TestCasesSimple generator, the suite-corpus generator and the bench generator.
func OpToLines(o *base.Op) []string {
	outStack := [][]string{nil}

	appendOut := func(at int, s string) int {
		out := outStack[at]
		pos := len(out)
		out = append(out, s)
		outStack[at] = out
		return pos
	}

	appendOuts := func(at int, ss []string) int {
		out := outStack[at]
		pos := len(out)
		out = append(out, ss...)
		outStack[at] = out
		return pos
	}

	intermed.Emit = func(format string, a ...interface{}) (
		n int, err error) {
		s := fmt.Sprintf(format, a...)
		s = strings.Replace(s, "LzScope", "true", -1)

		appendOut(len(outStack)-1, s)

		return len(s), nil
	}

	intermed.EmitLift = func(format string, a ...interface{}) (
		n int, err error) {
		s := fmt.Sprintf(format, a...)
		s = strings.Replace(s, "LzScope", "true", -1)

		appendOut(0, s)

		return len(s), nil
	}

	intermed.EmitPush = func(path, pathItem string) string {
		outStack = append(outStack, nil)

		return path + "_" + pathItem
	}

	emitPopsCaptured := map[string]captured{}

	intermed.EmitPop = func(path, pathItem string) {
		out := outStack[len(outStack)-1]

		outStack[len(outStack)-1] = nil
		outStack = outStack[0 : len(outStack)-1]

		at := len(outStack) - 1
		pos := appendOuts(at, out)

		emitPopsCaptured[path+"_"+pathItem] = captured{
			At:  at,
			Pos: pos,
			Out: out,
		}
	}

	intermed.EmitCaptured = func(path, pathItem, orig string) {
		key := path
		if pathItem != "" {
			key = key + "_" + pathItem
		}

		captured, ok := emitPopsCaptured[key]
		if !ok {
			if false {
				fmt.Printf("====================================================\n")
				fmt.Printf("UNKNOWN CAPTURED - key: %s, ok: %t\n", key, ok)
				fmt.Printf("  emitPopsCaptured: %+v\n", emitPopsCaptured)
			}

			intermed.Emit(orig)

			return
		}

		capturedOut := captured.Out

		// Scan backwards until a func/closure.
		start := len(capturedOut) - 1
		for start >= 0 {
			if strings.Index(capturedOut[start], " func(") > 0 {
				break
			}

			start = start - 1
		}

		// Emit the body of the last func/closure.
		var out []string

		scopes := 0 // Count scope / braces of IF statements.

		for i := start + 1; i < len(capturedOut); i++ {
			trimmed := strings.TrimSpace(capturedOut[i])
			if len(trimmed) <= 0 {
				continue
			}

			if strings.HasPrefix(trimmed, "return ") { // Filter away return lines.
				continue
			}

			if strings.HasPrefix(trimmed, "if ") ||
				strings.HasPrefix(trimmed, "for ") {
				scopes += 1
			}

			if strings.HasSuffix(trimmed, "}") { // Ignore IF close braces.
				if scopes > 0 {
					scopes -= 1
				} else {
					continue
				}
			}

			out = append(out, capturedOut[i])
		}

		// Collapse sibling lines that look like...
		//    a = foo
		//    bar := a
		// Into...
		//    bar := foo
		for i := 1; i < len(out); i++ {
			if len(out[i-1]) > 0 {
				prev := strings.Split(strings.TrimSpace(out[i-1]), " = ")
				if len(prev) == 2 {
					curr := strings.Split(strings.TrimSpace(out[i]), " := ")
					if len(curr) == 2 &&
						prev[0] == curr[1] {
						out[i-1] = curr[0] + " := " + prev[1]
						out[i] = ""
					}
				}
			}
		}

		appendOuts(len(outStack)-1, out)

		if captured.At >= 0 {
			clearFuncLines(outStack[captured.At][captured.Pos : captured.Pos+len(captured.Out)])

			captured.At = -1
		}
	}

	intermed.ExprCatalog["exprStr"] = func(
		lzVars *base.Vars, labels base.Labels,
		params []interface{}, path string) (lzExprFunc base.ExprFunc) {
		intermed.EmitLift(
			"var lzExprStrFunc%s = glue.ExprStr(lzVars, %#v, %#v, %#v)\n",
			path, labels, params, path)

		intermed.Emit("lzVal = lzExprStrFunc%s(lzVals, lzYieldErr)\n", path)

		return lzExprFunc
	}

	// TODO: Need to handle exprTree when in compiled mode?

	// Datastore ops hit ExecOp's default branch (ExecOpEx). In compiled mode we
	// emit them as an interpreted "island": a glue.DatastoreOp(<baked op>, ...)
	// call. The op node is bakeable as a Go literal (its Params are int Temps
	// indices, not live objects); the live query-plan objects are supplied at
	// the generated program's runtime via lzVars.Temps (see SetupCompiledSuite).
	// This is the datastore-scan bridge: compiled operators above, interpreted
	// scan/fetch below, runtime data passed in through lzVars.
	intermed.ExecOpEx = func(o *base.Op, lzVars *base.Vars,
		lzYieldVals base.YieldVals, lzYieldErr base.YieldErr, path, pathItem string) {
		lit, ok := BakeOp(o)
		if !ok {
			intermed.Emit("UNBAKEABLE_DATASTORE_OP_%s // forces a compile error\n", o.Kind)
			return
		}
		intermed.Emit("glue.DatastoreOp(%s, lzVars, lzYieldVals, lzYieldErr, %q, %q)\n",
			lit, path, pathItem)
	}

	intermed.ExecOp(o,
		&base.Vars{Ctx: &base.Ctx{ExprCatalog: intermed.ExprCatalog}},
		nil, nil, "Top", "EO")

	if len(outStack) != 1 {
		panic(fmt.Sprintf("len(outStack) should be height 1, got: %d,\n"+
			" op: %+v\n outStack: %+v",
			len(outStack), o, outStack))
	}

	return outStack[len(outStack)-1]
}

// bakeParam renders a single op param as a Go literal expression. Datastore op
// params are int Temps-indices (and, after exprTree->exprStr rewriting, nested
// string/[]interface{} trees), so only those primitive shapes are supported;
// anything else (e.g. a live object) returns ok=false so the caller can skip.
func bakeParam(v interface{}) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "nil", true
	case bool:
		return fmt.Sprintf("%v", x), true
	case int:
		return strconv.Itoa(x), true
	case int64:
		return fmt.Sprintf("int64(%d)", x), true
	case string:
		return fmt.Sprintf("%q", x), true
	case base.Val:
		return fmt.Sprintf("base.Val(%q)", string(x)), true
	case []interface{}:
		parts := make([]string, len(x))
		for i, e := range x {
			s, ok := bakeParam(e)
			if !ok {
				return "", false
			}
			parts[i] = s
		}
		return "[]interface{}{" + strings.Join(parts, ", ") + "}", true
	default:
		return "", false
	}
}

// BakeOp renders a base.Op subtree as a Go literal expression. Used to emit
// datastore ops (and their children) as the argument to a generated
// glue.DatastoreOp(...) call. Returns ok=false if any param isn't bakeable.
func BakeOp(o *base.Op) (string, bool) {
	if o == nil {
		return "nil", true
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "&base.Op{Kind: %q", o.Kind)

	sb.WriteString(", Labels: base.Labels{")
	for i, l := range o.Labels {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%q", l)
	}
	sb.WriteString("}")

	if len(o.Params) > 0 {
		ps, ok := bakeParam(o.Params)
		if !ok {
			return "", false
		}
		sb.WriteString(", Params: ")
		sb.WriteString(ps)
	}

	if len(o.Children) > 0 {
		sb.WriteString(", Children: []*base.Op{")
		for i, c := range o.Children {
			if i > 0 {
				sb.WriteString(", ")
			}
			cs, ok := BakeOp(c)
			if !ok {
				return "", false
			}
			sb.WriteString(cs)
		}
		sb.WriteString("}")
	}

	sb.WriteString("}")
	return sb.String(), true
}

func clearFuncLines(lines []string) {
	var stack []string // Tracks nesting of lines.

	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])

		clear := false

		// NOTE: Assumes func() and its start brace are on same line.
		if strings.Index(trimmed, " func(") > 0 {
			stack = append(stack, "func")
		} else if strings.HasSuffix(trimmed, "{") {
			stack = append(stack, "{")
		}

		for _, v := range stack {
			if v == "func" {
				clear = true
				break
			}
		}

		if strings.HasSuffix(trimmed, "}") {
			stack = stack[0 : len(stack)-1]
		}

		// Only keep "var foo = initializer" idioms of the lifted /
		// hosted variables.
		hasVar := strings.HasPrefix(trimmed, "var ")
		hasAssign := strings.Index(trimmed, " = ") > 0

		if !(hasVar && hasAssign) {
			clear = true
		}

		if clear {
			lines[i] = ""
		}
	}
}
