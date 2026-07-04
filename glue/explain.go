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
	"bytes"
	"fmt"
	"strings"

	"github.com/couchbase/query/expression"

	"github.com/couchbase/n1k1/base"
)

// boxedMarker flags a project/filter expression that the engine can't evaluate on
// its native byte path, so every row is converted into a cbq value.Value and handed
// to expression.Evaluate -- correct, but GC-heavy. An unmarked expression is native
// (evaluated directly on the row's raw JSON bytes, no allocation). A distinct
// non-ASCII marker so it stands out against the ASCII projection-list brackets.
const boxedMarker = "⟨boxed⟩"

// FormatConvPlan renders n1k1's converted op tree as an indented text tree. For
// project and filter operators it flags each boxed expression with boxedMarker;
// unmarked expressions are native.
//
// The verdict is the same static ExprTreeOptimize decision the engine makes when
// it builds the expression closure (see ExprTree). It assumes no correlated / WITH
// scope; an expression under such a scope is forced to boxed at run time even if
// shown unmarked here (ExprTree's scoped gate), so this is a best-effort static
// view, not a runtime measurement.
func FormatConvPlan(op *base.Op) string {
	var b strings.Builder
	formatConvOp(&b, op, 0)
	return b.String()
}

func formatConvOp(b *strings.Builder, op *base.Op, depth int) {
	if op == nil {
		return
	}
	fmt.Fprintf(b, "%s%s", strings.Repeat("  ", depth), op.Kind)

	switch op.Kind {
	case "project":
		// Params is a list of projections, each an ["exprTree", expr] param list.
		b.WriteString("  [")
		for i, pr := range op.Params {
			if i > 0 {
				b.WriteString(", ")
			}
			writeExprVerdict(b, op, pr)
		}
		b.WriteByte(']')
	case "filter":
		// Params is itself one ["exprTree", expr] param list.
		b.WriteString("  ")
		writeExprVerdict(b, op, []interface{}(op.Params))
	default:
		if len(op.Labels) > 0 {
			fmt.Fprintf(b, "  %v", op.Labels)
		}
	}
	b.WriteByte('\n')

	for _, ch := range op.Children {
		formatConvOp(b, ch, depth+1)
	}
}

// writeExprVerdict renders one expression param list as its SQL, appending
// boxedMarker only when the engine would box it (native expressions are unmarked).
// A non-exprTree param (an already-native catalog form, or the rare exprStr) has no
// cbq expression to inspect, so it prints just the catalog name.
func writeExprVerdict(b *strings.Builder, op *base.Op, param interface{}) {
	e, ok := exprTreeParam(param)
	if !ok {
		if pl, ok := param.([]interface{}); ok && len(pl) > 0 {
			if name, _ := pl[0].(string); name != "" {
				b.WriteString(name)
				return
			}
		}
		b.WriteString("?")
		return
	}
	b.WriteString(e.String())
	if !exprIsNative(inputLabels(op), e) {
		b.WriteString(" " + boxedMarker)
	}
}

// exprTreeParam extracts the cbq expression from an ["exprTree", expr] param list
// -- the only expression form that can fall back to the boxed lane. Other forms
// (an already-native catalog param, or the rare exprStr) return ok=false.
func exprTreeParam(param interface{}) (expression.Expression, bool) {
	pl, ok := param.([]interface{})
	if !ok || len(pl) < 2 {
		return nil, false
	}
	if name, _ := pl[0].(string); name != "exprTree" {
		return nil, false
	}
	e, ok := pl[1].(expression.Expression)
	return e, ok
}

// exprIsNative reports whether the engine would evaluate e on the native byte path
// (vs boxing it for cbq's expression.Evaluate), against the given input labels.
// It mirrors ExprTree's unscoped decision (strict=false); see writeExprVerdict for
// the scoped caveat.
func exprIsNative(labels base.Labels, e expression.Expression) bool {
	var buf bytes.Buffer
	_, ok := ExprTreeOptimize(labels, stripCovers(e), &buf, false)
	return ok
}

// inputLabels returns the labels an op's expressions resolve against -- its input
// (child) labels, matching what the engine passes to MakeExprFunc (see
// OpProject/OpFilter). nil for a leaf.
func inputLabels(op *base.Op) base.Labels {
	if len(op.Children) > 0 {
		return op.Children[0].Labels
	}
	return nil
}

// ExprCoverage counts, over an op tree's project and filter expressions, how many
// the engine evaluates natively (raw bytes, no garbage) vs boxed (converted to a
// cbq value.Value per row for expression.Evaluate). It's the static, build-time
// companion to the runtime boxed-eval counter: it says how many distinct
// expressions fall back, not how many rows flow through them. Same scope and
// (unscoped) verdict as FormatConvPlan.
func ExprCoverage(op *base.Op) (native, boxed int) {
	if op == nil {
		return 0, 0
	}
	tally := func(param interface{}) {
		if e, ok := exprTreeParam(param); ok {
			if exprIsNative(inputLabels(op), e) {
				native++
			} else {
				boxed++
			}
		}
	}
	switch op.Kind {
	case "project":
		for _, pr := range op.Params {
			tally(pr)
		}
	case "filter":
		tally([]interface{}(op.Params))
	}
	for _, ch := range op.Children {
		n, b := ExprCoverage(ch)
		native, boxed = native+n, boxed+b
	}
	return native, boxed
}
