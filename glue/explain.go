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

// FormatConvPlan renders n1k1's converted op tree as an indented text tree. For
// project and filter operators it annotates each expression with the lane the
// engine would use to evaluate it:
//
//   - [native]: evaluated directly on the row's raw JSON bytes (no allocation).
//   - [boxed]:  n1k1 can't optimize it, so every row is converted into a cbq
//     value.Value and handed to expression.Evaluate -- correct, but GC-heavy.
//
// The verdict is the same static ExprTreeOptimize decision the engine makes when
// it builds the expression closure (see ExprTree). It assumes no correlated / WITH
// scope; an expression under such a scope is forced to [boxed] at run time even if
// shown [native] here (ExprTree's scoped gate), so this is a best-effort static
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

// writeExprVerdict renders one expression param list as "<sql> [native|boxed]".
// A non-exprTree param (an already-native catalog form, or the rare exprStr) has
// no cbq expression to inspect, so it prints just the catalog name.
func writeExprVerdict(b *strings.Builder, op *base.Op, param interface{}) {
	pl, ok := param.([]interface{})
	if !ok || len(pl) < 2 {
		b.WriteString("?")
		return
	}
	name, _ := pl[0].(string)
	if name != "exprTree" {
		b.WriteString(name)
		return
	}
	e, ok := pl[1].(expression.Expression)
	if !ok {
		b.WriteString("exprTree")
		return
	}
	verdict := "boxed"
	// The expression resolves against the operator's input (child) labels -- the
	// same labels the engine passes to MakeExprFunc (see OpProject/OpFilter).
	if len(op.Children) > 0 {
		var buf bytes.Buffer
		// strict=false: report the unscoped (lenient) verdict, matching the common
		// top-level project/filter case. A scoped op (correlated subquery / WITH /
		// recursive CTE) evaluates with strict=true, so this can over-report
		// "native" for an expr that reaches into a parent scope -- acceptable for a
		// best-effort plan display.
		if _, ok := ExprTreeOptimize(op.Children[0].Labels, stripCovers(e), &buf, false); ok {
			verdict = "native"
		}
	}
	fmt.Fprintf(b, "%s [%s]", e.String(), verdict)
}
