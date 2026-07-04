//go:build n1ql

//  Copyright (c) 2019 Couchbase, Inc.
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
	"strings"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

var OptimizableFuncs = map[string]string{}

func init() {
	// The common case: the cbq Function.Name() and the n1k1 ExprCatalog name are
	// identical, so optSelf registers name -> name. Grouped by family; see the
	// referenced engine files for each.
	optSelf(
		"eq", "lt", "le", "gt", "ge", // comparisons (expr_cmp.go)
		"add", "sub", "mult", "div", "mod", "idiv", "imod", "neg", // arithmetic (expr_arith.go)
		"abs", "ceil", "floor", "sqrt", "exp", "ln", "log", "sign", // unary math (expr_math.go)
		"degrees", "radians", "sin", "cos", "tan", "asin", "acos", "atan",
		"upper", "lower", "length", "title", // unary string (expr_str.go)
		"contains", "position0", "position1", // binary string (expr_str.go)
		"power", "atan2", // binary math (expr_math.go)
		"to_boolean", "to_string", "to_number", // type conversions (expr_type.go)
		"and", "or", // three-valued logical (expr_logic.go)
		"not",                                            // unary predicate (expr_pred.go)
		"ifnull", "ifmissing", "ifmissingornull", "nvl",  // conditional-unknown (expr_cond.go)
		"between",                    // ternary (expr_between.go)
		"in",                         // membership (expr_in.go)
		"concat",                     // string concat `||` (expr_concat.go)
		"nullif", "missingif",        // (expr_null.go)
		"greatest", "least",          // (expr_greatest.go)
		"element",                    // array element `arr[idx]` (expr_nav.go)
		"is_array", "is_number", "is_string", "is_boolean", "is_object", "is_atom", // type checks (expr_type.go)
	)

	// The unknown predicates (expr_pred.go): cbq Function.Name() is the
	// no-underscore form, but the n1k1 ExprCatalog name has underscores.
	OptimizableFuncs["isnull"] = "is_null"
	OptimizableFuncs["isnotnull"] = "is_not_null"
	OptimizableFuncs["ismissing"] = "is_missing"
	OptimizableFuncs["isnotmissing"] = "is_not_missing"
	OptimizableFuncs["isvalued"] = "is_valued"
	OptimizableFuncs["isnotvalued"] = "is_not_valued"
}

// optSelf registers each name as optimizable to a native ExprCatalog func of the
// same name -- the common case where cbq's Function.Name() equals the n1k1 name.
func optSelf(names ...string) {
	for _, name := range names {
		OptimizableFuncs[name] = name
	}
}

// ExprTreeOptimize attempts to optimize a N1QL
// query/expression.Expression tree into a n1k1 expr params tree.
// strict, when true, makes a Field reference that does NOT match a real label
// prefix (i.e. one that would fall back to the whole-row "." / ".*" default) a
// hard failure rather than a local-row navigation. Callers pass strict=true when
// a scope is active (correlated subquery / WITH / recursive CTE): there, an
// identifier absent from the local labels may belong to the parent scope, and
// the native path can't see the parent -- so we must only take it when every
// field reference provably resolves to a local label. See ExprTree's scoped gate.
func ExprTreeOptimize(labels base.Labels, e expression.Expression,
	buf *bytes.Buffer, strict bool) (params []interface{}, ok bool) {
	if c, ok := e.(*expression.Constant); ok {
		// A MISSING constant has no JSON form -- value.WriteJSON emits "null",
		// which would wrongly become NULL. Emit an empty json constant instead;
		// ExprJson yields a zero-length Val, i.e. MISSING.
		if c.Value().Type() == value.MISSING {
			return []interface{}{"json", ""}, true
		}

		buf.Reset()

		if c.Value().WriteJSON(nil, buf, "", "", true) != nil {
			return nil, false
		}

		return []interface{}{"json", buf.String()}, true
	}

	if field, ok := e.(*expression.Field); ok {
		// A case-insensitive field reference (`name`i) matches a field name
		// ignoring case. Our native path navigation below is case-sensitive, so
		// hand any case-insensitive step in the chain to query's Field.Evaluate
		// (the general expr path), which does the case-insensitive lookup.
		if fieldChainCaseInsensitive(field) {
			return nil, false
		}

		fieldPath, ok := ExprFieldPath(field)
		if !ok {
			return nil, false
		}

		// Default to the whole-row label present in labels: "." normally, or
		// the ".*" star-spread row (SELECT path.*), whose stored val is the
		// object itself. A field with no more-specific path label match then
		// resolves against that whole row rather than a missing "." label.
		labelBest := "."
		if labels.IndexOf(".") < 0 && labels.IndexOf(".*") >= 0 {
			labelBest = ".*"
		}
		iBest := -1
	OUTER:
		for i := 0; i < len(fieldPath); i++ {
			labelMaybe := "." + LabelSuffix(strings.Join(fieldPath[0:i+1], `","`))

			for _, label := range labels {
				if label == labelMaybe {
					labelBest = label
					iBest = i
					continue OUTER
				}
			}
		}

		// Under an active scope, a field that matched no real label prefix
		// (iBest < 0, so labelBest is the whole-row "." / ".*" default) may be a
		// parent-scope identifier that the native labelPath can't resolve -- it
		// would silently navigate the local row and yield MISSING. Refuse it so
		// the caller keeps the (parent-aware) cbq fallback for this expression.
		if strict && iBest < 0 {
			return nil, false
		}

		params = []interface{}{"labelPath", labelBest}
		for _, x := range fieldPath[iBest+1:] {
			params = append(params, x)
		}

		return params, true
	}

	// CASE is not an expression.Function; lower both forms to a flat native
	// "case" param list [cond, then, cond, then, ..., else?]. Children() gives:
	//   SearchedCase: [when1, then1, ..., else?]
	//   SimpleCase:   [searchTerm, when1, then1, ..., else?]
	// SimpleCase desugars to searched form with cond = eq(searchTerm, when).
	if sc, ok := e.(*expression.SearchedCase); ok {
		params = []interface{}{"case"}
		for _, child := range sc.Children() {
			cp, ok := ExprTreeOptimize(labels, child, buf, strict)
			if !ok {
				return nil, false
			}
			params = append(params, cp)
		}
		return params, true
	}

	if sc, ok := e.(*expression.SimpleCase); ok {
		children := sc.Children()
		searchP, ok := ExprTreeOptimize(labels, children[0], buf, strict)
		if !ok {
			return nil, false
		}
		params = []interface{}{"case"}
		i := 1
		for i+1 < len(children) { // (when, then) pairs
			whenP, ok := ExprTreeOptimize(labels, children[i], buf, strict)
			if !ok {
				return nil, false
			}
			thenP, ok := ExprTreeOptimize(labels, children[i+1], buf, strict)
			if !ok {
				return nil, false
			}
			params = append(params, []interface{}{"eq", searchP, whenP}, thenP)
			i += 2
		}
		if i < len(children) { // trailing else
			elseP, ok := ExprTreeOptimize(labels, children[i], buf, strict)
			if !ok {
				return nil, false
			}
			params = append(params, elseP)
		}
		return params, true
	}

	f, ok := e.(expression.Function)
	if !ok {
		return nil, false
	}

	name, ok := OptimizableFuncs[f.Name()]
	if !ok {
		return nil, false
	}

	// The native arithmetic harness (engine/expr_arith.go) handles the binary
	// operators and unary neg only. cbq's add/mult are n-ary; the >2-operand
	// forms fall back to cbq rather than silently dropping operands.
	operands := f.Operands()

	// Logical AND / OR are n-ary in cbq but the native harness is binary
	// (engine/expr_logic.go). Fold into right-nested binary applications, which
	// is exact under three-valued logic (base.LogicAnd2/LogicOr2 short-circuit on
	// a decided operand and the unknown-precedence is idempotent under nesting).
	// e.g. AND(a,b,c) -> ["and", a, ["and", b, c]].
	if name == "and" || name == "or" {
		n := len(operands)
		if n < 2 {
			return nil, false
		}
		acc, ok := ExprTreeOptimize(labels, operands[n-1], buf, strict)
		if !ok {
			return nil, false
		}
		for i := n - 2; i >= 0; i-- {
			lhs, ok := ExprTreeOptimize(labels, operands[i], buf, strict)
			if !ok {
				return nil, false
			}
			acc = []interface{}{name, lhs, acc}
		}
		return acc, true
	}

	switch name {
	case "add", "mult", "sub", "div", "mod", "idiv", "imod", "in",
		"power", "atan2",
		"contains", "position0", "position1",
		"nullif", "missingif", "element":
		// These native harnesses are two-operand; cbq's n-ary forms fall back.
		// (ifnull/ifmissing/ifmissingornull/nvl and greatest/least are n-ary.)
		if len(operands) != 2 {
			return nil, false
		}
	case "neg",
		"abs", "ceil", "floor", "sqrt", "exp", "ln", "log", "sign",
		"degrees", "radians", "sin", "cos", "tan", "asin", "acos", "atan",
		"upper", "lower", "length", "title",
		"to_boolean", "to_string", "to_number",
		"not", "is_null", "is_not_null",
		"is_missing", "is_not_missing", "is_valued", "is_not_valued",
		"is_array", "is_number", "is_string", "is_boolean", "is_object", "is_atom":
		if len(operands) != 1 {
			return nil, false
		}
	case "between":
		if len(operands) != 3 {
			return nil, false
		}
	}

	params = append(params, name)

	for _, operand := range operands {
		child, ok := ExprTreeOptimize(labels, operand, buf, strict)
		if !ok {
			return nil, false
		}

		params = append(params, child)
	}

	return params, true
}

// fieldChainCaseInsensitive reports whether e is a field/identifier reference
// where any step is case-insensitive (`name`i) -- which our case-sensitive
// native path navigation can't honor.
func fieldChainCaseInsensitive(e expression.Expression) bool {
	switch x := e.(type) {
	case *expression.Field:
		if x.CaseInsensitive() {
			return true
		}
		return fieldChainCaseInsensitive(x.First())
	case *expression.Identifier:
		return x.CaseInsensitive()
	}
	return false
}
