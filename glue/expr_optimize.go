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
	OptimizableFuncs["eq"] = "eq"
	OptimizableFuncs["lt"] = "lt"
	OptimizableFuncs["le"] = "le"
	OptimizableFuncs["gt"] = "gt"
	OptimizableFuncs["ge"] = "ge"

	// Arithmetic (see engine/expr_arith.go, base/arith.go).
	OptimizableFuncs["add"] = "add"
	OptimizableFuncs["sub"] = "sub"
	OptimizableFuncs["mult"] = "mult"
	OptimizableFuncs["div"] = "div"
	OptimizableFuncs["mod"] = "mod"
	OptimizableFuncs["idiv"] = "idiv"
	OptimizableFuncs["imod"] = "imod"
	OptimizableFuncs["neg"] = "neg"

	// Unary predicates (see engine/expr_pred.go). Keys are the cbq Function
	// Name() (the canonical no-underscore forms set by each constructor's
	// Init(), e.g. "isnull"); values are the n1k1 ExprCatalog names.
	OptimizableFuncs["not"] = "not"
	OptimizableFuncs["isnull"] = "is_null"
	OptimizableFuncs["isnotnull"] = "is_not_null"
	OptimizableFuncs["ismissing"] = "is_missing"
	OptimizableFuncs["isnotmissing"] = "is_not_missing"
	OptimizableFuncs["isvalued"] = "is_valued"
	OptimizableFuncs["isnotvalued"] = "is_not_valued"

	// Conditional-unknown selectors (see engine/expr_cond.go); variadic, so no
	// arity guard -- the native harness is n-ary.
	OptimizableFuncs["ifnull"] = "ifnull"
	OptimizableFuncs["ifmissing"] = "ifmissing"
	OptimizableFuncs["ifmissingornull"] = "ifmissingornull"
	OptimizableFuncs["nvl"] = "nvl"

	// Ternary (see engine/expr_between.go).
	OptimizableFuncs["between"] = "between"

	// Membership (see engine/expr_in.go).
	OptimizableFuncs["in"] = "in"

	// String concatenation `||` (see engine/expr_concat.go); n-ary, no guard.
	OptimizableFuncs["concat"] = "concat"

	// Type checks (see engine/expr_type.go); underscore Name()s.
	OptimizableFuncs["is_array"] = "is_array"
	OptimizableFuncs["is_number"] = "is_number"
	OptimizableFuncs["is_string"] = "is_string"
	OptimizableFuncs["is_boolean"] = "is_boolean"
	OptimizableFuncs["is_object"] = "is_object"
	OptimizableFuncs["is_atom"] = "is_atom"
}

// ExprTreeOptimize attempts to optimize a N1QL
// query/expression.Expression tree into a n1k1 expr params tree.
func ExprTreeOptimize(labels base.Labels, e expression.Expression,
	buf *bytes.Buffer) (params []interface{}, ok bool) {
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
			cp, ok := ExprTreeOptimize(labels, child, buf)
			if !ok {
				return nil, false
			}
			params = append(params, cp)
		}
		return params, true
	}

	if sc, ok := e.(*expression.SimpleCase); ok {
		children := sc.Children()
		searchP, ok := ExprTreeOptimize(labels, children[0], buf)
		if !ok {
			return nil, false
		}
		params = []interface{}{"case"}
		i := 1
		for i+1 < len(children) { // (when, then) pairs
			whenP, ok := ExprTreeOptimize(labels, children[i], buf)
			if !ok {
				return nil, false
			}
			thenP, ok := ExprTreeOptimize(labels, children[i+1], buf)
			if !ok {
				return nil, false
			}
			params = append(params, []interface{}{"eq", searchP, whenP}, thenP)
			i += 2
		}
		if i < len(children) { // trailing else
			elseP, ok := ExprTreeOptimize(labels, children[i], buf)
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
	switch name {
	case "add", "mult", "sub", "div", "mod", "idiv", "imod", "in":
		// These native harnesses are two-operand; cbq's n-ary forms fall back.
		// (ifnull/ifmissing/ifmissingornull/nvl are now n-ary -- no guard.)
		if len(operands) != 2 {
			return nil, false
		}
	case "neg",
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
		child, ok := ExprTreeOptimize(labels, operand, buf)
		if !ok {
			return nil, false
		}

		params = append(params, child)
	}

	return params, true
}
