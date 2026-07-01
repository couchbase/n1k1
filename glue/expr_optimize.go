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
}

// ExprTreeOptimize attempts to optimize a N1QL
// query/expression.Expression tree into a n1k1 expr params tree.
func ExprTreeOptimize(labels base.Labels, e expression.Expression,
	buf *bytes.Buffer) (params []interface{}, ok bool) {
	if c, ok := e.(*expression.Constant); ok {
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
	case "add", "mult", "sub", "div", "mod", "idiv", "imod":
		if len(operands) != 2 {
			return nil, false
		}
	case "neg",
		"not", "is_null", "is_not_null",
		"is_missing", "is_not_missing", "is_valued", "is_not_valued":
		if len(operands) != 1 {
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
