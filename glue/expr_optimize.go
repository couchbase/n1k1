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
}

// ExprTreeOptimize attempts to optimize a N1QL
// query/expression.Expression tree into a n1k1 expr params tree.
func ExprTreeOptimize(labels base.Labels, e expression.Expression,
	buf *bytes.Buffer) (params []interface{}, ok bool) {
	if c, ok := e.(*expression.Constant); ok {
		buf.Reset()

		if c.Value().WriteJSON(buf, "", "", true) != nil {
			return nil, false
		}

		return []interface{}{"json", buf.String()}, true
	}

	if field, ok := e.(*expression.Field); ok {
		fieldPath, ok := ExprFieldPath(field)
		if !ok {
			return nil, false
		}

		labelBest := "."
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

	params = append(params, name)

	for _, operand := range f.Operands() {
		child, ok := ExprTreeOptimize(labels, operand, buf)
		if !ok {
			return nil, false
		}

		params = append(params, child)
	}

	return params, true
}
