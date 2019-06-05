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

	"github.com/couchbase/query/expression"
)

var OptimizableFuncs = map[string]string{}

func init() {
	OptimizableFuncs["eq"] = "eq"
}

// ExprTreeOptimize attempts to optimize a N1QL
// query/expression.Expression tree into a n1k1 expr params tree.
func ExprTreeOptimize(e expression.Expression, buf *bytes.Buffer) (
	params []interface{}, ok bool) {
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

		params = []interface{}{"labelPath", "."}
		for _, x := range fieldPath {
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
		child, ok := ExprTreeOptimize(operand, buf)
		if !ok {
			return nil, false
		}

		params = append(params, child)
	}

	return params, true
}
