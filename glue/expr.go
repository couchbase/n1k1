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

// The glue package integrates the existing couchbase.com/query
// package for parsing, expressions, etc. for backwards compatibility.
package glue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/value"
)

// ExprStr parses and evaluates a N1QL expression string using the
// query/expression/parser package, providing backwards compatibility
// at the cost of performance from data conversions.
func ExprStr(vars *base.Vars, labels base.Labels,
	params []interface{}, path string) (exprFunc base.ExprFunc) {
	exprStr := params[0].(string)

	expr, err := parser.Parse(exprStr)
	if err != nil {
		return func(vals base.Vals, yieldErr base.YieldErr) base.Val {
			yieldErr(err)
			return base.ValMissing
		}
	}

	paramsTree := append([]interface{}{expr}, params[1:]...)

	return ExprTree(vars, labels, paramsTree, path)
}

// ExprTree evaluates a N1QL query/expression.Expression tree, for
// backwards compatibility at the cost of performance from data
// conversions.
func ExprTree(vars *base.Vars, labels base.Labels,
	params []interface{}, path string) (exprFunc base.ExprFunc) {
	expr := params[0].(expression.Expression)

	var buf bytes.Buffer

	paramsOut, ok := ExprTreeOptimize(labels, expr, &buf)
	if ok {
		// TODO: Compiled approach should probably invoke something
		// like vars.MakeExprFunc().
		return n1k1.MakeExprFunc(vars, labels, paramsOut, path, "")
	}

	context, ok := vars.Temps[0].(expression.Context)
	if !ok {
		context = &ExprGlueContext{MyNow: vars.Ctx.Now}
	}

	cv, err := NewConvertVals(labels)
	if err != nil {
		return func(vals base.Vals, yieldErr base.YieldErr) base.Val {
			yieldErr(err)
			return base.ValMissing
		}
	}

	// TODO: Need to propagate the vars to the expression, too,
	// perhaps related to bindings?

	return func(vals base.Vals, yieldErr base.YieldErr) base.Val {
		v, err := cv.Convert(vals)
		if err != nil {
			yieldErr(err)
			return base.ValMissing
		}

		vResult, err := expr.Evaluate(v, context)
		if err != nil {
			yieldErr(err)
			return base.ValMissing
		}

		buf.Reset()

		err = vResult.WriteJSON(&buf, "", "", true)
		if err != nil {
			yieldErr(err)
			return base.ValMissing
		}

		// TODO: Need to convert back any annotations or attachments
		// that are associated with the vResult?  The params[1], for
		// example, might hold the wanted output labels, if any.

		return base.Val(buf.Bytes())
	}
}

// --------------------------------------------------------

// ConvertVals is able to convert base.Vals to value.Value based on
// the directives provided by the Labels.
type ConvertVals struct {
	Labels     base.Labels
	LabelPaths [][]string // The len(LabelPaths) == len(Labels).
}

func NewConvertVals(labels base.Labels) (*ConvertVals, error) {
	// Analyze the labels to associated paths, if any.
	var paths [][]string

	for _, label := range labels {
		var path []string

		// Ex label: `.["address","city"]`.
		if len(label) > 1 && label[0] == '.' {
			err := json.Unmarshal([]byte(label[1:]), &path)
			if err != nil {
				return nil, err
			}
		}

		paths = append(paths, path)
	}

	return &ConvertVals{Labels: labels, LabelPaths: paths}, nil
}

// --------------------------------------------------------

// Convert merges the base.Vals into a single value.Value, according
// to the directives provided in ConvertVals.Labels.
func (s *ConvertVals) Convert(vals base.Vals) (value.Value, error) {
	if len(s.Labels) != len(vals) {
		return nil, fmt.Errorf("Convert, Labels.len(%+v) != vals.len(%+v)",
			s.Labels, vals)
	}

	var v value.Value // The result of the merged vals.

OUTER:
	for i, label := range s.Labels {
		switch label[0] {
		case '.': // Label is a path into v of where to set vals[i].
			if label == "." {
				if v != nil {
					return nil, fmt.Errorf("Convert, v non-nil on '.'")
				}

				// TODO: Is vals[i] always JSON encoded so the skip
				// validation is safe here?
				v = value.NewParsedValue(vals[i], true)

				continue OUTER
			}

			if v == nil {
				v = value.NewValue(map[string]interface{}{})
			}

			subObj := v // Navigate down to the right subObj.

			path := s.LabelPaths[i]

			for j := 0; j < len(path)-1; j++ {
				subObjNext, ok := subObj.Field(path[j])
				if ok {
					subObj = subObjNext
				} else {
					m := map[string]interface{}{}

					err := subObj.SetField(path[j], m)
					if err != nil {
						return nil, fmt.Errorf("subObj: %+v, key: %s, err: %v", subObj, path[j], err)
					}

					subObj = value.NewValue(m)
				}
			}

			if len(vals[i]) > 0 {
				// TODO: Is vals[i] always JSON encoded so the skip
				// validation is safe here?
				vv := value.NewParsedValue(vals[i], true)

				err := subObj.SetField(path[len(path)-1], vv)
				if err != nil {
					return nil, err
				}
			} else {
				subObj.UnsetField(path[len(path)-1])
			}

		case '^': // The label is an attachment name for vals[i].
			if len(vals[i]) > 0 {
				// TODO: Is vals[i] always JSON encoded?
				vv := value.NewParsedValue(vals[i], true)

				av, ok := v.(value.AnnotatedValue)
				if !ok {
					av = value.NewAnnotatedValue(v)
				}

				if label[1:] == "id" {
					av.SetId(vv)
				} else {
					// Ex label:" ^aggregates|foo".
					// TODO: strings.Split() creates garbage.
					kk := strings.Split(label[1:], "|")
					if len(kk) > 1 {
						var att map[string]value.Value

						v := av.GetAttachment(kk[0])
						if v == nil {
							att = map[string]value.Value{}

							av.SetAttachment(kk[0], att)
						} else {
							att = v.(map[string]value.Value)
						}

						att[kk[1]] = value.NewValue(vv)
					} else {
						av.SetAttachment(label[1:], vv)
					}
				}

				v = av
			}

		case '=': // The label means vals[i] is a BINARY value.
			if v != nil {
				return nil, fmt.Errorf("Convert, v non-nil on '='")
			}

			v = value.NewBinaryValue(vals[i])

			// Continue loop as remaining labels might be annotations.

		default:
			return nil, fmt.Errorf("Convert, unknown label[0]: %s", label)
		}
	}

	return v, nil
}

// --------------------------------------------------------

// ExprGlueContext implements query/expression.Context interface.
type ExprGlueContext struct {
	MyNow                time.Time
	MyAuthenticatedUsers []string
	MyDatastoreVersion   string
}

func (e *ExprGlueContext) Now() time.Time {
	return e.MyNow
}

func (e *ExprGlueContext) AuthenticatedUsers() []string {
	return e.MyAuthenticatedUsers
}

func (e *ExprGlueContext) DatastoreVersion() string {
	return e.MyDatastoreVersion
}

func (e *ExprGlueContext) EvaluateStatement(statement string,
	namedArgs map[string]value.Value,
	positionalArgs value.Values,
	subquery, readonly bool) (value.Value, uint64, error) {
	return nil, 0, nil // TODO.
}
