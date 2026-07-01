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

// The glue package integrates the existing couchbase.com/query
// package for parsing, expressions, etc. for backwards compatibility.
package glue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"

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

	// Inside a correlated subquery, skip the optimized expr path: it evaluates
	// using only this sub-op's labels and can't see the outer row, so a
	// correlated reference would resolve to MISSING. The general path below
	// wraps each row as a scope over the correlation parent (see below).
	correlated := false
	if gc, ok := vars.Temps[0].(*GlueContext); ok && gc.corrParent != nil {
		correlated = true
	}

	if !correlated {
		paramsOut, ok := ExprTreeOptimize(labels, expr, &buf)
		if ok {
			// TODO: Compiled approach should probably invoke something
			// like vars.MakeExprFunc().
			return engine.MakeExprFunc(vars, labels, paramsOut, path, "")
		}
	}

	context, ok := vars.Temps[0].(expression.Context)
	if !ok {
		context = NewExprGlueContext(vars.Ctx.Now)
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

		// Inside a correlated subquery, give this sub-row a scope over the outer
		// row so identifiers not found here (e.g. an outer keyspace alias) fall
		// through to the parent. corrParent is nil outside a correlated subquery,
		// so uncorrelated / outer evaluation is unaffected.
		if gc, ok := context.(*GlueContext); ok && gc.corrParent != nil {
			if m, ok := v.Actual().(map[string]interface{}); ok {
				v = value.NewScopeValue(m, gc.corrParent)
			}
		}

		vResult, err := expr.Evaluate(v, context)
		if err != nil {
			yieldErr(err)
			return base.ValMissing
		}

		// A projected expression that evaluates to MISSING is omitted from the
		// result object (N1QL semantics). Signal that by yielding an empty val,
		// which ConvertVals.Convert turns into UnsetField. (NULL is kept.)
		if vResult == nil || vResult.Type() == value.MISSING {
			return base.ValMissing
		}

		buf.Reset()

		err = vResult.WriteJSON(nil, &buf, "", "", true)
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

		// Ex label: `.["address","city"]`. The ".*" star-spread label carries
		// no path (handled specially in Convert), so don't JSON-parse it.
		if len(label) > 1 && label[0] == '.' && label != ".*" {
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

	// The labels emit each keyspace doc as a `.`-path label immediately
	// followed by its `^id` attachment (see conv.go). lastParent/lastKey track
	// the most-recently-set doc field so a following `^id` can attach the
	// metadata to that sub-value (so META(alias).id resolves), rather than to
	// the outer row. Reset whenever the preceding label wasn't a doc-set.
	var lastParent value.Value
	var lastKey string

OUTER:
	for i, label := range s.Labels {
		switch label[0] {
		case '.': // Label is a path into v of where to set vals[i].
			lastParent, lastKey = nil, ""

			if label == ".*" {
				// Star spread: merge vals[i]'s fields into v when it is an
				// object; a non-object contributes nothing (so SELECT path.*
				// over a scalar yields {}). v is forced to an object so the
				// result is {} rather than null when nothing merges.
				if v == nil {
					v = value.NewValue(map[string]interface{}{})
				}

				if len(vals[i]) > 0 {
					sv := value.NewParsedValue(vals[i], true)
					if sv.Type() == value.OBJECT {
						for fk, fv := range sv.Fields() {
							if err := v.SetField(fk, fv); err != nil {
								return nil, err
							}
						}
					}
				}

				continue OUTER
			}

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

				// Remember this doc-set so a following `^id` attaches the
				// document metadata to it (enables META(alias).id).
				lastParent, lastKey = subObj, path[len(path)-1]
			} else {
				subObj.UnsetField(path[len(path)-1])
			}

		case '^': // The label is an attachment name for vals[i].
			if len(vals[i]) > 0 {
				// TODO: Is vals[i] always JSON encoded?
				vv := value.NewParsedValue(vals[i], true)

				if label[1:] == "id" && lastParent != nil {
					// Attach the doc id to the preceding doc sub-value, so
					// META(alias).id resolves against that keyspace's value.
					cur, _ := lastParent.Field(lastKey)
					av, ok := cur.(value.AnnotatedValue)
					if !ok {
						av = value.NewAnnotatedValue(cur)
					}
					av.SetId(vv)
					if err := lastParent.SetField(lastKey, av); err != nil {
						return nil, err
					}

					continue OUTER
				}

				av, ok := v.(value.AnnotatedValue)
				if !ok {
					if v == nil {
						// An orphan attachment -- e.g. a leading ^id with no
						// preceding doc `.`-label, as in `SELECT <letvar> FROM ks
						// LET ...` over an index-only scan (no Fetch). Back it with
						// an empty object so a following `.`-field is settable
						// (NewAnnotatedValue(nil) wraps NULL, which isn't).
						v = value.NewValue(map[string]interface{}{})
					}
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

						v := av.GetAttachment(attKey(kk[0]))
						if v == nil {
							att = map[string]value.Value{}

							av.SetAttachment(attKey(kk[0]), att)
						} else {
							att = v.(map[string]value.Value)
						}

						att[kk[1]] = value.NewValue(vv)
					} else {
						av.SetAttachment(attKey(label[1:]), vv)
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

// ExprGlueContext implements query/expression.Context. It embeds query's
// own lightweight expression.IndexContext to inherit no-op/default
// implementations of the (large, evolving) interface, overriding only Now().
// This keeps n1k1 compiling as query adds Context methods over time.
type ExprGlueContext struct {
	*expression.IndexContext

	MyNow time.Time
}

func NewExprGlueContext(now time.Time) *ExprGlueContext {
	return &ExprGlueContext{IndexContext: &expression.IndexContext{}, MyNow: now}
}

func (e *ExprGlueContext) Now() time.Time {
	return e.MyNow
}

// --------------------------------------------------------

// attKey maps a n1k1 attachment label name to query's value-package int16
// attachment key (attachments became int16-keyed since 2019). Only the
// "aggregates" attachment is used by n1k1's generated ops today.
func attKey(name string) int16 {
	switch name {
	case "aggregates":
		return value.ATT_AGGREGATES
	default:
		return value.ATT_AGGREGATES // TODO: extend if other attachment names appear.
	}
}
