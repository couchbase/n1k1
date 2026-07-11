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
	"sync/atomic"
	"time"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/expression/search"
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

// exprResetScope is a sentinel passed as the 2nd exprTree param by the star
// (".*") projection: ExprTree then evaluates its row WITHOUT the corrParent /
// withScope scope wrap, so SELECT * spreads only the row's own fields (query's
// ResetParent(nil) equivalent). See VisitInitialProject and ExprTree.
const exprResetScope = "^resetScope"

// ExprTree evaluates a N1QL query/expression.Expression tree, for
// backwards compatibility at the cost of performance from data
// conversions.
func ExprTree(vars *base.Vars, labels base.Labels,
	params []interface{}, path string) (exprFunc base.ExprFunc) {
	// Copy the boxed expression before stripCovers rewrites it: stripCovers maps the tree
	// IN PLACE (MapChildren writes operands[i]), and a cbq expression also caches during
	// Evaluate -- neither is concurrency-safe. The K-way merge / co-advance run per-file
	// (or per-branch) children on separate producer goroutines that SHARE this op's
	// params[0] expression object, so each op-setup must own its tree. The copy is once
	// per op-setup (not per row), so it stays off the hot path.
	expr := stripCovers(params[0].(expression.Expression).Copy())

	var buf bytes.Buffer

	// resetScope: the star (".*") projection passes this marker so its row is NOT
	// scoped over corrParent/withScope below -- SELECT * must spread only the row's
	// own fields, not the hidden scope vars (WITH aliases, or an outer correlated
	// row). Mirrors query's ResetParent(nil) for the star term
	// (execution/project_initial.go). See VisitInitialProject.
	resetScope := len(params) > 1 && params[1] == exprResetScope

	// "scoped" evaluation wraps each row as a scope over a parent so identifiers
	// not in the row resolve to the outer correlated row (corrParent) or the WITH
	// aliases (withScope). When scoped we must skip the optimized expr path: it
	// evaluates using only this op's labels and can't see the parent. The star
	// (resetScope) never needs the parent, so it stays on the fast path.
	gc, isGlue := vars.Temps[0].(*GlueContext)
	scoped := isGlue && !resetScope && (gc.corrParent != nil || gc.withScope != nil)

	// Try the native (byte-oriented, allocation-avoiding) expr path. When a scope
	// is active (scoped), pass strict=true: ExprTreeOptimize then accepts the
	// expression only if every field reference provably resolves to a local
	// label. Otherwise an identifier that actually lives in the parent scope
	// (corrParent / withScope) would be silently mis-navigated against the local
	// row and yield MISSING -- which is why the scoped case historically skipped
	// the native path wholesale. A scoped expression that is fully local (e.g. a
	// recursive CTE step's arithmetic over its own `FROM <cte>` row, or a
	// correlated subquery predicate touching only the subquery's own fields) thus
	// still avoids the per-row Convert round-trip; anything referencing the parent
	// fails strict and falls through to the parent-aware cbq fallback below.
	paramsOut, ok := ExprTreeOptimize(labels, expr, &buf, scoped)
	if ok {
		// TODO: Compiled approach should probably invoke something
		// like vars.MakeExprFunc().
		return engine.MakeExprFunc(vars, labels, paramsOut, path, "")
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

	// Count each row taking this boxed lane into the request's root counter (see
	// GlueContext.BoxedEvals). Resolved once here; nil for a non-request context.
	var boxedCtr *int64
	if isGlue {
		boxedCtr = &gc.getRoot().boxedEvals
	}

	return func(vals base.Vals, yieldErr base.YieldErr) base.Val {
		if boxedCtr != nil {
			atomic.AddInt64(boxedCtr, 1)
		}

		v, err := cv.Convert(vals)
		if err != nil {
			yieldErr(err)
			return base.ValMissing
		}

		// Scope this row over its parent so identifiers not in the row resolve to
		// the outer correlated row (corrParent) or the WITH aliases (withScope);
		// corrParent wins when both are set (a subquery's rows scope over the outer
		// row). The star projection is exempt (resetScope -> scoped is false), so
		// SELECT * spreads only the row's own fields, not these hidden scope vars.
		if scoped {
			parent := gc.corrParent
			if parent == nil {
				parent = gc.withScope
			}
			// A no-FROM sub-SELECT (e.g. the innermost `SELECT RAW a` in
			// `SELECT RAW (SELECT RAW a)`) yields a single empty row that Convert
			// maps to a nil value. Scope an EMPTY object over the parent so the
			// outer identifier still resolves through it -- and, crucially, so a
			// nested subquery chains its parent through this scope (the outer
			// subquery's empty row becomes the inner subquery's corrParent, whose
			// own empty row scopes over it, reaching the outermost row). Without
			// this the identifier is unresolvable and the row collapses.
			if v == nil {
				v = value.NewScopeValue(map[string]interface{}{}, parent)
			} else if av, ok := v.(value.AnnotatedValue); ok {
				// Prefer SetParent: when v already wraps a ScopeValue it re-parents
				// in place, preserving v's annotations -- notably a subquery
				// aggregate's "^aggregates" attachment, which SUM(...)/COUNT(...)
				// read back. But annotatedValue.SetParent returns nil when the
				// underlying value is NOT a ScopeValue (the common case -- Convert
				// backs an aggregate row with a plain object). Don't let that nil out
				// the row (it would make the aggregate re-evaluate against a nil item
				// -- the correlated-aggregate 'nil item'); instead rebuild an
				// annotated ScopeValue over the parent and copy the annotations
				// forward so the aggregate / META() attachment still resolves.
				if sv := av.SetParent(parent); sv != nil {
					v = sv
				} else if m, ok := av.Actual().(map[string]interface{}); ok {
					nav := value.NewAnnotatedValue(value.NewScopeValue(m, parent))
					nav.CopyAnnotations(av)
					v = nav
				}
			} else if m, ok := v.Actual().(map[string]interface{}); ok {
				v = value.NewScopeValue(m, parent)
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

	// byteMode + byte{KeyTokens,Indices} are the precomputed plan for
	// ConvertBytes, the boxing-free JSON encoder (see self.go). Derived once
	// from Labels, since the label set is fixed for a result.
	byteMode      byteMode
	byteKeyTokens [][]byte // For byteFields: one `"key":` token per emitted field.
	byteIndices   []int    // For byteFields: the val index feeding each field.
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

	cv := &ConvertVals{Labels: labels, LabelPaths: paths}
	cv.planBytes()

	return cv, nil
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

				if label[1:] == "smeta" && lastParent != nil {
					// Attach the FTS search-meta ({outname:{score,id}}) to the
					// preceding doc sub-value under value.ATT_SMETA, so
					// SEARCH_META(alias)/SEARCH_SCORE(alias) resolve against that
					// keyspace's value (they read this attachment; see idx_fts.go /
					// datastore_scan.go:DatastoreScanFTS).
					cur, _ := lastParent.Field(lastKey)
					av, ok := cur.(value.AnnotatedValue)
					if !ok {
						av = value.NewAnnotatedValue(cur)
					}
					av.SetAttachment(value.ATT_SMETA, vv)
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

	jsRT *jsSharedRuntime // per-context JS UDF runtime; see ext_jsvm.go / jsRuntimeHost.
}

func NewExprGlueContext(now time.Time) *ExprGlueContext {
	return &ExprGlueContext{IndexContext: &expression.IndexContext{}, MyNow: now}
}

func (e *ExprGlueContext) Now() time.Time {
	return e.MyNow
}

// jsShared / dropJSShared satisfy jsRuntimeHost so a JS UDF evaluated through the
// fallback context (no GlueContext in vars.Temps[0]) still reuses one runtime.
func (e *ExprGlueContext) jsShared() *jsSharedRuntime {
	if e.jsRT == nil {
		e.jsRT = newJSSharedRuntime()
	}
	return e.jsRT
}

func (e *ExprGlueContext) dropJSShared() { e.jsRT = nil }

// --------------------------------------------------------

// stripCovers replaces every expression.Cover node in the tree with the plain
// expression it wraps (its Covered()). The cbq planner rewrites a covering
// index scan's projected/filtered field references into Cover nodes, whose
// Evaluate() reads a per-value "cover" slot that only a real GSI runtime sets --
// n1k1 never sets it, so a Cover would always evaluate to MISSING. n1k1 instead
// materializes the document (VisitIndexScan synthesizes a datastore-fetch for a
// covering scan), so once the Covers are peeled back to their underlying field
// accesses they resolve normally against the fetched doc. Cover-less trees pass
// through unchanged. See DESIGN-indexing.md "covering scans".
func stripCovers(expr expression.Expression) expression.Expression {
	if expr == nil {
		return nil
	}
	cs := &coverStripper{}
	cs.SetMapper(cs)
	cs.SetMapFunc(func(e expression.Expression) (expression.Expression, error) {
		if cov, ok := e.(*expression.Cover); ok {
			return cs.Map(cov.Covered()) // peel, and recurse (handles nested covers)
		}
		return e, e.MapChildren(cs)
	})
	out, err := cs.Map(expr)
	if err != nil {
		return expr // on any mapper error, fall back to the original tree
	}
	return out
}

type coverStripper struct {
	expression.MapperBase
}

// stripSearch replaces every SEARCH() function in the tree with TRUE. Used by
// conv's VisitFilter when an FTS scan is present: the bleve Search already
// selected the matching docs, so the SEARCH() term in the residual filter is
// satisfied -- and n1k1 has no live FTS verify to re-evaluate it (it would
// evaluate false and drop every row). A `SEARCH(...) AND x` filter thus reduces to
// `x`; a bare `SEARCH(...)` becomes `TRUE` (pass all fetched docs). See idx_fts.go.
func stripSearch(expr expression.Expression) expression.Expression {
	if expr == nil {
		return nil
	}
	ss := &searchStripper{}
	ss.SetMapper(ss)
	ss.SetMapFunc(func(e expression.Expression) (expression.Expression, error) {
		if _, ok := e.(*search.Search); ok {
			return expression.TRUE_EXPR, nil
		}
		return e, e.MapChildren(ss)
	})
	out, err := ss.Map(expr)
	if err != nil {
		return expr
	}
	return out
}

type searchStripper struct {
	expression.MapperBase
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
