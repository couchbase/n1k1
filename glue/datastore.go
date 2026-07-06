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
	"encoding/json"
	"fmt"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
)

func DatastoreOp(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	// Count rows out of a datastore scan (and drive live progress checkpoints);
	// a no-op when stats are off or the kind isn't a counter-contributing scan.
	yieldVals = countingYield(o, vars, yieldVals)

	switch o.Kind {
	case "datastore-scan-records":
		DatastoreScanRecords(o, vars, yieldVals, yieldErr)
	case "datastore-scan-primary":
		DatastoreScanPrimary(o, vars, yieldVals, yieldErr)
	case "datastore-scan-index":
		DatastoreScanIndex(o, vars, yieldVals, yieldErr)
	case "datastore-scan-index-cover":
		DatastoreScanIndexCovering(o, vars, yieldVals, yieldErr)
	case "datastore-scan-fts":
		DatastoreScanFTS(o, vars, yieldVals, yieldErr)
	case "datastore-scan-keys":
		DatastoreScanKeys(o, vars, yieldVals, yieldErr)
	case "datastore-fetch":
		DatastoreFetch(o, vars, yieldVals, yieldErr, path, pathNext)
	case "expr-scan":
		ExprScanOp(o, vars, yieldVals, yieldErr)
	case "js-stream":
		JSStreamOp(o, vars, yieldVals, yieldErr)
	case "with-recursive":
		WithRecursiveOp(o, vars, yieldVals, yieldErr)
	case "project-exclude":
		ProjectExcludeOp(o, vars, yieldVals, yieldErr, path, pathNext)
	case "agg-metadata":
		DatastoreAggMetadata(o, vars, yieldVals, yieldErr)
	case "agg-columnar":
		DatastoreAggColumnar(o, vars, yieldVals, yieldErr)
	}

	// Live progress pulse: a scan invocation just finished. Each pass yields far
	// fewer rows than countingYield's per-row checkpoint interval (a nested-loop
	// join's inner scan is a handful of rows re-run many times), so without a pulse
	// here YieldStats would rarely fire and the display wouldn't animate. The
	// receiver throttles to ~10 Hz, so pulsing per invocation is cheap. Gated to
	// counter-contributing scans (StatsBase >= 0) and to stats being on.
	if o.StatsBase >= 0 && vars.Ctx != nil && vars.Ctx.Stats != nil && vars.Ctx.YieldStats != nil {
		_ = vars.Ctx.YieldStats(vars.Ctx.Stats)
	}
}

// ProjectExcludeOp implements SELECT * EXCLUDE <path>... over a lone unprefixed
// star. It runs its child (the "." star projection, whose single val is the
// projected object) and, per row, deletes the excluded paths from that object
// -- reusing query's expression.GetReferences (resolve each EXCLUDE expression
// to a path against the row, matching getExclusions(singleQualification=true))
// and expression.DeleteFromObject (deep-delete, incl. nested/array-index paths).
// This mirrors execution/project_initial.go's unprefixed-star exclude branch.
func ProjectExcludeOp(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	idx, ok := o.Params[0].(int)
	if !ok {
		yieldErr(fmt.Errorf("project-exclude: expected int Temps index, got %T", o.Params[0]))
		return
	}
	excludes, ok := vars.Temps[idx].(expression.Expressions)
	if !ok {
		yieldErr(fmt.Errorf("project-exclude: no expressions at Temps[%d]", idx))
		return
	}
	ctx, _ := vars.Temps[0].(expression.Context)

	// The star projection yields one val (the ".*" object); star index within the
	// row is the single ".*" label position.
	starIdx := 0
	for i, l := range o.Labels {
		if l == ".*" {
			starIdx = i
			break
		}
	}

	inner := func(vals base.Vals) {
		var m map[string]interface{}
		if err := json.Unmarshal(vals[starIdx], &m); err != nil || m == nil {
			yieldVals(vals) // not an object (or empty): nothing to exclude
			return
		}

		av := value.NewAnnotatedValue(value.NewValue(m))
		av.SetSelf(true) // resolve unqualified EXCLUDE paths against the row (self)

		refs, _, err := expression.GetReferences(excludes, av, ctx, true)
		if err != nil {
			yieldErr(err)
			return
		}
		for _, ref := range refs {
			expression.DeleteFromObject(m, ref)
		}

		jv, err := json.Marshal(m)
		if err != nil {
			yieldErr(err)
			return
		}
		out := append(base.Vals{}, vals...)
		out[starIdx] = base.Val(jv)
		yieldVals(out)
	}

	vars.Ctx.ExecOp(o.Children[0], vars, inner, yieldErr, pathNext, "PE")
}

// ExprScanOp implements a FROM-clause expression scan (FROM <expr>/<subquery>/
// <cte> AS alias). It evaluates the expression (from a vars.Temps slot) at
// runtime -- so a subquery or CTE binding runs through the engine + datastore
// via GlueContext.EvaluateSubquery -- then yields one row per element of the
// resulting array (a non-array value yields a single row), under the alias label.
func ExprScanOp(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr) {
	idx, ok := o.Params[0].(int)
	if !ok {
		yieldErr(fmt.Errorf("expr-scan: expected int Temps index, got %T", o.Params[0]))
		return
	}
	expr, ok := vars.Temps[idx].(expression.Expression)
	if !ok {
		yieldErr(fmt.Errorf("expr-scan: no expression at Temps[%d]", idx))
		return
	}

	var ctx expression.Context
	var item value.Value // the scope to evaluate against
	if c, ok := vars.Temps[0].(*GlueContext); ok {
		ctx = c
		// Inside a WITH RECURSIVE step, corrParent holds {alias: workingSet}, so a
		// `FROM <recursive-cte>` (this scan, over the identifier) resolves to the
		// latest working set. nil otherwise (constant / subquery bindings).
		item = c.corrParent
	} else if c, ok := vars.Temps[0].(expression.Context); ok {
		ctx = c
	}

	v, err := expr.Evaluate(item, ctx)
	if err != nil {
		yieldErr(err)
		return
	}

	// FROM <expr> treats the value as a collection: an array yields one row per
	// element, any other value one row -- EXCEPT MISSING, which yields zero rows
	// (you can't iterate over a missing field). NULL still yields one (null) row.
	// This matches cbq's ExpressionScan (_ARRAY_MISSING_VALUE is the empty slice).
	if v != nil && v.Type() != value.MISSING {
		jv, err := json.Marshal(v)
		if err != nil {
			yieldErr(err)
			return
		}
		val := base.Val(jv)
		if _, ok := base.ArrayYield(val, yieldVals, nil); !ok {
			yieldVals(base.Vals{val}) // not an array: a single row
		}
	}

	// Signal a clean end-of-stream: buffering parents (e.g. ORDER BY, which drains
	// its heap on yieldErr(nil)) need it, as scans/temp-yield-var do.
	yieldErr(nil)
}
