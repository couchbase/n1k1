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
	"encoding/json"
	"fmt"

	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
)

// Implicit safety caps for a recursive CTE with no explicit OPTIONS, matching
// query's execution/with.go (_MAX_RECUR_DEPTH / _MAX_IMPLICIT_DOCS) -- they stop
// an unbounded recursion (e.g. a missing termination predicate) from looping
// forever. An explicit OPTIONS {"levels":..} / {"documents":..} replaces the
// corresponding implicit cap (as in query).
const (
	maxRecurDepth = 100
	maxRecurDocs  = 10000
)

// WithRecursiveOp runs a WITH RECURSIVE CTE's fixpoint and yields the accumulated
// result rows under the alias label. The binding (an expression.With) is in a
// vars.Temps slot: Expression() is the anchor, RecursiveExpression() the step.
//
// It mirrors query's execution/with.go: evaluate the anchor, then repeatedly
// evaluate the step with the CTE alias bound to the latest working set (via
// GlueContext.corrParent, which the step's `FROM <cte>` expr-scan reads),
// dedup/cycle-restricting and accumulating, until the step yields nothing (or a
// level/doc limit is hit). Honors UNION dedup, the CYCLE clause (w.CycleFields),
// and OPTIONS limits (w.Config: "levels" / "documents"); implicit caps bound the
// loop when a limit isn't set.
func WithRecursiveOp(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr) {
	idx, ok := o.Params[0].(int)
	if !ok {
		yieldErr(fmt.Errorf("with-recursive: expected int Temps index, got %T", o.Params[0]))
		return
	}
	w, ok := vars.Temps[idx].(expression.With)
	if !ok {
		yieldErr(fmt.Errorf("with-recursive: no binding at Temps[%d]", idx))
		return
	}
	gctx, ok := vars.Temps[0].(*GlueContext)
	if !ok {
		yieldErr(fmt.Errorf("with-recursive: Temps[0] is not a *GlueContext"))
		return
	}

	alias := w.Alias()

	// OPTIONS limits (levels / documents); -1 means "unset -> use implicit cap".
	level, document, cerr := recurConfig(w.Config())
	if cerr != nil {
		yieldErr(cerr)
		return
	}
	implicitMaxDepth := int64(-1)
	if level == -1 {
		implicitMaxDepth = maxRecurDepth
	}
	implicitMaxDocs := int64(-1)
	if document == -1 {
		implicitMaxDocs = maxRecurDocs
	}

	cycleFields := w.CycleFields() // CYCLE clause (may be nil)
	union := w.IsUnion()
	trackSet := map[string]bool{}   // UNION dedup (full item)
	trackCycle := map[string]bool{} // CYCLE detection (cycle-field key)

	dedup := func(items []interface{}) ([]interface{}, error) {
		return dedupCycleRestrict(items, cycleFields, union, trackCycle, trackSet, gctx)
	}

	asArray := func(v value.Value) ([]interface{}, error) {
		if v == nil {
			return nil, nil
		}
		a, ok := v.Actual().([]interface{})
		if !ok {
			return nil, fmt.Errorf("with-recursive: %q did not evaluate to an array", alias)
		}
		return a, nil
	}

	// Anchor.
	av, err := w.Expression().Evaluate(nil, gctx)
	if err != nil {
		yieldErr(err)
		return
	}
	workRes, err := asArray(av)
	if err != nil {
		yieldErr(err)
		return
	}
	if workRes, err = dedup(workRes); err != nil {
		yieldErr(err)
		return
	}

	// Fixpoint: accumulate the working set, then re-run the step with the alias
	// bound to it, until it produces nothing new (or a limit trips).
	var final []interface{}
	var idoc, ilevel int64
	for len(workRes) > 0 {
		// Level limit (recursion depth): explicit OPTIONS wins, else implicit cap.
		if level > -1 && ilevel > level {
			break
		} else if implicitMaxDepth > -1 && ilevel > implicitMaxDepth {
			gctx.Warning(errors.NewRecursiveImplicitDepthLimitError(alias, implicitMaxDepth))
			break
		}

		// Document limit: explicit OPTIONS wins, else implicit cap.
		if document > -1 && idoc > document {
			if document < int64(len(final)) {
				final = final[:document]
			}
			break
		} else if implicitMaxDocs > -1 && idoc > implicitMaxDocs {
			if implicitMaxDocs < int64(len(final)) {
				final = final[:implicitMaxDocs]
			}
			gctx.Warning(errors.NewRecursiveImplicitDocLimitError(alias, implicitMaxDocs))
			break
		}

		final = append(final, workRes...)
		idoc += int64(len(workRes))
		ilevel++

		// Bind the CTE alias to the latest working set so the step's
		// `FROM <cte>` (an expr-scan) reads it via corrParent.
		scope := value.NewScopeValue(
			map[string]interface{}{alias: value.NewValue(workRes)}, nil)
		prev := gctx.corrParent
		gctx.corrParent = scope
		sv, serr := w.RecursiveExpression().Evaluate(scope, gctx)
		gctx.corrParent = prev
		if serr != nil {
			yieldErr(serr)
			return
		}

		next, aerr := asArray(sv)
		if aerr != nil {
			yieldErr(aerr)
			return
		}
		if workRes, err = dedup(next); err != nil {
			yieldErr(err)
			return
		}
	}

	for _, it := range final {
		b, err := json.Marshal(it)
		if err != nil {
			yieldErr(err)
			return
		}
		yieldVals(base.Vals{base.Val(b)})
	}
	yieldErr(nil)
}

// dedupCycleRestrict filters items in place: for UNION it drops items already
// seen (trackSet, by canonical JSON); for a CYCLE clause it drops items whose
// cycle-field key (hopVal) was already seen (trackCycle) -- cycle detection.
// Both tracking maps persist across iterations. Ports query's
// execution/with.go dedupAndCycleRestrict.
func dedupCycleRestrict(items []interface{}, cycleFields expression.Expressions,
	union bool, trackCycle, trackSet map[string]bool, ctx expression.Context) ([]interface{}, error) {
	if !union && cycleFields == nil {
		return items, nil
	}

	end := 0
	for _, item := range items {
		keep := true

		if union {
			b, _ := json.Marshal(item)
			k := string(b)
			if trackSet[k] {
				keep = false
			} else {
				trackSet[k] = true
			}
		}

		if cycleFields != nil {
			hv, err := hopVal(value.NewValue(item), cycleFields, ctx)
			if err != nil {
				return nil, fmt.Errorf("with-recursive: cycle key eval: %v", err)
			}
			if keep {
				b, _ := json.Marshal(hv)
				k := string(b)
				if trackCycle[k] {
					keep = false
				} else {
					trackCycle[k] = true
				}
			}
		}

		if keep {
			items[end] = item
			end++
		}
	}

	return items[:end:end], nil
}

// hopVal builds the cycle-detection key for an item: a map of each CYCLE field's
// text -> its value (skipping MISSING). Ports query's execution/with.go hopVal.
func hopVal(item value.Value, cycleFields expression.Expressions,
	ctx expression.Context) (map[string]interface{}, error) {
	val := map[string]interface{}{}
	for _, exp := range cycleFields {
		fval, err := exp.Evaluate(item, ctx)
		if err != nil {
			return nil, err
		}
		if fval.Type() != value.MISSING {
			val[exp.String()] = fval.Actual()
		}
	}
	return val, nil
}

// recurConfig parses a recursive CTE's OPTIONS object (w.Config): "levels" and
// "documents" numeric limits. Returns -1 for an unset limit. Ports query's
// execution/with.go processConfig.
func recurConfig(config value.Value) (level, document int64, err error) {
	level, document = -1, -1
	if config == nil {
		return level, document, nil
	}
	if config.Type() != value.OBJECT {
		return -1, -1, fmt.Errorf("with-recursive: OPTIONS is not an object (%v)", config.Type())
	}
	for field := range config.Fields() {
		fv, _ := config.Field(field)
		if fv.Type() != value.NUMBER {
			return -1, -1, fmt.Errorf("with-recursive: OPTIONS %q must be numeric", field)
		}
		v, _ := fv.Actual().(float64)
		switch field {
		case "levels":
			level = int64(v)
		case "documents":
			document = int64(v)
		default:
			return -1, -1, errors.NewInvalidConfigOptions(field)
		}
	}
	return level, document, nil
}
