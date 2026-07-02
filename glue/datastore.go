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
	case "with-recursive":
		WithRecursiveOp(o, vars, yieldVals, yieldErr)
	}
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

	if v != nil {
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
