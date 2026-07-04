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

import "github.com/couchbase/n1k1/base"

// DiscardElision enables the discard-elision pass (dead-value elimination): a
// projection chain feeding a value-agnostic group (a COUNT(*) / COUNT(<const>)
// with no GROUP BY) produces per-row values that nothing reads, so those
// projections are spliced out of the op tree and the group counts its
// grandchild's rows directly. See DESIGN-exprs.md ("discard-elision"). Kept as a
// toggle so tests can assert on/off produce identical results.
var DiscardElision = true

// elideDiscarded rewrites op IN PLACE (recursively): under each value-agnostic
// group, splice out the chain of 1:1 `project` ops directly below it. Safe
// because (1) `project` is strictly 1:1 -- it never adds/drops rows, so the count
// the group produces is unchanged; (2) a value-agnostic group references none of
// its input's value labels, so those projected values are dead; (3) the op tree
// is single-parent, so a spliced project fed only this group. Only `project` ops
// are spliced -- never filter/order/limit (which change the row count) -- so the
// walk stops at the first non-project child.
func elideDiscarded(op *base.Op) {
	if op == nil {
		return
	}

	if op.Kind == "group" && valueAgnosticGroup(op) {
		for len(op.Children) == 1 && op.Children[0].Kind == "project" {
			op.Children = op.Children[0].Children // drop the dead project
		}
	}

	for _, child := range op.Children {
		elideDiscarded(child)
	}
}

// valueAgnosticGroup reports whether a "group" op reads none of its input's value
// labels -- i.e. it only counts rows. True when there are no GROUP BY keys
// (Params[0] empty) and every aggregate operand (Params[1]) is a constant
// ("json") term. COUNT(*) qualifies: its star operand is projected as
// ["json","true"] (see conv.go VisitFinalGroup). COUNT(x)/SUM(x)/etc. do NOT --
// their operand is an ["exprTree", ...] that reads the row; nor does GROUP AS
// (an ["exprTree", ObjectConstruct]); nor any GROUP BY.
func valueAgnosticGroup(op *base.Op) bool {
	if len(op.Params) < 2 {
		return false
	}
	if groups, _ := op.Params[0].([]interface{}); len(groups) != 0 {
		return false // has GROUP BY keys -> reads the input
	}
	aggExprs, ok := op.Params[1].([]interface{})
	if !ok || len(aggExprs) == 0 {
		return false
	}
	for _, a := range aggExprs {
		term, ok := a.([]interface{})
		if !ok || len(term) == 0 || term[0] != "json" {
			return false // a non-constant operand reads the row's values
		}
	}
	return true
}
