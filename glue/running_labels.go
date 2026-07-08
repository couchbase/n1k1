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
	"strings"

	"github.com/couchbase/n1k1/base"
)

// aggregatesLabelPrefix marks a group op's aggregate output labels
// ("^aggregates|<expr>"); the text after it is the aggregate's expression, in the
// same order as base.RunningAggRow.Aggs. See glue/conv.go VisitGroup.
const aggregatesLabelPrefix = "^aggregates|"

// RunningAggLabels walks a converted plan and returns, for each group op that
// publishes running aggregates (a "group" op carrying "^aggregates|<expr>"
// labels), the per-aggregate labels (base.RunningAggLabel: alias + expr) in that
// op's aggregate layout order -- the same order as base.RunningAggRow.Aggs. Group
// ops appear in plan pre-order.
//
// The op-path a running row carries (base.RunningAggRow.Op, the runtime EmitPush
// path) is a different scheme from a plan node's position, so a caller matches a
// running row to an entry by aggregate count (the common query has one group op).
// Returns nil for a nil plan or a plan with no running-aggregate group. The result
// is hung on base.Stats.RunningAggLabels so the display path (live + final) can
// label the running block without a plan.
func RunningAggLabels(plan *base.Op) [][]base.RunningAggLabel {
	var out [][]base.RunningAggLabel

	var walk func(op, nearestProject *base.Op)
	walk = func(op, nearestProject *base.Op) {
		if op == nil {
			return
		}
		if op.Kind == "group" {
			if labels := groupAggLabels(op, nearestProject); labels != nil {
				out = append(out, labels)
			}
		}
		// Descend tracking the nearest enclosing project, so a group's aliases
		// come from the projection above it (which holds alias<->expr pairs).
		if op.Kind == "project" {
			nearestProject = op
		}
		for _, ch := range op.Children {
			walk(ch, nearestProject)
		}
	}
	walk(plan, nil)

	return out
}

// groupAggLabels pairs a group op's aggregate expressions (from its
// "^aggregates|<expr>" labels) with the SQL alias of any projection term that is
// exactly that aggregate. project may be nil (no enclosing projection found), in
// which case every Alias is "".
func groupAggLabels(group, project *base.Op) []base.RunningAggLabel {
	var exprs []string
	for _, l := range group.Labels {
		if strings.HasPrefix(l, aggregatesLabelPrefix) {
			exprs = append(exprs, l[len(aggregatesLabelPrefix):])
		}
	}
	if len(exprs) == 0 {
		return nil // A group with no aggregates publishes no running aggregates.
	}

	// Map each bare-aggregate projection term's expression text -> its alias.
	// A term nested in a larger expression (ROUND(SUM..)) renders to a different
	// string and simply won't match, leaving that aggregate's Alias "".
	aliasByExpr := map[string]string{}
	if project != nil {
		for k, lbl := range project.Labels {
			if k >= len(project.Params) {
				break
			}
			alias := labelAlias(lbl)
			if alias == "" {
				continue // "." (RAW), ".*" (star), or a non-alias attachment.
			}
			if e, ok := exprTreeParam(project.Params[k]); ok {
				aliasByExpr[e.String()] = alias
			}
		}
	}

	out := make([]base.RunningAggLabel, len(exprs))
	for i, e := range exprs {
		out[i] = base.RunningAggLabel{Alias: aliasByExpr[e], Expr: e}
	}
	return out
}

// labelAlias decodes a projection output label -- built as "." + LabelSuffix(alias)
// == `.["<alias>"]` -- back to the bare alias. Returns "" for the whole-row "."
// (SELECT RAW), the ".*" star spread, or any "^"-attachment, none of which name a
// single output column.
func labelAlias(label string) string {
	if strings.HasPrefix(label, `.["`) && strings.HasSuffix(label, `"]`) {
		return label[3 : len(label)-2]
	}
	return ""
}
