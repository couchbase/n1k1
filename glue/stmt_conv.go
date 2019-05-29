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
	"fmt"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/plan"

	"github.com/couchbase/n1k1/base"
)

type Termer interface {
	Term() *algebra.KeyspaceTerm
}

// Conv implements the conversion of a couchbase/query/plan into a
// n1k1 base.Op tree. It implements the plan.Visitor interface.
type Conv struct {
	Store   *Store
	Aliases map[string]string
	Temps   []interface{}
	Prev    plan.Operator
}

func (c *Conv) AddAlias(kt *algebra.KeyspaceTerm) {
	if kt.Namespace() != "#system" {
		c.Aliases[kt.Alias()] = kt.Path().ProtectedString()
	}
}

func (c *Conv) AddTemp(t interface{}) int {
	rv := len(c.Temps)
	c.Temps = append(c.Temps, t)
	return rv
}

// -------------------------------------------------------------------

// Scan

func (c *Conv) VisitPrimaryScan(o *plan.PrimaryScan) (interface{}, error) {
	return &base.Op{
		Kind:   "datastore-scan-primary",
		Labels: base.Labels{"^id"},
		Params: []interface{}{c.AddTemp(o)},
	}, nil
}

func (c *Conv) VisitPrimaryScan3(o *plan.PrimaryScan3) (interface{}, error) { return NA(o) }
func (c *Conv) VisitParentScan(o *plan.ParentScan) (interface{}, error)     { return NA(o) }

func (c *Conv) VisitIndexScan(o *plan.IndexScan) (interface{}, error) {
	return &base.Op{
		Kind:   "datastore-scan-index",
		Labels: base.Labels{"^id"},
		Params: []interface{}{c.AddTemp(o)},
	}, nil
}

func (c *Conv) VisitIndexScan2(o *plan.IndexScan2) (interface{}, error) { return NA(o) }
func (c *Conv) VisitIndexScan3(o *plan.IndexScan3) (interface{}, error) { return NA(o) }

func (c *Conv) VisitKeyScan(o *plan.KeyScan) (interface{}, error) {
	return &base.Op{
		Kind:   "datastore-scan-keys",
		Labels: base.Labels{"^id"},
		Params: []interface{}{c.AddTemp(o)},
	}, nil
}

func (c *Conv) VisitValueScan(o *plan.ValueScan) (interface{}, error) { return NA(o) }

func (c *Conv) VisitDummyScan(o *plan.DummyScan) (interface{}, error) {
	return &base.Op{Kind: "nil"}, nil
}

func (c *Conv) VisitCountScan(o *plan.CountScan) (interface{}, error)           { return NA(o) }
func (c *Conv) VisitIndexCountScan(o *plan.IndexCountScan) (interface{}, error) { return NA(o) }
func (c *Conv) VisitIndexCountScan2(o *plan.IndexCountScan2) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitIndexCountDistinctScan2(o *plan.IndexCountDistinctScan2) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitDistinctScan(o *plan.DistinctScan) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitUnionScan(o *plan.UnionScan) (interface{}, error)         { return NA(o) }
func (c *Conv) VisitIntersectScan(o *plan.IntersectScan) (interface{}, error) { return NA(o) }
func (c *Conv) VisitOrderedIntersectScan(o *plan.OrderedIntersectScan) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitExpressionScan(o *plan.ExpressionScan) (interface{}, error) { return NA(o) }

// FTS Search

func (c *Conv) VisitIndexFtsSearch(o *plan.IndexFtsSearch) (interface{}, error) { return NA(o) }

// Fetch

func (c *Conv) VisitFetch(o *plan.Fetch) (interface{}, error) {
	c.AddAlias(o.Term())
	c.Prev = o

	labelSuffix := ""
	if o.Term().As() != "" {
		labelSuffix = `["` + o.Term().As() + `"]`
	}

	return &base.Op{
		Kind:   "datastore-fetch",
		Labels: base.Labels{"." + labelSuffix, "^id"},
		Params: []interface{}{c.AddTemp(o)},
	}, nil
}

func (c *Conv) VisitDummyFetch(o *plan.DummyFetch) (interface{}, error) { return NA(o) }

// Join

func (c *Conv) VisitJoin(o *plan.Join) (interface{}, error) {
	// Allocate a vars.Temps slot to hold evaluated keys.
	varsTempsSlot := c.AddTemp(nil)

	labelSuffix := ""
	if o.Term().As() != "" {
		labelSuffix = `["` + o.Term().As() + `"]`
	}

	prevSuffix := ""
	if termer, ok := c.Prev.(Termer); ok && termer != nil && termer.Term().As() != "" {
		prevSuffix = `["` + termer.Term().As() + `"]`
	}

	c.Prev = o // Allows for chainable joins.

	return &base.Op{
		Kind:   "joinKeys-inner",
		Labels: base.Labels{"." + prevSuffix, "^id", "." + labelSuffix, "^id"}, // TODO.
		Params: []interface{}{
			// The vars.Temps slot that holds evaluated keys.
			varsTempsSlot,
			// The expression that will evaluate to the keys.
			[]interface{}{"exprStr", o.Term().JoinKeys().String()},
		},
		Children: []*base.Op{&base.Op{
			Kind:   "datastore-fetch",
			Labels: base.Labels{"." + labelSuffix, "^id"},
			Params: []interface{}{c.AddTemp(o)},
			Children: []*base.Op{&base.Op{
				Kind:   "temp-yield-var",
				Labels: base.Labels{"^id"},
				Params: []interface{}{varsTempsSlot},
			}},
		}},
	}, nil
}

func (c *Conv) VisitIndexJoin(o *plan.IndexJoin) (interface{}, error) { return NA(o) }
func (c *Conv) VisitNest(o *plan.Nest) (interface{}, error)           { return NA(o) }
func (c *Conv) VisitIndexNest(o *plan.IndexNest) (interface{}, error) { return NA(o) }

func (c *Conv) VisitUnnest(o *plan.Unnest) (interface{}, error) {
	labelSuffix := ""
	if o.Term().As() != "" {
		labelSuffix = `["` + o.Term().As() + `"]`
	}

	prevSuffix := ""
	if termer, ok := c.Prev.(Termer); ok && termer != nil && termer.Term().As() != "" {
		prevSuffix = `["` + termer.Term().As() + `"]`
	}

	c.Prev = o // Allows for chainable joins.

	return &base.Op{
		Kind:   "unnest-inner",
		Labels: base.Labels{"." + prevSuffix, "^id", "." + labelSuffix}, // TODO.
		Params: []interface{}{
			// The expression to unnest.
			"exprStr", o.Term().Expression().String(),
		},
		Children: []*base.Op{&base.Op{
			Kind:   "noop",
			Labels: base.Labels{"." + labelSuffix},
		}},
	}, nil
}

func (c *Conv) VisitNLJoin(o *plan.NLJoin) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitNLNest(o *plan.NLNest) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitHashJoin(o *plan.HashJoin) (interface{}, error) { return NA(o) }
func (c *Conv) VisitHashNest(o *plan.HashNest) (interface{}, error) { return NA(o) }

// Let + Letting, With

func (c *Conv) VisitLet(o *plan.Let) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitWith(o *plan.With) (interface{}, error) { return NA(o) }

// Filter

func (c *Conv) VisitFilter(o *plan.Filter) (interface{}, error) { return NA(o) }

// Group

func (c *Conv) VisitInitialGroup(o *plan.InitialGroup) (interface{}, error) { return NA(o) }
func (c *Conv) VisitIntermediateGroup(o *plan.IntermediateGroup) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitFinalGroup(o *plan.FinalGroup) (interface{}, error) { return NA(o) }

// Window functions

func (c *Conv) VisitWindowAggregate(o *plan.WindowAggregate) (interface{}, error) {
	return NA(o)
}

// Project

func (c *Conv) VisitInitialProject(o *plan.InitialProject) (interface{}, error) {
	op := &base.Op{
		Kind:   "project",
		Params: make([]interface{}, 0, len(o.Terms())),
	}

	for _, term := range o.Terms() {
		op.Params = append(op.Params,
			[]interface{}{"exprStr", term.Result().Expression().String()})
	}

	return op, nil
}

func (c *Conv) VisitFinalProject(o *plan.FinalProject) (interface{}, error) {
	// TODO: Need to convert projections back into a SELF'ish single object?
	return nil, nil
}

func (c *Conv) VisitIndexCountProject(o *plan.IndexCountProject) (interface{}, error) {
	return NA(o)
}

// Distinct

func (c *Conv) VisitDistinct(o *plan.Distinct) (interface{}, error) { return NA(o) }

// Set operators

func (c *Conv) VisitUnionAll(o *plan.UnionAll) (interface{}, error)         { return NA(o) }
func (c *Conv) VisitIntersectAll(o *plan.IntersectAll) (interface{}, error) { return NA(o) }
func (c *Conv) VisitExceptAll(o *plan.ExceptAll) (interface{}, error)       { return NA(o) }

// Order, Paging

func (c *Conv) VisitOrder(o *plan.Order) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitOffset(o *plan.Offset) (interface{}, error) { return NA(o) }
func (c *Conv) VisitLimit(o *plan.Limit) (interface{}, error)   { return NA(o) }

// Mutations

func (c *Conv) VisitSendInsert(o *plan.SendInsert) (interface{}, error) { return NA(o) }
func (c *Conv) VisitSendUpsert(o *plan.SendUpsert) (interface{}, error) { return NA(o) }
func (c *Conv) VisitSendDelete(o *plan.SendDelete) (interface{}, error) { return NA(o) }
func (c *Conv) VisitClone(o *plan.Clone) (interface{}, error)           { return NA(o) }
func (c *Conv) VisitSet(o *plan.Set) (interface{}, error)               { return NA(o) }
func (c *Conv) VisitUnset(o *plan.Unset) (interface{}, error)           { return NA(o) }
func (c *Conv) VisitSendUpdate(o *plan.SendUpdate) (interface{}, error) { return NA(o) }
func (c *Conv) VisitMerge(o *plan.Merge) (interface{}, error)           { return NA(o) }

// Framework

func (c *Conv) VisitAlias(o *plan.Alias) (interface{}, error) { return NA(o) }

func (c *Conv) VisitAuthorize(o *plan.Authorize) (interface{}, error) {
	// TODO: Need a real authorize operation here one day?
	return o.Child().Accept(c)
}

func (c *Conv) VisitParallel(o *plan.Parallel) (interface{}, error) {
	// TODO: One day implement parallel correctly, but stay serial for now.
	return o.Child().Accept(c)
}

func (c *Conv) VisitSequence(o *plan.Sequence) (rv interface{}, err error) {
	// Convert plan.Sequence's children into a branch of descendants.
	for _, child := range o.Children() {
		v, err := child.Accept(c)
		if err != nil {
			return nil, err
		}

		if v != nil {
			if rv != nil {
				// The first plan.Sequence child will become the deepest descendant.
				v.(*base.Op).Children = append(
					append([]*base.Op(nil), rv.(*base.Op)),
					v.(*base.Op).Children...)
			}

			rv = v
		}
	}

	return rv, nil
}

func (c *Conv) VisitDiscard(o *plan.Discard) (interface{}, error) { return NA(o) }
func (c *Conv) VisitStream(o *plan.Stream) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitCollect(o *plan.Collect) (interface{}, error) { return NA(o) }

// Index DDL

func (c *Conv) VisitCreatePrimaryIndex(o *plan.CreatePrimaryIndex) (interface{}, error) {
	return NA(o)
}

func (c *Conv) VisitCreateIndex(o *plan.CreateIndex) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitDropIndex(o *plan.DropIndex) (interface{}, error)       { return NA(o) }
func (c *Conv) VisitAlterIndex(o *plan.AlterIndex) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitBuildIndexes(o *plan.BuildIndexes) (interface{}, error) { return NA(o) }

// Roles

func (c *Conv) VisitGrantRole(o *plan.GrantRole) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitRevokeRole(o *plan.RevokeRole) (interface{}, error) { return NA(o) }

// Explain

func (c *Conv) VisitExplain(o *plan.Explain) (interface{}, error) { return NA(o) }

// Prepare

func (c *Conv) VisitPrepare(o *plan.Prepare) (interface{}, error) { return NA(o) }

// Infer

func (c *Conv) VisitInferKeyspace(o *plan.InferKeyspace) (interface{}, error) { return NA(o) }

// Function statements

func (c *Conv) VisitCreateFunction(o *plan.CreateFunction) (interface{}, error) { return NA(o) }
func (c *Conv) VisitDropFunction(o *plan.DropFunction) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitExecuteFunction(o *plan.ExecuteFunction) (interface{}, error) {
	return NA(o)
}

// Index Advisor

func (c *Conv) VisitIndexAdvice(o *plan.IndexAdvice) (interface{}, error) { return NA(o) }
func (c *Conv) VisitAdvise(o *plan.Advise) (interface{}, error)           { return NA(o) }

// Update Statistics

func (c *Conv) VisitUpdateStatistics(o *plan.UpdateStatistics) (interface{}, error) {
	return NA(o)
}

// -------------------------------------------------------------------

func NA(o interface{}) (interface{}, error) { return nil, fmt.Errorf("NA: %#v", o) }
