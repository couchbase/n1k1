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
	"errors"

	"github.com/couchbase/query/plan"
)

// ErrUnimpl represents an unimplemented API or feature.
var ErrUnimpl = errors.New("unimpl")

// Exec represents a conversion and execution of a query-plan. It
// implements the plan.Visitor interface.
type Exec struct {
}

func (e *Exec) Unimpl(op interface{}) (interface{}, error) { return nil, ErrUnimpl }

// Scan
func (e *Exec) VisitPrimaryScan(op *plan.PrimaryScan) (interface{}, error)       { return e.Unimpl(op) }
func (e *Exec) VisitPrimaryScan3(op *plan.PrimaryScan3) (interface{}, error)     { return e.Unimpl(op) }
func (e *Exec) VisitParentScan(op *plan.ParentScan) (interface{}, error)         { return e.Unimpl(op) }
func (e *Exec) VisitIndexScan(op *plan.IndexScan) (interface{}, error)           { return e.Unimpl(op) }
func (e *Exec) VisitIndexScan2(op *plan.IndexScan2) (interface{}, error)         { return e.Unimpl(op) }
func (e *Exec) VisitIndexScan3(op *plan.IndexScan3) (interface{}, error)         { return e.Unimpl(op) }
func (e *Exec) VisitKeyScan(op *plan.KeyScan) (interface{}, error)               { return e.Unimpl(op) }
func (e *Exec) VisitValueScan(op *plan.ValueScan) (interface{}, error)           { return e.Unimpl(op) }
func (e *Exec) VisitDummyScan(op *plan.DummyScan) (interface{}, error)           { return e.Unimpl(op) }
func (e *Exec) VisitCountScan(op *plan.CountScan) (interface{}, error)           { return e.Unimpl(op) }
func (e *Exec) VisitIndexCountScan(op *plan.IndexCountScan) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitIndexCountScan2(op *plan.IndexCountScan2) (interface{}, error) {
	return e.Unimpl(op)
}
func (e *Exec) VisitIndexCountDistinctScan2(op *plan.IndexCountDistinctScan2) (interface{}, error) {
	return e.Unimpl(op)
}
func (e *Exec) VisitDistinctScan(op *plan.DistinctScan) (interface{}, error)   { return e.Unimpl(op) }
func (e *Exec) VisitUnionScan(op *plan.UnionScan) (interface{}, error)         { return e.Unimpl(op) }
func (e *Exec) VisitIntersectScan(op *plan.IntersectScan) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitOrderedIntersectScan(op *plan.OrderedIntersectScan) (interface{}, error) {
	return e.Unimpl(op)
}
func (e *Exec) VisitExpressionScan(op *plan.ExpressionScan) (interface{}, error) { return e.Unimpl(op) }

// FTS Search
func (e *Exec) VisitIndexFtsSearch(op *plan.IndexFtsSearch) (interface{}, error) { return e.Unimpl(op) }

// Fetch
func (e *Exec) VisitFetch(op *plan.Fetch) (interface{}, error)           { return e.Unimpl(op) }
func (e *Exec) VisitDummyFetch(op *plan.DummyFetch) (interface{}, error) { return e.Unimpl(op) }

// Join
func (e *Exec) VisitJoin(op *plan.Join) (interface{}, error)           { return e.Unimpl(op) }
func (e *Exec) VisitIndexJoin(op *plan.IndexJoin) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitNest(op *plan.Nest) (interface{}, error)           { return e.Unimpl(op) }
func (e *Exec) VisitIndexNest(op *plan.IndexNest) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitUnnest(op *plan.Unnest) (interface{}, error)       { return e.Unimpl(op) }
func (e *Exec) VisitNLJoin(op *plan.NLJoin) (interface{}, error)       { return e.Unimpl(op) }
func (e *Exec) VisitNLNest(op *plan.NLNest) (interface{}, error)       { return e.Unimpl(op) }
func (e *Exec) VisitHashJoin(op *plan.HashJoin) (interface{}, error)   { return e.Unimpl(op) }
func (e *Exec) VisitHashNest(op *plan.HashNest) (interface{}, error)   { return e.Unimpl(op) }

// Let + Letting, With
func (e *Exec) VisitLet(op *plan.Let) (interface{}, error)   { return e.Unimpl(op) }
func (e *Exec) VisitWith(op *plan.With) (interface{}, error) { return e.Unimpl(op) }

// Filter
func (e *Exec) VisitFilter(op *plan.Filter) (interface{}, error) { return e.Unimpl(op) }

// Group
func (e *Exec) VisitInitialGroup(op *plan.InitialGroup) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitIntermediateGroup(op *plan.IntermediateGroup) (interface{}, error) {
	return e.Unimpl(op)
}
func (e *Exec) VisitFinalGroup(op *plan.FinalGroup) (interface{}, error) { return e.Unimpl(op) }

// Window functions
func (e *Exec) VisitWindowAggregate(op *plan.WindowAggregate) (interface{}, error) {
	return e.Unimpl(op)
}

// Project
func (e *Exec) VisitInitialProject(op *plan.InitialProject) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitFinalProject(op *plan.FinalProject) (interface{}, error)     { return e.Unimpl(op) }
func (e *Exec) VisitIndexCountProject(op *plan.IndexCountProject) (interface{}, error) {
	return e.Unimpl(op)
}

// Distinct
func (e *Exec) VisitDistinct(op *plan.Distinct) (interface{}, error) { return e.Unimpl(op) }

// Set operators
func (e *Exec) VisitUnionAll(op *plan.UnionAll) (interface{}, error)         { return e.Unimpl(op) }
func (e *Exec) VisitIntersectAll(op *plan.IntersectAll) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitExceptAll(op *plan.ExceptAll) (interface{}, error)       { return e.Unimpl(op) }

// Order
func (e *Exec) VisitOrder(op *plan.Order) (interface{}, error) { return e.Unimpl(op) }

// Paging
func (e *Exec) VisitOffset(op *plan.Offset) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitLimit(op *plan.Limit) (interface{}, error)   { return e.Unimpl(op) }

// Insert
func (e *Exec) VisitSendInsert(op *plan.SendInsert) (interface{}, error) { return e.Unimpl(op) }

// Upsert
func (e *Exec) VisitSendUpsert(op *plan.SendUpsert) (interface{}, error) { return e.Unimpl(op) }

// Delete
func (e *Exec) VisitSendDelete(op *plan.SendDelete) (interface{}, error) { return e.Unimpl(op) }

// Update
func (e *Exec) VisitClone(op *plan.Clone) (interface{}, error)           { return e.Unimpl(op) }
func (e *Exec) VisitSet(op *plan.Set) (interface{}, error)               { return e.Unimpl(op) }
func (e *Exec) VisitUnset(op *plan.Unset) (interface{}, error)           { return e.Unimpl(op) }
func (e *Exec) VisitSendUpdate(op *plan.SendUpdate) (interface{}, error) { return e.Unimpl(op) }

// Merge
func (e *Exec) VisitMerge(op *plan.Merge) (interface{}, error) { return e.Unimpl(op) }

// Framework
func (e *Exec) VisitAlias(op *plan.Alias) (interface{}, error)         { return e.Unimpl(op) }
func (e *Exec) VisitAuthorize(op *plan.Authorize) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitParallel(op *plan.Parallel) (interface{}, error)   { return e.Unimpl(op) }
func (e *Exec) VisitSequence(op *plan.Sequence) (interface{}, error)   { return e.Unimpl(op) }
func (e *Exec) VisitDiscard(op *plan.Discard) (interface{}, error)     { return e.Unimpl(op) }
func (e *Exec) VisitStream(op *plan.Stream) (interface{}, error)       { return e.Unimpl(op) }
func (e *Exec) VisitCollect(op *plan.Collect) (interface{}, error)     { return e.Unimpl(op) }

// Index DDL
func (e *Exec) VisitCreatePrimaryIndex(op *plan.CreatePrimaryIndex) (interface{}, error) {
	return e.Unimpl(op)
}
func (e *Exec) VisitCreateIndex(op *plan.CreateIndex) (interface{}, error)   { return e.Unimpl(op) }
func (e *Exec) VisitDropIndex(op *plan.DropIndex) (interface{}, error)       { return e.Unimpl(op) }
func (e *Exec) VisitAlterIndex(op *plan.AlterIndex) (interface{}, error)     { return e.Unimpl(op) }
func (e *Exec) VisitBuildIndexes(op *plan.BuildIndexes) (interface{}, error) { return e.Unimpl(op) }

// Roles
func (e *Exec) VisitGrantRole(op *plan.GrantRole) (interface{}, error)   { return e.Unimpl(op) }
func (e *Exec) VisitRevokeRole(op *plan.RevokeRole) (interface{}, error) { return e.Unimpl(op) }

// Explain
func (e *Exec) VisitExplain(op *plan.Explain) (interface{}, error) { return e.Unimpl(op) }

// Prepare
func (e *Exec) VisitPrepare(op *plan.Prepare) (interface{}, error) { return e.Unimpl(op) }

// Infer
func (e *Exec) VisitInferKeyspace(op *plan.InferKeyspace) (interface{}, error) { return e.Unimpl(op) }

// Function statements
func (e *Exec) VisitCreateFunction(op *plan.CreateFunction) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitDropFunction(op *plan.DropFunction) (interface{}, error)     { return e.Unimpl(op) }
func (e *Exec) VisitExecuteFunction(op *plan.ExecuteFunction) (interface{}, error) {
	return e.Unimpl(op)
}

// Index Advisor
func (e *Exec) VisitIndexAdvice(op *plan.IndexAdvice) (interface{}, error) { return e.Unimpl(op) }
func (e *Exec) VisitAdvise(op *plan.Advise) (interface{}, error)           { return e.Unimpl(op) }

// Update Statistics
func (e *Exec) VisitUpdateStatistics(op *plan.UpdateStatistics) (interface{}, error) {
	return e.Unimpl(op)
}
