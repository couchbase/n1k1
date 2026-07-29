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
	"fmt"
	"os"
	"time"

	"github.com/couchbase/n1k1/base"
)

// CombineAggregate folds partial accumulators of a mergeable aggregate, exercising
// the commutative-monoid contract end-to-end: each batch is aggregated independently
// to a PARTIAL state (Init + one Update per value), the partials are combined pairwise
// via base.Agg.Merge, and the folded state is finalized to a single Val. For a
// mergeable aggregate this equals aggregating the concatenation of every batch --
// `combine(part(A), part(B)) == aggregate(A ∪ B)` -- which is the seam a windowed
// (incremental) or sharded (parallel) caller uses instead of re-folding raw rows.
//
// n1k1's GROUP BY is single-pass (one accumulator per key, no combine site), so this
// is the ONLY place partials are merged today; automatic two-phase/parallel grouping
// is future work. It errors if NAME isn't a registered aggregate or isn't mergeable
// (Merge == nil). JS aggregates become mergeable by defining NAME_merge (see
// ext_jsvm_agg.go); native ones by setting base.Agg.Merge.
func (s *Session) CombineAggregate(name string, batches [][]base.Val) (base.Val, error) {
	idx, ok := base.AggCatalog[name]
	if !ok {
		return nil, fmt.Errorf("CombineAggregate: no such aggregate %q", name)
	}
	agg := base.Aggs[idx]
	if agg.Merge == nil {
		return nil, fmt.Errorf("CombineAggregate: aggregate %q is not mergeable (define %s_merge)", name, name)
	}

	tmpDir, vars := MakeVars("", "n1k1combine")
	defer os.RemoveAll(tmpDir)

	// A *GlueContext at Temps[0] is what the JS-aggregate bridge resolves its runtime
	// from (jsSharedFromVars); native aggregates ignore it.
	gctx := NewGlueContext(time.Now())
	gctx.InitSubqueries(s.Store, s.Namespace, nil, nil)
	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, gctx)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}
	vc := vars.Ctx.ValComparer

	// part reduces one batch to a PARTIAL accumulator (double-buffered, exactly the
	// fold op_group.go runs per group key).
	part := func(vals []base.Val) []byte {
		state := agg.Init(vars, nil)
		var buf []byte
		for _, v := range vals {
			buf, _, _ = agg.Update(vars, v, buf[:0], state, vc)
			state, buf = buf, state // the new accumulator is in buf; swap for the next row
		}
		return state
	}

	if len(batches) == 0 {
		batches = [][]base.Val{nil} // an empty combine is the aggregate over no rows
	}

	acc := part(batches[0])
	for _, b := range batches[1:] {
		acc = agg.Merge(vars, acc, part(b), nil)
	}

	v, _, _ := agg.Result(vars, acc, nil)
	return append(base.Val(nil), v...), nil // copy: v may alias a fold buffer
}
