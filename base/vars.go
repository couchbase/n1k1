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

package base

import (
	"io"
	"time"

	"github.com/couchbase/rhmap/store"
)

// Vars are used for runtime variables, config, etc, which might
// change during the request. Vars are chainable using the Next field
// to allow for scoping. Vars are not concurrent safe.
type Vars struct {
	Temps []interface{}
	Next  *Vars // The root Vars has nil Next.
	Ctx   *Ctx
}

// -----------------------------------------------------

// ChainCloner lets a temp resource that is otherwise shallow-copied by
// ChainExtend hand back a per-actor clone instead, so mutable per-goroutine
// state it carries doesn't get shared across concurrent actors. Temps[0] (the
// evaluation context) implements this: it holds dynamically-scoped state (an
// outer correlated row) that each UNION ALL actor must mutate independently.
// Kept as an interface{}-returning method so base stays decoupled from glue.
type ChainCloner interface {
	// ChainClone returns a clone safe for one concurrent actor to mutate.
	ChainClone() interface{}
}

// ChainExtend returns a new Vars linked to the Vars chain, which is
// safely usable by a concurrent goroutine and useful for shadowing.
//
// The temps are shallow-copied (not left empty): they hold the read-only
// convert-time resources -- plan objects and FROM-expression trees that ops
// like datastore-scan / expr-scan read by slot index -- which each concurrent
// actor (e.g. a UNION ALL contributor) still needs. Any per-run resource an
// actor creates is TempSet into its own copy, so actors don't share mutable
// state.
//
// The one exception is Temps[0], the evaluation context: it is a single shared
// pointer, so a shallow copy would let concurrent actors race on the
// dynamically-scoped state it carries (e.g. the outer row of a correlated
// subquery). If it implements ChainCloner, each actor gets its own clone.
func (v *Vars) ChainExtend() *Vars {
	temps := make([]interface{}, len(v.Temps))
	copy(temps, v.Temps)
	if len(temps) > 0 {
		if cc, ok := temps[0].(ChainCloner); ok {
			temps[0] = cc.ChainClone()
		}
	}
	return &Vars{
		Temps: temps,
		Next:  v,
		Ctx:   v.Ctx.Clone(),
	}
}

// -----------------------------------------------------

// TempSet associates a resource with a temp slot and also closes any
// resource that was previously in that slot.
func (v *Vars) TempSet(idx int, resource interface{}) {
	prev := v.Temps[idx]
	if prev != nil {
		closer, ok := prev.(io.Closer)
		if ok {
			closer.Close()
		}
	}

	v.Temps[idx] = resource
}

// -----------------------------------------------------

// TempGetHeap casts the retrieved temp resource into a heap.
func (v *Vars) TempGetHeap(idx int) (rv *store.Heap) {
	r := v.Temps[idx]
	if r != nil {
		rv, _ = r.(*store.Heap)
	}

	return rv
}

// -----------------------------------------------------

// Ctx represents the runtime context for a request, where a Ctx is
// immutable for the lifetime of the request and is concurrent safe.
type Ctx struct {
	Now time.Time

	ExprCatalog map[string]ExprCatalogFunc

	// ValComparer is not concurrent safe. See Clone().
	ValComparer *ValComparer

	// YieldStats may be invoked concurrently by multiple goroutines. It is
	// handed the request's shared *Stats (Stats below), or nil when stats are
	// off; implementors must tolerate a nil argument.
	YieldStats YieldStats

	// Stats is the request's shared counter core, sized once by LayoutStats at
	// setup, or nil when stats are off (the default, zero-cost path). Cloned by
	// pointer in Clone(), so every actor bumps into the one backing array. See
	// stats.go and DESIGN-stats.md.
	Stats *Stats

	// RunningAggJobs are this actor's live-aggregate refreshers: one per GROUP op in
	// THIS actor's branch, registered at op setup (RegisterRunningAgg) and run at this
	// actor's checkpoint (RefreshRunningAggs). Because Ctx is cloned per actor (Clone
	// resets this to nil) and a job writes only its op's own fixed Stats.RunningAggs
	// slot, each running-aggregate buffer has exactly ONE writer goroutine even when a
	// GROUP BY runs inside each parallel UNION ALL branch. Interpreter-only.
	RunningAggJobs []RunningAggJob

	// Warn records a non-fatal advisory (e.g. divide-by-zero) during
	// evaluation. It is cbq-free by design (a plain string), so engine/base
	// stay decoupled from couchbase/query; glue wires it to the request's
	// warning collector. May be nil (warnings then dropped).
	Warn func(warning string)

	// TempDir is the path to a temporary directory that can be used
	// while processing the request, where the temporary directory
	// might be shared amongst concurrent requests.
	TempDir string

	ExecOp func(*Op, *Vars, YieldVals, YieldErr, string, string)

	// Pipe, when non-nil, serves this request's datastore leaf ops (scans,
	// fetches) instead of the process-global datastore dispatch -- so a query can
	// read inline in-memory data (engine.MemPipe) or a remote source without
	// changing its plan. Copied by pointer in Clone (shared across UNION ALL
	// actors, read-only during a run). See base.DatastorePipe.
	Pipe DatastorePipe

	AllocMap   func() (*store.RHStore, error)
	RecycleMap func(*store.RHStore)

	AllocHeap   func() (*store.Heap, error)
	RecycleHeap func(*store.Heap)

	AllocChunks   func() (*store.Chunks, error)
	RecycleChunks func(*store.Chunks)

	// AllocBatch/RecycleBatch pool []Vals batches across the whole request, so the
	// throwaway Stage instances a nested-loop join spins up per inner re-scan can
	// reuse batch + Val buffers instead of re-allocating them (Stage.Recycled is
	// per-instance and dies with the Stage). AllocBatch returns a recycled batch or
	// nil -- it never blocks (so it can't deadlock); RecycleBatch returns one to the
	// pool (bounded; drops extras for GC). nil => Stage falls back to its own
	// per-instance Recycled. Shared across Ctx.Clone() (func fields are copied, so
	// every actor's clone closes over the one pool).
	AllocBatch   func() []Vals
	RecycleBatch func([]Vals)

	// TODO: Other things that might appear here might be request ID,
	// request-specific allocators or resources, etc.
}

// -----------------------------------------------------

// Clone returns a copy of the given Ctx, which is safe for another
// goroutine to use safely.
func (ctx *Ctx) Clone() (ctxCopy *Ctx) {
	ctxCopy = &Ctx{}
	*ctxCopy = *ctx
	ctxCopy.ValComparer = NewValComparer()

	// Each actor tracks only its OWN branch's running-aggregate refreshers, so it refreshes
	// (and thus single-writes) only its own ops' Stats.RunningAggs slots. Starting
	// empty avoids sharing the parent's job slice across goroutines.
	ctxCopy.RunningAggJobs = nil

	return ctxCopy
}

// -----------------------------------------------------

// RunningAggJob is one op's live-aggregate refresher, bound to that op's fixed
// Stats.RunningAggs slot. fill re-decodes the op's current accumulators into the
// given (reused) per-op buffer; it runs on the owning actor's goroutine only.
type RunningAggJob struct {
	slot int
	fill func(*RunningAggs)
}

// RunningAggRegister records a live-aggregate refresher for one op (slot =
// Op.RunningAggSlot), called once at the op's setup on the owning actor's goroutine.
// Appends to the per-actor RunningAggJobs (no cross-goroutine sharing, so no lock).
// A negative slot or nil fill is ignored.
func (ctx *Ctx) RunningAggRegister(slot int, fill func(*RunningAggs)) {
	if ctx == nil || slot < 0 || fill == nil {
		return
	}
	ctx.RunningAggJobs = append(ctx.RunningAggJobs, RunningAggJob{slot: slot, fill: fill})
}

// RunningAggsRefresh re-fills THIS actor's running-aggregate slots from its registered jobs. It
// is called at the synchronous YieldStats checkpoint (on this actor's goroutine,
// between row yields), so each job reads coherent, non-mutating accumulator bytes
// from its own group map. It writes only this actor's own slots (single writer per
// slot); runningAggsMu is held only to fence a concurrent snapshot reader, and only at
// this ~10 Hz checkpoint -- never on the per-row hot path. Allocation-free in
// steady state (the per-op buffers are reused). A no-op when stats/running-aggregate is off.
func (ctx *Ctx) RunningAggsRefresh() {
	if ctx == nil || ctx.Stats == nil || len(ctx.RunningAggJobs) == 0 {
		return
	}

	s := ctx.Stats
	s.runningAggsMu.Lock()
	for _, j := range ctx.RunningAggJobs {
		if j.slot < 0 || j.slot >= len(s.RunningAggs) {
			continue
		}
		p := &s.RunningAggs[j.slot]
		p.reset()
		j.fill(p)
	}
	s.runningAggsMu.Unlock()
}
