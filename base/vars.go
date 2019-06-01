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

// ChainExtend returns a new Vars linked to the Vars chain, which is
// safely usable by a concurrent goroutine and useful for shadowing.
func (v *Vars) ChainExtend() *Vars {
	return &Vars{
		Temps: make([]interface{}, len(v.Temps)),
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

	// YieldStats may be invoked concurrently by multiple goroutines.
	YieldStats YieldStats

	// TempDir is the path to a temporary directory that can be used
	// while processing the request, where the temporary directory
	// might be shared amongst concurrent requests.
	TempDir string

	AllocMap   func() (*store.RHStore, error)
	RecycleMap func(*store.RHStore)

	AllocHeap   func() (*store.Heap, error)
	RecycleHeap func(*store.Heap)

	AllocChunks   func() (*store.Chunks, error)
	RecycleChunks func(*store.Chunks)

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

	return ctxCopy
}
