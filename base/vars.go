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
	"time"

	"github.com/couchbase/rhmap/store"
)

// Vars are used for runtime variables, config, etc.  Vars are
// chainable using the Next field to allow for scoping.
type Vars struct {
	Labels Labels
	Vals   Vals  // Same len() as Labels.
	Next   *Vars // The root Vars has nil Next.
	Ctx    *Ctx
}

// ChainExtend returns a new Vars linked to the Vars chain, which is
// safely usable by a concurrent goroutine and useful for shadowing.
func (v *Vars) ChainExtend() *Vars {
	return &Vars{Next: v, Ctx: v.Ctx.Clone()}
}

// -----------------------------------------------------

// Ctx represents the runtime context for a request.
type Ctx struct {
	Now time.Time

	ExprCatalog map[string]ExprCatalogFunc

	// ValComparer is not concurrent safe. See Clone().
	ValComparer *ValComparer

	// YieldStats may be invoked concurrently by multiple goroutines.
	YieldStats YieldStats

	TempDir string

	AllocMap   func() (*store.RHStore, error)
	RecycleMap func(*store.RHStore)

	AllocChunks   func() (*store.Chunks, error)
	RecycleChunks func(*store.Chunks)

	// TODO: Other things that might appear here might be request ID,
	// request-specific allocators or resources, etc.
}

// Clone returns a copy of the given Ctx, which is safe for another
// goroutine to use safely.
func (ctx *Ctx) Clone() (ctxCopy *Ctx) {
	ctxCopy = &Ctx{}
	*ctxCopy = *ctx
	ctxCopy.ValComparer = NewValComparer()

	return ctxCopy
}
