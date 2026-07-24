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

// Package rt holds the cbq-free request runtime that both the interpreter
// (glue.MakeVars) and a compiled standalone EXECUTE child (glue.compiledMain)
// need: the spill-backed allocator pools that GROUP BY / ORDER BY / hash-join /
// DISTINCT hang off base.Ctx. It imports only base + engine + rhmap/store, so the
// compiled child -- which must build without cbq -- can construct the SAME Ctx the
// interpreter uses. Keeping this shared is deliberate: an earlier hand-rolled child
// Ctx omitted these func fields, so every aggregate/group query nil-panicked in the
// child while the interpreter was fine. See DESIGN-prepare.md.
package rt

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/couchbase/rhmap/store"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

const maxRecycledBatches = 256 // cap batch retention; drop extras for GC.

// SpillState holds a request's spill-backed allocator pools (map / heap / chunks / batch) and
// the temp dir they put files under. The temp dir is created LAZILY (ensureDir) on first spill,
// so a query that never spills pays no mkdir/rmdir.
//
// The default is a fresh state per request (NewSpillCtx / MakeVars). A Session may instead hold
// ONE state across its queries (NewSpillCtxState) so the rhmap store / batch buffers recycle and
// the temp dir is created at most once per connection -- call Cleanup at Session.Close. A Session
// runs one query at a time, so a shared state is used single-threaded; the mutexes guard only the
// rare multi-actor recycle within a single query (e.g. a GROUP inside a parallel UNION ALL branch).
type SpillState struct {
	tmpDir  string
	dirOnce sync.Once
	dirErr  error
	counter uint64

	mm             sync.Mutex // guards the map/heap/chunks recycle slots.
	recycledMap    *store.RHStore
	recycledHeap   *store.Heap
	recycledChunks *store.Chunks

	bm              sync.Mutex // guards the batch free-list (hot path, kept off mm).
	recycledBatches [][]base.Vals
}

// NewSpillState makes a spill state rooted at tmpDir (created lazily on first spill).
func NewSpillState(tmpDir string) *SpillState { return &SpillState{tmpDir: tmpDir} }

// ensureDir creates tmpDir the first time an allocator actually needs to put a file there --
// so scan/filter/project queries, and any GROUP/ORDER that stays within the in-memory
// StartSize, pay no mkdir. See DESIGN-concurrency.md.
func (st *SpillState) ensureDir() error {
	st.dirOnce.Do(func() {
		if st.tmpDir != "" {
			st.dirErr = os.MkdirAll(st.tmpDir, 0o755)
		}
	})
	return st.dirErr
}

// Cleanup closes any recycled stores and removes the temp dir. Idempotent. A Session that holds
// a shared state across queries calls this at Close so the reused files/dir don't outlive the
// connection. (A fresh per-request state is instead cleaned by the caller's defer os.RemoveAll.)
func (st *SpillState) Cleanup() {
	st.mm.Lock()
	if st.recycledMap != nil {
		st.recycledMap.Close()
		st.recycledMap = nil
	}
	if st.recycledHeap != nil {
		st.recycledHeap.Close()
		st.recycledHeap = nil
	}
	if st.recycledChunks != nil {
		st.recycledChunks.Close()
		st.recycledChunks = nil
	}
	st.mm.Unlock()
	if st.tmpDir != "" {
		os.RemoveAll(st.tmpDir)
	}
}

func (st *SpillState) allocMap() (*store.RHStore, error) {
	st.mm.Lock()
	defer st.mm.Unlock()

	if st.recycledMap != nil {
		rv := st.recycledMap
		st.recycledMap = nil
		return rv, nil
	}
	if err := st.ensureDir(); err != nil {
		return nil, err
	}

	options := store.DefaultRHStoreFileOptions
	pathPrefix := fmt.Sprintf("%s/%d", st.tmpDir, atomic.AddUint64(&st.counter, 1))

	sf, err := store.CreateRHStoreFile(pathPrefix, options)
	if err != nil {
		return nil, err
	}
	return &sf.RHStore, nil
}

func (st *SpillState) recycleMap(m *store.RHStore) {
	st.mm.Lock()
	defer st.mm.Unlock()

	if m != nil {
		if st.recycledMap == nil {
			st.recycledMap = m
			st.recycledMap.Reset()
			return
		}
		m.Close()
	}
}

func (st *SpillState) allocHeap() (*store.Heap, error) {
	st.mm.Lock()
	defer st.mm.Unlock()

	if st.recycledHeap != nil {
		rv := st.recycledHeap
		st.recycledHeap = nil
		return rv, nil
	}
	if err := st.ensureDir(); err != nil {
		return nil, err
	}

	pathPrefix := fmt.Sprintf("%s/%d", st.tmpDir, atomic.AddUint64(&st.counter, 1))

	heapChunkSizeBytes := 1024 * 1024      // TODO: Config.
	dataChunkSizeBytes := 16 * 1024 * 1024 // TODO: Config.

	return &store.Heap{
		LessFunc: func(a, b []byte) bool {
			// TODO: Is this the right default heap less-func?
			return bytes.Compare(a, b) < 0
		},
		Heap: &store.Chunks{
			PathPrefix:     pathPrefix,
			FileSuffix:     ".heap",
			ChunkSizeBytes: heapChunkSizeBytes,
		},
		Data: &store.Chunks{
			PathPrefix:     pathPrefix,
			FileSuffix:     ".data",
			ChunkSizeBytes: dataChunkSizeBytes,
		},
	}, nil
}

func (st *SpillState) recycleHeap(m *store.Heap) {
	st.mm.Lock()
	defer st.mm.Unlock()

	if m != nil {
		if st.recycledHeap == nil {
			st.recycledHeap = m
			st.recycledHeap.Reset()
			return
		}
		m.Close()
	}
}

func (st *SpillState) allocChunks() (*store.Chunks, error) {
	st.mm.Lock()
	defer st.mm.Unlock()

	if st.recycledChunks != nil {
		rv := st.recycledChunks
		st.recycledChunks = nil
		return rv, nil
	}
	if err := st.ensureDir(); err != nil {
		return nil, err
	}

	options := store.DefaultRHStoreFileOptions
	pathPrefix := fmt.Sprintf("%s/%d", st.tmpDir, atomic.AddUint64(&st.counter, 1))

	return &store.Chunks{
		PathPrefix:     pathPrefix,
		FileSuffix:     ".rhchunk,",
		ChunkSizeBytes: options.ChunkSizeBytes,
	}, nil
}

func (st *SpillState) recycleChunks(c *store.Chunks) {
	st.mm.Lock()
	defer st.mm.Unlock()

	if c != nil {
		if st.recycledChunks == nil {
			st.recycledChunks = c
			st.recycledChunks.BytesTruncate(0)
			return
		}
		c.Close()
	}
}

func (st *SpillState) allocBatch() []base.Vals {
	st.bm.Lock()
	defer st.bm.Unlock()

	if n := len(st.recycledBatches); n > 0 {
		rv := st.recycledBatches[n-1]
		st.recycledBatches[n-1] = nil
		st.recycledBatches = st.recycledBatches[:n-1]
		return rv
	}
	return nil
}

func (st *SpillState) recycleBatch(batch []base.Vals) {
	if batch == nil {
		return
	}
	st.bm.Lock()
	if len(st.recycledBatches) < maxRecycledBatches {
		st.recycledBatches = append(st.recycledBatches, batch)
	}
	st.bm.Unlock()
}

// NewSpillCtx builds a base.Ctx wired with a FRESH spill state rooted at tmpDir -- the default
// per-request path (MakeVars) and the one the compiled standalone EXECUTE child emits. Both the
// interpreter and the child route through here so the pools can never diverge.
func NewSpillCtx(tmpDir string) *base.Ctx { return NewSpillCtxState(NewSpillState(tmpDir)) }

// NewSpillCtxState builds a base.Ctx over an EXISTING (possibly Session-shared) spill state, so
// the rhmap store / batch buffers recycle across the state's queries and its temp dir is created
// at most once. The caller sets request-specific fields (Pipe, Halt, Stats, Warn, ...) on the
// returned Ctx afterward. The per-query Ctx is otherwise fresh -- only the allocator pools are
// shared -- so per-query Ctx state (RunningAggJobs, Stats, ...) never leaks across queries.
func NewSpillCtxState(st *SpillState) *base.Ctx {
	return &base.Ctx{
		ValComparer:   base.NewValComparer(),
		ExprCatalog:   engine.ExprCatalog,
		YieldStats:    func(stats *base.Stats) base.YieldStatsControl { return base.YieldStatsControl{} },
		TempDir:       st.tmpDir,
		ExecOp:        engine.ExecOp,
		AllocMap:      st.allocMap,
		RecycleMap:    st.recycleMap,
		AllocHeap:     st.allocHeap,
		RecycleHeap:   st.recycleHeap,
		AllocChunks:   st.allocChunks,
		RecycleChunks: st.recycleChunks,
		AllocBatch:    st.allocBatch,
		RecycleBatch:  st.recycleBatch,
	}
}
