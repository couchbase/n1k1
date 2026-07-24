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

// NewSpillCtx builds a base.Ctx wired with the request's spill-backed allocator
// pools (map/heap/chunks/batch) rooted at tmpDir, plus the engine's ExprCatalog and
// ExecOp. The caller sets request-specific fields (Pipe, Halt, Stats, Warn, ...) on
// the returned Ctx afterward. This is the one place the pools are defined; both the
// interpreter and the compiled child route through it so they can never diverge.
func NewSpillCtx(tmpDir string) *base.Ctx {
	var counter uint64

	// ensureDir lazily creates tmpDir the first time an allocator actually needs to put a
	// file there. MakeVars no longer pre-creates it, so a query that never spills (every
	// scan/filter/project, and any GROUP/ORDER that stays within the in-memory StartSize)
	// pays no mkdir/rmdir syscalls -- which pprof showed as per-query overhead once the
	// file-scan cost is removed. See DESIGN-concurrency.md.
	var dirOnce sync.Once
	var dirErr error
	ensureDir := func() error {
		dirOnce.Do(func() {
			if tmpDir != "" {
				dirErr = os.MkdirAll(tmpDir, 0o755)
			}
		})
		return dirErr
	}

	var mm sync.Mutex

	var recycledMap *store.RHStore
	var recycledHeap *store.Heap
	var recycledChunks *store.Chunks

	// Request-scoped batch pool (see base.Ctx.AllocBatch): a bounded free-list of
	// []Vals, so the many throwaway Stages a nested-loop join creates per inner
	// re-scan reuse batch/Val buffers instead of re-allocating. Own mutex to keep
	// the hot per-batch path off mm (which guards the rarer map/heap/chunk pools).
	var bm sync.Mutex
	var recycledBatches [][]base.Vals
	const maxRecycledBatches = 256 // cap retention; drop extras for GC

	return &base.Ctx{
		ValComparer: base.NewValComparer(),
		ExprCatalog: engine.ExprCatalog,
		YieldStats:  func(stats *base.Stats) base.YieldStatsControl { return base.YieldStatsControl{} },
		TempDir:     tmpDir,
		ExecOp:      engine.ExecOp,
		AllocMap: func() (*store.RHStore, error) {
			mm.Lock()
			defer mm.Unlock()

			if recycledMap != nil {
				rv := recycledMap
				recycledMap = nil
				return rv, nil
			}

			if err := ensureDir(); err != nil {
				return nil, err
			}

			options := store.DefaultRHStoreFileOptions

			counterMine := atomic.AddUint64(&counter, 1)

			pathPrefix := fmt.Sprintf("%s/%d", tmpDir, counterMine)

			sf, err := store.CreateRHStoreFile(pathPrefix, options)
			if err != nil {
				return nil, err
			}

			return &sf.RHStore, nil
		},
		RecycleMap: func(m *store.RHStore) {
			mm.Lock()
			defer mm.Unlock()

			if m != nil {
				if recycledMap == nil {
					recycledMap = m
					recycledMap.Reset()
					return
				}

				m.Close()
			}
		},
		AllocHeap: func() (*store.Heap, error) {
			mm.Lock()
			defer mm.Unlock()

			if recycledHeap != nil {
				rv := recycledHeap
				recycledHeap = nil
				return rv, nil
			}

			if err := ensureDir(); err != nil {
				return nil, err
			}

			counterMine := atomic.AddUint64(&counter, 1)

			pathPrefix := fmt.Sprintf("%s/%d", tmpDir, counterMine)

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
		},
		RecycleHeap: func(m *store.Heap) {
			mm.Lock()
			defer mm.Unlock()

			if m != nil {
				if recycledHeap == nil {
					recycledHeap = m
					recycledHeap.Reset()
					return
				}

				m.Close()
			}
		},
		AllocChunks: func() (*store.Chunks, error) {
			mm.Lock()
			defer mm.Unlock()

			if recycledChunks != nil {
				rv := recycledChunks
				recycledChunks = nil
				return rv, nil
			}

			if err := ensureDir(); err != nil {
				return nil, err
			}

			options := store.DefaultRHStoreFileOptions

			counterMine := atomic.AddUint64(&counter, 1)

			pathPrefix := fmt.Sprintf("%s/%d", tmpDir, counterMine)

			return &store.Chunks{
				PathPrefix:     pathPrefix,
				FileSuffix:     ".rhchunk,",
				ChunkSizeBytes: options.ChunkSizeBytes,
			}, nil
		},
		RecycleChunks: func(c *store.Chunks) {
			mm.Lock()
			defer mm.Unlock()

			if c != nil {
				if recycledChunks == nil {
					recycledChunks = c
					recycledChunks.BytesTruncate(0)
					return
				}

				c.Close()
			}
		},
		AllocBatch: func() []base.Vals {
			bm.Lock()
			defer bm.Unlock()

			if n := len(recycledBatches); n > 0 {
				rv := recycledBatches[n-1]
				recycledBatches[n-1] = nil
				recycledBatches = recycledBatches[:n-1]
				return rv
			}
			return nil
		},
		RecycleBatch: func(batch []base.Vals) {
			if batch == nil {
				return
			}
			bm.Lock()
			if len(recycledBatches) < maxRecycledBatches {
				recycledBatches = append(recycledBatches, batch)
			}
			bm.Unlock()
		},
	}
}
