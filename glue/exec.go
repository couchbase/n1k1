//go:build n1ql

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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/rhmap/store"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"

	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"
)

// ServiceRequestEx runs a planned statement through n1k1's own operators.
// (Formerly took a query/server.Request as its first arg -- dropped as part of
// decoupling n1k1 from query/server, which pulled in cgo deps. The arg was
// unused.)
func ServiceRequestEx(p plan.Operator,
	ctx *GlueContext, timeout time.Duration, asyncReadyCB func()) bool {
	texter, ok := p.(interface{ Text() string })
	if !ok || !strings.HasSuffix(texter.Text(), " n1k1 */") {
		return false
	}

	// Attempt to convert the plan.Operator to base.Op.
	op, temps, err := ExecConv(p)
	if err != nil || op == nil {
		fmt.Printf("ServiceRequestEx: op: %v,\n  err: %v\n", op, err)

		return false // We saw an unsupported operator.
	}

	cv, err := NewConvertVals(op.Labels)
	if err != nil {
		fmt.Printf("ServiceRequestEx: NewConvertVals, op: %v, err: %v\n", op, err)

		return false // We couldn't create a convert-vals.
	}

	go asyncReadyCB()

	tmpDir, vars := MakeVars("", "n1k1TmpDir") // TODO: Config.

	defer os.RemoveAll(tmpDir)

	vars.Temps = vars.Temps[:0]

	vars.Temps = append(vars.Temps, ctx)

	vars.Temps = append(vars.Temps, temps[1:]...)

	debug := strings.HasSuffix(texter.Text(), " debug n1k1 */")
	if debug {
		fmt.Printf("ServiceRequestEx, p: %#v\n", p)

		jop, _ := json.MarshalIndent(op, " ", " ")
		fmt.Printf("  jop: %s\n", jop)

		fmt.Printf("  tmpDir: %s\n", tmpDir)
		fmt.Printf("  vars.Temps: %#v\n", vars.Temps)
	}

	for i := 0; i < 16; i++ { // TODO: Config.
		vars.Temps = append(vars.Temps, nil)
	}

	err = nil

	yieldErr := func(errIn error) {
		if errIn != nil && err == nil {
			err = errIn // Keep first err.
		}
	}

	yieldVals := func(vals base.Vals) {
		v, err := cv.Convert(vals)
		if err == nil {
			item, ok := v.(value.AnnotatedValue)
			if !ok {
				item = value.NewAnnotatedValue(v)
			}

			ok = ctx.Result(item)

			_ = ok // TODO: Do something with the ok?

			// TODO: Handle non-nil err?
		}
	}

	// TODO: YieldStats.

	// TODO: Better allocators / recyclers.

	// TODO: The SetUp() method disappeared after CB 6.5, but
	// perhaps was replaced by some other method or call path.
	// ctx.SetUp()

	vars.Ctx.ExecOp(op, vars, yieldVals, yieldErr, "", "")

	if debug {
		fmt.Printf("  n1k1 err: %v\n", err)
	}

	ctx.CloseResults()

	return true
}

func MakeVars(dir, prefix string) (string, *base.Vars) {
	// TODO: Use os.MkdirTemp()?
	// TODO: Need err propagation & cleanup of temp dir?
	tmpDir, _ := ioutil.TempDir(dir, prefix)

	var counter uint64

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

	return tmpDir, &base.Vars{
		Temps: make([]interface{}, 16),
		Ctx: &base.Ctx{
			ValComparer: base.NewValComparer(),
			ExprCatalog: engine.ExprCatalog,
			YieldStats:  func(stats *base.Stats) error { return nil },
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
		},
	}
}
