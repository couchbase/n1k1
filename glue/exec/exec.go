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

package exec

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/rhmap/store"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/execution"
	"github.com/couchbase/query/plan"
)

var Debug = 0

func init() {
	v := os.Getenv("CB_N1K1_GLUE_EXEC_DEBUG")
	if v != "" {
		i, err := strconv.Atoi(v)
		if err == nil {
			Debug = i
		}
	}
}

// ExecRequest represents the subset of query/server's Request
// interface that's needed for execution.
type ExecRequest interface {
	Output() execution.Output
}

// ExecParams represents additional params that are not already
// available from the context or the request.
type ExecParams struct {
	Timeout time.Duration
}

func ExecMaybe(context *execution.Context, request ExecRequest,
	prepared plan.Operator, params ExecParams) bool {
	fmt.Printf("ExecMaybe, prepared: %v\n", prepared)

	op, temps, err := ExecConv(prepared)
	if err != nil || op == nil {
		return false
	}

	tmpDir, vars := MakeVars()

	defer os.RemoveAll(tmpDir)

	vars.Temps = vars.Temps[:0]

	vars.Temps = append(vars.Temps, context)

	vars.Temps = append(vars.Temps, temps[1:]...)

	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	// n1k1.ExecOp(op, vars, yieldVals, yieldErr, "", "")

	return false // true
}

func MakeVars() (string, *base.Vars) {
	tmpDir, _ := ioutil.TempDir("", "n1k1TmpDir")

	var counter uint64

	var mm sync.Mutex

	var recycledMap *store.RHStore
	var recycledHeap *store.Heap
	var recycledChunks *store.Chunks

	return tmpDir, &base.Vars{
		Temps: make([]interface{}, 16),
		Ctx: &base.Ctx{
			ValComparer: base.NewValComparer(),
			ExprCatalog: n1k1.ExprCatalog,
			YieldStats:  func(stats *base.Stats) error { return nil },
			TempDir:     tmpDir,
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
		},
	}
}
