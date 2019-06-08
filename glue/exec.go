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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/rhmap/store"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/execution"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/server"
	"github.com/couchbase/query/value"
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

func ServiceRequestEx(r server.Request, p plan.Operator,
	ctx *execution.Context, timeout time.Duration, asyncReadyCB func()) bool {
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

	ctx.SetUp()

	n1k1.ExecOp(op, vars, yieldVals, yieldErr, "", "")

	if debug {
		fmt.Printf("  n1k1 err: %v\n", err)
	}

	ctx.CloseResults()

	return true
}

func MakeVars(dir, prefix string) (string, *base.Vars) {
	tmpDir, _ := ioutil.TempDir(dir, prefix)

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
