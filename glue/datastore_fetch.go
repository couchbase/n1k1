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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
)

func init() {
	// Escape hatch for A/B profiling and debugging: N1K1_FETCH_CBQ=1 forces the
	// legacy cbq keyspace.Fetch path (value boxing + encoding/json) instead of the
	// native byte read.
	if os.Getenv("N1K1_FETCH_CBQ") != "" {
		DatastoreFetchNative = false
	}
}

type Keyspacer interface {
	Keyspace() datastore.Keyspace
}

type SubPathser interface {
	SubPaths() []string
}

// DatastoreFetchNative enables the native byte-path fetch: for a classic
// directory-backed file keyspace it reads each `<dir>/<key>.json` directly into a
// reused buffer and yields those raw JSON bytes as base.Val, skipping cbq's
// keyspace.Fetch (which boxes a value.AnnotatedValue and re-parses via
// encoding/json -- ~71% of the allocations in a nested-loop-join profile; see
// DESIGN-data.md "Allocation model"). Flip off to A/B against the cbq path. When
// the keyspace isn't eligible (a synthetic flat-root / single-file keyspace, or a
// subpath projection was pushed down), fetch falls back to cbq's Fetch.
var DatastoreFetchNative = true

func DatastoreFetch(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	plan := vars.Temps[o.Params[0].(int)].(Keyspacer)

	keyspace := plan.Keyspace()

	var subPaths []string
	if subPathser, ok := plan.(SubPathser); ok {
		subPaths = subPathser.SubPaths()
	}

	// Native fast-path eligibility, resolved once: the classic cbq file keyspace
	// layout (<root>/<ns>/<keyspace>/<key>.json), and no subpath projection (which
	// only cbq's Fetch honors -- yielding the whole doc would be a superset). A
	// synthetic flat-root (RecordsDir) or single-file (RecordsFile) keyspace is not
	// a directory of standalone <key>.json files, so it stays on the cbq path.
	nativeDir := ""
	if DatastoreFetchNative && len(subPaths) == 0 {
		_, isFlat := keyspace.(interface{ RecordsDir() string })
		_, isFile := keyspace.(interface{ RecordsFile() string })
		if !isFlat && !isFile {
			if dir, err := keyspaceDir(keyspace); err == nil {
				nativeDir = dir
			}
		}
	}

	batchSize := 200 // TODO: Configurability.
	batchChSize := 0 // TODO: Configurability.

	stage := base.NewStage(1, batchChSize, vars, yieldVals, yieldErr)

	stage.StartActor(func(vars *base.Vars, yieldVals base.YieldVals,
		yieldErr base.YieldErr, actorData interface{}) {

		vars.Ctx.ExecOp(o.Children[0], vars, yieldVals, yieldErr, pathNext, "DF")
	}, nil, batchSize)

	var vals base.Vals

	var keys []string // Same len() as batch.

	fetchMap := map[string]value.AnnotatedValue{}

	var docBuf []byte // Reused across keys on the native path (single drain goroutine).
	var idBuf []byte
	var buf bytes.Buffer // Reused for cbq-fallback WriteJSON.

	stage.ProcessBatchesFromActors(func(batch []base.Vals) {
		keys = keys[:0]

		for _, vals := range batch {
			// The incoming ^id is a canonical-JSON string (e.g. `"key"`); decode it
			// with jsonparser (the engine's parser) rather than encoding/json, so the
			// native path pulls in no standard-JSON machinery. GetString with no path
			// keys decodes the root string token (quotes + escapes). A non-JSON-string
			// (BINARY key) falls back to the raw bytes.
			key, err := jsonparser.GetString(vals[0])
			if err != nil {
				key = string(vals[0]) // BINARY key.
			}

			keys = append(keys, key)
		}

		// ---- Native byte path: read <dir>/<key>.json into a reused buffer. ----
		if nativeDir != "" {
			for _, key := range keys {
				if key == "" {
					continue
				}

				doc, ok, err := readDocFileInto(nativeDir, key, &docBuf)
				if err != nil {
					yieldErr(fmt.Errorf("DatastoreFetch (native), key %q: %v", key, err))
					continue
				}
				if !ok || len(doc) == 0 {
					continue // Missing / empty file => non-existent doc; skip (matches cbq).
				}

				// "^id" must be canonical JSON (a quoted string) so Convert reads it
				// as a string; the incoming key can arrive unquoted (ON KEYS split).
				idBuf = strconv.AppendQuote(idBuf[:0], key)

				vals = append(vals[:0], base.Val(doc)) // Label ".".
				vals = append(vals, idBuf)             // Label "^id".

				yieldVals(vals)
			}
			return
		}

		// ---- Fallback: cbq's Fetch (subpath projection, synthetic keyspaces). ----
		for k := range fetchMap {
			// TODO: Will golang's fetchMap resize downwards, or keep
			// the same buckets?
			// TODO: Need a Fetch API that allows us to use rhmap.
			delete(fetchMap, k)
		}

		errs := keyspace.Fetch(keys, fetchMap, datastore.NULL_QUERY_CONTEXT, subPaths, nil /* projection */, false /* useSubDoc */)
		for _, err := range errs {
			yieldErr(fmt.Errorf("DatastoreFetch, err: %v", err))
		}

		// Keep the same ordering as the batch.
		for _, key := range keys {
			if key != "" {
				v, ok := fetchMap[key]
				if ok && v != nil {
					// TODO: Propagate other meta info like cas, type,
					// flags, expiration if needed?
					//
					// TODO: Handle when v is BINARY?

					buf.Reset()

					err := v.WriteJSON(nil, &buf, "", "", true)

					jv := buf.Bytes()

					if err == nil && len(jv) > 0 {
						idBuf = strconv.AppendQuote(idBuf[:0], key)

						vals = append(vals[:0], jv) // Label ".".
						vals = append(vals, idBuf)  // Label "^id".

						yieldVals(vals)
					}
				}
			}
		}
	})

	stage.M.Lock()
	stage.YieldErr(stage.Err)
	stage.M.Unlock()

	// TODO: Recycle stage.
}

// readDocFileInto reads the document file backing key (`<dir>/<key>.json`) into
// the reused, growable buffer *bufp, returning the JSON bytes borrowed from that
// buffer (valid only until the next call -- the YieldVals copy-on-retain contract
// applies, exactly as for the scan's reused row buffer). ok is false when the file
// doesn't exist (a non-existent doc, which the caller skips -- matching cbq's file
// keyspace, which ignores os.IsNotExist). It rejects path-traversal keys the same
// way cbq's keyspace.keyPath does, so a crafted key can't escape the keyspace dir.
func readDocFileInto(dir, key string, bufp *[]byte) (doc []byte, ok bool, err error) {
	p := filepath.Join(dir, key+".json")

	// Path-traversal guard (mirrors couchbase/query datastore/file keyspace.keyPath):
	// reject any key that resolves outside dir.
	if rel, e := filepath.Rel(dir, p); e != nil ||
		rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return nil, false, fmt.Errorf("invalid key %q", key)
	}

	f, e := os.Open(p)
	if e != nil {
		if os.IsNotExist(e) {
			return nil, false, nil
		}
		return nil, false, e
	}
	defer f.Close()

	fi, e := f.Stat()
	if e != nil {
		return nil, false, e
	}
	n := fi.Size()
	if n <= 0 {
		return nil, true, nil // Empty file -> skipped by the caller (len(doc)==0).
	}

	// Grow the reused buffer to fit; read the whole doc at offset 0 via ReadAt
	// (io.ReaderAt) into it -- one copy into memory we own and recycle, no per-doc
	// heap allocation. (Slicing to the bytes actually read tolerates a shrink race.)
	if int64(cap(*bufp)) < n {
		*bufp = make([]byte, n)
	}
	nRead, e := f.ReadAt((*bufp)[:n], 0)
	if e != nil && e != io.EOF {
		return nil, false, e
	}

	return (*bufp)[:nRead], true, nil
}
