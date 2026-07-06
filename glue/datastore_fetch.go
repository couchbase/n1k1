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
	"unsafe"

	"github.com/buger/jsonparser"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"
)

func init() {
	// Escape hatches for A/B profiling and debugging.
	if os.Getenv(base.DefEnv("N1K1_FETCH_CBQ", "set to use cbq keyspace.Fetch (value boxing + encoding/json)")) != "" {
		DatastoreFetchCBQ = false
	}
	if os.Getenv(base.DefEnv("N1K1_FETCH_NOCACHE", "set for no per-request caching when fetching from data store")) != "" {
		DatastoreFetchCache = false
	}
	if os.Getenv(base.DefEnv("N1K1_FETCH_ACTOR", "set to use concurrent actor-based fetching from data store")) != "" {
		DatastoreFetchActor = true
	}
}

// DatastoreFetchCache memoizes native-fetch doc bytes per request (see
// GlueContext.fetchCache): a nested-loop join re-fetches the same docs O(NxM)
// times, so after the first pass every fetch is a map hit -- no re-open, no
// re-read, no allocation. env N1K1_FETCH_NOCACHE=1 disables.
var DatastoreFetchCache = true

// DatastoreFetchCacheMaxBytes caps the per-request doc cache so a fetch over a huge
// keyspace can't grow the heap without bound; a miss past the cap reads into a
// reused buffer instead of caching.
var DatastoreFetchCacheMaxBytes = 64 << 20 // 64 MiB.

// fetchCacheGet returns the cached bytes for (dir, key) -- owned, immutable -- if
// any. Keyed by the existing dir and key strings, so a hit allocates nothing.
func (c *GlueContext) fetchCacheGet(dir, key string) ([]byte, bool) {
	c = c.getRoot() // the cache lives on the root, shared across UNION ALL clones
	c.fetchCacheMu.Lock()
	var b []byte
	var ok bool
	if m := c.fetchCache[dir]; m != nil {
		b, ok = m[key]
	}
	c.fetchCacheMu.Unlock()
	return b, ok
}

// fetchCachePut stores an owned copy of b under (dir, key) once, while under the
// byte cap, and returns the cached slice; it returns nil when the cache is full,
// so the caller yields its (borrowed) read buffer instead. First writer wins.
func (c *GlueContext) fetchCachePut(dir, key string, b []byte) []byte {
	c = c.getRoot() // the cache lives on the root, shared across UNION ALL clones
	c.fetchCacheMu.Lock()
	defer c.fetchCacheMu.Unlock()

	m := c.fetchCache[dir]
	if m != nil {
		if existing, ok := m[key]; ok {
			return existing
		}
	}
	if c.fetchCacheN+len(b) > DatastoreFetchCacheMaxBytes {
		return nil
	}
	cp := append([]byte(nil), b...) // Owned, immutable copy (stable for the request).
	if c.fetchCache == nil {
		c.fetchCache = make(map[string]map[string][]byte)
	}
	if m == nil {
		m = make(map[string][]byte)
		c.fetchCache[dir] = m
	}
	// Clone the key: callers may pass an unsafe zero-copy key aliasing a batch
	// buffer (see DatastoreFetch), so the map must own a stable copy.
	m[strings.Clone(key)] = cp
	c.fetchCacheN += len(cp)
	return cp
}

type Keyspacer interface {
	Keyspace() datastore.Keyspace
}

type SubPathser interface {
	SubPaths() []string
}

// DatastoreFetchCBQ enables the native byte-path fetch: for a classic
// directory-backed file keyspace it reads each `<dir>/<key>.json` directly into a
// reused buffer and yields those raw JSON bytes as base.Val, skipping cbq's
// keyspace.Fetch (which boxes a value.AnnotatedValue and re-parses via
// encoding/json -- ~71% of the allocations in a nested-loop-join profile; see
// DESIGN-data.md "Allocation model"). Flip off to A/B against the cbq path. When
// the keyspace isn't eligible (a synthetic flat-root / single-file keyspace, or a
// subpath projection was pushed down), fetch falls back to cbq's Fetch.
var DatastoreFetchCBQ = true

// DatastoreFetchActor picks DatastoreFetch's execution mode. Default (false) runs
// the scan inline in the calling goroutine, feeding each scanned ^id straight into
// the fetch -- no producer goroutine, channel, or per-row batch deep-copy. That's
// the win for a nested-loop join's re-driven inner (re-run per outer row, consumed
// immediately), where the actor's producer/consumer channel hand-off dominated CPU
// and its batch materialization dominated allocs.
//
// Set true to keep the concurrent actor: the scan runs in its own goroutine feeding
// batches over a channel, so a *high-latency* source (e.g. downloading files from
// S3) overlaps with fetch/downstream processing. Both modes share the same
// fetchOne/flushCbq logic. (A future high-latency datastore would select this
// per-keyspace rather than globally.) env N1K1_FETCH_ACTOR=1 enables it.
var DatastoreFetchActor = false

func DatastoreFetch(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	plan := vars.Temps[o.Params[0].(int)].(Keyspacer)

	keyspace := plan.Keyspace()
	// A `FROM $1 AS d USE KEYS ...` (a positional/named-parameter keyspace) leaves
	// the plan's keyspace nil -- n1k1 can't resolve a keyspace name at runtime.
	// Fail cleanly rather than nil-deref down in KeyspaceDir/KeyspaceRecordsOpen.
	if keyspace == nil {
		yieldErr(fmt.Errorf("DatastoreFetch: unresolved keyspace (parameterized FROM not supported)"))
		return
	}

	// The per-request GlueContext hosts the doc cache (persists across this fetch
	// op's re-invocations by an outer nested-loop join). nil-tolerant: no cache then.
	gctx, _ := vars.Temps[0].(*GlueContext)

	var subPaths []string
	if subPathser, ok := plan.(SubPathser); ok {
		subPaths = subPathser.SubPaths()
	}

	// Native byte-path directories, resolved once. A key is dispatched by its form
	// (below): a container record id `<relpath>#<line>@<offset>` seeks into a
	// multi-doc file under containerDir; a plain key reads `<dir>/<key>.json` under
	// nativeDir. They usually resolve to the same keyspace directory -- the id form
	// picks the reader -- so a keyspace holding both standalone .json docs and .jsonl
	// containers works. Anything neither path handles falls back to cbq's Fetch.
	//
	//   - nativeDir (classic <key>.json): only for a real cbq file keyspace
	//     (<root>/<ns>/<keyspace>) with no subpath projection (which only cbq's
	//     Fetch honors -- yielding the whole doc would be a superset). A synthetic
	//     flat/single-file keyspace has no <key>.json files, so it stays "".
	//   - containerDir (multi-doc records): the keyspace's data directory, for any
	//     keyspace -- flat (RecordsDir / the RecordsFile's dir) or classic
	//     (KeyspaceDir). Set regardless of subpaths: cbq can't fetch a container
	//     record at all, so yielding the whole doc is the only correct option.
	nativeDir := ""
	containerDir := ""
	if DatastoreFetchCBQ {
		_, isFlat := keyspace.(interface{ RecordsDir() string })
		_, isFile := keyspace.(interface{ RecordsFile() string })
		if !isFlat && !isFile {
			if dir, err := KeyspaceDir(keyspace); err == nil {
				if len(subPaths) == 0 {
					nativeDir = dir
				}
				containerDir = dir
			}
		} else {
			containerDir = containerBaseDir(keyspace)
		}
	}

	var vals base.Vals

	var cbqKeys []string // Keys neither native path handled; deferred to cbq.

	fetchMap := map[string]value.AnnotatedValue{}

	var docBuf []byte // Reused across keys on the native path (single drain goroutine).
	var idBuf []byte
	var buf bytes.Buffer // Reused for cbq-fallback WriteJSON.

	useCache := DatastoreFetchCache && gctx != nil

	// yieldDoc emits one fetched doc: "." = doc bytes, "^id" = the key as canonical
	// JSON (a quoted string, so Convert reads it as a string; the incoming key can
	// arrive unquoted from an ON KEYS split).
	yieldDoc := func(doc []byte, key string) {
		idBuf = strconv.AppendQuote(idBuf[:0], key)
		vals = append(vals[:0], base.Val(doc)) // Label ".".
		vals = append(vals, idBuf)             // Label "^id".
		yieldVals(vals)
	}

	// fetchOne decodes one scanned ^id and dispatches it: a native/container read
	// yields the doc immediately; anything neither native path handles is deferred to
	// cbqKeys for the batched cbq Fetch (flushCbq). Shared by both drivers below, and
	// only ever called sequentially, so it reuses docBuf/idBuf/vals freely.
	fetchOne := func(idVal base.Val) {
		// Decode the ^id (a canonical-JSON string, e.g. `"key"`) to the plain doc key.
		// Mirrors jsonparser.GetString, but on the no-escape fast path returns an
		// UNSAFE zero-copy string aliasing idVal's bytes -- valid only until the caller
		// reuses idVal (the scan's next row), i.e. after this returns. The two spots
		// that retain a key past that -- fetchCachePut's map store and cbqKeys --
		// strings.Clone it. BINARY / escaped keys take safe, owning fallbacks.
		var key string
		v, dt, _, e := jsonparser.Get(idVal)
		switch {
		case e != nil || dt != jsonparser.String:
			key = string(idVal) // BINARY / non-string key: owned copy.
		case bytes.IndexByte(v, '\\') >= 0:
			key, _ = jsonparser.ParseString(v) // escaped: unescape (allocates; rare).
		case len(v) > 0:
			key = unsafe.String(unsafe.SliceData(v), len(v)) // zero-copy alias.
		}
		if key == "" {
			return
		}

		// ---- Container record: seek to the byte offset baked into the id. ----
		// A key with an `@<offset>` suffix is a seekable multi-doc record.
		if containerDir != "" {
			if _, _, isContainer := parseContainerKey(key); isContainer {
				var doc []byte
				if useCache {
					doc, _ = gctx.fetchCacheGet(containerDir, key) // Owned; nil on miss.
				}
				if doc == nil {
					// Miss: parse `<relpath>#<line>@<offset>`, open the container file,
					// seek to the record's byte offset, read its one line into the reused
					// buffer, then cache an owned copy so a nested-loop join's re-fetches
					// are hits.
					b, ok2, err := readContainerRecord(containerDir, key, &docBuf)
					if err != nil {
						yieldErr(fmt.Errorf("DatastoreFetch (container), key %q: %v", key, err))
						return
					}
					if !ok2 || len(b) == 0 {
						return // Missing record => non-existent doc; skip (matches cbq).
					}
					doc = b // Borrowed from docBuf (valid until the next read)...
					if useCache {
						if cached := gctx.fetchCachePut(containerDir, key, b); cached != nil {
							doc = cached // ...unless cached, then owned + stable for the request.
						}
					}
				}
				yieldDoc(doc, key)
				return
			}
		}

		// ---- Classic byte path: cache-hit, else read <dir>/<key>.json. ----
		if nativeDir != "" {
			var doc []byte
			if useCache {
				doc, _ = gctx.fetchCacheGet(nativeDir, key) // Owned; nil on miss. No path built.
			}
			if doc == nil {
				// Miss: resolve+guard the path (only here, not per hit), read the
				// whole doc into the reused buffer (no per-read alloc), then cache
				// an owned copy so every later fetch of this key -- the common case
				// under a nested-loop join -- is a hit needing no path or read.
				p, ok := docPath(nativeDir, key)
				if !ok {
					yieldErr(fmt.Errorf("DatastoreFetch (native): invalid key %q", key))
					return
				}
				b, ok2, err := readWholeFileInto(p, &docBuf)
				if err != nil {
					yieldErr(fmt.Errorf("DatastoreFetch (native), key %q: %v", key, err))
					return
				}
				if !ok2 || len(b) == 0 {
					return // Missing / empty file => non-existent doc; skip (matches cbq).
				}
				doc = b // Borrowed from docBuf (valid until the next read)...
				if useCache {
					if cached := gctx.fetchCachePut(nativeDir, key, b); cached != nil {
						doc = cached // ...unless cached, then owned + stable for the request.
					}
				}
			}
			yieldDoc(doc, key)
			return
		}

		// Neither native path applies (a synthetic keyspace, or a subpath projection
		// over classic <key>.json): defer to cbq's Fetch. Clone the key -- it may be an
		// unsafe batch-aliasing string, and cbq's Fetch (and fetchMap) can retain it.
		cbqKeys = append(cbqKeys, strings.Clone(key))
	}

	// flushCbq fetches the deferred keys (subpath projection / synthetic keyspaces)
	// via cbq's batched Fetch and yields them in key order. Shared by both drivers.
	flushCbq := func() {
		if len(cbqKeys) == 0 {
			return
		}

		// ---- Fallback: cbq's Fetch (subpath projection, synthetic keyspaces). ----
		for k := range fetchMap {
			// TODO: Will golang's fetchMap resize downwards, or keep
			// the same buckets?
			// TODO: Need a Fetch API that allows us to use rhmap.
			delete(fetchMap, k)
		}

		errs := keyspace.Fetch(cbqKeys, fetchMap, datastore.NULL_QUERY_CONTEXT, subPaths, nil /* projection */, false /* useSubDoc */)
		for _, err := range errs {
			yieldErr(fmt.Errorf("DatastoreFetch, err: %v", err))
		}

		// Keep the same ordering as the keys.
		for _, key := range cbqKeys {
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
						yieldDoc(jv, key)
					}
				}
			}
		}
	}

	// Two drivers sharing fetchOne/flushCbq (see DatastoreFetchActor):
	//
	//   - inline (default): drive the scan in THIS goroutine, feeding each scanned ^id
	//     straight into fetchOne. No producer goroutine, channel, or per-row batch
	//     deep-copy -- the scanned row is consumed before the scan reuses its buffer.
	//     Ideal for a nested-loop join's re-driven inner (re-run per outer row).
	//   - actor: drive the scan in a concurrent actor feeding batches over a channel,
	//     so a high-latency source (e.g. downloading from S3) overlaps with fetch and
	//     downstream processing.
	if !DatastoreFetchActor {
		cbqKeys = cbqKeys[:0]

		var scanErr error
		vars.Ctx.ExecOp(o.Children[0], vars,
			func(row base.Vals) { fetchOne(row[0]) },
			func(err error) {
				if err != nil && scanErr == nil {
					scanErr = err // capture (like Stage.Err); reported once as the terminal error
				}
			}, pathNext, "DF")

		if scanErr == nil {
			flushCbq()
		}
		yieldErr(scanErr) // terminal signal (nil == done), mirroring stage.YieldErr(stage.Err)
		return
	}

	// Actor mode: the scan runs in its own goroutine, producing ^id batches over a
	// channel that the consumer below drains -- so a slow producer overlaps with fetch.
	batchSize := 200 // TODO: Configurability.
	stage := base.NewStage(1, 0 /* batchChSize */, vars, yieldVals, yieldErr)

	stage.StartActor(func(vars *base.Vars, yieldVals base.YieldVals,
		yieldErr base.YieldErr, actorData interface{}) {
		vars.Ctx.ExecOp(o.Children[0], vars, yieldVals, yieldErr, pathNext, "DF")
	}, nil, batchSize)

	stage.ProcessBatchesFromActors(func(batch []base.Vals) {
		cbqKeys = cbqKeys[:0]
		for _, bv := range batch {
			fetchOne(bv[0])
		}
		flushCbq()
	})

	stage.M.Lock()
	stage.YieldErr(stage.Err)
	stage.M.Unlock()

	// TODO: Recycle stage.
}

// docPath resolves key to its backing file `<dir>/<key>.json`, rejecting
// path-traversal keys the same way cbq's file keyspace.keyPath does (so a crafted
// key like "../../etc/passwd" can't escape the keyspace dir). ok is false for a
// rejected key. The path also serves as the doc cache key.
func docPath(dir, key string) (p string, ok bool) {
	p = filepath.Join(dir, key+".json")
	if rel, e := filepath.Rel(dir, p); e != nil ||
		rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	return p, true
}

// readWholeFileInto reads the whole file at p into the reused, growable buffer
// *bufp via io.ReaderAt.ReadAt(buf, 0) -- one copy into memory we own and recycle,
// no per-doc heap allocation. The returned slice borrows *bufp (valid only until
// the next call; the caller either copies it into the doc cache or relies on the
// YieldVals copy-on-retain contract). ok is false when the file doesn't exist (a
// non-existent doc, which the caller skips -- matching cbq's file keyspace, which
// ignores os.IsNotExist).
func readWholeFileInto(p string, bufp *[]byte) (doc []byte, ok bool, err error) {
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

	if int64(cap(*bufp)) < n {
		*bufp = make([]byte, n)
	}
	nRead, e := f.ReadAt((*bufp)[:n], 0) // Slicing to bytes-read tolerates a shrink race.
	if e != nil && e != io.EOF {
		return nil, false, e
	}

	return (*bufp)[:nRead], true, nil
}

// containerBaseDir returns the directory a container keyspace's `<relpath>` record
// ids are relative to: the walked directory for a RecordsDir keyspace, or the
// file's own directory for a single RecordsFile keyspace -- matching how
// records.Walk / records.File assign each record's id prefix, so a fetch resolves
// the same path the scan named. Returns "" when the keyspace isn't a container.
func containerBaseDir(keyspace datastore.Keyspace) string {
	if rd, ok := keyspace.(interface{ RecordsDir() string }); ok && rd.RecordsDir() != "" {
		return rd.RecordsDir()
	}
	if rf, ok := keyspace.(interface{ RecordsFile() string }); ok && rf.RecordsFile() != "" {
		return filepath.Dir(rf.RecordsFile())
	}
	return ""
}

// parseContainerKey splits a seekable container record id
// `<relpath>#<line>@<offset>` into its dir-relative file path and byte offset
// (a JSONL line's start, or a multi-doc YAML document's start). ok is false when
// the id has no `@<offset>` suffix -- a record whose bytes aren't randomly
// seekable (a compressed .gz/.zst, CSV, JSON-array, or top-level-YAML-sequence
// record), so key-based fetch of it isn't supported. Parses from the right (last
// '@', then the '#' before it) so a `<relpath>` containing '#'/'@' still resolves.
func parseContainerKey(key string) (relpath string, off int64, ok bool) {
	at := strings.LastIndexByte(key, '@')
	if at < 0 {
		return "", 0, false
	}
	n, err := strconv.ParseInt(key[at+1:], 10, 64)
	if err != nil || n < 0 {
		return "", 0, false
	}
	h := strings.LastIndexByte(key[:at], '#')
	if h <= 0 { // also rejects an empty relpath (h == 0)
		return "", 0, false
	}
	return key[:h], n, true
}

// containerFilePath joins a container keyspace's base dir and a record's
// dir-relative path, rejecting a path that escapes the dir (the same traversal
// guard as docPath, so a crafted key can't read outside the keyspace).
func containerFilePath(dir, relpath string) (string, bool) {
	p := filepath.Join(dir, filepath.FromSlash(relpath))
	if rel, e := filepath.Rel(dir, p); e != nil ||
		rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	return p, true
}

// readContainerRecord resolves one container record: it parses key's
// `<relpath>@<offset>`, opens the file, seeks to the record's byte offset, and
// reads that one record, returning its bytes. A JSONL record is one line (read
// into the reused buffer *bufp, whitespace-trimmed); a YAML record is one
// document (decoded to JSON bytes -- see records.DecodeYAMLDoc), since a YAML
// document spans multiple lines. ok is false -- with no error -- for an
// unsupported (non-seekable) key or a missing file, both of which the caller
// treats as a non-existent doc and skips.
func readContainerRecord(dir, key string, bufp *[]byte) (doc []byte, ok bool, err error) {
	relpath, off, ok := parseContainerKey(key)
	if !ok {
		return nil, false, nil
	}
	p, ok := containerFilePath(dir, relpath)
	if !ok {
		return nil, false, fmt.Errorf("invalid container key %q", key)
	}
	f, e := os.Open(p)
	if e != nil {
		if os.IsNotExist(e) {
			return nil, false, nil
		}
		return nil, false, e
	}
	defer f.Close()

	if records.IsYAMLFile(relpath) {
		if _, e := f.Seek(off, io.SeekStart); e != nil {
			return nil, false, e
		}
		return records.DecodeYAMLDoc(f)
	}

	line, e := readLineAtInto(f, off, bufp)
	if e != nil {
		return nil, false, e
	}
	return bytes.TrimSpace(line), true, nil
}

// readLineAtInto seeks f to off and reads the one line beginning there into the
// reused, growable buffer *bufp, returning it WITHOUT the trailing newline. Reads
// in chunks (no per-call bufio allocation) until '\n' or EOF.
func readLineAtInto(f *os.File, off int64, bufp *[]byte) ([]byte, error) {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	buf := (*bufp)[:0]
	var chunk [8192]byte
	for {
		n, err := f.Read(chunk[:])
		if n > 0 {
			if i := bytes.IndexByte(chunk[:n], '\n'); i >= 0 {
				buf = append(buf, chunk[:i]...)
				*bufp = buf
				return buf, nil
			}
			buf = append(buf, chunk[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				*bufp = buf
				return buf, nil
			}
			return nil, err
		}
	}
}
