//go:build n1ql

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

package glue

// Session-scoped materialization (IDEA-0027): CREATE TEMP KEYSPACE <name> AS
// <select> runs the SELECT once, captures its result rows in memory, and registers
// them as a queryable keyspace that LATER statements in the SAME session can
// `SELECT ... FROM <name>` -- so a staged/hierarchical detector pipeline (scan the
// GB bundle -> keep the small findings -> correlate them) stays in one session and
// one SQL++ dialect, no shell-out to jsonl files (the layout trap of the manual
// approach) and no re-parse across processes.
//
// It is an ephemeral analogue of the file-backed INSERT INTO materialize
// (insert.go): the rows live only for the session and vanish when it ends. The
// keyspace is exposed exactly like the synthetic flat/glob keyspaces (flat.go /
// glob.go) -- a metadata-only virtual keyspace advertising a primary index so the
// planner emits a PrimaryScan -- but instead of pointing the records-scan at a
// directory it advertises RecordsSource, served from the captured rows (see
// KeyspaceRecordsOpen).
//
// The captured rows are held in a store.Heap (the same append-only, insertion-
// ordered, spillable primitive the corpus shared-scan cache uses): chunk 0 stays
// in memory, so a small findings set never touches disk, and a large one AUTOMATICALLY
// spills its overflow to mmap'd temp files under a per-session dir -- so a materialize
// that outgrows RAM degrades to disk rather than OOMing. CREATE streams each row
// straight into the heap (bounded memory regardless of result size); DROP / OR REPLACE
// / session close the heap, which removes its spill files.
//
// The datastore wrapper (tempDatastore/tempNamespace) sits INNERMOST in the chain
// (below the flat/secondary-index wrappers), so those keep their concrete type
// identity for the type-asserts that gate .index et al., and delegate unknown names
// down to it. It consults the session's LIVE registry, so a keyspace created by one
// statement is visible to the next.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/virtual"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/rhmap/store"

	"github.com/couchbase/n1k1/records"
)

// tempNamespaceName is the namespace TEMP KEYSPACEs live in -- the session default,
// so a bare `FROM <name>` resolves them.
const tempNamespaceName = flatRootNamespace // "default"

// TempKeyspaces is a session's registry of materialized keyspaces (each backed by a
// spillable heap: chunk 0 in RAM, overflow on disk). It is owned by the Store (so it
// shares the datastore's lifetime and is reachable from the wrapper) and mutated by
// the CREATE/DROP TEMP KEYSPACE handlers. Concurrency-safe: the wrapper reads it
// during planning/scanning while a handler may be writing (a nested subquery run
// could resolve keyspaces mid-materialize).
type TempKeyspaces struct {
	mu  sync.RWMutex
	ks  map[string]*tempKeyspace
	ns  *tempNamespace // parents the virtual keyspaces; set by wrapTempKeyspaces
	dir string         // lazily-created temp dir holding heap spill files
	seq int            // unique per-heap prefix counter
}

func newTempKeyspaces() *TempKeyspaces {
	return &TempKeyspaces{ks: map[string]*tempKeyspace{}}
}

// NewHeap allocates an empty, append-only, insertion-ordered, spillable heap for a
// temp keyspace's rows, under this registry's (lazily-created) session temp dir.
// Chunk 0 is in-memory; overflow spills to mmap'd files there. The caller streams
// rows in via PushBytes, then hands the heap to Put.
func (t *TempKeyspaces) NewHeap() (*store.Heap, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dir == "" {
		d, err := os.MkdirTemp("", "n1k1-tempks-")
		if err != nil {
			return nil, fmt.Errorf("temp keyspace: creating spill dir: %w", err)
		}
		t.dir = d
	}
	t.seq++
	prefix := filepath.Join(t.dir, strconv.Itoa(t.seq))
	return &store.Heap{
		Heap: &store.Chunks{PathPrefix: prefix, FileSuffix: ".heap", ChunkSizeBytes: tempHeapChunkBytes},
		Data: &store.Chunks{PathPrefix: prefix, FileSuffix: ".data", ChunkSizeBytes: tempHeapDataChunkBytes},
	}, nil
}

// Heap chunk sizes for a temp keyspace: chunk 0 lives in memory and holds this many
// bytes before overflow spills to mmap'd files. Vars (not consts) only so a test can
// shrink them to exercise the spill path without materializing 16 MiB of rows.
var (
	tempHeapChunkBytes     = 1024 * 1024      // per index-chunk (offset/size entries)
	tempHeapDataChunkBytes = 16 * 1024 * 1024 // per data-chunk (row bytes)
)

// Put materializes name over the rows already pushed into heap, replacing (and
// closing the spill files of) any existing temp keyspace of that name. The registry
// takes ownership of heap.
func (t *TempKeyspaces) Put(name string, heap *store.Heap) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ns == nil {
		return fmt.Errorf("temp keyspace: registry not wired to a datastore")
	}
	vks, verr := virtual.NewVirtualKeyspace(t.ns, []string{tempNamespaceName, name})
	if verr != nil {
		return fmt.Errorf("temp keyspace %q: %v", name, verr)
	}
	if prev, ok := t.ks[name]; ok && prev.heap != nil {
		prev.heap.Close() // OR REPLACE: drop the old rows + spill files.
	}
	k := &tempKeyspace{Keyspace: vks, name: name, heap: heap}
	k.indexer = newFlatIndexer(k)
	t.ks[name] = k
	return nil
}

// Drop removes name (closing its heap + spill files), reporting whether it existed.
func (t *TempKeyspaces) Drop(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	k, ok := t.ks[name]
	if !ok {
		return false
	}
	if k.heap != nil {
		k.heap.Close()
	}
	delete(t.ks, name)
	return true
}

// Close drops every temp keyspace (closing all heaps + spill files) and removes the
// session temp dir. Called when the session is closed / re-opened. Idempotent.
func (t *TempKeyspaces) Close() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range t.ks {
		if k.heap != nil {
			k.heap.Close()
		}
	}
	t.ks = map[string]*tempKeyspace{}
	if t.dir != "" {
		os.RemoveAll(t.dir)
		t.dir = ""
	}
}

func (t *TempKeyspaces) get(name string) (*tempKeyspace, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if k, ok := t.ks[name]; ok {
		return k, true
	}
	// Case-insensitive fallback, matching the fork's identifier resolution.
	for n, k := range t.ks {
		if strings.EqualFold(n, name) {
			return k, true
		}
	}
	return nil, false
}

// Names returns the registered temp-keyspace names, sorted.
func (t *TempKeyspaces) Names() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]string, 0, len(t.ks))
	for n := range t.ks {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// IsTempKeyspace reports whether ks is a session TEMP KEYSPACE (used by the .tables
// framing classifier to tag it as in-memory rather than probe for files).
func IsTempKeyspace(ks datastore.Keyspace) bool {
	_, ok := ks.(*tempKeyspace)
	return ok
}

// wrapTempKeyspaces layers a temp-keyspace overlay over ds (which must be the raw
// file datastore -- this is the INNERMOST wrapper). It returns the wrapped datastore
// and wires reg.ns so later Puts can parent their virtual keyspaces here.
func wrapTempKeyspaces(ds datastore.Datastore, reg *TempKeyspaces) datastore.Datastore {
	w := &tempDatastore{Datastore: ds, reg: reg}
	// Capture the real "default" namespace for delegation. Guard the error: the
	// fork's file datastore can return a non-nil-but-invalid namespace for a missing
	// dir (it panics on use), so keep inner ONLY when err == nil (as globDatastore does).
	inner, ierr := ds.NamespaceByName(tempNamespaceName)
	if ierr != nil {
		inner = nil
	}
	w.ns = &tempNamespace{ds: w, reg: reg, inner: inner}
	reg.ns = w.ns
	return w
}

// --------------------------------------------------------- datastore wrapper

// tempDatastore embeds the inner datastore (promoting its ~40 methods) and overlays
// the temp keyspaces on its "default" namespace. Every other namespace delegates
// straight to the inner datastore, so nothing is hidden.
type tempDatastore struct {
	datastore.Datastore
	reg *TempKeyspaces
	ns  *tempNamespace
}

func (d *tempDatastore) NamespaceById(id string) (datastore.Namespace, errors.Error) {
	return d.NamespaceByName(id)
}

func (d *tempDatastore) NamespaceByName(name string) (datastore.Namespace, errors.Error) {
	if strings.EqualFold(name, tempNamespaceName) {
		return d.ns, nil
	}
	return d.Datastore.NamespaceByName(name)
}

// --------------------------------------------------------- namespace

// tempNamespace is the "default" namespace with the session's temp keyspaces
// overlaid on top of the real "default" (inner). It implements the full
// datastore.Namespace interface explicitly (not by embedding) because inner may be
// nil, mirroring globNamespace.
type tempNamespace struct {
	ds    *tempDatastore
	reg   *TempKeyspaces
	inner datastore.Namespace // real "default", or nil when the tree has none
}

func (p *tempNamespace) Datastore() datastore.Datastore { return p.ds }
func (p *tempNamespace) Id() string                     { return tempNamespaceName }
func (p *tempNamespace) Name() string                   { return tempNamespaceName }

func (p *tempNamespace) KeyspaceIds() ([]string, errors.Error) { return p.KeyspaceNames() }

func (p *tempNamespace) KeyspaceNames() ([]string, errors.Error) {
	seen := map[string]bool{}
	var out []string
	for _, n := range p.reg.Names() {
		out = append(out, n)
		seen[strings.ToLower(n)] = true
	}
	if p.inner != nil {
		if rn, err := p.inner.KeyspaceNames(); err == nil {
			for _, n := range rn {
				if !seen[strings.ToLower(n)] {
					out = append(out, n)
				}
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func (p *tempNamespace) KeyspaceById(id string) (datastore.Keyspace, errors.Error) {
	return p.KeyspaceByName(id)
}

func (p *tempNamespace) KeyspaceByName(name string) (datastore.Keyspace, errors.Error) {
	if k, ok := p.reg.get(name); ok {
		return k, nil
	}
	if p.inner != nil {
		return p.inner.KeyspaceByName(name)
	}
	return nil, errors.NewError(nil, "namespace default: no keyspace "+name)
}

func (p *tempNamespace) BucketIds() ([]string, errors.Error)   { return nil, nil }
func (p *tempNamespace) BucketNames() ([]string, errors.Error) { return nil, nil }
func (p *tempNamespace) BucketById(string) (datastore.Bucket, errors.Error) {
	return nil, errors.NewError(nil, "temp: no buckets")
}
func (p *tempNamespace) BucketByName(string) (datastore.Bucket, errors.Error) {
	return nil, errors.NewError(nil, "temp: no buckets")
}

func (p *tempNamespace) Objects(creds *auth.Credentials, filter func(string) bool,
	preload bool) ([]datastore.Object, errors.Error) {
	var out []datastore.Object
	for _, n := range p.reg.Names() {
		out = append(out, datastore.Object{Id: n, Name: n, IsKeyspace: true})
	}
	if p.inner != nil {
		if ro, err := p.inner.Objects(creds, filter, preload); err == nil {
			out = append(out, ro...)
		}
	}
	return out, nil
}

// --------------------------------------------------------- keyspace

// tempKeyspace embeds a metadata-only virtual keyspace (promoting its Keyspace
// methods), advertises a primary index (so the planner emits a PrimaryScan), and
// serves its captured rows from the spillable heap via RecordsSource (see
// KeyspaceRecordsOpen).
type tempKeyspace struct {
	datastore.Keyspace
	name    string      // for synthetic per-row ids ("<name><i>")
	heap    *store.Heap // captured rows, insertion-ordered; chunk 0 in RAM, overflow on disk
	indexer datastore.Indexer
}

// RecordsSource serves the captured rows from the heap (no files, no framing). It is
// the duck-typed hook KeyspaceRecordsOpen checks first. Each RecordsSource is an
// independent cursor (so a self-join gets two), all reading the same immutable heap.
func (k *tempKeyspace) RecordsSource(_ records.WalkOptions) (records.Source, error) {
	return &heapRecordsSource{heap: k.heap, name: k.name}, nil
}

func (k *tempKeyspace) Indexer(datastore.IndexType) (datastore.Indexer, errors.Error) {
	return k.indexer, nil
}
func (k *tempKeyspace) Indexers() ([]datastore.Indexer, errors.Error) {
	return []datastore.Indexer{k.indexer}, nil
}

// heapRecordsSource is a records.Source over a temp keyspace's store.Heap: it replays
// the captured rows in insertion order (Get(i) for i in [0, CurItems)), synthesizing
// a positional id per row. The Doc bytes are borrowed from the heap (an in-memory
// chunk or an mmap'd spill file) and the id from a reused buffer -- both valid only
// until the next Next, exactly the file sources' borrowed-slice contract.
type heapRecordsSource struct {
	heap  *store.Heap
	name  string
	i     int64
	idBuf []byte
}

func (h *heapRecordsSource) Next(rec *records.Record) (bool, error) {
	if h.heap == nil || h.i >= h.heap.CurItems {
		return false, nil
	}
	b, err := h.heap.Get(h.i)
	if err != nil {
		return false, err
	}
	h.idBuf = strconv.AppendInt(append(h.idBuf[:0], h.name...), h.i, 10)
	rec.ID, rec.Doc, rec.Loc = h.idBuf, b, records.RecordLoc{}
	h.i++
	return true, nil
}

func (h *heapRecordsSource) Close() error { return nil }
