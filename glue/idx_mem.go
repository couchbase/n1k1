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

// In-memory secondary index. A mmap-free alternative to the bbolt backend
// (idx_si.go), so secondary indexes work in the WASM/browser build (which
// excludes bbolt) and, as an opt-in (SecondaryIndexMode="mem"), in the native
// build too. See web/DESIGN.md.
//
// It reuses everything storage-independent from the bbolt path: the catalog
// definitions (idx_si_catalog.go), the order-preserving key encoding and the
// span-scan comparison logic (idx_si_encode.go), and the index interface
// the engine dispatches on (idx_native.go). Only the backing store differs: a
// sorted []entry built by scanning the keyspace once, binary-searched per span
// instead of a bbolt B+tree cursor. It rebuilds when the source signature
// changes (same freshness model as bbolt), cached process-wide so repeated
// planning/queries don't re-scan.

import (
	"bytes"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/records"
)

// maybeMemIndexes wraps ds so keyspaces with catalog-declared (non-FTS) indexes
// advertise in-memory secondary indexes to the planner. Mirrors
// maybeSecondaryIndexes (idx_si.go) but with the mem backend; it's the only
// secondary-index path in the WASM build (see idx_wasm.go) and the opt-in
// SecondaryIndexMode="mem" path natively.
func maybeMemIndexes(dataRoot string, ds datastore.Datastore) (datastore.Datastore, error) {
	cat, err := loadCatalog(dataRoot)
	if err != nil {
		return ds, err
	}
	if cat == nil || len(cat.Indexes) == 0 {
		return ds, nil
	}
	return &memDatastore{Datastore: ds, root: dataRoot, cat: cat}, nil
}

// --------------------------------------------------------------- datastore wrap

type memDatastore struct {
	datastore.Datastore
	root string
	cat  *catalog
}

func (d *memDatastore) NamespaceById(id string) (datastore.Namespace, errors.Error) {
	ns, err := d.Datastore.NamespaceById(id)
	if err != nil {
		return ns, err
	}
	return &memNamespace{Namespace: ns, mds: d}, nil
}

func (d *memDatastore) NamespaceByName(name string) (datastore.Namespace, errors.Error) {
	ns, err := d.Datastore.NamespaceByName(name)
	if err != nil {
		return ns, err
	}
	return &memNamespace{Namespace: ns, mds: d}, nil
}

type memNamespace struct {
	datastore.Namespace
	mds *memDatastore
}

func (p *memNamespace) Datastore() datastore.Datastore { return p.mds }

func (p *memNamespace) KeyspaceById(id string) (datastore.Keyspace, errors.Error) {
	ks, err := p.Namespace.KeyspaceById(id)
	if err != nil {
		return ks, err
	}
	return p.wrap(ks), nil
}

func (p *memNamespace) KeyspaceByName(name string) (datastore.Keyspace, errors.Error) {
	ks, err := p.Namespace.KeyspaceByName(name)
	if err != nil {
		return ks, err
	}
	return p.wrap(ks), nil
}

func (p *memNamespace) wrap(ks datastore.Keyspace) datastore.Keyspace {
	// Only non-FTS defs -- the mem backend does range/equality GSI, not FTS.
	var defs []*indexDef
	for _, d := range p.mds.cat.indexesFor(p.Name(), ks.Name()) {
		if !d.isFTS() {
			defs = append(defs, d)
		}
	}
	if len(defs) == 0 {
		return ks
	}
	return &memKeyspace{Keyspace: ks, mds: p.mds, defs: defs}
}

// ----------------------------------------------------------------- keyspace

type memKeyspace struct {
	datastore.Keyspace
	mds  *memDatastore
	defs []*indexDef

	once sync.Once
	ix   *memIndexer
}

func (k *memKeyspace) indexer() *memIndexer {
	k.once.Do(func() {
		ix := &memIndexer{ks: k}
		for _, def := range k.defs {
			mi, err := openMemIndex(k, def)
			if err != nil {
				// Don't fail the query -- just don't advertise this index, so the
				// planner falls back to a primary scan.
				continue
			}
			ix.indexes = append(ix.indexes, mi)
		}
		k.ix = ix
	})
	return k.ix
}

func (k *memKeyspace) Indexers() ([]datastore.Indexer, errors.Error) {
	base, err := k.Keyspace.Indexers()
	if err != nil {
		return base, err
	}
	return append(base, k.indexer()), nil
}

func (k *memKeyspace) Indexer(name datastore.IndexType) (datastore.Indexer, errors.Error) {
	if name == datastore.GSI {
		return k.indexer(), nil
	}
	return k.Keyspace.Indexer(name)
}

// ------------------------------------------------------------------- indexer

// memIndexer is a minimal read-only datastore.Indexer of type GSI advertising the
// keyspace's in-memory indexes. Mutating methods are no-ops/errors: definitions
// live in the catalog, not DDL.
type memIndexer struct {
	ks      *memKeyspace
	indexes []*memIndex
}

func (ix *memIndexer) BucketId() string          { return "" }
func (ix *memIndexer) ScopeId() string           { return "" }
func (ix *memIndexer) KeyspaceId() string        { return ix.ks.Id() }
func (ix *memIndexer) Name() datastore.IndexType { return datastore.GSI }

func (ix *memIndexer) IndexIds() ([]string, errors.Error) {
	rv := make([]string, len(ix.indexes))
	for i, mi := range ix.indexes {
		rv[i] = mi.Id()
	}
	return rv, nil
}

func (ix *memIndexer) IndexNames() ([]string, errors.Error) {
	rv := make([]string, len(ix.indexes))
	for i, mi := range ix.indexes {
		rv[i] = mi.Name()
	}
	return rv, nil
}

func (ix *memIndexer) IndexById(id string) (datastore.Index, errors.Error) {
	return ix.IndexByName(id)
}

func (ix *memIndexer) IndexByName(name string) (datastore.Index, errors.Error) {
	for _, mi := range ix.indexes {
		if mi.Name() == name {
			return mi, nil
		}
	}
	return nil, errors.NewError(nil, "mem index: no index "+name)
}

func (ix *memIndexer) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) { return nil, nil }

func (ix *memIndexer) Indexes() ([]datastore.Index, errors.Error) {
	rv := make([]datastore.Index, len(ix.indexes))
	for i, mi := range ix.indexes {
		rv[i] = mi
	}
	return rv, nil
}

func (ix *memIndexer) CreatePrimaryIndex(requestId, name string, with value.Value) (
	datastore.PrimaryIndex, errors.Error) {
	return nil, errors.NewError(nil, "mem index: CreatePrimaryIndex not supported")
}

func (ix *memIndexer) CreateIndex(requestId, name string, seekKey, rangeKey expression.Expressions,
	where expression.Expression, with value.Value) (datastore.Index, errors.Error) {
	return nil, errors.NewError(nil, "mem index: CREATE INDEX not supported (define in .n1k1/catalog.json)")
}

func (ix *memIndexer) BuildIndexes(requestId string, names ...string) errors.Error       { return nil }
func (ix *memIndexer) Refresh() errors.Error                                             { return nil }
func (ix *memIndexer) MetadataVersion() uint64                                           { return 0 }
func (ix *memIndexer) SetLogLevel(level logging.Level)                                   {}
func (ix *memIndexer) SetConnectionSecurityConfig(c *datastore.ConnectionSecurityConfig) {}

// --------------------------------------------------------------- the index

// memIndex is an in-memory datastore.Index: a sorted slice of encoded
// key+docID entries, scanned by binary search. Implements index.
type memIndex struct {
	ks      *memKeyspace
	def     *indexDef
	entries [][]byte // each = encodeValue(keys...) ++ docID, sorted by bytes.Compare
}

var _ index = (*memIndex)(nil)

func (mi *memIndex) indexDefn() *indexDef             { return mi.def }
func (mi *memIndex) KeyspaceId() string               { return mi.ks.Id() }
func (mi *memIndex) Id() string                       { return mi.def.Name }
func (mi *memIndex) Name() string                     { return mi.def.Name }
func (mi *memIndex) Type() datastore.IndexType        { return datastore.GSI }
func (mi *memIndex) Indexer() datastore.Indexer       { return mi.ks.indexer() }
func (mi *memIndex) SeekKey() expression.Expressions  { return nil }
func (mi *memIndex) RangeKey() expression.Expressions { return mi.def.rangeKey }
func (mi *memIndex) Condition() expression.Expression { return mi.def.condition }
func (mi *memIndex) IsPrimary() bool                  { return false }

func (mi *memIndex) State() (datastore.IndexState, string, errors.Error) {
	return datastore.ONLINE, "", nil
}

func (mi *memIndex) Statistics(requestId string, span *datastore.Span) (
	datastore.Statistics, errors.Error) {
	return nil, nil // safe while useCBO=false
}

func (mi *memIndex) Drop(requestId string) errors.Error {
	return errors.NewError(nil, "mem index: DROP INDEX not supported (edit .n1k1/catalog.json)")
}

// Scan satisfies datastore.Index for a single span, closing the sender when done
// (n1k1's own DatastoreScanIndex drives multi-span scans via scanSpan below).
func (mi *memIndex) Scan(requestId string, span *datastore.Span, distinct bool,
	limit int64, cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	defer conn.Sender().Close()
	mi.scanSpan(span, limit, nil, false, conn)
}

// scanSpan binary-searches to the span's low bound then walks the sorted entries
// in N1QL collation order, applying the same boundary/inclusion rules as the
// bbolt path (idx_si.go's scanSpan). Does NOT close the sender.
func (mi *memIndex) scanSpan(span *datastore.Span, limit int64,
	seen map[string]bool, projectKeys bool, conn *datastore.IndexConnection) {
	if limit <= 0 {
		limit = int64(1) << 62
	}

	n := len(mi.def.rangeKey)
	lowEnc := encodeSeq(span.Range.Low)
	highEnc := encodeSeq(span.Range.High)
	incl := span.Range.Inclusion

	// First entry whose bytes are >= the low bound (empty low => from the start).
	start := 0
	if len(lowEnc) > 0 {
		start = sort.Search(len(mi.entries), func(i int) bool {
			return bytes.Compare(mi.entries[i], lowEnc) >= 0
		})
	}

	var sent int64
	for i := start; i < len(mi.entries) && sent < limit; i++ {
		k := mi.entries[i]
		compEnds, docID, ok := splitKey(k, n)
		if !ok {
			continue // malformed -- skip defensively
		}

		if len(span.Range.Low) > 0 {
			p := k[:compEnds[len(span.Range.Low)-1]]
			c := bytes.Compare(p, lowEnc)
			if c < 0 {
				continue
			}
			if c == 0 && incl&datastore.LOW == 0 {
				continue // exclusive low
			}
		}

		if len(span.Range.High) > 0 {
			p := k[:compEnds[len(span.Range.High)-1]]
			c := bytes.Compare(p, highEnc)
			if c > 0 {
				break // past high; ordered walk -> done
			}
			if c == 0 && incl&datastore.HIGH == 0 {
				continue // exclusive high (later entries may still qualify)
			}
		}

		if seen != nil {
			if seen[string(docID)] {
				continue
			}
			seen[string(docID)] = true
		}

		entry := &datastore.IndexEntry{PrimaryKey: string(docID)}
		if projectKeys {
			entry.EntryKey = decodeKeyComponents(k, compEnds)
		}
		if !conn.Sender().SendEntry(entry) {
			break
		}
		sent++
	}
}

// ------------------------------------------------------------------- build

// memIndexCache caches built in-memory indexes process-wide, keyed by
// root|namespace|keyspace|defHash, so repeated planning/queries reuse a built
// index; a changed source signature triggers a rebuild.
var memIndexCache = struct {
	sync.Mutex
	m map[string]*memSlot
}{m: map[string]*memSlot{}}

type memSlot struct {
	mu  sync.Mutex
	mi  *memIndex
	sig string
}

func memSlotFor(key string) *memSlot {
	memIndexCache.Lock()
	defer memIndexCache.Unlock()
	s := memIndexCache.m[key]
	if s == nil {
		s = &memSlot{}
		memIndexCache.m[key] = s
	}
	return s
}

// openMemIndex returns a built, ready-to-scan in-memory index for def on ks,
// rebuilding (once, cached) when the keyspace source changed.
func openMemIndex(ks *memKeyspace, def *indexDef) (*memIndex, error) {
	ns := ks.Namespace().Name()
	srcDir := filepath.Join(ks.mds.root, ns, ks.Name())
	key := ks.mds.root + "|" + ns + "|" + ks.Name() + "|" + def.defHash()

	slot := memSlotFor(key)
	slot.mu.Lock()
	defer slot.mu.Unlock()

	sig, err := sourceSignature(srcDir)
	if err != nil {
		return nil, err
	}
	if slot.mi != nil && slot.sig == sig {
		slot.mi.ks = ks // re-home to the current keyspace wrapper
		return slot.mi, nil
	}

	entries, err := buildMemEntries(srcDir, def)
	if err != nil {
		return nil, err
	}
	slot.mi = &memIndex{ks: ks, def: def, entries: entries}
	slot.sig = sig
	return slot.mi, nil
}

// buildMemEntries scans the keyspace's record files, evaluates the key/where
// expressions per doc, and returns the sorted encode(keys)+docID entries -- the
// in-memory analogue of idx_si.go's buildIndex.
func buildMemEntries(srcDir string, def *indexDef) ([][]byte, error) {
	ctx := NewGlueContext(time.Now())

	opts := ScanWalkOptions
	opts.PathPrefix = ""
	src, err := records.Walk(srcDir, opts)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	var entries [][]byte
	var rec records.Record
	for {
		ok, err := src.Next(&rec)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		doc := value.NewValue(append([]byte(nil), rec.Doc...))

		if def.condition != nil {
			cv, err := def.condition.Evaluate(doc, ctx)
			if err != nil {
				return nil, err
			}
			if !cv.Truth() {
				continue
			}
		}

		var keyBuf []byte
		skip := false
		for _, ke := range def.rangeKey {
			kv, err := ke.Evaluate(doc, ctx)
			if err != nil {
				return nil, err
			}
			if kv.Type() == value.MISSING {
				skip = true // missing leading key -> not indexed (GSI semantics)
				break
			}
			keyBuf = encodeValue(keyBuf, kv)
		}
		if skip {
			continue
		}
		keyBuf = append(keyBuf, rec.ID...)
		entries = append(entries, keyBuf)
	}

	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i], entries[j]) < 0
	})
	return entries, nil
}
