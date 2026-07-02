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

// Phase 1 GSI: a bbolt-backed secondary index, advertised to the cbq planner so
// selective queries use an IndexScan instead of a full primary scan. See
// DESIGN-indexing.md.
//
// The whole feature is n1k1-side with ZERO fork edits (mirroring the data-source
// work): the cbq planner collects candidate indexes by iterating every indexer
// from keyspace.Indexers() (planner/build_scan.go:allIndexes), so we advertise a
// GSI index simply by *wrapping* the file datastore's namespaces/keyspaces to
// append a siIndexer -- no SecondaryIndexes fork seam needed. The read path is
// already complete: conv's VisitIndexScan -> datastore-scan-index ->
// DatastoreScanIndex -> secondaryIndex.Scan yields docIDs, and the following
// Fetch uses the (embedded, real) keyspace's Fetch to read the docs.
//
// Definitions come from .n1k1/catalog.json (si_catalog.go). Freshness is the
// simple, static-data model the user asked for: the built bbolt file records a
// signature of the source directory (file count + newest mtime); on open we
// rebuild only if that signature changed. No fingerprint manifest yet -- if a
// query needs stronger freshness it can delete the .n1k1 artifact to force a
// rebuild.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/records"
)

const (
	siEntriesBucket = "entries" // encode(keys)+docID -> nil
	siMetaBucket    = "meta"     // "sig" -> source signature
	siSigKey        = "sig"
)

// maybeSecondaryIndexes wraps ds so keyspaces advertise the catalog's secondary indexes. It
// returns ds unchanged when there's no .n1k1/catalog.json or it defines no
// indexes (the common case -- no metadata, behave exactly as before).
func maybeSecondaryIndexes(dataRoot string, ds datastore.Datastore) (datastore.Datastore, error) {
	cat, err := loadCatalog(dataRoot)
	if err != nil {
		return ds, err
	}
	if cat == nil || len(cat.Indexes) == 0 {
		return ds, nil
	}
	return &siDatastore{Datastore: ds, root: dataRoot, cat: cat}, nil
}

// ------------------------------------------------------ datastore/ns wrappers

type siDatastore struct {
	datastore.Datastore
	root string
	cat  *catalog
}

func (d *siDatastore) NamespaceById(id string) (datastore.Namespace, errors.Error) {
	ns, err := d.Datastore.NamespaceById(id)
	if err != nil {
		return ns, err
	}
	return &siNamespace{Namespace: ns, sds: d}, nil
}

func (d *siDatastore) NamespaceByName(name string) (datastore.Namespace, errors.Error) {
	ns, err := d.Datastore.NamespaceByName(name)
	if err != nil {
		return ns, err
	}
	return &siNamespace{Namespace: ns, sds: d}, nil
}

type siNamespace struct {
	datastore.Namespace
	sds *siDatastore
}

func (p *siNamespace) Datastore() datastore.Datastore { return p.sds }

func (p *siNamespace) KeyspaceById(id string) (datastore.Keyspace, errors.Error) {
	ks, err := p.Namespace.KeyspaceById(id)
	if err != nil {
		return ks, err
	}
	return p.wrap(ks), nil
}

func (p *siNamespace) KeyspaceByName(name string) (datastore.Keyspace, errors.Error) {
	ks, err := p.Namespace.KeyspaceByName(name)
	if err != nil {
		return ks, err
	}
	return p.wrap(ks), nil
}

func (p *siNamespace) wrap(ks datastore.Keyspace) datastore.Keyspace {
	defs := p.sds.cat.indexesFor(p.Name(), ks.Name())
	if len(defs) == 0 {
		return ks // no indexes for this keyspace -- don't wrap.
	}
	return &siKeyspace{Keyspace: ks, sds: p.sds, defs: defs}
}

// ------------------------------------------------------------------ keyspace

type siKeyspace struct {
	datastore.Keyspace
	sds  *siDatastore
	defs []*indexDef

	once    sync.Once
	indexer *siIndexer
}

func (k *siKeyspace) secondaryIndexer() *siIndexer {
	k.once.Do(func() {
		ix := &siIndexer{ks: k}
		for _, def := range k.defs {
			si, err := openSecondaryIndex(k, def)
			if err != nil {
				// Don't fail the query -- just don't advertise this index, so the
				// planner falls back to a primary scan. Surface why on stderr.
				logging.Errorf("secondary index: index %q on %s:%s unavailable: %v",
					def.Name, k.Namespace().Name(), k.Name(), err)
				continue
			}
			ix.indexes = append(ix.indexes, si)
		}
		k.indexer = ix
	})
	return k.indexer
}

func (k *siKeyspace) Indexers() ([]datastore.Indexer, errors.Error) {
	base, err := k.Keyspace.Indexers()
	if err != nil {
		return base, err
	}
	return append(base, k.secondaryIndexer()), nil
}

func (k *siKeyspace) Indexer(name datastore.IndexType) (datastore.Indexer, errors.Error) {
	if name == datastore.GSI {
		return k.secondaryIndexer(), nil
	}
	return k.Keyspace.Indexer(name)
}

// ------------------------------------------------------------------- indexer

// siIndexer is a minimal read-only datastore.Indexer of type GSI advertising
// the keyspace's built secondary indexes. Mutating methods are no-ops/errors:
// definitions are declared in the catalog, not via DDL.
type siIndexer struct {
	ks      *siKeyspace
	indexes []*secondaryIndex
}

func (ix *siIndexer) BucketId() string          { return "" }
func (ix *siIndexer) ScopeId() string           { return "" }
func (ix *siIndexer) KeyspaceId() string         { return ix.ks.Id() }
func (ix *siIndexer) Name() datastore.IndexType { return datastore.GSI }

func (ix *siIndexer) IndexIds() ([]string, errors.Error) {
	rv := make([]string, len(ix.indexes))
	for i, si := range ix.indexes {
		rv[i] = si.Id()
	}
	return rv, nil
}

func (ix *siIndexer) IndexNames() ([]string, errors.Error) {
	rv := make([]string, len(ix.indexes))
	for i, si := range ix.indexes {
		rv[i] = si.Name()
	}
	return rv, nil
}

func (ix *siIndexer) IndexById(id string) (datastore.Index, errors.Error) {
	return ix.IndexByName(id)
}

func (ix *siIndexer) IndexByName(name string) (datastore.Index, errors.Error) {
	for _, si := range ix.indexes {
		if si.Name() == name {
			return si, nil
		}
	}
	return nil, errors.NewError(nil, "secondary index: no index "+name)
}

func (ix *siIndexer) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) {
	return nil, nil
}

func (ix *siIndexer) Indexes() ([]datastore.Index, errors.Error) {
	rv := make([]datastore.Index, len(ix.indexes))
	for i, si := range ix.indexes {
		rv[i] = si
	}
	return rv, nil
}

func (ix *siIndexer) CreatePrimaryIndex(requestId, name string, with value.Value) (
	datastore.PrimaryIndex, errors.Error) {
	return nil, errors.NewError(nil, "secondary index: CreatePrimaryIndex not supported")
}

func (ix *siIndexer) CreateIndex(requestId, name string, seekKey, rangeKey expression.Expressions,
	where expression.Expression, with value.Value) (datastore.Index, errors.Error) {
	return nil, errors.NewError(nil, "secondary index: CREATE INDEX not supported (define in .n1k1/catalog.json)")
}

func (ix *siIndexer) BuildIndexes(requestId string, names ...string) errors.Error { return nil }
func (ix *siIndexer) Refresh() errors.Error                                        { return nil }
func (ix *siIndexer) MetadataVersion() uint64                                      { return 0 }
func (ix *siIndexer) SetLogLevel(level logging.Level)                              {}
func (ix *siIndexer) SetConnectionSecurityConfig(c *datastore.ConnectionSecurityConfig) {}

// --------------------------------------------------------------- the index

// secondaryIndex is a bbolt-backed datastore.Index (base interface only, so the
// planner emits plan.IndexScan, which conv already converts).
type secondaryIndex struct {
	ks  *siKeyspace
	def *indexDef
	db  *bolt.DB
}

func (si *secondaryIndex) KeyspaceId() string                  { return si.ks.Id() }
func (si *secondaryIndex) Id() string                          { return si.def.Name }
func (si *secondaryIndex) Name() string                        { return si.def.Name }
func (si *secondaryIndex) Type() datastore.IndexType           { return datastore.GSI }
func (si *secondaryIndex) Indexer() datastore.Indexer          { return si.ks.secondaryIndexer() }
func (si *secondaryIndex) SeekKey() expression.Expressions     { return nil }
func (si *secondaryIndex) RangeKey() expression.Expressions    { return si.def.rangeKey }
func (si *secondaryIndex) Condition() expression.Expression    { return si.def.condition }
func (si *secondaryIndex) IsPrimary() bool                     { return false }

func (si *secondaryIndex) State() (datastore.IndexState, string, errors.Error) {
	return datastore.ONLINE, "", nil
}

func (si *secondaryIndex) Statistics(requestId string, span *datastore.Span) (
	datastore.Statistics, errors.Error) {
	return nil, nil // safe while useCBO=false
}

func (si *secondaryIndex) Drop(requestId string) errors.Error {
	return errors.NewError(nil, "secondary index: DROP INDEX not supported (edit .n1k1/catalog.json)")
}

// Scan satisfies datastore.Index for a single span, closing the sender when
// done. n1k1's own DatastoreScanIndex instead drives multi-span scans via
// scanSpan (below) so it can share one sender across spans; this method covers
// the single-span interface contract and any other caller.
func (si *secondaryIndex) Scan(requestId string, span *datastore.Span, distinct bool,
	limit int64, cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	defer conn.Sender().Close()
	si.scanSpan(span, limit, nil, conn)
}

// scanSpan walks the bbolt B+tree in N1QL collation order (guaranteed by the
// order-preserving key encoding, si_encode.go), seeking to the span's low bound
// and emitting each in-range entry's docID. Because the encoding is
// order-preserving for scalars, a real seek prunes the walk; boundary inclusion
// is applied exactly by comparing the encoded key prefixes. It does NOT close
// the sender (the caller owns that, so several spans can share one connection),
// and skips docIDs already in seen (dedup across a multi-span scan; pass nil to
// disable).
func (si *secondaryIndex) scanSpan(span *datastore.Span, limit int64,
	seen map[string]bool, conn *datastore.IndexConnection) {
	if limit <= 0 {
		limit = int64(1) << 62
	}

	n := len(si.def.rangeKey)
	lowEnc := encodeSeq(span.Range.Low)
	highEnc := encodeSeq(span.Range.High)
	incl := span.Range.Inclusion

	err := si.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(siEntriesBucket))
		if b == nil {
			return nil
		}
		cur := b.Cursor()

		var k []byte
		if len(lowEnc) > 0 {
			k, _ = cur.Seek(lowEnc)
		} else {
			k, _ = cur.First()
		}

		var sent int64
		for ; k != nil && sent < limit; k, _ = cur.Next() {
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

			if !conn.Sender().SendEntry(&datastore.IndexEntry{PrimaryKey: string(docID)}) {
				break
			}
			sent++
		}
		return nil
	})
	if err != nil {
		conn.Error(errors.NewError(err, "secondary-index scan"))
	}
}

// encodeSeq encodes a sequence of bound values (one per sarged key) into a
// single comparable byte prefix.
func encodeSeq(vals value.Values) []byte {
	if len(vals) == 0 {
		return nil
	}
	var out []byte
	for _, v := range vals {
		out = encodeValue(out, v)
	}
	return out
}

// ------------------------------------------------------------------- build

// openIndexes caches opened secondary indexes by bbolt path across the process.
// A bbolt file takes an exclusive OS lock, so a fresh Store (each carries its own
// keyspace wrappers) re-opening the same index file would block on the lock; and
// re-opening per plan would be wasteful. n1k1 is a single-process CLI, so a
// process-global cache (matching the engine.ExecOpEx global style) is the natural
// fit -- the design's "cache per keyspace so repeated planning doesn't reopen
// bbolt". A cache hit re-validates freshness and rebuilds in place if the source
// changed.
var openIndexes = struct {
	sync.Mutex
	m map[string]*secondaryIndex
}{m: map[string]*secondaryIndex{}}

// openSecondaryIndex opens (building/rebuilding as needed) the bbolt file backing
// def on ks, and returns the ready-to-scan index. Repeated calls for the same
// on-disk index return the cached instance (re-checking freshness).
func openSecondaryIndex(ks *siKeyspace, def *indexDef) (*secondaryIndex, error) {
	ns := ks.Namespace().Name()
	instDir := filepath.Join(ks.sds.root, sidecarDir, ns, ks.Name(), "idx",
		fmt.Sprintf("%s__si__%s", fsSafe(def.Name), def.defHash()))
	dbPath := filepath.Join(instDir, "data.bolt")

	srcDir := filepath.Join(ks.sds.root, ns, ks.Name())
	sig, err := sourceSignature(srcDir)
	if err != nil {
		return nil, err
	}

	openIndexes.Lock()
	defer openIndexes.Unlock()

	// Cache hit: reuse the open DB, rebuilding the entries in place if the source
	// changed since it was built.
	if si, ok := openIndexes.m[dbPath]; ok {
		fresh, err := indexFresh(si.db, sig)
		if err != nil {
			return nil, err
		}
		if !fresh {
			if err := buildIndex(si.db, ks, def, srcDir, sig); err != nil {
				return nil, err
			}
		}
		// Re-home to the current keyspace wrapper (Indexer()/Fetch route through it).
		si.ks = ks
		return si, nil
	}

	if err := os.MkdirAll(instDir, 0o755); err != nil {
		return nil, err
	}

	db, err := bolt.Open(dbPath, 0o644, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}

	fresh, err := indexFresh(db, sig)
	if err != nil {
		db.Close()
		return nil, err
	}
	if !fresh {
		if err := buildIndex(db, ks, def, srcDir, sig); err != nil {
			db.Close()
			return nil, err
		}
	}

	si := &secondaryIndex{ks: ks, def: def, db: db}
	openIndexes.m[dbPath] = si
	return si, nil
}

// buildIndex (re)populates the bbolt entries bucket by scanning the keyspace's
// files n1k1-native (records: union of files, recurse, decode, gzip),
// evaluating the key/where expressions per doc, and inserting
// encode(keyValues)+docID. v1 rebuilds the whole index in one transaction.
func buildIndex(db *bolt.DB, ks *siKeyspace, def *indexDef, srcDir, sig string) error {
	// Evaluate expressions with a lightweight context (build time -- Now() is the
	// only context method a simple field/scalar expression might touch).
	ctx := NewGlueContext(time.Now())

	opts := ScanWalkOptions
	opts.PathPrefix = ""
	src, err := records.Walk(srcDir, opts)
	if err != nil {
		return fmt.Errorf("secondary-index build, walk %q: %w", srcDir, err)
	}
	defer src.Close()

	return db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte(siEntriesBucket)); err != nil &&
			err != bolt.ErrBucketNotFound {
			return err
		}
		b, err := tx.CreateBucket([]byte(siEntriesBucket))
		if err != nil {
			return err
		}

		var rec records.Record
		var keyBuf []byte
		for {
			ok, err := src.Next(&rec)
			if err != nil {
				return fmt.Errorf("secondary-index build, next: %w", err)
			}
			if !ok {
				break
			}

			doc := value.NewValue(append([]byte(nil), rec.Doc...))

			// Partial-index condition: skip docs that don't satisfy WHERE.
			if def.condition != nil {
				cv, err := def.condition.Evaluate(doc, ctx)
				if err != nil {
					return fmt.Errorf("secondary-index build, where eval: %w", err)
				}
				if !cv.Truth() {
					continue
				}
			}

			keyBuf = keyBuf[:0]
			skip := false
			for _, ke := range def.rangeKey {
				kv, err := ke.Evaluate(doc, ctx)
				if err != nil {
					return fmt.Errorf("secondary-index build, key eval: %w", err)
				}
				// A MISSING leading key means the doc isn't in the index (matches
				// GSI semantics: missing leading key -> not indexed).
				if kv.Type() == value.MISSING {
					skip = true
					break
				}
				keyBuf = encodeValue(keyBuf, kv)
			}
			if skip {
				continue
			}
			keyBuf = append(keyBuf, rec.ID...)

			if err := b.Put(append([]byte(nil), keyBuf...), nil); err != nil {
				return err
			}
		}

		// Record the source signature so the next open can skip a rebuild.
		mb, err := tx.CreateBucketIfNotExists([]byte(siMetaBucket))
		if err != nil {
			return err
		}
		return mb.Put([]byte(siSigKey), []byte(sig))
	})
}

// indexFresh reports whether the built index's stored signature matches the
// current source signature (so no rebuild is needed).
func indexFresh(db *bolt.DB, sig string) (bool, error) {
	var fresh bool
	err := db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte(siMetaBucket))
		eb := tx.Bucket([]byte(siEntriesBucket))
		if mb == nil || eb == nil {
			return nil
		}
		fresh = string(mb.Get([]byte(siSigKey))) == sig
		return nil
	})
	return fresh, err
}

// sourceSignature summarizes a keyspace directory for change detection: file
// count and the newest mtime (nanoseconds) over the whole tree. This is the
// simple "assume static data, validate by timestamp" model the user asked for --
// adding/removing/touching any file changes the signature and forces a rebuild.
func sourceSignature(dir string) (string, error) {
	var count int64
	var newest int64
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Don't descend into the sidecar itself if it's ever nested here.
			if d.Name() == sidecarDir {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		count++
		if mt := info.ModTime().UnixNano(); mt > newest {
			newest = mt
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(count))
	binary.BigEndian.PutUint64(b[8:16], uint64(newest))
	return fmt.Sprintf("%x", b), nil
}

// fsSafe sanitizes an index name for use as a directory segment.
func fsSafe(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			return r
		default:
			return '_'
		}
	}, name)
}
