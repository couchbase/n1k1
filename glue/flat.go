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

// Flat discovery (DESIGN-data.md scenarios B, B2, B3): when a datastore root holds
// record files *directly* -- or the CLI arg is a single record file, or a grab-bag
// directory (loose files alongside unrelated subdirs, e.g. ~/Desktop) -- n1k1
// "fakes" the metadata so the cbq planner accepts `FROM <keyspace>`. It exposes a
// synthetic "default" namespace whose keyspaces exist only as planner-facing
// metadata (no physical <namespace>/<keyspace> dir); each advertises a primary
// index so the planner emits a PrimaryScan, and n1k1's records-scan then reads the
// backing directory (RecordsDir) or file (RecordsFile). Entirely n1k1-side (no fork
// change), reusing the fork's datastore/virtual metadata-only building blocks.

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/virtual"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/timestamp"

	"github.com/couchbase/n1k1/records"
)

const flatRootNamespace = "default"

// IsFlatDatastore reports whether ds is a synthetic flat / grab-bag / single-file
// datastore (from maybeFlat / maybeFlatFile). Secondary indexes aren't supported
// on these layouts -- they need the classic <namespace>/<keyspace> directory tree
// -- so the CLI uses this to refuse .index create/etc. honestly rather than
// silently no-op (buildIndexesConcurrent skips a non-siDatastore).
func IsFlatDatastore(ds datastore.Datastore) bool {
	_, ok := ds.(*flatDatastore)
	return ok
}

// maybeFlat wraps ds so a directory's loose top-level record files are queryable,
// covering two directory shapes:
//
//   - Pure flat root (record files directly under path, NO subdirs; scenario B):
//     one synthetic keyspace named after the directory basename = the union of
//     those files.
//   - Grab-bag dir (record files at the top AND subdirs, e.g. ~/Desktop;
//     scenario B3): each top-level record file becomes its own keyspace by stem,
//     so `FROM <stem>` queries that one file. The union-by-basename is skipped
//     here because records.Walk would recurse into the unrelated subdirs.
//
// A real "default" namespace's keyspaces (the classic <ns>/<keyspace> layout) are
// merged in, and other real namespaces still resolve, so this only ADDS keyspaces.
// Returns ds unchanged when the directory has no top-level record files.
func maybeFlat(path string, ds datastore.Datastore) datastore.Datastore {
	abs, err := filepath.Abs(path)
	if err != nil {
		return ds
	}
	files, hasSubdir := topLevelRecordFiles(abs)
	if len(files) == 0 {
		return ds
	}

	keyspaces := map[string]*flatKeyspace{}
	if !hasSubdir {
		// Scenario B: union of all files, keyed by the directory basename.
		base := filepath.Base(filepath.Clean(abs))
		if base != "" && base != "." && base != string(filepath.Separator) {
			keyspaces[base] = &flatKeyspace{dir: abs}
		}
	} else {
		// Scenario B3: one keyspace per top-level *structured* file, by stem
		// (first-seen wins on a stem collision, e.g. a.json + a.csv). Extracted
		// documents (PDF/DOCX/XLSX) are skipped so a folder of documents doesn't
		// flood the keyspace list -- query one explicitly with `n1k1 <file.pdf>`.
		for _, name := range files {
			if !records.IsStructuredFile(name) {
				continue
			}
			ks := records.Stem(name)
			if ks == "" || ks == "." {
				continue
			}
			if _, dup := keyspaces[ks]; dup {
				continue
			}
			keyspaces[ks] = &flatKeyspace{file: filepath.Join(abs, name)}
		}
	}

	// Merge with a real "default" namespace (classic layout) if one exists.
	var real datastore.Namespace
	if rd, rerr := ds.NamespaceByName(flatRootNamespace); rerr == nil {
		real = rd
	}
	return wrapFlatKeyspaces(ds, keyspaces, real)
}

// maybeFlatFile wraps ds with a synthetic default:<stem> keyspace for a single
// record-file CLI arg (scenario B2): the arg is one JSONL/NDJSON/JSON/CSV/... file
// (optionally .gz), keyed by its base name minus format/compression extensions
// (events.jsonl -> events, orders.jsonl.gz -> orders). Its RecordsFile points the
// records-scan at just that file. Returns ds unchanged if path isn't a record file.
func maybeFlatFile(path string, ds datastore.Datastore) datastore.Datastore {
	if !records.IsRecordFile(path) {
		return ds
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return ds
	}
	name := records.Stem(abs)
	if name == "" || name == "." {
		return ds
	}
	return wrapFlatKeyspaces(ds, map[string]*flatKeyspace{name: {file: abs}}, nil)
}

// wrapFlatKeyspaces builds a synthetic "default" namespace exposing keyspaces (each
// given its own primary-index indexer), merged over an optional real "default"
// namespace. Returns ds unchanged if no keyspace can be constructed.
func wrapFlatKeyspaces(ds datastore.Datastore, keyspaces map[string]*flatKeyspace,
	real datastore.Namespace) datastore.Datastore {
	if len(keyspaces) == 0 {
		return ds
	}
	w := &flatDatastore{Datastore: ds}
	ns := &flatNamespace{datastore: w, keyspaces: map[string]*flatKeyspace{}, real: real}
	for name, ks := range keyspaces {
		vks, verr := virtual.NewVirtualKeyspace(ns, []string{flatRootNamespace, name})
		if verr != nil {
			continue
		}
		ks.Keyspace = vks
		ks.indexer = newFlatIndexer(ks)
		ns.keyspaces[name] = ks
	}
	if len(ns.keyspaces) == 0 {
		return ds
	}
	w.ns = ns
	return w
}

// topLevelRecordFiles returns the sorted names of decodable record files directly
// under dir (not recursing), and whether dir contains any subdirectory.
func topLevelRecordFiles(dir string) (files []string, hasSubdir bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false
	}
	for _, e := range entries {
		if e.IsDir() {
			hasSubdir = true
		} else if records.IsRecordFile(e.Name()) {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files, hasSubdir
}

// --------------------------------------------------------- datastore wrapper

// flatDatastore embeds the real datastore (promoting its ~40 methods) and exposes
// a synthetic "default" namespace on top, delegating other namespaces to the real
// datastore so nothing is hidden.
type flatDatastore struct {
	datastore.Datastore
	ns *flatNamespace
}

func (d *flatDatastore) NamespaceIds() ([]string, errors.Error)   { return d.namespaceNames() }
func (d *flatDatastore) NamespaceNames() ([]string, errors.Error) { return d.namespaceNames() }

// namespaceNames is the synthetic "default" plus any real namespaces (dedup'd).
func (d *flatDatastore) namespaceNames() ([]string, errors.Error) {
	out := []string{flatRootNamespace}
	if real, err := d.Datastore.NamespaceNames(); err == nil {
		for _, n := range real {
			if !strings.EqualFold(n, flatRootNamespace) {
				out = append(out, n)
			}
		}
	}
	return out, nil
}

func (d *flatDatastore) NamespaceById(id string) (datastore.Namespace, errors.Error) {
	return d.NamespaceByName(id)
}

func (d *flatDatastore) NamespaceByName(name string) (datastore.Namespace, errors.Error) {
	if strings.EqualFold(name, flatRootNamespace) {
		return d.ns, nil
	}
	return d.Datastore.NamespaceByName(name)
}

// --------------------------------------------------------- namespace

// flatNamespace is the synthetic "default" namespace. It holds the synthetic
// keyspaces and, when the real datastore also has a "default" namespace (the
// classic <ns>/<keyspace> layout), merges/delegates to it.
type flatNamespace struct {
	datastore *flatDatastore
	keyspaces map[string]*flatKeyspace
	real      datastore.Namespace // optional real "default" to merge + delegate to
}

func (p *flatNamespace) Datastore() datastore.Datastore { return p.datastore }
func (p *flatNamespace) Id() string                     { return flatRootNamespace }
func (p *flatNamespace) Name() string                   { return flatRootNamespace }

func (p *flatNamespace) KeyspaceIds() ([]string, errors.Error)   { return p.keyspaceNames() }
func (p *flatNamespace) KeyspaceNames() ([]string, errors.Error) { return p.keyspaceNames() }

func (p *flatNamespace) keyspaceNames() ([]string, errors.Error) {
	seen := map[string]bool{}
	var out []string
	for n := range p.keyspaces {
		out = append(out, n)
		seen[strings.ToLower(n)] = true
	}
	if p.real != nil {
		if rn, err := p.real.KeyspaceNames(); err == nil {
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

func (p *flatNamespace) KeyspaceById(id string) (datastore.Keyspace, errors.Error) {
	return p.KeyspaceByName(id)
}

func (p *flatNamespace) KeyspaceByName(name string) (datastore.Keyspace, errors.Error) {
	for n, ks := range p.keyspaces {
		if strings.EqualFold(n, name) {
			return ks, nil
		}
	}
	if p.real != nil {
		return p.real.KeyspaceByName(name)
	}
	return nil, errors.NewError(nil, "flat: no keyspace "+name)
}

func (p *flatNamespace) BucketIds() ([]string, errors.Error)   { return nil, nil }
func (p *flatNamespace) BucketNames() ([]string, errors.Error) { return nil, nil }

func (p *flatNamespace) BucketById(name string) (datastore.Bucket, errors.Error) {
	return nil, errors.NewError(nil, "flat: no buckets")
}
func (p *flatNamespace) BucketByName(name string) (datastore.Bucket, errors.Error) {
	return nil, errors.NewError(nil, "flat: no buckets")
}

func (p *flatNamespace) Objects(creds *auth.Credentials, filter func(string) bool,
	preload bool) ([]datastore.Object, errors.Error) {
	var out []datastore.Object
	for n := range p.keyspaces {
		out = append(out, datastore.Object{Id: n, Name: n, IsKeyspace: true})
	}
	if p.real != nil {
		if ro, err := p.real.Objects(creds, filter, preload); err == nil {
			out = append(out, ro...)
		}
	}
	return out, nil
}

// --------------------------------------------------------- keyspace

// flatKeyspace embeds a metadata-only virtual keyspace (promoting its Keyspace
// methods) and overrides index advertising to expose a primary index, plus
// RecordsDir/RecordsFile so the records-scan reads the flat root directory (or,
// for a per-file keyspace, the one file). Exactly one of dir/file is set.
type flatKeyspace struct {
	datastore.Keyspace
	dir     string // flat root: keyspace = union of files under this dir; also the
	//                 walk base for a glob keyspace
	file    string // single file (scenario B2/B3): keyspace = this one file
	glob    string // glob (Mode 2b): absolute doublestar pattern (base = dir)
	indexer datastore.Indexer
}

// RecordsDir is consulted by DatastoreScanRecords to locate the physical
// directory: for a flat root the keyspace's data lives at the root itself, not
// at <root>/<ns>/<keyspace>. For a glob keyspace it is the walk base (the longest
// metacharacter-free prefix), which makes synthetic IDs base-relative and lets the
// native byte-path fetch read <base>/<relpath>. Empty in single-file mode.
func (k *flatKeyspace) RecordsDir() string { return k.dir }

// RecordsFile, when non-empty, points DatastoreScanRecords at a single record
// file rather than a directory to walk (scenarios B2/B3).
func (k *flatKeyspace) RecordsFile() string { return k.file }

// RecordsGlob, when ok, gives an absolute doublestar pattern to expand INSTEAD of
// walking the whole directory -- how a glob keyspace (Mode 2b) restricts the scan
// to just the pattern's matches. Resolved at scan time (KeyspaceRecordsOpen) so the
// query's -formats lockdown applies and freshly-added files are picked up.
func (k *flatKeyspace) RecordsGlob() (string, bool) { return k.glob, k.glob != "" }

func (k *flatKeyspace) Indexer(name datastore.IndexType) (datastore.Indexer, errors.Error) {
	return k.indexer, nil
}
func (k *flatKeyspace) Indexers() ([]datastore.Indexer, errors.Error) {
	return []datastore.Indexer{k.indexer}, nil
}

// --------------------------------------------------------- indexer + primary

// flatIndexer embeds a virtual indexer (promoting the bulk of the Indexer
// interface) and advertises a single primary index so the planner emits a
// PrimaryScan.
type flatIndexer struct {
	datastore.Indexer
	primary datastore.PrimaryIndex
}

func newFlatIndexer(ks datastore.Keyspace) *flatIndexer {
	vidx := virtual.NewVirtualIndex(ks, "#primary", nil, nil, nil, nil, nil,
		true /* isPrimary */, false, false, -1, "", nil,
		datastore.INDEX_MODE_VIRTUAL, nil)
	return &flatIndexer{
		Indexer: virtual.NewVirtualIndexer([]string{flatRootNamespace}),
		primary: &flatPrimaryIndex{Index: vidx},
	}
}

func (ix *flatIndexer) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) {
	return []datastore.PrimaryIndex{ix.primary}, nil
}
func (ix *flatIndexer) Indexes() ([]datastore.Index, errors.Error) {
	return []datastore.Index{ix.primary}, nil
}
func (ix *flatIndexer) IndexIds() ([]string, errors.Error)   { return []string{ix.primary.Id()}, nil }
func (ix *flatIndexer) IndexNames() ([]string, errors.Error) { return []string{ix.primary.Name()}, nil }

func (ix *flatIndexer) IndexById(id string) (datastore.Index, errors.Error) { return ix.primary, nil }
func (ix *flatIndexer) IndexByName(name string) (datastore.Index, errors.Error) {
	return ix.primary, nil
}

// flatPrimaryIndex adapts a virtual (primary) index into a datastore.PrimaryIndex
// by supplying the one method VirtualIndex lacks -- base ScanEntries. It's never
// actually invoked: conv routes PrimaryScan to n1k1's records-scan op, which
// reads the directory rather than driving the index. It exists only so the
// planner sees a primary index.
type flatPrimaryIndex struct {
	datastore.Index
}

func (p *flatPrimaryIndex) ScanEntries(requestId string, limit int64,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	conn.Sender().Close() // not used; records-scan reads the directory
}
