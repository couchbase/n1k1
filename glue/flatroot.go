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

// Flat-root support (DESIGN-data.md scenario B): when a datastore root holds
// record files *directly* (no <namespace>/<keyspace> subdirs), n1k1 "fakes" the
// metadata so the cbq planner accepts `FROM <basename>` -- a synthetic "default"
// namespace + basename keyspace that exists only as planner-facing metadata (no
// physical namespace/keyspace dir). The keyspace advertises a primary index so
// the planner emits a PrimaryScan; n1k1's records-scan then reads the *root*
// directory itself (via RecordsDir below). This is entirely n1k1-side -- no fork
// change -- reusing the fork's datastore/virtual metadata-only building blocks.

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/virtual"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/timestamp"

	"github.com/couchbase/n1k1/records"
)

const flatRootNamespace = "default"

// maybeFlatRoot wraps ds with a synthetic default:<basename> keyspace when path
// is a flat root (record files directly under it, and no real namespaces). It
// returns ds unchanged otherwise.
func maybeFlatRoot(path string, ds datastore.Datastore) datastore.Datastore {
	// If the file datastore already found real namespaces (subdirs), it's the
	// normal <ns>/<keyspace> layout -- leave it alone.
	if names, err := ds.NamespaceNames(); err != nil || len(names) > 0 {
		return ds
	}
	if !dirHasRecordFile(path) {
		return ds
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return ds
	}
	base := filepath.Base(filepath.Clean(abs))
	if base == "" || base == "." || base == string(filepath.Separator) {
		return ds
	}

	w := &flatDatastore{Datastore: ds}
	ns := &flatNamespace{datastore: w, ksName: base}
	vks, verr := virtual.NewVirtualKeyspace(ns, []string{flatRootNamespace, base})
	if verr != nil {
		return ds
	}
	ks := &flatKeyspace{Keyspace: vks, dir: abs}
	ks.indexer = newFlatIndexer(ks)
	ns.ks = ks
	w.ns = ns
	return w
}

// dirHasRecordFile reports whether dir directly contains a decodable record file.
func dirHasRecordFile(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && records.IsRecordFile(e.Name()) {
			return true
		}
	}
	return false
}

// --------------------------------------------------------- datastore wrapper

// flatDatastore embeds the real datastore (promoting its ~40 methods) and
// overrides only namespace lookup to expose the single synthetic namespace.
type flatDatastore struct {
	datastore.Datastore
	ns *flatNamespace
}

func (d *flatDatastore) NamespaceIds() ([]string, errors.Error)   { return []string{flatRootNamespace}, nil }
func (d *flatDatastore) NamespaceNames() ([]string, errors.Error) { return []string{flatRootNamespace}, nil }

func (d *flatDatastore) NamespaceById(id string) (datastore.Namespace, errors.Error) {
	return d.NamespaceByName(id)
}

func (d *flatDatastore) NamespaceByName(name string) (datastore.Namespace, errors.Error) {
	if strings.EqualFold(name, flatRootNamespace) {
		return d.ns, nil
	}
	return nil, errors.NewError(nil, "flat-root: no namespace "+name)
}

// --------------------------------------------------------- namespace

// flatNamespace is the synthetic "default" namespace holding one keyspace.
type flatNamespace struct {
	datastore *flatDatastore
	ksName    string
	ks        *flatKeyspace
}

func (p *flatNamespace) Datastore() datastore.Datastore { return p.datastore }
func (p *flatNamespace) Id() string                     { return flatRootNamespace }
func (p *flatNamespace) Name() string                   { return flatRootNamespace }

func (p *flatNamespace) KeyspaceIds() ([]string, errors.Error)   { return []string{p.ksName}, nil }
func (p *flatNamespace) KeyspaceNames() ([]string, errors.Error) { return []string{p.ksName}, nil }

func (p *flatNamespace) KeyspaceById(id string) (datastore.Keyspace, errors.Error) {
	return p.KeyspaceByName(id)
}

func (p *flatNamespace) KeyspaceByName(name string) (datastore.Keyspace, errors.Error) {
	if strings.EqualFold(name, p.ksName) {
		return p.ks, nil
	}
	return nil, errors.NewError(nil, "flat-root: no keyspace "+name)
}

func (p *flatNamespace) BucketIds() ([]string, errors.Error)   { return nil, nil }
func (p *flatNamespace) BucketNames() ([]string, errors.Error) { return nil, nil }

func (p *flatNamespace) BucketById(name string) (datastore.Bucket, errors.Error) {
	return nil, errors.NewError(nil, "flat-root: no buckets")
}
func (p *flatNamespace) BucketByName(name string) (datastore.Bucket, errors.Error) {
	return nil, errors.NewError(nil, "flat-root: no buckets")
}

func (p *flatNamespace) Objects(creds *auth.Credentials, filter func(string) bool,
	preload bool) ([]datastore.Object, errors.Error) {
	return []datastore.Object{{Id: p.ksName, Name: p.ksName, IsKeyspace: true}}, nil
}

// --------------------------------------------------------- keyspace

// flatKeyspace embeds a metadata-only virtual keyspace (promoting its Keyspace
// methods) and overrides index advertising to expose a primary index, plus
// RecordsDir so the records-scan reads the flat root directory.
type flatKeyspace struct {
	datastore.Keyspace
	dir     string
	indexer datastore.Indexer
}

// RecordsDir is consulted by DatastoreScanRecords to locate the physical
// directory: for a flat root the keyspace's data lives at the root itself, not
// at <root>/<ns>/<keyspace>.
func (k *flatKeyspace) RecordsDir() string { return k.dir }

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

func (ix *flatIndexer) IndexById(id string) (datastore.Index, errors.Error)   { return ix.primary, nil }
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
