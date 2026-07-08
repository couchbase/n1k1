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

// Inline glob keyspaces (DESIGN-data.md "Mode 2b"): a backtick-quoted glob used as
// a FROM keyspace name -- `FROM `./data/**/*.json`` -- resolves to a synthetic
// keyspace = the union of the glob's matches. No cbq grammar/parser change: the
// backticks make it one quoted identifier (the parser hands us the literal string),
// and this wrapper recognizes a glob-shaped name in the "default" namespace.
//
// The wrapper sits INNERMOST (file datastore -> glob -> flat/si), so the outer
// flat/secondary-index wrappers keep their type identities (IsFlatDatastore / the
// *siDatastore assertions) while still delegating unknown keyspace names DOWN to
// here (flatNamespace via its `real`, siNamespace via its embedded Namespace).
// Non-glob names pass straight through.
//
// Base directory (DESIGN-data.md Mode 2b, prefix convention -- no $ROOT sigil):
//   - "./..." or "../..."  -> CWD-relative (explicit, DuckDB-parity)
//   - "/..."               -> absolute
//   - bare "foo/bar/..."   -> datastore-root-relative (matches how bare keyspace
//                             names already resolve under the root; CWD fallback)
//
// The pattern is resolved to a concrete file list at SCAN time (KeyspaceRecordsOpen
// via RecordsGlob), so the query's -formats lockdown applies and new files show up.

import (
	"path/filepath"
	"strings"

	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/virtual"
	"github.com/couchbase/query/errors"

	"github.com/couchbase/n1k1/records"
)

// maybeGlob wraps ds so a glob-shaped keyspace name in the "default" namespace
// resolves to a union-of-matches keyspace. dataRoot anchors bare (root-relative)
// globs. Always safe to install: non-glob names delegate unchanged.
func maybeGlob(dataRoot string, ds datastore.Datastore) datastore.Datastore {
	return &globDatastore{Datastore: ds, dataRoot: dataRoot}
}

type globDatastore struct {
	datastore.Datastore
	dataRoot string
}

func (d *globDatastore) NamespaceById(id string) (datastore.Namespace, errors.Error) {
	return d.NamespaceByName(id)
}

func (d *globDatastore) NamespaceByName(name string) (datastore.Namespace, errors.Error) {
	if strings.EqualFold(name, flatRootNamespace) {
		// Always synthesize the "default" namespace so a glob-only tree (no physical
		// <root>/default dir, which the fork's file datastore requires) still
		// resolves globs -- mirroring how flatNamespace fakes default. The real
		// default (if any) rides along as `inner` for non-glob names. Guard on the
		// error: the fork's file datastore returns a non-nil-but-invalid namespace
		// for a missing dir (it panics on use), so keep inner ONLY when err == nil.
		inner, ierr := d.Datastore.NamespaceByName(name)
		if ierr != nil {
			inner = nil
		}
		return &globNamespace{inner: inner, ds: d}, nil
	}
	return d.Datastore.NamespaceByName(name)
}

// globNamespace is the synthetic "default" namespace: glob-shaped names resolve to
// a union-of-matches keyspace; every other name (and all enumeration) delegates to
// the real "default" (inner) when present. It implements the full datastore.
// Namespace interface explicitly (not by embedding) because inner may be nil.
type globNamespace struct {
	inner datastore.Namespace // real "default", or nil when the tree has none
	ds    *globDatastore
}

func (p *globNamespace) Datastore() datastore.Datastore { return p.ds }
func (p *globNamespace) Id() string                     { return flatRootNamespace }
func (p *globNamespace) Name() string                   { return flatRootNamespace }

func (p *globNamespace) KeyspaceIds() ([]string, errors.Error)   { return p.KeyspaceNames() }
func (p *globNamespace) KeyspaceNames() ([]string, errors.Error) {
	if p.inner != nil {
		return p.inner.KeyspaceNames() // globs are query-time, not enumerable
	}
	return nil, nil
}

func (p *globNamespace) KeyspaceById(id string) (datastore.Keyspace, errors.Error) {
	return p.KeyspaceByName(id)
}

func (p *globNamespace) KeyspaceByName(name string) (datastore.Keyspace, errors.Error) {
	if records.HasGlobMeta(name) {
		ks, err := newGlobKeyspace(p, name, p.ds.dataRoot)
		if err != nil {
			return nil, errors.NewError(err, "glob keyspace "+name)
		}
		return ks, nil
	}
	if p.inner != nil {
		return p.inner.KeyspaceByName(name)
	}
	return nil, errors.NewError(nil, "namespace default: no keyspace "+name)
}

func (p *globNamespace) BucketIds() ([]string, errors.Error)   { return nil, nil }
func (p *globNamespace) BucketNames() ([]string, errors.Error) { return nil, nil }
func (p *globNamespace) BucketById(string) (datastore.Bucket, errors.Error) {
	return nil, errors.NewError(nil, "glob: no buckets")
}
func (p *globNamespace) BucketByName(string) (datastore.Bucket, errors.Error) {
	return nil, errors.NewError(nil, "glob: no buckets")
}

func (p *globNamespace) Objects(creds *auth.Credentials, filter func(string) bool,
	preload bool) ([]datastore.Object, errors.Error) {
	if p.inner != nil {
		return p.inner.Objects(creds, filter, preload)
	}
	return nil, nil
}

// newGlobKeyspace builds a metadata-only virtual keyspace (with a primary index so
// the planner emits a PrimaryScan) backed by the absolute glob pattern; the
// records-scan expands it at scan time. The walk base is computed without touching
// the filesystem, so keyspace resolution stays cheap (no walk at plan time).
func newGlobKeyspace(ns datastore.Namespace, name, dataRoot string) (datastore.Keyspace, error) {
	absGlob := globAbsPattern(name, dataRoot)
	vks, verr := virtual.NewVirtualKeyspace(ns, []string{flatRootNamespace, name})
	if verr != nil {
		return nil, verr
	}
	ks := &flatKeyspace{Keyspace: vks, dir: records.GlobBase(absGlob), glob: absGlob}
	ks.indexer = newFlatIndexer(ks)
	return ks, nil
}

// globAbsPattern resolves a glob keyspace name to an absolute pattern per the
// Mode 2b prefix convention. `**`/`*`/etc. survive filepath.Clean (they aren't
// "."/".." elements), so cleaning only normalizes separators.
func globAbsPattern(name, dataRoot string) string {
	switch {
	case filepath.IsAbs(name):
		return filepath.Clean(name)
	case strings.HasPrefix(name, "./") || strings.HasPrefix(name, "../"):
		if abs, err := filepath.Abs(name); err == nil { // CWD-relative
			return abs
		}
		return filepath.Clean(name)
	default: // bare -> datastore-root-relative (CWD fallback when no root)
		root := dataRoot
		if root == "" {
			root = "."
		}
		if abs, err := filepath.Abs(filepath.Join(root, name)); err == nil {
			return abs
		}
		return filepath.Join(root, name)
	}
}
