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

// Late binding via logical keyspaces (DESIGN-prepare.md phases 1-2, "Late binding:
// a prepared corpus over a new, differently-named bundle"). A PREPARE++ detector
// corpus is authored ONCE against a stable *logical* vocabulary (`indexer_log`,
// `orders`), but every incoming support bundle lays its files out differently
// (`indexer.log.3`, `indexer_2024.log`, `orders_2024Q4.json`). Rather than edit the
// detectors per bundle, this is ordinary prepared-statement late binding applied to
// FILES: a detector says `FROM <logical>`, and a per-bundle MANIFEST resolves the
// LOGICAL name to a PHYSICAL glob pattern at bind time. Point the manifest at the
// next bundle's root and re-resolve -- same corpus, new bundle, no detector edits.
//
// This is a NEAR-PARALLEL of glob.go's inline-glob wrapper (globDatastore /
// globNamespace / newGlobKeyspace), with ONE added indirection: where glob.go treats
// a glob-SHAPED keyspace name as its own pattern, the binding wrapper looks a plain
// LOGICAL identifier up in the manifest to GET a glob pattern, then resolves it via
// the very same newGlobKeyspace machinery (a metadata-only virtual keyspace whose
// records-scan expands the pattern at scan time). The one twist: the bound keyspace's
// IDENTITY (Name/Id/QualifiedName) is the LOGICAL name, NOT the glob pattern -- so
// CorpusCompile, which groups detectors by keyspace.QualifiedName() to build one
// shared fused scan per keyspace, FUSES two detectors that both say `FROM indexer_log`
// into a single scan (see corpus.go). newBoundKeyspace achieves this by building the
// virtual keyspace's path from the LOGICAL name while stashing the resolved glob in
// the flatKeyspace's dir/glob fields (which drive RecordsGlob/RecordsDir at scan
// time) -- no wrapper keyspace needed.
//
// FAIL LOUDLY (the crucial safety property): a bound logical keyspace whose glob
// matches ZERO files is a HARD ERROR at resolution/bind (surfaced as a plan error),
// NOT an empty keyspace -- "resolves to nothing should error at EXECUTE, not quietly
// yield an empty (falsely 'clean') findings table." A logical name a detector
// references that is neither bound nor a real keyspace hits the normal "no keyspace"
// error (already loud) -- the binding wrapper simply delegates it down and doesn't
// swallow it.
//
// LAYERING (mirrors glob.go's rationale): the binding wrapper sits INNERMOST, right
// above the glob wrapper (file -> glob -> binding -> flat/si), so the outer flat /
// secondary-index wrappers keep their type identities (IsFlatDatastore / the
// *siDatastore assertions) while still delegating unknown names DOWN to here. A plain
// logical identifier carries no glob metacharacter, so the glob wrapper passes it
// straight through to us; a name NOT in the manifest we pass straight down unchanged,
// so real / inline-glob / flat keyspaces all still resolve as before. maybeBind is a
// no-op when the manifest is empty, so the plain FileStore path is byte-for-byte
// unchanged.
//
// DEFERRED (phase-2 "field drift" half, and the rest of the resolution ladder; noted,
// not built here):
//   - Field-shape ADAPTERS / normalization -- a bundle names a field `severity` where
//     the detector reads `level`. MVP handles NAME/FILE drift (this binding) only.
//   - CONVENTION (regex) and CONTENT-SNIFFING (schema / line-shape) resolution rungs
//     -- glob is the one explicit rung of the ladder for now.
//   - A bind-invariant COMPILED (go-build) artifact -- at the interpreter tier a
//     "rebind" is just: point at a new bundle root + re-resolve + re-CorpusCompile
//     (cheap). The manifest + detectors are the durable artifacts.

import (
	"fmt"
	"sort"
	"strings"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/virtual"
	"github.com/couchbase/query/errors"

	"github.com/couchbase/n1k1/records"
)

// Binding is a per-bundle manifest: a LOGICAL keyspace name (a plain identifier the
// detector corpus references via `FROM <logical>`) mapped to a PHYSICAL glob pattern
// (relative to the bundle root, using the same base-dir convention as an inline glob
// -- see globAbsPattern: bare = root-relative, ./ or ../ = CWD, / = absolute). An
// explicit single file path is a degenerate glob (no metacharacters), so it is
// covered too. The binding is per-bundle (per Store/root); the detector corpus is
// bundle-INDEPENDENT.
type Binding map[string]string

// maybeBind wraps ds so a keyspace name present in the manifest resolves to a
// union-of-matches keyspace whose IDENTITY is the logical name (so same-logical
// detectors fuse in CorpusCompile) but whose data is the glob's matches. dataRoot
// anchors bare (root-relative) patterns. A name NOT in the manifest delegates down
// unchanged. Returns ds untouched when the manifest is empty (the plain,
// binding-free path pays nothing).
func maybeBind(dataRoot string, b Binding, ds datastore.Datastore) datastore.Datastore {
	if len(b) == 0 {
		return ds
	}
	// Copy the manifest so a later mutation of the caller's map can't retroactively
	// change what a live Store resolves.
	m := make(Binding, len(b))
	for k, v := range b {
		m[k] = v
	}
	return &bindingDatastore{Datastore: ds, dataRoot: dataRoot, binding: m}
}

type bindingDatastore struct {
	datastore.Datastore
	dataRoot string
	binding  Binding
}

func (d *bindingDatastore) NamespaceById(id string) (datastore.Namespace, errors.Error) {
	return d.NamespaceByName(id)
}

func (d *bindingDatastore) NamespaceByName(name string) (datastore.Namespace, errors.Error) {
	inner, err := d.Datastore.NamespaceByName(name)
	if err != nil {
		return nil, err
	}
	// Bound keyspaces live only in the "default" namespace (the file datastore's
	// single namespace, always synthesized by the inner glob wrapper). Other
	// namespaces pass straight through.
	if strings.EqualFold(name, flatRootNamespace) {
		return &bindingNamespace{Namespace: inner, ds: d}, nil
	}
	return inner, nil
}

// bindingNamespace is the "default" namespace with the manifest layered in front of
// the inner (glob) namespace: a manifested logical name resolves to a bound glob
// keyspace; every other name -- and all enumeration -- delegates to the embedded
// inner namespace (so glob-shaped names, real keyspaces, and flat keyspaces resolve
// exactly as without a binding).
type bindingNamespace struct {
	datastore.Namespace // the inner "default" (a globNamespace), for delegation + enumeration
	ds                  *bindingDatastore
}

func (p *bindingNamespace) Datastore() datastore.Datastore { return p.ds }

func (p *bindingNamespace) KeyspaceById(id string) (datastore.Keyspace, errors.Error) {
	return p.KeyspaceByName(id)
}

func (p *bindingNamespace) KeyspaceByName(name string) (datastore.Keyspace, errors.Error) {
	if pattern, ok := p.binding()[name]; ok {
		ks, err := newBoundKeyspace(p, name, pattern, p.ds.dataRoot)
		if err != nil {
			// Fail loudly: an empty-glob binding (or a bad pattern) is a hard error at
			// resolution, surfaced up the planner as a plan error -- never a silently
			// empty keyspace.
			return nil, errors.NewError(err, "bound keyspace "+name)
		}
		return ks, nil
	}
	// Not in the manifest: delegate down unchanged. An unbound name that is also not a
	// real / glob / flat keyspace produces the inner namespace's normal "no keyspace"
	// error here -- already loud; we don't swallow it.
	return p.Namespace.KeyspaceByName(name)
}

func (p *bindingNamespace) binding() Binding { return p.ds.binding }

// KeyspaceNames merges the manifest's logical names with the inner namespace's names
// (dedup'd, sorted) so enumeration (.tables) advertises bound keyspaces too. (Bound
// keyspaces are resolvable but not otherwise enumerable -- like inline globs -- so
// without this they wouldn't appear.)
func (p *bindingNamespace) KeyspaceNames() ([]string, errors.Error) {
	seen := map[string]bool{}
	var out []string
	for n := range p.binding() {
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	if inner, err := p.Namespace.KeyspaceNames(); err == nil {
		for _, n := range inner {
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func (p *bindingNamespace) KeyspaceIds() ([]string, errors.Error) { return p.KeyspaceNames() }

// newBoundKeyspace resolves a manifest entry (logical name -> glob pattern) to a
// metadata-only virtual keyspace backed by the pattern's matches, PARALLEL to
// newGlobKeyspace but with two differences: (1) the virtual keyspace's path is built
// from the LOGICAL name, so Name()/Id()/QualifiedName() report the logical name
// (giving same-logical detectors one shared QualifiedName -> one fused scan in
// CorpusCompile), while the resolved absolute glob rides in the flatKeyspace's
// dir/glob fields (which drive RecordsDir/RecordsGlob at scan time); and (2) it
// FAILS LOUDLY -- a pattern that matches zero files is a hard error, not an empty
// keyspace. The zero-match walk here is the bind-time resolution cost (a bound
// keyspace is explicit and per-bundle, so an eager walk is acceptable -- unlike the
// deliberately-lazy inline-glob path).
func newBoundKeyspace(ns datastore.Namespace, logical, pattern, dataRoot string) (datastore.Keyspace, error) {
	absGlob := globAbsPattern(pattern, dataRoot)

	// Fail loudly on an empty resolution. GlobFiles applies the same -formats
	// eligibility (ScanWalkOptions) the scan itself uses, so "matches nothing" means
	// "no file this query could actually read", not merely "no path".
	_, files, err := records.GlobFiles(absGlob, ScanWalkOptions)
	if err != nil {
		return nil, fmt.Errorf("resolving pattern %q: %v", pattern, err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf(
			"logical keyspace %q resolves to no files (pattern %q -> %q); "+
				"refusing to bind to an empty keyspace (would falsely read as clean)",
			logical, pattern, absGlob)
	}

	vks, verr := virtual.NewVirtualKeyspace(ns, []string{flatRootNamespace, logical})
	if verr != nil {
		return nil, verr
	}
	ks := &flatKeyspace{Keyspace: vks, dir: records.GlobBase(absGlob), glob: absGlob}
	ks.indexer = newFlatIndexer(ks)
	return ks, nil
}
