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

// sources.go is the multiple-data-sources entry point (DESIGN-data.md §2, Phase 1):
// open a session over several independent local roots at once -- dirs, single files,
// or globs -- each becoming a sibling keyspace under one namespace, joinable in a
// single SQL++ query. E.g. a local Google Drive mirror + ~/Documents + a SharePoint
// mirror. It is a thin layer over the existing Binding seam (binding.go): each source
// is turned into a name -> absolute-path/glob manifest entry, resolved by the same
// glob machinery (union-of-matches virtual keyspace, fail-loud on zero matches,
// enumerated by .tables). The datastore itself is built over a synthetic EMPTY root,
// so nothing but the named sources appears.
package glue

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/couchbase/n1k1/records"
)

// Source names one data source for OpenSessionSources. Name is the keyspace it
// becomes ("" = derive from Path); Path is a directory, a single file, or a glob
// (absolute, ~-prefixed, ./ ../ or bare-relative -- bare/relative anchor at CWD).
type Source struct {
	Name string
	Path string
}

// OpenSessionSources opens a session over multiple local data sources, each becoming
// a sibling keyspace under the namespace (default "default"), so one SQL++ query can
// join across them. Names are taken from Source.Name, or derived from the path (a
// file's stem, a dir's or glob-base's basename); two sources that resolve to the same
// keyspace name are a hard error (pass an explicit Name to disambiguate). A bare
// directory is expanded to a recursive union (`dir/**`) of its decodable files. A
// source that matches zero files is a hard error at query time (the Binding seam's
// fail-loud), never a silently empty keyspace.
//
// Phase 1 is local-filesystem only (a Google Drive / OneDrive / SharePoint mirror is a
// local mount, so those are covered); a remote object-store URI (s3://, gs://, …) as a
// source is rejected here -- heterogeneous federation is DESIGN-data.md §2 Phase 2.
func OpenSessionSources(sources []Source, namespace string) (*Session, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("no data sources given")
	}
	b := make(Binding, len(sources))
	origin := make(map[string]string, len(sources)) // name -> original arg, for a clear collision error
	for _, s := range sources {
		path := expandTilde(strings.TrimSpace(s.Path))
		if path == "" {
			return nil, fmt.Errorf("empty data-source path")
		}
		if records.IsObjectStoreURI(path) {
			return nil, fmt.Errorf(
				"data source %q: remote/object-store sources are not yet supported here "+
					"(local mirrors are; heterogeneous federation is DESIGN-data.md §2 Phase 2)", path)
		}
		name := strings.TrimSpace(s.Name)
		if name == "" {
			dn, err := deriveSourceName(path)
			if err != nil {
				return nil, err
			}
			name = dn
		}
		pat, err := sourcePattern(path)
		if err != nil {
			return nil, err
		}
		if prev, ok := origin[name]; ok {
			return nil, fmt.Errorf(
				"two data sources map to keyspace %q (%q and %q); pass a name (name=path) to disambiguate",
				name, prev, s.Path)
		}
		b[name] = pat
		origin[name] = s.Path
	}
	root, err := syntheticSourcesRoot()
	if err != nil {
		return nil, err
	}
	return OpenSessionBound(root, namespace, b)
}

// sourcePattern turns a (tilde-expanded) source path into the absolute path/glob a
// Binding entry stores. It anchors a bare/relative path at CWD (so it doesn't depend
// on the synthetic root), and expands a plain existing directory into a recursive
// union `dir/**` of its decodable files (a bare dir, matched literally, would match
// no files and trip the fail-loud). A single file or an explicit glob passes through.
func sourcePattern(path string) (string, error) {
	abs := path
	if !filepath.IsAbs(abs) {
		if a, err := filepath.Abs(abs); err == nil {
			abs = a
		}
	}
	if !records.HasGlobMeta(abs) {
		if fi, err := os.Stat(abs); err == nil && fi.IsDir() {
			abs = filepath.Join(abs, "**") // recurse: union of all decodable files under dir
		}
	}
	return abs, nil
}

// deriveSourceName derives a keyspace name from a nameless source path: the basename
// of a glob's literal base, a directory's basename, or a file's stem (a trailing
// compression + format extension stripped). It errors when nothing usable remains
// (e.g. a rootless glob like `**/*.json`), so the caller must pass an explicit name.
func deriveSourceName(path string) (string, error) {
	p := path
	if records.HasGlobMeta(p) {
		p = records.GlobBase(p)
	}
	base := filepath.Base(strings.TrimRight(p, `/\`))
	for _, ext := range []string{".gz", ".zst"} { // strip a single compression suffix
		base = strings.TrimSuffix(base, ext)
	}
	if e := filepath.Ext(base); e != "" { // then a single format suffix (events.jsonl -> events)
		base = strings.TrimSuffix(base, e)
	}
	base = strings.TrimSpace(base)
	if base == "" || base == "." || base == ".." || base == string(filepath.Separator) {
		return "", fmt.Errorf(
			"cannot derive a keyspace name from source %q; pass one as name=path", path)
	}
	return base, nil
}

// expandTilde expands a leading ~ or ~/ to the user's home dir (the shell does this
// for unquoted args, but a quoted glob like '~/x/**' reaches us literally). ~user is
// not expanded (rare; left as-is so it fails loudly rather than mis-resolving).
func expandTilde(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			if p == "~" {
				return home
			}
			return filepath.Join(home, p[2:])
		}
	}
	return p
}

// syntheticSourcesRoot returns a process-wide empty directory used as the datastore
// root for a multi-source session: file discovery finds nothing there, so only the
// named sources (bindings) become keyspaces. One empty dir is reused for the whole
// process (bindings are per-Store, so sharing the root is safe).
var (
	sourcesRootOnce sync.Once
	sourcesRootDir  string
	sourcesRootErr  error
)

func syntheticSourcesRoot() (string, error) {
	sourcesRootOnce.Do(func() {
		sourcesRootDir, sourcesRootErr = os.MkdirTemp("", "n1k1-sources-")
	})
	return sourcesRootDir, sourcesRootErr
}
