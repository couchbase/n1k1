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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

// SourcesConfig is the on-disk shape of a -sources config file (JSON / YAML / TOML) --
// the declarative twin of the positional CLI source list (DESIGN-data.md §2). It
// solves what the command line can't: a source path with spaces, many sources kept in
// version control, and (Phase 2) per-source options. The map key is the keyspace name.
//
//	sources:
//	  drive:  { path: "~/Google Drive/**" }   # object form
//	  docs:   "~/Documents"                   # string shorthand (just a path)
type SourcesConfig struct {
	Sources map[string]SourceSpec `json:"sources"`
}

// SourceSpec is one entry of a SourcesConfig. It unmarshals from either a bare path
// string ("~/x/**") or an object ({path: "~/x/**", ...}). The non-Path fields are
// Phase 2 (parsed so the file format is stable, but rejected by LoadSources until the
// federating composite datastore lands).
type SourceSpec struct {
	Path      string `json:"path"`
	Formats   string `json:"formats,omitempty"`   // Phase 2: per-source -formats lockdown
	Namespace string `json:"namespace,omitempty"` // Phase 2: place under a non-default namespace
	Sorted    string `json:"sorted,omitempty"`    // Phase 2: declared sort key (sortedness contract)
}

// UnmarshalJSON accepts both a JSON string (shorthand for {path: <string>}) and an
// object, so a config can write `docs: "~/Documents"` or `docs: {path: "~/Documents"}`.
func (s *SourceSpec) UnmarshalJSON(b []byte) error {
	if t := bytes.TrimSpace(b); len(t) > 0 && t[0] == '"' {
		return json.Unmarshal(t, &s.Path)
	}
	type raw SourceSpec // avoid recursing into this method
	return json.Unmarshal(b, (*raw)(s))
}

// LoadSources reads a -sources config file into a deterministic (name-sorted) []Source
// for OpenSessionSources. It uses n1k1's own decoders (records.DecodeConfigFile), so
// JSON/YAML/TOML all work. A relative source path anchors at the CONFIG FILE's
// directory (so a config is portable regardless of CWD); ~ and absolute paths pass
// through. Per-source options are rejected for now (Phase 2, see SourceSpec).
func LoadSources(path string) ([]Source, error) {
	jb, err := records.DecodeConfigFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading sources config %q: %w", path, err)
	}
	var cfg SourcesConfig
	if err := json.Unmarshal(jb, &cfg); err != nil {
		return nil, fmt.Errorf("parsing sources config %q: %w", path, err)
	}
	if len(cfg.Sources) == 0 {
		return nil, fmt.Errorf("sources config %q has no `sources`", path)
	}
	base, _ := filepath.Abs(filepath.Dir(path))
	names := make([]string, 0, len(cfg.Sources))
	for name := range cfg.Sources {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic keyspace order
	out := make([]Source, 0, len(names))
	for _, name := range names {
		spec := cfg.Sources[name]
		if strings.TrimSpace(spec.Path) == "" {
			return nil, fmt.Errorf("source %q in %q has no path", name, path)
		}
		if spec.Formats != "" || spec.Namespace != "" || spec.Sorted != "" {
			return nil, fmt.Errorf(
				"source %q: per-source options (formats/namespace/sorted) are not yet supported "+
					"(DESIGN-data.md §2 Phase 2)", name)
		}
		out = append(out, Source{Name: name, Path: configRelPath(spec.Path, base)})
	}
	return out, nil
}

// OpenSessionSourcesFile is LoadSources + OpenSessionSources: open a session directly
// from a -sources config file.
func OpenSessionSourcesFile(path, namespace string) (*Session, error) {
	sources, err := LoadSources(path)
	if err != nil {
		return nil, err
	}
	return OpenSessionSources(sources, namespace)
}

// configRelPath anchors a config's relative source path at the config file's dir
// (configBase, already absolute), so a config is portable regardless of CWD. A ~,
// absolute, or object-store path passes through untouched (OpenSessionSources expands
// ~ and rejects remote URIs in Phase 1).
func configRelPath(p, configBase string) string {
	p = strings.TrimSpace(p)
	if p == "" || strings.HasPrefix(p, "~") || filepath.IsAbs(p) || records.IsObjectStoreURI(p) {
		return p
	}
	return filepath.Join(configBase, p)
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
