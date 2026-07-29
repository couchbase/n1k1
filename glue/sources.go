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

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/util"

	"github.com/couchbase/n1k1/records"
)

// Source names one data source for OpenSessionSources. Name is the keyspace it becomes
// ("" = derive from Path); Path is a local directory, a single file, or a glob
// (absolute, ~-prefixed, ./ ../ or bare-relative -- bare/relative anchor at CWD), a
// local Apache Iceberg table directory, or an object-store URI (s3://, gs://, abfs://)
// naming a remote Parquet object or Iceberg table.
type Source struct {
	Name string
	Path string
}

// OpenSessionSources opens a session over multiple data sources, each becoming a
// sibling keyspace under the namespace (default "default"), so one SQL++ query can join
// across them (DESIGN-data.md §2). Sources may be HETEROGENEOUS kinds -- a local dir/
// glob/file, a local or remote Iceberg table, a remote Parquet object -- federated into
// one namespace by building a flatKeyspace per source (each carrying the fields its kind
// needs; KeyspaceRecordsOpen already routes on them) over an inert base datastore.
//
// Names are taken from Source.Name or derived from the path (a file's stem, a dir/glob-
// base/table basename); two sources resolving to the same name are a hard error (pass an
// explicit Name). A bare directory becomes a recursive union (`dir/**`) of its decodable
// files. A local source matching zero files is a hard error (fail-loud), never a
// silently empty keyspace.
func OpenSessionSources(sources []Source, namespace string) (*Session, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("no data sources given")
	}
	keyspaces := make(map[string]*flatKeyspace, len(sources))
	origin := make(map[string]string, len(sources)) // name -> original arg, for a clear collision error
	for _, s := range sources {
		path := expandTilde(strings.TrimSpace(s.Path))
		if path == "" {
			return nil, fmt.Errorf("empty data-source path")
		}
		name := strings.TrimSpace(s.Name)
		if name == "" {
			dn, err := deriveSourceName(path)
			if err != nil {
				return nil, err
			}
			name = dn
		}
		if prev, ok := origin[name]; ok {
			return nil, fmt.Errorf(
				"two data sources map to keyspace %q (%q and %q); pass a name (name=path) to disambiguate",
				name, prev, s.Path)
		}
		ks, err := sourceFlatKeyspace(path)
		if err != nil {
			return nil, err
		}
		keyspaces[name] = ks
		origin[name] = s.Path
	}

	// Federate: an inert (empty) base datastore satisfies cbq's planner, TEMP KEYSPACEs
	// overlay at the bottom, and the source keyspaces are the synthetic "default"
	// namespace (wrapFlatKeyspaces wires each a virtual keyspace + primary indexer).
	base, err := inertBaseDatastore()
	if err != nil {
		return nil, err
	}
	temp := newTempKeyspaces()
	ds := wrapTempKeyspaces(base, temp)
	ds = wrapFlatKeyspaces(ds, keyspaces, nil)
	store := &Store{
		Datastore:       ds,
		IndexApiVersion: datastore.INDEX_API_MAX,
		FeatureControls: util.DEF_N1QL_FEAT_CTRL,
		Temp:            temp,
	}
	if err := store.InitParser(); err != nil {
		return nil, err
	}
	if namespace == "" {
		namespace = "default"
	}
	return &Session{Store: store, Namespace: namespace}, nil
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

// sourceFlatKeyspace classifies one (tilde-expanded) source path into the flatKeyspace
// its KIND needs -- the heart of heterogeneous federation. KeyspaceRecordsOpen later
// routes on these fields: iceberg -> records.OpenIcebergTable, parquetURL -> a remote
// Parquet reader, dir/glob -> a local walk. The order matters: object-store URIs and
// Iceberg tables are recognized before the generic local dir/glob path.
func sourceFlatKeyspace(path string) (*flatKeyspace, error) {
	// Object-store URI (s3:// / gs:// / abfs://): a *.parquet object, else an Iceberg table.
	if records.IsObjectStoreURI(path) {
		if strings.HasSuffix(strings.ToLower(path), ".parquet") {
			return &flatKeyspace{parquetURL: path}, nil
		}
		metadataLoc := path
		if !strings.HasSuffix(path, ".metadata.json") { // a bare table dir: resolve current metadata
			resolved, rerr := records.ResolveObjectStoreIcebergMetadata(path)
			if rerr != nil {
				return nil, fmt.Errorf("data source %q: %w", path, rerr)
			}
			metadataLoc = resolved
		}
		tableDir, _, ok := records.SplitIcebergMetadataLocation(metadataLoc)
		if !ok {
			return nil, fmt.Errorf(
				"data source %q: cannot resolve an Iceberg table (expected .../<table>/metadata/<file>.metadata.json)", path)
		}
		return &flatKeyspace{dir: tableDir, iceberg: metadataLoc}, nil
	}

	abs := path
	if a, err := filepath.Abs(abs); err == nil {
		abs = a
	}
	// Local Iceberg table directory (has metadata/<v>.metadata.json).
	if meta, ok := records.IcebergTableMetadata(abs); ok {
		return &flatKeyspace{dir: abs, iceberg: meta}, nil
	}

	// Local dir / glob / single file: resolve to an absolute pattern, then fail loudly on
	// a zero-match resolution (never a silently empty keyspace). A single local Parquet
	// file rides this path too (Parquet is an ordinary record format).
	pat, err := sourcePattern(path)
	if err != nil {
		return nil, err
	}
	base, files, gerr := records.GlobFiles(pat, ScanWalkOptions)
	if gerr != nil {
		return nil, fmt.Errorf("data source %q: %v", path, gerr)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf(
			"data source %q resolves to no files (pattern %q); refusing to open an empty keyspace", path, pat)
	}
	return &flatKeyspace{dir: base, glob: pat}, nil
}

// sourcePattern turns a (tilde-expanded) local source path into the absolute path/glob
// a flatKeyspace's glob field stores. It anchors a bare/relative path at CWD, and
// expands a plain existing directory into a recursive union `dir/**` of its decodable
// files (a bare dir, matched literally, would match no files and trip the fail-loud).
// A single file or an explicit glob passes through.
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
