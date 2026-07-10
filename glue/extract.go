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

// extract_cache.go memoizes an extractor recipe's per-file describe() result in the
// .n1k1 sidecar, so the expensive, format-specific describe pass (regex/time sniffing
// + a head-sample measurement of sortedness, min/max key, disorder bound) runs ONCE
// per file across all queries and processes -- "describe once, reuse forever"
// (DESIGN-data.md §4). The memoized {ExtractSpec, SortedSourceMeta} is persisted as
// JSON under <root>/<sidecar>/extract/, keyed by the file fingerprint (relpath, size,
// mtime) (DESIGN-data.md §5 "Per-file fingerprint"). On a scan opening a recipe-
// matched file, a matching fingerprint skips describe() entirely; a mismatch (a
// changed file) re-describes that one file and atomically rewrites its cache.
//
// The wiring: records exposes a pure-Go DescribeMemo seam (records/recipe.go) that
// OpenFile calls per recipe-matched open; this file installs describeMemoized into
// it in init(). records stays sidecar-unaware -- all the .n1k1 knowledge (root
// resolution, fingerprint, atomic write) lives here in glue, beside the other
// sidecar readers/writers (idx_si_catalog.go's catalog, idx_mem.go's index blobs).
//
// The per-row scan path is untouched: describe/cache I/O is cold (planning), so the
// JSON garbage here is fine; SpecApply still runs natively on the borrowed-slice byte
// lane with no per-row allocations.

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"

	"github.com/couchbase/query/datastore"
)

// extractSidecarSub is the subdirectory under the sidecar (<root>/.n1k1/) holding the
// memoized describe results, one JSON file per source file. Sibling of catalog.json
// and cache/ (the in-memory index blobs); see DESIGN-indexing.md "Sidecar layout".
const extractSidecarSub = "extract"

// extractCacheVersion is the engine's extract PRODUCER version: a monotonically
// increasing integer bumped whenever the describe/measure/SpecApply logic that
// SHAPES a cached result changes (e.g. adding framing:json changed what describe
// produces for a JSONL file). It is folded into every cache entry's producer
// fingerprint, so an engine upgrade that changes extraction re-describes rather than
// serving a result shaped by the old logic (DESIGN-data.md §5 "producer_version").
// The recipe's own Fingerprint is folded in alongside it, so a changed recipe
// invalidates too -- see producerFingerprint. Bump this on any such logic change.
const extractCacheVersion = 1

// ExtractDescribeRuns counts how many times describe() ACTUALLY ran (a cache miss or
// a changed file) since process start; ExtractCacheHits counts memoized reuses (a
// matching fingerprint, describe skipped). Test observability, matching
// ColumnProjectionApplied's style in datastore_scan.go.
var (
	ExtractDescribeRuns int64
	ExtractCacheHits    int64
)

func init() {
	// Install the sidecar-backed memoization into the records describe seam. Set once
	// at package init (before any query), so every recipe-matched open goes through
	// the cache. records defaults the seam to nil (uncached) for a bare import.
	records.DescribeMemo = describeMemoized
}

// -------------------------------------------------------------- data-root registry

// dataRoots holds the absolute datastore roots FileStore has opened, so a describe of
// an absolute file path can find the sidecar it belongs under (the file's dataset
// root). Read-mostly (written once per FileStore at startup, read per describe);
// guarded because describe can run concurrently across keyspaces.
var (
	dataRootsMu sync.RWMutex
	dataRoots   []string
)

// registerDataRoot records a datastore's on-disk root (its file:// path) so later
// describe/SortedSourceMetaFor calls can map a file under it back to the sidecar
// (<root>/.n1k1/). Idempotent; called from FileStore. A relative path is absolutized
// so it matches the absolutized file paths the walk yields.
func registerDataRoot(root string) {
	// Symlink-resolve so the registered root is in the SAME canonical space as the
	// per-file resolveAbs() lookups (a symlinked bundle root would otherwise never
	// prefix-match its own files -- see resolveAbs). stmt.go already resolves before
	// calling, but a direct caller (e.g. a unit test) may not; doing it here is the
	// single source of truth.
	abs := resolveAbs(root)
	dataRootsMu.Lock()
	defer dataRootsMu.Unlock()
	for _, r := range dataRoots {
		if r == abs {
			return
		}
	}
	dataRoots = append(dataRoots, abs)
}

// dataRootFor returns the registered datastore root containing absPath -- the longest
// matching prefix, so a nested store wins over an ancestor -- or "" when none is
// known. A "" root means describe results can't be located under a sidecar, so they
// run uncached (still correct, just not memoized).
func dataRootFor(absPath string) string {
	dataRootsMu.RLock()
	defer dataRootsMu.RUnlock()
	best := ""
	for _, r := range dataRoots {
		if absPath == r || strings.HasPrefix(absPath, r+string(filepath.Separator)) {
			if len(r) > len(best) {
				best = r
			}
		}
	}
	return best
}

// resolveAbs returns path as an absolute, symlink-resolved path -- the canonical form
// used for both the registered data roots and the per-file cache key. FileStore
// EvalSymlinks-resolves a data root before registerDataRoot (cbcollect bundles are
// commonly symlinked: `support-bundle-ex01 -> cbcollect_info_...`), so a file opened
// via the SYMLINK path must be resolved the same way here or dataRootFor won't
// prefix-match it and the describe silently runs uncached. Best-effort: a stat/resolve
// failure falls back to the plain absolute path (still correct, just possibly uncached).
func resolveAbs(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	if resolved, rerr := filepath.EvalSymlinks(abs); rerr == nil {
		return resolved
	}
	return abs
}

// -------------------------------------------------------------- the memoized describe

// extractCacheEntry is the JSON persisted per source file under
// <root>/<sidecar>/extract/. The fingerprint (Relpath, Size, MtimeNanos) gates reuse:
// a matching fingerprint means describe() need not re-run. Relpath is stored (not just
// hashed into the filename) so a hash collision is caught and treated as a miss.
type extractCacheEntry struct {
	Relpath    string                   `json:"relpath"`
	Size       int64                    `json:"size"`
	MtimeNanos int64                    `json:"mtime_nanos"`
	Producer   string                   `json:"producer"` // engine version + recipe fingerprint
	Spec       records.ExtractSpec      `json:"spec"`
	Meta       records.SortedSourceMeta `json:"meta"`
}

// producerFingerprint folds the engine's extract producer version and the claiming
// recipe's Fingerprint into one string stored in (and checked against) each cache
// entry. A mismatch on read -- a bumped engine version OR a changed/swapped recipe --
// is treated as a miss, so the file re-describes instead of serving a stale spec
// (DESIGN-data.md §5 "config_fingerprint"). Pre-invalidation entries (no producer
// field) decode to "" and never match, so an upgrade naturally re-describes them.
func producerFingerprint(recipeFP string) string {
	return "v" + strconv.Itoa(extractCacheVersion) + "|" + recipeFP
}

// describeMemoized is the records.DescribeMemo seam: it wraps a recipe's describe()
// with the .n1k1 sidecar cache. On a fingerprint hit it returns the cached spec+meta
// (describe skipped); on a miss or a changed file it runs describe() and atomically
// writes the cache. A reader tolerates a concurrently-updating sidecar -- a missing,
// truncated, unparseable, or fingerprint-mismatched cache file is simply a miss, never
// fatal (DESIGN-data.md §5 "readers must tolerate a concurrently-updating sidecar").
func describeMemoized(path string, describe records.DescribeFunc, recipeFP string) (records.ExtractSpec, records.SortedSourceMeta, error) {
	abs := resolveAbs(path)
	root := dataRootFor(abs)
	fi, statErr := os.Stat(abs)
	if root == "" || statErr != nil {
		// No sidecar location, or we can't fingerprint the file -> run uncached (any
		// real open error surfaces at SpecApply/records.OpenFile time).
		atomic.AddInt64(&ExtractDescribeRuns, 1)
		spec, meta, derr := describe(path)
		logDescribe("uncached", path, spec, meta, derr)
		return spec, meta, derr
	}

	rel, relErr := filepath.Rel(root, abs)
	if relErr != nil {
		rel = filepath.Base(abs)
	}
	size, mtime := fi.Size(), fi.ModTime().UnixNano()
	producer := producerFingerprint(recipeFP)
	cachePath := extractCachePath(root, rel)

	if blob, e := os.ReadFile(cachePath); e == nil {
		var ent extractCacheEntry
		if json.Unmarshal(blob, &ent) == nil &&
			ent.Relpath == rel && ent.Size == size && ent.MtimeNanos == mtime &&
			ent.Producer == producer {
			atomic.AddInt64(&ExtractCacheHits, 1)
			logDescribe("cached", path, ent.Spec, ent.Meta, nil)
			return ent.Spec, ent.Meta, nil
		}
	}

	// Miss, changed file, or a stale producer (bumped engine version / changed recipe):
	// describe, then persist for the next query/process.
	atomic.AddInt64(&ExtractDescribeRuns, 1)
	spec, meta, derr := describe(path)
	if derr != nil {
		return spec, meta, derr
	}
	logDescribe("fresh", path, spec, meta, nil)
	writeExtractCache(cachePath, extractCacheEntry{
		Relpath: rel, Size: size, MtimeNanos: mtime, Producer: producer, Spec: spec, Meta: meta,
	})
	return spec, meta, nil
}

// logDescribe emits a per-file extract-describe diagnostic (base.Logf, level 1) --
// which recipe/format claimed the file and the sorted-source metadata describe()
// produced (or cache hit/miss) -- the main handle for debugging extract recipes.
// Cold path (once per file at plan time), so the string work is fine.
func logDescribe(how, path string, spec records.ExtractSpec, meta records.SortedSourceMeta, err error) {
	if !base.LogEnabled(1) {
		return
	}
	if err != nil {
		base.Logf(1, "glue/extract", "describe FAILED (%s), file: %s, err: %v", how, path, err)
		return
	}
	framing := spec.Framing.Kind
	if framing == "" {
		framing = "line"
	}
	base.Logf(1, "glue/extract", "describe (%s), file: %s, format: %s, framing: %s, sort_key: %s, sorted: %s, min_ts: %d, max_ts: %d, records: %d",
		how, path, spec.Format, framing, meta.SortKeyLabel, meta.Sortedness, meta.MinKey, meta.MaxKey, meta.RecordCount)
}

// extractCachePath is where a file's memoized describe result lives:
// <root>/<sidecar>/extract/<sha1(relpath)>.json. Hashing the relpath keeps the
// filename flat and filesystem-safe regardless of the source's nesting; the relpath
// itself is stored inside the entry to detect the (astronomically unlikely) collision.
func extractCachePath(root, relpath string) string {
	sum := sha1.Sum([]byte(relpath))
	name := hex.EncodeToString(sum[:]) + ".json"
	return filepath.Join(root, sidecarDir, extractSidecarSub, name)
}

// writeExtractCache persists one cache entry atomically: write a temp file in the
// SAME directory (so the rename is a same-filesystem atomic replace), then rename it
// into place -- a concurrent reader sees either the old file or the new, never a
// partial write (DESIGN-data.md §5 "atomic rename-into-place"). Best-effort: any I/O
// failure (e.g. a read-only WASM fs) just leaves the result uncached, never fatal --
// the next open simply re-describes.
func writeExtractCache(cachePath string, ent extractCacheEntry) {
	blob, err := json.MarshalIndent(&ent, "", "  ")
	if err != nil {
		return
	}
	dir := filepath.Dir(cachePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	tmp, err := os.CreateTemp(dir, "extract-*.json.tmp")
	if err != nil {
		return
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(blob); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return
	}
	if err := os.Rename(tmpName, cachePath); err != nil {
		os.Remove(tmpName)
	}
}

// -------------------------------------------------------------- Track-B lookup seam

// SortedSourceMetaFor returns the memoized SortedSourceMeta for one resolved file
// path -- the per-file seam the Track-B merge optimizer / a later A->B wiring step
// reads at plan time to decide concat-vs-heap merge and time-range pruning
// (DESIGN-merging.md; DESIGN-data.md §5 "Extract/sorted-source fields"). It resolves
// the file's extractor recipe, then runs describe() through the sidecar cache (a hit
// skips describe). ok is false when no recipe claims the file (a plain JSON/CSV
// keyspace has no sorted-source contract). Cold path (planning); safe to call before
// or independently of a scan.
func SortedSourceMetaFor(path string) (meta records.SortedSourceMeta, ok bool, err error) {
	abs := resolveAbs(path)
	rp := records.RecipeFor(recipeMatchPath(abs))
	if rp == nil {
		return records.SortedSourceMeta{}, false, nil
	}
	_, meta, err = describeMemoized(abs, rp.Describe, rp.Fingerprint)
	return meta, true, err
}

// FileSortedSourceMeta pairs a keyspace file with its memoized SortedSourceMeta.
type FileSortedSourceMeta struct {
	Path    string                   // absolute file path
	Relpath string                   // dataset-root-relative path (the fingerprint key)
	Meta    records.SortedSourceMeta // memoized describe measurement
}

// SortedSourceMetasForKeyspace returns the memoized SortedSourceMeta for every recipe-
// matched file in a keyspace, in walk order -- the input a K-way merge over a multi-
// file keyspace consumes (per-input sortedness + min/max key -> disjoint ranges concat
// vs heap merge). Non-recipe files (plain JSON/CSV, which have no sorted-source
// contract) are skipped. Cold path (planning); describe results come from the sidecar
// cache. gctx (optional) supplies the per-request walk-file cache; nil walks fresh.
func SortedSourceMetasForKeyspace(ks datastore.Keyspace, gctx *GlueContext) ([]FileSortedSourceMeta, error) {
	dir, err := KeyspaceDir(ks)
	if err != nil {
		return nil, err
	}
	opts := ScanWalkOptions
	var files []string
	if gctx != nil {
		files, err = gctx.walkFiles(dir, opts)
	} else {
		files, err = records.WalkFiles(dir, opts)
	}
	if err != nil {
		return nil, err
	}

	var out []FileSortedSourceMeta
	for _, f := range files {
		abs := resolveAbs(f)
		rp := records.RecipeFor(recipeMatchPath(abs))
		if rp == nil {
			continue // a plain structured file: no sorted-source contract.
		}
		_, meta, derr := describeMemoized(abs, rp.Describe, rp.Fingerprint)
		if derr != nil {
			return out, derr
		}
		rel := abs
		if root := dataRootFor(abs); root != "" {
			if r, rerr := filepath.Rel(root, abs); rerr == nil {
				rel = r
			}
		}
		out = append(out, FileSortedSourceMeta{Path: abs, Relpath: rel, Meta: meta})
	}
	return out, nil
}

// KeyspaceFramingKind classifies how a keyspace's files turn into records, for the
// .tables/.schema listing (IDEA-0007): a whole-file text blob vs an inherently
// multi-record structured format vs recipe-framed rows. Derived cheaply from the file
// listing plus content-free recipe/extension matching -- no file content is read.
type KeyspaceFramingKind int

const (
	KeyspaceEmpty      KeyspaceFramingKind = iota // no eligible record files
	KeyspaceBlob                                  // whole-file: one record per file
	KeyspaceStructured                            // jsonl/csv/json/...: multi-record
	KeyspaceRecipe                                // an extract recipe frames the files
	KeyspaceMixed                                 // files of differing kinds
)

// KeyspaceFraming is a keyspace's record-framing summary for display.
type KeyspaceFraming struct {
	Kind   KeyspaceFramingKind
	Recipe string // recipe name, when Kind==KeyspaceRecipe
	Format string // inner-ext, e.g. "jsonl"/"csv", when Kind==KeyspaceStructured
	Files  int    // eligible record-file count
}

// Label is a short human tag: "recipe=<name>", a structured format ("jsonl"),
// "whole-file" (a one-row-per-file blob), "mixed", or "empty".
func (f KeyspaceFraming) Label() string {
	switch f.Kind {
	case KeyspaceRecipe:
		return "recipe=" + f.Recipe
	case KeyspaceStructured:
		if f.Format != "" {
			return f.Format
		}
		return "structured"
	case KeyspaceBlob:
		return "whole-file"
	case KeyspaceMixed:
		return "mixed"
	default:
		return "empty"
	}
}

// KeyspaceFramingFor classifies a keyspace's record framing (IDEA-0007) so a listing
// can tell a query-ready multi-record keyspace from a whole-file text blob. It lists
// the keyspace's files (a directory read / glob expand, mirroring KeyspaceRecordsOpen's
// resolution) and classifies each by the content-free recipe registry (RecipeFor) and
// structured-extension test (IsStructuredFile). No file content is read -- safe on the
// interactive listing path even over a huge (e.g. 240 MB log) keyspace.
func KeyspaceFramingFor(ks datastore.Keyspace) (KeyspaceFraming, error) {
	files, err := keyspaceFiles(ks)
	if err != nil {
		return KeyspaceFraming{}, err
	}
	kf := KeyspaceFraming{Files: len(files)}
	recipes := map[string]bool{}
	formats := map[string]bool{}
	blobs := 0
	for _, f := range files {
		abs := resolveAbs(f)
		if rp := records.RecipeFor(recipeMatchPath(abs)); rp != nil {
			recipes[rp.Name] = true
		} else if records.IsStructuredFile(abs) {
			formats[formatExt(abs)] = true
		} else {
			blobs++
		}
	}
	switch {
	case len(files) == 0:
		kf.Kind = KeyspaceEmpty
	case len(recipes) == 1 && len(formats) == 0 && blobs == 0:
		kf.Kind = KeyspaceRecipe
		for n := range recipes {
			kf.Recipe = n
		}
	case len(recipes) == 0 && len(formats) == 1 && blobs == 0:
		kf.Kind = KeyspaceStructured
		for f := range formats {
			kf.Format = f
		}
	case len(recipes) == 0 && len(formats) == 0 && blobs > 0:
		kf.Kind = KeyspaceBlob
	default:
		kf.Kind = KeyspaceMixed
	}
	return kf, nil
}

// keyspaceFiles lists a keyspace's eligible record files WITHOUT opening them,
// mirroring KeyspaceRecordsOpen's glob -> single-file -> directory resolution.
func keyspaceFiles(ks datastore.Keyspace) ([]string, error) {
	opts := ScanWalkOptions
	if g, ok := ks.(interface{ RecordsGlob() (string, bool) }); ok {
		if pattern, has := g.RecordsGlob(); has {
			_, files, err := records.GlobFiles(pattern, opts)
			return files, err
		}
	}
	if rf, ok := ks.(interface{ RecordsFile() string }); ok && rf.RecordsFile() != "" {
		return []string{rf.RecordsFile()}, nil
	}
	dir, err := KeyspaceDir(ks)
	if err != nil {
		return nil, err
	}
	return records.WalkFiles(dir, opts)
}

// formatExt is the display format tag for a structured file: its inner extension
// (seeing through one .gz/.zst), dot-stripped -- "events.jsonl.gz" -> "jsonl".
func formatExt(path string) string {
	name := filepath.Base(path)
	for _, z := range []string{".gz", ".zst"} {
		if strings.HasSuffix(name, z) {
			name = name[:len(name)-len(z)]
			break
		}
	}
	return strings.TrimPrefix(filepath.Ext(name), ".")
}

// recipeMatchPath returns the path RecipeFor should match against: the dataset-root-
// relative path when the file sits under a known store root (so path-anchored recipe
// regexps like `(^|/)diag\.log$` behave), else the bare absolute path.
func recipeMatchPath(absPath string) string {
	if root := dataRootFor(absPath); root != "" {
		if rel, err := filepath.Rel(root, absPath); err == nil {
			return rel
		}
	}
	return absPath
}
