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

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/query/datastore"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"
)

// index is an n1k1-built secondary index the engine can scan directly (via
// scanSpan), as opposed to the file datastore's #primary or any other
// datastore.Index. Both the bbolt-backed secondaryIndex (idx_si.go, native only)
// and the in-memory memIndex (idx_mem.go, all builds) implement it, so the core
// conversion/scan paths (conv.go, datastore_scan.go) dispatch on this interface
// rather than a concrete type -- letting the WASM build (no bbolt) still get real
// IndexScans from mem indexes. See DESIGN-indexing.md and web/DESIGN.md.
type index interface {
	datastore.Index

	// indexDefn returns the catalog definition backing this index (its range
	// keys, condition, coverable key paths). Named to avoid clashing with the
	// implementations' `def` field.
	indexDefn() *indexDef

	// scanSpan emits the docIDs (and, when projectKeys, decoded key values)
	// matching one span, WITHOUT closing the sender -- so several spans can share
	// one IndexConnection. docIDs already in seen are skipped (pass nil to
	// disable dedup). This is the multi-span-friendly primitive n1k1's
	// DatastoreScanIndex drives; the datastore.Index.Scan method wraps a single
	// span and owns the close.
	//
	// A time-scoped bolt index HYBRID-SERVES inside this call (DESIGN-indexing.md
	// rung 2): the span's matches among below-floor containers are evaluated with
	// the SAME key/condition expressions into the SAME encoding, then MERGE-emitted
	// in encoded-key order with the b+tree walk -- so the union is complete AND the
	// emission stays key-ordered (the planner elides ORDER BY when the index
	// provides the order; an out-of-order hybrid append would silently mis-sort).
	// An index whose hybridServes() is false keeps the disclosure warning instead.
	//
	// orderFree (base.Op.OrderFree, from markIndexScanOrderFree) says the plan does
	// NOT depend on emission order -- the hybrid half may then STREAM its matches
	// unbuffered instead of collect+sort+merge.
	scanSpan(span *datastore.Span, limit int64, seen map[string]bool,
		projectKeys, orderFree bool, conn *datastore.IndexConnection)
}

// sourceSignature summarizes a keyspace directory for change detection: file
// count and the newest mtime (nanoseconds) over the whole tree. This is the
// simple "assume static data, validate by timestamp" model -- adding, removing,
// or touching any file changes the signature and forces an index rebuild. Shared
// by the bbolt (idx_si.go) and in-memory (idx_mem.go) backends. since, when
// non-zero, restricts the signature to containers modified at/after it (a
// TIME-SCOPED index -- indexDef.Since): out-of-scope file churn then cannot
// stale the index, matching what its walks would (not) see.
func sourceSignature(dir string, since time.Time) (string, error) {
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
		if !since.IsZero() && info.ModTime().Before(since) {
			return nil // out of the index's time scope: invisible to it
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

// scopedIndex is what the scan-time disclosure needs from an index: its def and
// its CURRENT effective floor (the backfill-lowered scope boundary; zero = the
// index covers everything, so no disclosure). Implemented by secondaryIndex and
// ftsIndex; asserted as an interface so wasm-neutral scan code names no !wasm type.
type scopedIndex interface {
	indexDefn() *indexDef
	scopeFloor() time.Time
}

// warnScopedIndex discloses a TIME-SCOPED index (indexDef.Since) serving a query:
// a scoped index is partial BY DECLARATION, and the contract (DESIGN-indexing.md
// "Newest-first / partial index builds") is that it must never answer as if
// complete -- so every request it serves carries one warning naming the CURRENT
// floor (backfill lowers it; at floor==0 the index is complete and the warning
// stops). Deduped per request (WarnOncef), so joins/rescans don't repeat it.
func warnScopedIndex(context *GlueContext, ix interface{}) {
	si, ok := ix.(scopedIndex)
	if context == nil || !ok {
		return
	}
	def, floor := si.indexDefn(), si.scopeFloor()
	if def == nil || def.Since == "" || floor.IsZero() {
		return // never scoped, or backfilled to complete: full coverage, no warning
	}
	if h, ok := ix.(interface{ hybridServes() bool }); ok && h.hybridServes() {
		return // hybrid serve: index ∪ scan-below-floor answers COMPLETELY
	}
	context.WarnOncef("index-scope:"+def.Name,
		"index %q is time-scoped (indexed back to %s): results reflect ONLY containers modified since then"+
			" (.index backfill %s indexes older data; at complete this warning stops)",
		def.Name, floor.UTC().Format(time.RFC3339), def.Name)
}

// indexWalkOptions returns the record-walk options an index build/catch-up scans
// with: the standard scan options, no path prefix, and -- for a non-zero floor --
// a FileFilter admitting only containers modified at/after it (whole containers;
// the walk sees the corpus as if older files did not exist). The floor is the
// index's EFFECTIVE scope boundary: the stored, backfill-lowered floor when one
// exists, else the def's declared Since.
func indexWalkOptions(floor time.Time) records.WalkOptions {
	opts := ScanWalkOptions
	opts.PathPrefix = ""
	if !floor.IsZero() {
		opts.FileFilter = func(_ string, info os.FileInfo) bool {
			return !info.ModTime().Before(floor)
		}
	}
	return opts
}

// indexSourceFiles resolves WHERE an indexable keyspace's records live: the walk
// base plus the sorted absolute file list, in the exact id space the scan path
// uses. It mirrors KeyspaceRecordsOpen's glob/file/dir branches (the single "where
// does a keyspace's data live" resolver), so a flat/bound/glob keyspace -- a
// read-only bundle reached through a bind manifest, ISSUE-27 -- indexes the SAME
// files, under the SAME record ids, its scans read. Keyspaces whose records aren't
// local files (session temp, Iceberg, remote Parquet) are not indexable: error.
// opts' eligibility + FileFilter (a time-scope floor) gate the listing.
func indexSourceFiles(ks datastore.Keyspace, opts records.WalkOptions) (string, []string, error) {
	ks = KeyspaceRecordsInner(ks)
	opts = applyKeyspaceFormats(ks, opts)
	if _, ok := ks.(interface {
		RecordsSource(records.WalkOptions) (records.Source, error)
	}); ok {
		return "", nil, fmt.Errorf("keyspace %q is a session temp keyspace (in-memory rows): not indexable", ks.Name())
	}
	if it, ok := ks.(interface{ IcebergMetadata() string }); ok && it.IcebergMetadata() != "" {
		return "", nil, fmt.Errorf("keyspace %q is an Apache Iceberg table: not indexable (it carries its own manifest statistics)", ks.Name())
	}
	if pq, ok := ks.(interface{ ParquetURL() string }); ok && pq.ParquetURL() != "" {
		return "", nil, fmt.Errorf("keyspace %q is a remote Parquet object: not indexable", ks.Name())
	}
	if g, ok := ks.(interface{ RecordsGlob() (string, bool) }); ok {
		if pattern, has := g.RecordsGlob(); has {
			base, files, err := records.GlobFiles(pattern, opts)
			if err != nil {
				return "", nil, err
			}
			return globWalkBase(base), files, nil
		}
	}
	if rf, ok := ks.(interface{ RecordsFile() string }); ok && rf.RecordsFile() != "" {
		f := rf.RecordsFile()
		if opts.FileFilter != nil {
			if fi, serr := os.Stat(f); serr != nil || !opts.FileFilter(f, fi) {
				return filepath.Dir(f), nil, nil // out of scope (e.g. below a floor)
			}
		}
		return filepath.Dir(f), []string{f}, nil
	}
	dir, err := KeyspaceDir(ks)
	if err != nil {
		return "", nil, err
	}
	files, err := records.WalkFiles(dir, opts)
	if err != nil {
		return "", nil, err
	}
	return dir, files, nil
}

// indexSourceOpen opens the records source an index-side walk reads (build,
// catch-up, backfill, hybrid scan-below-floor): indexSourceFiles' listing served
// through the same per-file framing (opts) the scan path uses.
func indexSourceOpen(ks datastore.Keyspace, opts records.WalkOptions) (records.Source, error) {
	base, files, err := indexSourceFiles(ks, opts)
	if err != nil {
		return nil, err
	}
	return records.WalkPrelisted(base, files, applyKeyspaceFormats(ks, opts)), nil
}

// indexSourceSignature summarizes an indexable keyspace for change detection --
// file count and newest mtime (nanoseconds) over the keyspace's OWN file list,
// restricted to the index's time scope (floor). The sourceSignature model, but
// listing via indexSourceFiles so the change-detection universe is exactly what
// builds and scans read: works for glob/bound/flat keyspaces, and only
// record-eligible files can stale the index.
func indexSourceSignature(ks datastore.Keyspace, floor time.Time) (string, error) {
	_, files, err := indexSourceFiles(ks, indexWalkOptions(floor))
	if err != nil {
		return "", err
	}
	var count, newest int64
	for _, f := range files {
		fi, serr := os.Stat(f)
		if serr != nil {
			continue // raced away; the next signature sees the truth
		}
		count++
		if mt := fi.ModTime().UnixNano(); mt > newest {
			newest = mt
		}
	}
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(count))
	binary.BigEndian.PutUint64(b[8:16], uint64(newest))
	return fmt.Sprintf("%x", b), nil
}

// floorFormat / floorParse serialize a stored floor: decimal UnixNano, with "0"
// meaning COMPLETE (backfilled to the beginning -- the index covers everything,
// distinct from "no floor stored", which means the def's declared Since applies).
func floorFormat(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}

func floorParse(b []byte) (time.Time, bool) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	if n == 0 {
		return time.Time{}, true // complete
	}
	return time.Unix(0, n), true
}

// statementOrderFree reports that stmt provably has NO ORDER BY anywhere (top
// level or any subquery), so no plan built from it can depend on an index scan's
// emission order. Detection is a text scan of the algebra's regenerated SQL --
// deliberately conservative: a string literal containing " ORDER BY " is a false
// NEGATIVE (we treat the statement as ordered), which only costs the hybrid
// stream optimization, never correctness. The plan side cannot answer this (see
// Conv.stmtOrderFree).
func statementOrderFree(stmt fmt.Stringer) bool {
	if stmt == nil {
		return false
	}
	return !strings.Contains(strings.ToUpper(stmt.String()), " ORDER BY ")
}

// markIndexScanOrderFree marks each datastore-scan-index(-cover) op whose
// emission order the FINAL plan tree cannot depend on (base.Op.OrderFree):
// either an order-offset-limit ancestor re-sorts anyway, or the statement has no
// ORDER BY at all (stmtOrderFree). A merge-scan subtree flips back to
// order-REQUIRED -- it consumes its children's order. Everything unknown stays
// the conservative default (order-sensitive): a time-scoped index then
// merge-emits its hybrid half in key order, which is always correct.
func markIndexScanOrderFree(op *base.Op, free bool) {
	if op == nil {
		return
	}
	switch {
	case op.Kind == "order-offset-limit":
		// Only a REAL sort re-establishes order: a bare OFFSET/LIMIT wrapper
		// reuses this kind with EMPTY order terms (VisitOffset/VisitLimit) and
		// passes its child's emission order straight through.
		if len(op.Params) > 0 {
			if exprs, ok := op.Params[0].([]interface{}); ok && len(exprs) > 0 {
				free = true
			}
		}
	case strings.Contains(op.Kind, "merge-scan"):
		free = false // a k-way merge RELIES on child emission order
	case op.Kind == "datastore-scan-index" || op.Kind == "datastore-scan-index-cover":
		op.OrderFree = free
	}
	for _, c := range op.Children {
		markIndexScanOrderFree(c, free)
	}
}
