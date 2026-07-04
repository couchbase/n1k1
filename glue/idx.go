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

	"github.com/couchbase/query/datastore"
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
	scanSpan(span *datastore.Span, limit int64, seen map[string]bool,
		projectKeys bool, conn *datastore.IndexConnection)
}

// sourceSignature summarizes a keyspace directory for change detection: file
// count and the newest mtime (nanoseconds) over the whole tree. This is the
// simple "assume static data, validate by timestamp" model -- adding, removing,
// or touching any file changes the signature and forces an index rebuild. Shared
// by the bbolt (idx_si.go) and in-memory (idx_mem.go) backends.
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
