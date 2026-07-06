//go:build n1ql && wasm

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

// WASM build wiring for the secondary-index subsystem. The bbolt GSI
// (idx_si.go) and bleve FTS (idx_fts.go, idx_si_suggest.go) backends depend on
// mmap/flock syscalls that don't exist under GOOS=js, so they're excluded from
// the browser build (`//go:build n1ql && !wasm`). The mmap-free in-memory
// backend (idx_mem.go) is compiled in all builds and IS the secondary-index
// path here: maybeSecondaryIndexes routes to it, so browser queries get real
// IndexScans (visible in EXPLAIN) over catalog-declared indexes.
package glue

import (
	"github.com/couchbase/query/datastore"
)

// maybeSecondaryIndexes uses the in-memory backend under WASM (bbolt is absent).
// Mirrors the !wasm signature in idx_si.go.
func maybeSecondaryIndexes(dataRoot string, ds datastore.Datastore) (datastore.Datastore, error) {
	return memIndexesMaybe(dataRoot, ds)
}
