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

// WASM build stub for the secondary-index subsystem. The real GSI (bbolt) and
// FTS (bleve) index files (idx_si.go, idx_fts.go, idx_si_suggest.go) depend on
// mmap/flock syscalls that don't exist under GOOS=js, so they're excluded from
// the browser build (`//go:build n1ql && !wasm`). This file supplies the handful
// of symbols the always-compiled core (stmt.go, conv.go, datastore_scan.go)
// still references, so glue compiles to WASM. At runtime no secondaryIndex is
// ever created (maybeSecondaryIndexes is a pass-through), so the type-assertion
// paths that mention *secondaryIndex are simply never taken -- the browser demo
// runs every query as a primary scan over the in-memory JSON datastore.
package glue

import (
	"github.com/couchbase/query/datastore"
)

// maybeSecondaryIndexes is a no-op under WASM: there are no on-disk bbolt/bleve
// indexes to advertise, so the datastore is returned unwrapped and every query
// plans as a primary scan. Mirrors the !wasm signature in idx_si.go.
func maybeSecondaryIndexes(dataRoot string, ds datastore.Datastore) (datastore.Datastore, error) {
	return ds, nil
}

// secondaryIndex is a stub of the bbolt-backed index type. It exists only so the
// `scan.Index().(*secondaryIndex)` type assertions in conv.go and
// datastore_scan.go compile; the assertions never succeed under WASM (nothing
// constructs one), so def and scanSpan are never dereferenced/called. The
// embedded datastore.Index gives it that interface's method set (a nil value,
// never invoked) so the type assertions are legal rather than "impossible".
type secondaryIndex struct {
	datastore.Index
	def *indexDef
}

// scanSpan is never invoked under WASM (no secondaryIndex is ever created); it
// exists to satisfy the call site in datastore_scan.go's scanSISpans.
func (si *secondaryIndex) scanSpan(span *datastore.Span, limit int64,
	seen map[string]bool, projectKeys bool, conn *datastore.IndexConnection) {
}
