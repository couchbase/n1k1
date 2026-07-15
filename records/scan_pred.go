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

package records

// Predicate pushdown -- a NEUTRAL, format-agnostic representation of a WHERE that a
// Source may use to skip data it can prove won't match (e.g. an Iceberg source pruning
// whole Parquet files by manifest column stats). Kept free of any format's expression
// types so it links in every build (incl. wasm) and no source is forced to depend on
// another's predicate library. See RowFilterer + records/iceberg.go.
//
// A ScanPredicate is only ever a HINT: the query engine still applies the real filter
// downstream, so a source that applies it partially, loosely (returns a superset), or
// not at all is always correct. That's what lets pushdown stay opt-in and best-effort.

// ScanClause is one pushable predicate on a single field. Op selects the shape:
//   - comparison ("eq"/"ne"/"lt"/"le"/"gt"/"ge"): uses Const.
//   - membership ("in"/"notin"): uses Consts.
//   - null test ("isnull"/"notnull"): uses neither.
type ScanClause struct {
	Field  string
	Op     string        // eq|ne|lt|le|gt|ge|in|notin|isnull|notnull
	Const  interface{}   // float64 | string | bool (comparisons)
	Consts []interface{} // membership list (in/notin)
}

// ScanPredicate is a flat conjunction or disjunction of clauses -- a single predicate is
// Mode "and" with one clause. It stays flat by design: under an AND, an unpushable conjunct
// is simply omitted (that only WIDENS the pruning filter, which is safe -- the engine still
// applies the real WHERE), so a mixed `pushable AND weird(...)` still prunes on the pushable
// part. An OR is all-or-nothing (dropping a branch would NARROW the filter and could prune
// matching rows), and genuinely nested boolean isn't represented (it just isn't pushed).
type ScanPredicate struct {
	Mode    string // "and" | "or"
	Clauses []ScanClause
}

// RowFilterer is a Source that accepts a ScanPredicate as a best-effort pruning hint.
// SetRowFilter MUST be called before the first Next. A returned error means the hint
// couldn't be applied and is treated by the caller as "no pushdown" (the engine's filter
// still runs) -- NEVER as a query failure. A source is free to not implement this at all.
type RowFilterer interface {
	SetRowFilter(ScanPredicate) error
}
