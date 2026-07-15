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

// Time-travel -- select a PAST snapshot of a table instead of its current one. Neutral of
// any format's types (so it links in every build), analogous to ScanPredicate/RowFilterer.
// Unlike a predicate (a silent optimization), a snapshot selection is EXPLICIT: the user
// asked for it, so a source applies it verbatim and an unknown snapshot surfaces as the
// scan's natural result (an error, or an empty read) rather than being silently ignored.

// ScanSnapshot selects a table snapshot. Mode "" means "current" (no time-travel).
type ScanSnapshot struct {
	Mode   string // "" (current) | "id" | "asof"
	ID     int64  // snapshot id, when Mode == "id"
	AsOfMs int64  // Unix milliseconds, when Mode == "asof" (the latest snapshot at/<= this)
}

// Snapshotter is a Source that can read a past snapshot. SetSnapshot MUST be called before
// the first Next. A zero-Mode ScanSnapshot is a no-op (current snapshot).
type Snapshotter interface {
	SetSnapshot(ScanSnapshot) error
}
