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

// memSource is a slice-backed Source: it serves a fixed, already-in-memory set of
// records (e.g. the captured result rows of a session TEMP KEYSPACE) with no file,
// decompression, or framing. The records' ID/Doc bytes are held for the source's
// lifetime, so -- unlike the file sources' reused buffers -- they honor the
// borrowed-slice contract trivially (they never change under the caller).
type memSource struct {
	recs []Record
	i    int
}

// NewMemSource returns a Source that yields recs in order, once. It does not copy
// recs, so the caller must not mutate the backing records after handing them over
// (a materialized keyspace treats them as immutable). Closing is a no-op.
func NewMemSource(recs []Record) Source { return &memSource{recs: recs} }

func (m *memSource) Next(rec *Record) (bool, error) {
	if m.i >= len(m.recs) {
		return false, nil
	}
	*rec = m.recs[m.i]
	m.i++
	return true, nil
}

func (m *memSource) Close() error { return nil }
