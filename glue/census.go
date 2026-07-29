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

// Schema census (DESIGN-census.md) — a time-aware, type-aware key-space census over
// an append-only corpus of schemaless records. For every (record-type, field-path,
// value-type) it counts docs and tracks the first/last time it was seen and the id
// of the record that first carried it. This is the substrate for "are my standing
// questions still connected to reality?": fields are born, die, fork spellings, and
// change shape in agent-exhaust / LLM-emitted data with no declared schema, and only
// a census with a TIME AXIS (not one-shot inference) surfaces that drift.
//
// Every column is a MERGEABLE aggregate (docs=SUM, first=MIN, last=MAX), so
// census(A) ⊎ census(B) == census(A ∪ B) — the property that makes an incremental,
// cursored census possible with no re-read (the reduce is a plain re-aggregation).
// Deliberately NOT here: coverage% (a ratio doesn't merge — keep docs + the per-type
// denominator apart and divide at read time) and COUNT(DISTINCT) (needs the whole
// set or a sketch — that's value-level census, a separate node).
//
// This runs as a self-contained scan+aggregate over records.Source (like Compose),
// aggregating in flight — so it emits ~one row per (type,path,valtype) rather than a
// row per record, which is what makes a native operator near-free versus the
// prototype's map/reduce over 184k array-carrying intermediate rows.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/n1k1/records"
)

// CensusOptions configures a census run.
type CensusOptions struct {
	TypeField string   // record-type discriminator (default "type"); "" bucket if absent
	TimeField string   // timestamp field for first/last-seen (default "timestamp")
	Depth     int      // max path depth, 1 or 2 (default 2)
	Exclude   []string // top-level keys NOT descended past depth 1 (key-space explosion guard)
}

// CensusRow is one (type, path, value-type) cell of the census. Coverage is left to
// the reader (docs / CensusResult.TypeTotals[type]) so the stored columns stay
// mergeable.
type CensusRow struct {
	Type      string `json:"type"`
	Path      string `json:"path"`
	ValType   string `json:"val_type"`
	Docs      int64  `json:"docs"`
	FirstSeen string `json:"first_seen,omitempty"`
	LastSeen  string `json:"last_seen,omitempty"`
	FirstID   string `json:"first_id,omitempty"` // id of the record that first carried this cell
}

// CensusResult is a whole census: the per-cell rows (sorted by type/path/val_type),
// the per-type record totals (the coverage denominator), and the total scanned.
type CensusResult struct {
	Rows       []CensusRow
	TypeTotals map[string]int64
	Records    int64
}

type censusCell struct {
	docs      int64
	first     string // min timestamp seen
	last      string // max timestamp seen
	firstComp string // min("<ts>|<id>") — argmin-as-MIN, so it merges
}

// Census scans a keyspace once and returns a per-(type,path,value-type) census. The
// keyspace resolves through the session (so a --bind-bound logical keyspace works).
func (s *Session) Census(keyspace string, opts CensusOptions) (*CensusResult, error) {
	if opts.TypeField == "" {
		opts.TypeField = "type"
	}
	if opts.TimeField == "" {
		opts.TimeField = "timestamp"
	}
	if opts.Depth <= 0 {
		opts.Depth = 2
	}
	exclude := map[string]bool{}
	for _, e := range opts.Exclude {
		exclude[e] = true
	}

	ns, nerr := s.Store.Datastore.NamespaceByName(s.Namespace)
	if nerr != nil {
		return nil, fmt.Errorf("namespace %q: %v", s.Namespace, nerr)
	}
	ks, kerr := ns.KeyspaceByName(keyspace)
	if kerr != nil {
		return nil, fmt.Errorf("keyspace %q: %v", keyspace, kerr)
	}

	gctx := NewGlueContext(time.Now())
	src, err := KeyspaceRecordsOpen(ks, ScanWalkOptions, gctx)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	cells := map[string]*censusCell{}
	typeTotals := map[string]int64{}
	var total int64
	var rec records.Record
	for {
		ok, e := src.Next(&rec)
		if e != nil {
			return nil, e
		}
		if !ok {
			break
		}
		var doc map[string]interface{}
		if json.Unmarshal(rec.Doc, &doc) != nil {
			continue // non-object record (scalar/array top-level): no key space to census
		}
		total++
		rt := stringField(doc, opts.TypeField)
		ts := stringField(doc, opts.TimeField)
		id := string(rec.ID)
		typeTotals[rt]++

		emit := func(path, vt string) {
			k := rt + "\x00" + path + "\x00" + vt
			c := cells[k]
			if c == nil {
				c = &censusCell{}
				cells[k] = c
			}
			c.docs++
			if ts != "" {
				if c.first == "" || ts < c.first {
					c.first = ts
				}
				if ts > c.last {
					c.last = ts
				}
				if comp := ts + "|" + id; c.firstComp == "" || comp < c.firstComp {
					c.firstComp = comp
				}
			}
		}
		for k, v := range doc {
			emit(k, jsonTypeName(v))
			if opts.Depth >= 2 && !exclude[k] {
				if child, isObj := v.(map[string]interface{}); isObj {
					for ck, cv := range child {
						emit(k+"."+ck, jsonTypeName(cv))
					}
				}
			}
		}
	}

	rows := make([]CensusRow, 0, len(cells))
	for k, c := range cells {
		p := strings.SplitN(k, "\x00", 3)
		row := CensusRow{Type: p[0], Path: p[1], ValType: p[2], Docs: c.docs, FirstSeen: c.first, LastSeen: c.last}
		if i := strings.IndexByte(c.firstComp, '|'); i >= 0 {
			row.FirstID = c.firstComp[i+1:]
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Type != rows[j].Type {
			return rows[i].Type < rows[j].Type
		}
		if rows[i].Path != rows[j].Path {
			return rows[i].Path < rows[j].Path
		}
		return rows[i].ValType < rows[j].ValType
	})
	return &CensusResult{Rows: rows, TypeTotals: typeTotals, Records: total}, nil
}

func stringField(doc map[string]interface{}, field string) string {
	if v, ok := doc[field].(string); ok {
		return v
	}
	return ""
}
