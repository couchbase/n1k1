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

// Census-cursor support (DESIGN-census.md Phase 3): an incremental census. Because
// every census column is a mergeable aggregate, census(A) ⊎ census(B) ==
// census(A ∪ B), so folding a window's partial census into an accumulated one needs
// NO re-read of history — just this monoid merge, plus a diff for the drift alarm.
// The accumulated census AND its watermark live together in one cursor-state file,
// so they commit atomically (retiring the two-store double-count wall an external
// two-file loop hits).

import "sort"

func censusKey(r CensusRow) string { return r.Type + "\x00" + r.Path + "\x00" + r.ValType }
func censusTP(r CensusRow) string  { return r.Type + "\x00" + r.Path }

// MergeCensus folds a window's partial census into an accumulated one: per
// (type,path,val_type) cell docs SUM, first_seen MIN, last_seen MAX (first_id
// follows the earlier first_seen); per-type totals and record counts sum. The
// result is a new CensusResult (inputs unmutated), rows sorted.
func MergeCensus(acc, window *CensusResult) *CensusResult {
	cells := map[string]CensusRow{}
	add := func(rows []CensusRow) {
		for _, r := range rows {
			k := censusKey(r)
			if cur, ok := cells[k]; ok {
				cells[k] = mergeCell(cur, r)
			} else {
				cells[k] = r
			}
		}
	}
	if acc != nil {
		add(acc.Rows)
	}
	if window != nil {
		add(window.Rows)
	}

	out := &CensusResult{TypeTotals: map[string]int64{}}
	for _, r := range cells {
		out.Rows = append(out.Rows, r)
	}
	sortCensusRows(out.Rows)
	for _, src := range []*CensusResult{acc, window} {
		if src == nil {
			continue
		}
		out.Records += src.Records
		for t, n := range src.TypeTotals {
			out.TypeTotals[t] += n
		}
	}
	return out
}

func mergeCell(a, b CensusRow) CensusRow {
	out := a
	out.Docs = a.Docs + b.Docs
	out.LastSeen = maxStr(a.LastSeen, b.LastSeen)
	// first_seen = the earlier non-empty timestamp; first_id follows it.
	af, bf := a.FirstSeen, b.FirstSeen
	switch {
	case af == "":
		out.FirstSeen, out.FirstID = bf, b.FirstID
	case bf == "":
		out.FirstSeen, out.FirstID = af, a.FirstID
	case bf < af:
		out.FirstSeen, out.FirstID = bf, b.FirstID
	default:
		out.FirstSeen, out.FirstID = af, a.FirstID
	}
	return out
}

func maxStr(a, b string) string {
	if b > a {
		return b
	}
	return a
}

func sortCensusRows(rows []CensusRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Type != rows[j].Type {
			return rows[i].Type < rows[j].Type
		}
		if rows[i].Path != rows[j].Path {
			return rows[i].Path < rows[j].Path
		}
		return rows[i].ValType < rows[j].ValType
	})
}

// CensusChange is one schema-drift event from folding a window into an accumulated
// census: a cell (type,path,val_type) the accumulated census had never seen.
type CensusChange struct {
	Op        string `json:"op"` // "field_added" | "type_changed"
	Type      string `json:"type"`
	Path      string `json:"path"`
	ValType   string `json:"val_type"`
	Docs      int64  `json:"docs"`                 // occurrences in THIS window
	FirstSeen string `json:"first_seen,omitempty"` // when it first appeared in the window
	FirstID   string `json:"first_id,omitempty"`
}

// CensusDrift diffs a window's cells against the prior accumulated census: a cell
// whose (type,path,val_type) is new is either a `type_changed` (its (type,path)
// existed with a different value-type) or a `field_added` (a wholly new path). For
// an append-only corpus these are the drift signals; a field never disappears (that
// is a stale-last_seen check, not a diff). Sorted for stable output.
func CensusDrift(prior, window []CensusRow) []CensusChange {
	seenTPV := map[string]bool{}
	seenTP := map[string]bool{}
	for _, r := range prior {
		seenTPV[censusKey(r)] = true
		seenTP[censusTP(r)] = true
	}
	var out []CensusChange
	for _, r := range window {
		if seenTPV[censusKey(r)] {
			continue
		}
		op := "field_added"
		if seenTP[censusTP(r)] {
			op = "type_changed"
		}
		out = append(out, CensusChange{
			Op: op, Type: r.Type, Path: r.Path, ValType: r.ValType,
			Docs: r.Docs, FirstSeen: r.FirstSeen, FirstID: r.FirstID,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type < out[j].Type
		}
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].ValType < out[j].ValType
	})
	return out
}
