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

// The `.index suggest` advisor (see DESIGN-indexing.md "adaptive auto-index").
// Samples a keyspace's docs, walks their scalar leaf paths, and proposes secondary
// indexes for the *high-cardinality* (selective) fields -- the ones a b-tree
// actually helps. It's read-only: the CLI renders the suggestions as an editable
// .n1k1/catalog.json fragment the user reviews and feeds back.
//
// Field eligibility (the path walk enforces it):
//   - scalar leaf, no array crossed  -> candidate (recurse into objects only, so
//     any path under an array is naturally never recorded);
//   - a path that is ever a non-scalar (object/array) -> excluded (covers type
//     drift and array-valued fields; array fields would need a separate array
//     index, out of scope here);
//   - oversized values, and fields present in too few docs, or with too LOW
//     cardinality (poor selectivity) -> excluded.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"

	"github.com/couchbase/query/datastore"

	"github.com/couchbase/n1k1/records"
)

// Advisor thresholds (deliberately simple + transparent; the Why string reports
// the numbers so the user can judge and the defaults needn't be perfect).
const (
	suggestDefaultSample = 1000
	suggestDistinctCap   = 2048 // stop counting distinct values past this (= "high")
	suggestMaxValueLen   = 512  // skip fields whose scalar values are huge index keys
	suggestMinDistinct   = 8    // below this, cardinality is too low to bother
	suggestUniqDenom     = 5    // require distinct*denom >= present (uniqueness >= 1/denom)
	suggestMaxDepth      = 6    // don't descend nested objects past this
)

// IndexSuggestion is one advised secondary index (a high-cardinality scalar field).
type IndexSuggestion struct {
	Namespace string
	Keyspace  string
	Field     string // the key expression (a field or dotted nested path)
	Name      string // suggested index name
	Sampled   int    // docs sampled
	Present   int    // docs where the field was a scalar leaf
	Distinct  int    // distinct values seen (capped at suggestDistinctCap)
	Capped    bool   // true if Distinct hit the cap (cardinality is at least that)
	Why       string // human-readable rationale
}

type pathStat struct {
	scalar    int
	nonScalar int
	distinct  map[string]struct{}
	capped    bool
	maxLen    int
}

// SuggestIndexes samples up to sampleN docs from each keyspace in namespace (or
// just `only`, if non-empty) and returns advised indexes for selective scalar
// fields, most-selective first. Already-declared leading keys are skipped.
//
// The second return is a human diagnostic that is non-empty only when no
// suggestions were produced -- it explains *why* (an empty keyspace, a sample too
// small to judge cardinality, or fields that just weren't selective enough), so
// the CLI can say something more useful than "nothing found". Sampling itself is
// correct for every layout/format (single-file, flat-root, multi-record, gzip,
// ...) because it goes through openKeyspaceRecords, the shared scan resolver.
func SuggestIndexes(store *Store, namespace, only string, sampleN int) ([]IndexSuggestion, string, error) {
	if sampleN <= 0 {
		sampleN = suggestDefaultSample
	}
	ns, nerr := store.Datastore.NamespaceByName(namespace)
	if nerr != nil {
		return nil, "", fmt.Errorf("namespace %q: %v", namespace, nerr)
	}
	names, kerr := ns.KeyspaceNames()
	if kerr != nil {
		return nil, "", fmt.Errorf("listing keyspaces: %v", kerr)
	}
	sort.Strings(names)

	var out []IndexSuggestion
	totalSampled, keyspaces := 0, 0
	for _, ksName := range names {
		if only != "" && !strings.EqualFold(ksName, only) {
			continue
		}
		keyspaces++
		ks, kerr := ns.KeyspaceByName(ksName)
		if kerr != nil {
			continue
		}
		stats, sampled, err := sampleKeyspace(ks, sampleN)
		if err != nil {
			return nil, "", fmt.Errorf("sampling %s: %v", ksName, err)
		}
		totalSampled += sampled
		existing := existingLeadingKeys(store.Datastore, namespace, ksName)
		out = append(out, scoreSuggestions(namespace, ksName, stats, sampled, existing)...)
	}

	// Most selective (highest distinct) first.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Distinct != out[j].Distinct {
			return out[i].Distinct > out[j].Distinct
		}
		return out[i].Field < out[j].Field
	})

	note := ""
	if len(out) == 0 {
		note = emptySuggestNote(totalSampled, keyspaces)
	}
	return out, note, nil
}

// emptySuggestNote explains why a sample produced no suggestions. The common
// surprise is a *tiny* sample: with fewer than suggestMinDistinct docs, no field
// can reach the distinct-value floor, so even an obviously-unique key like "id"
// is (deliberately) not advised off so little evidence -- say that plainly.
func emptySuggestNote(sampled, keyspaces int) string {
	switch {
	case keyspaces == 0:
		return "no keyspaces to sample"
	case sampled == 0:
		return "sampled 0 docs — the keyspace looks empty"
	case sampled < suggestMinDistinct:
		return fmt.Sprintf("sampled only %d doc(s) — too few to judge cardinality "+
			"(a field needs ≥%d distinct values before it's worth indexing); add more data",
			sampled, suggestMinDistinct)
	default:
		return fmt.Sprintf("sampled %d docs — no field was selective enough "+
			"(needs ≥%d distinct values, present in ≥50%% of docs, and mostly-unique)",
			sampled, suggestMinDistinct)
	}
}

// sampleKeyspace walks up to sampleN docs, accumulating per-path stats.
func sampleKeyspace(ks datastore.Keyspace, sampleN int) (map[string]*pathStat, int, error) {
	opts := ScanWalkOptions
	opts.PathPrefix = ""
	src, err := openKeyspaceRecords(ks, opts)
	if err != nil {
		return nil, 0, err
	}
	defer src.Close()

	stats := map[string]*pathStat{}
	var rec records.Record
	sampled := 0
	for sampled < sampleN {
		ok, err := src.Next(&rec)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			break
		}
		walkScalarPaths(rec.Doc, "", stats, 0)
		sampled++
	}
	return stats, sampled, nil
}

// walkScalarPaths records each scalar leaf's path (recursing into objects only, so
// array-nested paths are never recorded), tracking presence, distinct values, and
// max value length per path.
func walkScalarPaths(doc []byte, prefix string, stats map[string]*pathStat, depth int) {
	if depth > suggestMaxDepth {
		return
	}
	_ = jsonparser.ObjectEach(doc, func(key, value []byte, dt jsonparser.ValueType, _ int) error {
		path := string(key)
		if prefix != "" {
			path = prefix + "." + path
		}
		ps := stats[path]
		if ps == nil {
			ps = &pathStat{distinct: map[string]struct{}{}}
			stats[path] = ps
		}
		switch dt {
		case jsonparser.Object:
			ps.nonScalar++
			walkScalarPaths(value, path, stats, depth+1)
		case jsonparser.Array:
			ps.nonScalar++ // array-valued: not a scalar b-tree candidate; don't recurse
		default: // String, Number, Boolean, Null
			ps.scalar++
			if len(value) > ps.maxLen {
				ps.maxLen = len(value)
			}
			if !ps.capped {
				ps.distinct[string(value)] = struct{}{}
				if len(ps.distinct) >= suggestDistinctCap {
					ps.capped = true
				}
			}
		}
		return nil
	})
}

// scoreSuggestions applies the eligibility + selectivity heuristic to one
// keyspace's path stats.
func scoreSuggestions(namespace, ksName string, stats map[string]*pathStat,
	sampled int, existing map[string]bool) []IndexSuggestion {
	var out []IndexSuggestion
	for path, ps := range stats {
		if ps.nonScalar > 0 || ps.scalar == 0 { // pure scalar leaf only (stable, no array)
			continue
		}
		if ps.maxLen > suggestMaxValueLen { // oversized index keys
			continue
		}
		present := ps.scalar
		if present*2 < sampled { // present in < 50% of sampled docs
			continue
		}
		distinct := len(ps.distinct)
		if distinct < suggestMinDistinct { // too low cardinality
			continue
		}
		if !ps.capped && distinct*suggestUniqDenom < present { // low uniqueness -> weak selectivity
			continue
		}
		if existing[path] { // already declared
			continue
		}
		distStr := strconv.Itoa(distinct)
		if ps.capped {
			distStr = ">=" + strconv.Itoa(suggestDistinctCap)
		}
		out = append(out, IndexSuggestion{
			Namespace: namespace, Keyspace: ksName, Field: path,
			Name:    "auto_" + fsSafe(ksName+"_"+path),
			Sampled: sampled, Present: present, Distinct: distinct, Capped: ps.capped,
			Why: fmt.Sprintf("sampled %d, present %d, distinct %s", sampled, present, distStr),
		})
	}
	return out
}

// existingLeadingKeys returns the set of leading-key expressions already declared
// for namespace:keyspace, so the advisor doesn't re-suggest them.
func existingLeadingKeys(ds datastore.Datastore, namespace, keyspace string) map[string]bool {
	m := map[string]bool{}
	sds, ok := ds.(*siDatastore)
	if !ok {
		return m
	}
	for _, def := range sds.cat.indexesFor(namespace, keyspace) {
		if len(def.Keys) > 0 {
			m[def.Keys[0]] = true
		}
	}
	return m
}
