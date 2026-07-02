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
// Samples a keyspace's docs, walks their scalar leaf paths, and proposes indexes
// keyed to the query shape each field fits -- a b-tree secondary index (gsi) for
// selective scalars (equality/range) and a bleve full-text index (fts) for text
// fields (SEARCH()). It's read-only: the CLI renders the suggestions as an editable
// .n1k1/catalog.json fragment the user reviews and feeds back.
//
// Field eligibility (the path walk enforces the scalar/array parts):
//   - scalar leaf, no array crossed -> candidate (recurse into objects only, so
//     any path under an array is naturally never recorded);
//   - a path that is ever a non-scalar (object/array) -> excluded (covers type
//     drift and array-valued fields; array fields would need a separate array
//     index, out of scope here);
//   - GSI: high-cardinality (selective), small-valued scalars; oversized, too-few,
//     or too-low-cardinality fields are excluded.
//   - FTS: "texty" strings (mostly multi-word or long) -- poor b-tree keys but good
//     full-text targets. Independent of cardinality/size, so a field can qualify
//     for both (suggested as both, tagged) or neither. With >=2 text fields, a
//     single whole-keyspace dynamic FTS is suggested too.

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

	// FTS ("texty") heuristic: a string field is a full-text candidate when its
	// values are mostly strings and either a good fraction are multi-token or the
	// average length is long -- the shape that wants SEARCH() not a b-tree.
	suggestTextyStrFrac = 80 // >= this % of a field's scalar values must be strings
	suggestTextyWSFrac  = 40 // texty if >= this % of the string values are multi-word
	suggestTextyAvgLen  = 24 // ... or the average string length is at least this
	suggestFTSDynamicN  = 2  // suggest a whole-keyspace dynamic FTS at >= this many text fields
)

// IndexSuggestion is one advised index. Kind is "gsi" (a high-cardinality scalar
// field -- equality/range) or "fts" (a text field -- SEARCH()); the same field can
// yield both when it fits both, each tagged with the query pattern it serves. A
// dynamic whole-keyspace FTS suggestion has an empty Field.
type IndexSuggestion struct {
	Namespace string
	Keyspace  string
	Kind      string // "gsi" | "fts"
	Field     string // the key expression (a field or dotted nested path); "" = dynamic FTS
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

	// String-value stats, for the FTS ("texty") heuristic: how many scalar values
	// were strings, how many of those contained internal whitespace (multi-token),
	// and the total string length (for an average). A field that is mostly
	// multi-word / long text is an FTS candidate, not a b-tree one.
	strCount int
	wsCount  int
	lenSum   int
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
		existing := existingIndexes(store.Datastore, namespace, ksName)
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
		return fmt.Sprintf("sampled %d docs — no field was a selective scalar "+
			"(needs ≥%d distinct values, present in ≥50%% of docs, mostly-unique) "+
			"nor a text field (mostly multi-word/long strings)",
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
			if dt == jsonparser.String {
				ps.strCount++
				ps.lenSum += len(value)
				if hasWhitespace(value) {
					ps.wsCount++
				}
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

// scoreSuggestions applies the eligibility heuristics to one keyspace's path
// stats, emitting both GSI (selective scalar) and FTS (text) suggestions. A field
// that fits both (e.g. a multi-word `title` that is also selective) yields BOTH,
// each tagged with the query pattern it serves, so the user keeps whichever matches
// their queries. When a keyspace has several text fields, a single whole-keyspace
// dynamic FTS index is suggested instead of/besides the per-field ones.
func scoreSuggestions(namespace, ksName string, stats map[string]*pathStat,
	sampled int, existing existingIdx) []IndexSuggestion {
	var out []IndexSuggestion
	textyFields := 0
	for path, ps := range stats {
		if ps.nonScalar > 0 || ps.scalar == 0 { // pure scalar leaf only (stable, no array)
			continue
		}
		present := ps.scalar
		if present*2 < sampled { // present in < 50% of sampled docs
			continue
		}

		// FTS candidate: a text field (mostly strings, and either multi-word or long).
		// FTS ignores the b-tree gates (cardinality, oversized keys) -- long prose is
		// exactly what it wants. A dynamic FTS already covers every field, so skip.
		if !existing.ftsDynamic && !existing.fts[path] && isTexty(ps, present) {
			textyFields++
			avg := ps.lenSum / max1(ps.strCount)
			wsPct := ps.wsCount * 100 / max1(ps.strCount)
			out = append(out, IndexSuggestion{
				Namespace: namespace, Keyspace: ksName, Kind: "fts", Field: path,
				Name:    "ft_" + fsSafe(ksName+"_"+path),
				Sampled: sampled, Present: present, Distinct: len(ps.distinct), Capped: ps.capped,
				Why: fmt.Sprintf("full-text SEARCH(): present %d, ~%d chars avg, %d%% multi-word",
					present, avg, wsPct),
			})
		}

		// GSI candidate: a selective, small, high-cardinality scalar (equality/range).
		if ps.maxLen > suggestMaxValueLen { // oversized b-tree keys
			continue
		}
		distinct := len(ps.distinct)
		if distinct < suggestMinDistinct { // too low cardinality
			continue
		}
		if !ps.capped && distinct*suggestUniqDenom < present { // low uniqueness -> weak selectivity
			continue
		}
		if existing.gsi[path] { // already declared
			continue
		}
		distStr := strconv.Itoa(distinct)
		if ps.capped {
			distStr = ">=" + strconv.Itoa(suggestDistinctCap)
		}
		out = append(out, IndexSuggestion{
			Namespace: namespace, Keyspace: ksName, Kind: "gsi", Field: path,
			Name:    "auto_" + fsSafe(ksName+"_"+path),
			Sampled: sampled, Present: present, Distinct: distinct, Capped: ps.capped,
			Why: fmt.Sprintf("equality/range: sampled %d, present %d, distinct %s",
				sampled, present, distStr),
		})
	}

	// A keyspace with several text fields is better served by one dynamic FTS index
	// (indexes every field) than many single-field ones -- suggest that too.
	if textyFields >= suggestFTSDynamicN && !existing.ftsDynamic {
		out = append(out, IndexSuggestion{
			Namespace: namespace, Keyspace: ksName, Kind: "fts", Field: "",
			Name: "ft_" + fsSafe(ksName) + "_all",
			Why: fmt.Sprintf("full-text SEARCH() across %d text fields (dynamic mapping, all fields)",
				textyFields),
		})
	}
	return out
}

// isTexty reports whether a field's values look like free text (an FTS candidate
// rather than a b-tree key): mostly string-valued, and either a good fraction are
// multi-token (contain whitespace) or the average length is long.
func isTexty(ps *pathStat, present int) bool {
	if ps.strCount*100 < present*suggestTextyStrFrac { // not predominantly strings
		return false
	}
	avg := ps.lenSum / max1(ps.strCount)
	wsPct := ps.wsCount * 100 / max1(ps.strCount)
	return wsPct >= suggestTextyWSFrac || avg >= suggestTextyAvgLen
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// hasWhitespace reports whether b contains an internal space/tab/newline (a
// multi-token string), the cheap "is this free text" signal.
func hasWhitespace(b []byte) bool {
	for _, c := range b {
		if c == ' ' || c == '\t' || c == '\n' {
			return true
		}
	}
	return false
}

// existingIdx captures what's already declared for a keyspace, per kind, so the
// advisor doesn't re-suggest it. Dedup is per-kind: a GSI index on `title` must not
// suppress an FTS suggestion for `title` (the "both" case), and vice versa.
type existingIdx struct {
	gsi        map[string]bool // gsi leading-key expressions already declared
	fts        map[string]bool // fts declared field-path keys
	ftsDynamic bool            // a dynamic (no-keys) fts index exists -> covers every field
}

// existingIndexes returns what's already declared for namespace:keyspace.
func existingIndexes(ds datastore.Datastore, namespace, keyspace string) existingIdx {
	e := existingIdx{gsi: map[string]bool{}, fts: map[string]bool{}}
	sds, ok := ds.(*siDatastore)
	if !ok {
		return e
	}
	for _, def := range sds.cat.indexesFor(namespace, keyspace) {
		if def.isFTS() {
			if len(def.Keys) == 0 {
				e.ftsDynamic = true
			}
			for _, k := range def.Keys {
				e.fts[k] = true
			}
			continue
		}
		if len(def.Keys) > 0 {
			e.gsi[def.Keys[0]] = true
		}
	}
	return e
}
