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

// THE FROZEN CENSUS ORACLE (test-only). This is the retired native Go census, kept
// as CI's independent second opinion on the shipped forkable censuses
// (builtin:census.sql++ and extensions/functions/js/census_agg.agg.js) — see
// TestCensusForkableDifferential. It is deliberately a _test.go file: no user
// surface, no docs, no support, and NO further optimization or features. An oracle
// does not need to be fast or featureful; it needs to be DIFFERENT — an independent
// implementation of the census spec that fails when a forkable implementation's
// author's belief is the bug (ISSUE-22: two real bugs — a missed polymorphic parent
// cell, an exclude-vs-omit conflation — were caught only by this differential,
// never by the implementations' own goldens). Treat edits here as spec changes:
// they require the same edit, independently reasoned, in both forkable censuses.
//
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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/n1k1/records"
)

// CensusOptions configures a census run.
type CensusOptions struct {
	TypeField string   // record-type discriminator (default "type"); "" bucket if absent
	TimeField string   // timestamp field for first/last-seen (default "timestamp")
	Depth     int      // max path depth, 1 or 2 (default 2)
	Exclude   []string // top-level keys NOT descended past depth 1 (key-space explosion guard)

	// Since, when non-nil, censuses ONLY records past this per-container append
	// watermark (the incremental / census-cursor case). nil censuses the whole
	// keyspace. Either way CensusResult.NewWater reports the extent scanned, so a
	// window's partial census merges into an accumulated one (the monoid).
	// SinceFP carries the committed boundary-record fingerprints (CursorState.WaterFP)
	// for rewrite detection; nil skips it.
	Since   map[string]int64
	SinceFP map[string]string
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
// the per-type record totals (the coverage denominator), the total scanned, and the
// recomputed append watermark (the extent scanned — a census-cursor's next `since`).
type CensusResult struct {
	Rows       []CensusRow
	TypeTotals map[string]int64
	Records    int64
	NewWater   map[string]int64
	// Rotated / Truncated / Rewritten disclose committed containers whose source
	// violated append-only this scan (RecordScanFilter.SourceAnomalies) -- a census
	// fold over a rotating corpus must know when evidence left, not just when it
	// arrived. Observed/ObservedFP feed the WaterFP commit (WaterFPMerge).
	Rotated    []string
	Truncated  []string
	Rewritten  []string
	Observed   map[string]int64
	ObservedFP map[string]string
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

	// Filter to records past the watermark (incremental census) and track the extent
	// scanned. A nil Since admits everything (a full census) and still reports the head.
	filter := NewRecordScanFilter(opts.Since, opts.SinceFP)
	scan := filter.wrap(src)

	cells := map[string]*censusCell{}
	typeTotals := map[string]int64{}
	var total int64
	var rec records.Record
	for {
		ok, e := scan.Next(&rec)
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
			// `_meta` is the ENGINE describing the container (-meta on), never corpus
			// schema — a census reporting the observer as part of the observed is
			// wrong by construction (ISSUE-20: 7 phantom fields, 100% coverage,
			// indistinguishable from data).
			if k == "_meta" {
				continue
			}
			emit(k, censusTypeName(v))
			if opts.Depth >= 2 && !exclude[k] {
				if child, isObj := v.(map[string]interface{}); isObj {
					for ck, cv := range child {
						emit(k+"."+ck, censusTypeName(cv))
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
	rotated, truncated, rewritten := filter.SourceAnomalies()
	return &CensusResult{Rows: rows, TypeTotals: typeTotals, Records: total,
		NewWater: filter.NewWater(), Rotated: rotated, Truncated: truncated, Rewritten: rewritten,
		Observed: filter.ObservedWater(), ObservedFP: filter.FingerprintWater()}, nil
}

// censusTypeName is jsonTypeName spelled to match SQL++ TYPE_NAME (so a census
// diffs cleanly against a TYPE_NAME-based one): boolean, not bool.
func censusTypeName(v interface{}) string {
	if t := jsonTypeName(v); t != "bool" {
		return t
	}
	return "boolean"
}

func stringField(doc map[string]interface{}, field string) string {
	if v, ok := doc[field].(string); ok {
		return v
	}
	return ""
}

// TestCensusForkableDifferential is ISSUE-22's ask made CI: the frozen Go oracle
// (above) is compared cell-for-cell against BOTH shipped forkable censuses --
// builtin:census.sql++ (read from the repo template; rendered with glue's own param
// machinery) and census_agg (the bundled extensions/functions/builtin_census_agg.js
// module) -- over an adversarial corpus. The shared core is
// (type, path, val_type) -> docs/first_seen/last_seen; implementation-specific
// columns (coverage, first_id, first_enc, docs_in_type, examples) are outside it.
// Three independent implementations in three languages: a bug in any one shows up as
// a named cell-level disagreement here, the failure mode goldens cannot catch (both
// bugs ISSUE-22 reports were invisible to the buggy implementation's own goldens).
func TestCensusForkableDifferential(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "c.jsonl"), []byte(strings.Join([]string{
		`{"type":"a","timestamp":"2026-01-02","uuid":"u2","message":{"id":"m1","model":"x"},"n":5}`,
		`{"type":"a","timestamp":"2026-01-01","uuid":"u1","message":{"id":"m0"},"n":9,"extra":true}`,
		`{"type":"a","message":{"id":"m2"},"n":1,"_meta":{"path":"injected","size":9}}`,
		`{"type":"b","timestamp":"2026-01-05","uuid":"u5","toolUseResult":{"deep":"skip"},"k":[1,2]}`,
		`{"note":"no-type-field","timestamp":"2026-01-09","deep":{"lvl2":{"lvl3":"stop"}}}`,
		`{"type":"c","solo":true}`, // no timestamps: first/last_seen omitted everywhere
		`{"type":"b","timestamp":"2026-01-06","poly":{"x":1}}`,
		`{"type":"b","timestamp":"2026-01-04","poly":"scalar-now"}`, // polymorphic parent cell
	}, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	if _, err := RegisterExtensionFile("../extensions/functions/builtin_census_agg.js"); err != nil {
		t.Fatalf("register bundled census_agg: %v", err)
	}
	template, err := os.ReadFile("../cmd/n1k1/builtins/census.sql++")
	if err != nil {
		t.Fatalf("read census.sql++ template: %v", err)
	}

	// The shared core: cell key -> "docs|first_seen|last_seen".
	coreAdd := func(core map[string]string, typ, path, vt string, docs int64, first, last string) {
		core[typ+"\x00"+path+"\x00"+vt] = fmt.Sprintf("%d|%s|%s", docs, first, last)
	}
	fromJSONRows := func(rows []map[string]interface{}) map[string]string {
		core := map[string]string{}
		for _, m := range rows {
			s := func(k string) string { v, _ := m[k].(string); return v }
			d, _ := m["docs"].(float64)
			coreAdd(core, s("type"), s("path"), s("val_type"), int64(d), s("first_seen"), s("last_seen"))
		}
		return core
	}

	oracleCore := func(opts CensusOptions) map[string]string {
		res, cerr := sess.Census("*.jsonl", opts)
		if cerr != nil {
			t.Fatalf("oracle census: %v", cerr)
		}
		core := map[string]string{}
		for _, r := range res.Rows {
			coreAdd(core, r.Type, r.Path, r.ValType, r.Docs, r.FirstSeen, r.LastSeen)
		}
		return core
	}

	sqlppCore := func(given map[string]string) map[string]string {
		params, perr := ScanQueryParams(string(template))
		if perr != nil {
			t.Fatalf("scan census.sql++ params: %v", perr)
		}
		resolved, rerr := ParamsResolve("census.sql++", params, given)
		if rerr != nil {
			t.Fatalf("resolve census.sql++ params: %v", rerr)
		}
		sql, serr := RenderStmtParams("census.sql++", string(template), params, resolved)
		if serr != nil {
			t.Fatalf("render census.sql++: %v", serr)
		}
		res, qerr := sess.Run(sql)
		if qerr != nil {
			t.Fatalf("run census.sql++: %v", qerr)
		}
		var rows []map[string]interface{}
		for _, raw := range res.Rows {
			var m map[string]interface{}
			if json.Unmarshal(raw, &m) != nil {
				t.Fatalf("census.sql++ row not an object: %s", raw)
			}
			rows = append(rows, m)
		}
		return fromJSONRows(rows)
	}

	aggCore := func() map[string]string {
		res, qerr := sess.Run("SELECT RAW census_agg(r) FROM `*.jsonl` AS r")
		if qerr != nil {
			t.Fatalf("run census_agg: %v", qerr)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("census_agg: want 1 row, got %d", len(res.Rows))
		}
		var cells []map[string]interface{}
		if err := json.Unmarshal(res.Rows[0], &cells); err != nil {
			t.Fatalf("census_agg cells: %v", err)
		}
		return fromJSONRows(cells)
	}

	diff := func(t *testing.T, name string, want, got map[string]string) {
		t.Helper()
		for k, w := range want {
			g, ok := got[k]
			if !ok {
				t.Errorf("%s: cell only in oracle: %q", name, strings.ReplaceAll(k, "\x00", ":"))
			} else if g != w {
				t.Errorf("%s: cell %q differs: oracle=%s got=%s",
					name, strings.ReplaceAll(k, "\x00", ":"), w, g)
			}
		}
		for k := range got {
			if _, ok := want[k]; !ok {
				t.Errorf("%s: cell only in %s: %q", name, name, strings.ReplaceAll(k, "\x00", ":"))
			}
		}
	}

	// census_agg's bundled config is fixed (type/timestamp, depth 2, no exclusions):
	// the three-way comparison runs on that variant; the other oracle-vs-sql++
	// variants exercise depth/exclude.
	t.Run("three-way depth-2", func(t *testing.T) {
		oracle := oracleCore(CensusOptions{Depth: 2})
		if len(oracle) == 0 {
			t.Fatal("oracle produced no cells")
		}
		diff(t, "census.sql++", oracle, sqlppCore(map[string]string{
			"keyspace": "*.jsonl", "type_field": "type", "time_field": "timestamp", "depth": "2"}))
		diff(t, "census_agg", oracle, aggCore())
	})
	t.Run("oracle-vs-sql++ exclude", func(t *testing.T) {
		diff(t, "census.sql++", oracleCore(CensusOptions{Depth: 2, Exclude: []string{"toolUseResult"}}),
			sqlppCore(map[string]string{"keyspace": "*.jsonl", "type_field": "type",
				"time_field": "timestamp", "depth": "2", "exclude": "toolUseResult"}))
	})
	t.Run("oracle-vs-sql++ depth-1", func(t *testing.T) {
		diff(t, "census.sql++", oracleCore(CensusOptions{Depth: 1}),
			sqlppCore(map[string]string{"keyspace": "*.jsonl", "type_field": "type",
				"time_field": "timestamp", "depth": "1"}))
	})
}
