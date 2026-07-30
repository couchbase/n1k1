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

package main

// `builtin:census.sql++` — the schema census expressed as PURE SQL++, an interim
// sibling of the native Go `builtin:census`. Same params, same output; the Go builtin
// is the comparison ORACLE (see TestCensusSQLMatchesOracle). The point is a census a
// user can READ and FORK into their own specialized census: swap TYPE_NAME for a
// value-bucketer, add a field, change the depth walk — it's just SQL++.
//
// The mergeable core (per (type, path, val_type): docs, first/last-seen, first_id) is
// one GROUP BY over a per-record MAP that flattens each object into path:type entries
// via OBJECT_PAIRS + TYPE_NAME. first_id is META().id (the container id
// path#line@offset) carried through an argmin-as-MIN(ts || "|" || id). Coverage is NOT
// in the core (a ratio doesn't merge — same reason the Go census keeps it out); it is a
// read-time docs/type-total division, computed here from a second per-type COUNT, so
// the emitted rows match the Go builtin exactly.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// censusSQL builds the pure-SQL++ census statement for a keyspace. typeField /
// timeField are the discriminator + timestamp field names (already defaulted);
// depth is 1 or 2; exclude are top-level keys not descended past depth 1.
func censusSQL(keyspace, typeField, timeField string, depth int, exclude []string) string {
	// Depth-2: for each object-valued top-level pair (minus excluded), emit its
	// children as "<parent>.<child>". A nested comprehension + ARRAY_FLATTEN (rather
	// than a correlated multi-range comprehension, which the engine doesn't support).
	depth2 := "[]"
	if depth >= 2 {
		when := `WHEN TYPE_NAME(q.val) = "object"`
		if len(exclude) > 0 {
			quoted := make([]string, len(exclude))
			for i, e := range exclude {
				quoted[i] = strconv.Quote(e)
			}
			when += " AND q.name NOT IN [" + strings.Join(quoted, ", ") + "]"
		}
		depth2 = `ARRAY_FLATTEN(
             (ARRAY (ARRAY {"pp": q.name || "." || c.name, "vt": TYPE_NAME(c.val)}
                       FOR c IN OBJECT_PAIRS(q.val) END)
                FOR q IN OBJECT_PAIRS(r) ` + when + ` END), 1)`
	}
	return `SELECT t.rt AS ` + "`type`" + `, pth.pp AS ` + "`path`" + `, pth.vt AS val_type,
       COUNT(*) AS docs,
       MIN(t.ts) AS first_seen,
       MAX(t.ts) AS last_seen,
       SPLIT(MIN(t.ts || "|" || t.id), "|")[1] AS first_id
FROM (
  SELECT META(r).id AS id, IFMISSING(r.` + "`" + typeField + "`" + `, "") AS rt, r.` + "`" + timeField + "`" + ` AS ts,
         ARRAY_CONCAT(
           (ARRAY {"pp": p.name, "vt": TYPE_NAME(p.val)} FOR p IN OBJECT_PAIRS(r) END),
           ` + depth2 + `
         ) AS paths
  FROM ` + "`" + keyspace + "`" + ` r
) t
UNNEST t.paths AS pth
GROUP BY t.rt, pth.pp, pth.vt`
}

// censusTotalsSQL builds the per-record-type total count (the coverage denominator).
func censusTotalsSQL(keyspace, typeField string) string {
	tf := "`" + typeField + "`"
	return `SELECT IFMISSING(r.` + tf + `, "") AS rt, COUNT(*) AS n FROM ` + "`" + keyspace + "`" + ` r GROUP BY IFMISSING(r.` + tf + `, "")`
}

// censusSQLRow mirrors the census SQL projection (the mergeable core columns).
type censusSQLRow struct {
	Type      string `json:"type"`
	Path      string `json:"path"`
	ValType   string `json:"val_type"`
	Docs      int64  `json:"docs"`
	FirstSeen string `json:"first_seen"`
	LastSeen  string `json:"last_seen"`
	FirstID   string `json:"first_id"`
}

// runBuiltinCensusSQL executes `.multi run --queries builtin:census.sql++?keyspace=...`
// as pure SQL++ and prints rows in the SAME shape as the native builtin:census (so the
// two are byte-comparable). Params match builtin:census: keyspace (required),
// type-field, time-field, depth (default 2), exclude (comma-list).
func (c *cli) runBuiltinCensusSQL(args multiArgs, r queriesRef) {
	keyspace := r.params["keyspace"]
	if keyspace == "" {
		fmt.Fprintf(c.stderr, "%s: .multi run --queries builtin:census.sql++: needs a keyspace, "+
			"e.g. builtin:census.sql++?keyspace=sessions\n", c.prog)
		c.failed = true
		return
	}
	typeField := r.params["type-field"]
	if typeField == "" {
		typeField = "type"
	}
	timeField := r.params["time-field"]
	if timeField == "" {
		timeField = "timestamp"
	}
	depth := 2
	if d := r.params["depth"]; d != "" {
		if n, e := strconv.Atoi(d); e == nil {
			depth = n
		}
	}
	var exclude []string
	if ex := r.params["exclude"]; ex != "" {
		for _, e := range strings.Split(ex, ",") {
			if e = strings.TrimSpace(e); e != "" {
				exclude = append(exclude, e)
			}
		}
	}

	sess, binding, err := c.multiSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		fmt.Fprintf(c.stderr, "%s: .multi run --queries builtin:census.sql++: aborting -- unresolved keyspace above\n", c.prog)
		c.failed = true
		return
	}

	rows, totals, err := runCensusSQL(sess, keyspace, typeField, timeField, depth, exclude)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run --queries builtin:census.sql++: %v\n", c.prog, err)
		c.failed = true
		return
	}
	c.emitCensusSQLRows(rows, totals)
}

// runCensusSQL runs the census + totals statements and returns the parsed rows and the
// per-type totals. Split out so the oracle test can call it directly.
func runCensusSQL(sess *glue.Session, keyspace, typeField, timeField string, depth int, exclude []string) ([]censusSQLRow, map[string]int64, error) {
	cres, err := sess.Run(censusSQL(keyspace, typeField, timeField, depth, exclude))
	if err != nil {
		return nil, nil, err
	}
	rows := make([]censusSQLRow, 0, len(cres.Rows))
	for _, raw := range cres.Rows {
		var row censusSQLRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, nil, fmt.Errorf("parse census row: %v", err)
		}
		rows = append(rows, row)
	}

	tres, err := sess.Run(censusTotalsSQL(keyspace, typeField))
	if err != nil {
		return nil, nil, err
	}
	totals := map[string]int64{}
	for _, raw := range tres.Rows {
		var tr struct {
			RT string `json:"rt"`
			N  int64  `json:"n"`
		}
		if err := json.Unmarshal(raw, &tr); err != nil {
			return nil, nil, fmt.Errorf("parse totals row: %v", err)
		}
		totals[tr.RT] = tr.N
	}
	return rows, totals, nil
}

// emitCensusSQLRows prints the census rows with read-time coverage, sorted by
// (type, path, val_type) and shaped exactly like emitCensus's output.
func (c *cli) emitCensusSQLRows(rows []censusSQLRow, totals map[string]int64) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Type != rows[j].Type {
			return rows[i].Type < rows[j].Type
		}
		if rows[i].Path != rows[j].Path {
			return rows[i].Path < rows[j].Path
		}
		return rows[i].ValType < rows[j].ValType
	})
	type rowOut struct {
		Type      string  `json:"type"`
		Path      string  `json:"path"`
		ValType   string  `json:"val_type"`
		Docs      int64   `json:"docs"`
		Coverage  float64 `json:"coverage"`
		FirstSeen string  `json:"first_seen,omitempty"`
		LastSeen  string  `json:"last_seen,omitempty"`
		FirstID   string  `json:"first_id,omitempty"`
	}
	for _, r := range rows {
		cov := 0.0
		if tot := totals[r.Type]; tot > 0 {
			cov = float64(r.Docs) / float64(tot)
		}
		b, _ := json.Marshal(rowOut{
			Type: r.Type, Path: r.Path, ValType: r.ValType, Docs: r.Docs,
			Coverage: cov, FirstSeen: r.FirstSeen, LastSeen: r.LastSeen, FirstID: r.FirstID,
		})
		fmt.Fprintln(c.out, string(b))
	}
	fmt.Fprintf(c.stderr, "%s%d cell(s) over %d record-type(s) [SQL++]\n",
		c.icon("📇 "), len(rows), len(totals))
}
