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
// value-bucketer, add a field, change the depth walk — it's just SQL++. (See the exact
// SQL with `.multi show --queries builtin:census.sql++`.)
//
// PERF: identical output to the Go builtin, but ~6x slower (measured 23.9s vs 3.85s over
// 195k records) — the MAP materializes ~1 array-carrying row per record and UNNEST fans
// it to millions of path rows before the GROUP BY, where the Go operator aggregates
// in-flight (~1 row per cell). So: builtin:census for production, census.sql++ to fork.
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

	builtinq "github.com/couchbase/n1k1/cmd/n1k1/builtins"
	"github.com/couchbase/n1k1/glue"
)

func init() {
	// census.sql++ needs a post-processing step the generic runner doesn't have:
	// the read-time coverage join against census_totals.sql++ (a stored ratio
	// wouldn't merge — numerator and denominator each do).
	builtinSQLOverride["census.sql++"] = (*cli).runBuiltinCensusSQL
}

// renderBuiltinSQL resolves + renders one embedded builtin with the given URI
// params (defaults applied, unknown/missing-required params error loudly).
func renderBuiltinSQL(name string, params map[string]string) (string, error) {
	q, ok := builtinq.Lookup(name)
	if !ok {
		return "", fmt.Errorf("no embedded builtin %q", name)
	}
	resolved, err := q.Resolve(params)
	if err != nil {
		return "", err
	}
	return q.Render(resolved)
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
// as pure SQL++ (the embedded builtins/census.sql++ template) and prints rows in the
// SAME shape as the native builtin:census (so the two are byte-comparable). Params
// are the file's declared set: keyspace (required), type-field, time-field, depth,
// exclude — census_totals.sql++ receives the subset it shares.
func (c *cli) runBuiltinCensusSQL(args multiArgs, r queriesRef, q builtinq.Query) {
	resolved, err := q.Resolve(r.params)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
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

	rows, totals, err := runCensusSQL(sess, resolved)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run --queries builtin:census.sql++: %v\n", c.prog, err)
		c.failed = true
		return
	}
	c.emitCensusSQLRows(rows, totals)
}

// runCensusSQL renders + runs the census and totals statements and returns the parsed
// rows and per-type totals. resolved is census.sql++'s full resolved param set;
// totals receives the subset it declares.
func runCensusSQL(sess *glue.Session, resolved map[string]string) ([]censusSQLRow, map[string]int64, error) {
	censusStmt, err := renderBuiltinSQL("census.sql++", resolved)
	if err != nil {
		return nil, nil, err
	}
	totalsStmt, err := renderBuiltinSQL("census_totals.sql++", map[string]string{
		"keyspace": resolved["keyspace"], "type-field": resolved["type-field"],
	})
	if err != nil {
		return nil, nil, err
	}
	cres, err := sess.Run(censusStmt)
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

	tres, err := sess.Run(totalsStmt)
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
