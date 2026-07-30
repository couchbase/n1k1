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

// emitCensus prints a time-aware, type-aware key-space census: per (record-type,
// field-path, value-type) it reports docs, coverage of its type, first/last-seen, and
// the id of the first record to carry it — the answer to "is a field being born /
// dying / changing shape?" that one-shot schema inference and stateless engines can't
// give. Reached via `.multi run --queries "builtin:census?keyspace=<ks>"` (the census
// is a queries source in the algebra, not a verb). See DESIGN-census.md.

import (
	"encoding/json"
	"fmt"

	"github.com/couchbase/n1k1/glue"
)

// emitCensus runs a census of keyspace under opts and prints it: NDJSON rows (one
// census cell per line, each with its read-time coverage of its record-type) plus a
// summary line. Shared by `.multi run --queries builtin:census` and the (soon-retired)
// `.multi census` verb.
func (c *cli) emitCensus(sess *glue.Session, keyspace string, opts glue.CensusOptions) {
	res, err := sess.Census(keyspace, opts)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: census: %v\n", c.prog, err)
		c.failed = true
		return
	}
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
	for _, r := range res.Rows {
		cov := 0.0
		if tot := res.TypeTotals[r.Type]; tot > 0 {
			cov = float64(r.Docs) / float64(tot)
		}
		b, _ := json.Marshal(rowOut{
			Type: r.Type, Path: r.Path, ValType: r.ValType, Docs: r.Docs,
			Coverage: cov, FirstSeen: r.FirstSeen, LastSeen: r.LastSeen, FirstID: r.FirstID,
		})
		fmt.Fprintln(c.out, string(b))
	}
	fmt.Fprintf(c.stderr, "%s%d cell(s) over %d record(s), %d record-type(s)\n",
		c.icon("📇 "), len(res.Rows), res.Records, len(res.TypeTotals))
}
