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

// `.multi census <keyspace>` — a time-aware, type-aware key-space census over a
// (possibly --bind-bound) keyspace: per (record-type, field-path, value-type) it
// reports docs, coverage of its type, first/last-seen, and the id of the first
// record to carry it. The answer to "is a field being born / dying / changing
// shape?" that one-shot schema inference and stateless engines can't give. See
// DESIGN-census.md.

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// cmdMultiCensus implements `.multi census <keyspace> [--bind <m>] [--type-field <f>]
// [--time-field <f>] [--depth 1|2] [--exclude a,b]`. Output is NDJSON (one census row
// per line — read by a program essentially always) plus a stderr summary.
func (c *cli) cmdMultiCensus(arg string) {
	var (
		keyspace  string
		bind      string
		typeField string
		timeField string
		depth     = 2
		exclude   []string
	)
	toks := splitArgsQuoted(arg)
	need := func(i *int, flag string) (string, bool) {
		*i++
		if *i >= len(toks) {
			fmt.Fprintf(c.stderr, "%s: .multi census: %s needs a value\n", c.prog, flag)
			c.failed = true
			return "", false
		}
		return toks[*i], true
	}
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if !strings.HasPrefix(t, "-") {
			if keyspace == "" {
				keyspace = t
				continue
			}
			fmt.Fprintf(c.stderr, "%s: .multi census: unexpected argument %q\n", c.prog, t)
			c.failed = true
			return
		}
		key, val, hasEq := t, "", false
		if eq := strings.IndexByte(t, '='); eq >= 0 {
			key, val, hasEq = t[:eq], t[eq+1:], true
		}
		ok := true
		switch strings.TrimLeft(key, "-") {
		case "bind":
			if !hasEq {
				val, ok = need(&i, "--bind")
			}
			bind = val
		case "type-field":
			if !hasEq {
				val, ok = need(&i, "--type-field")
			}
			typeField = val
		case "time-field":
			if !hasEq {
				val, ok = need(&i, "--time-field")
			}
			timeField = val
		case "depth":
			if !hasEq {
				val, ok = need(&i, "--depth")
			}
			if n, e := strconv.Atoi(val); e == nil {
				depth = n
			}
		case "exclude":
			if !hasEq {
				val, ok = need(&i, "--exclude")
			}
			for _, e := range strings.Split(val, ",") {
				if e = strings.TrimSpace(e); e != "" {
					exclude = append(exclude, e)
				}
			}
		default:
			fmt.Fprintf(c.stderr, "%s: .multi census: unknown flag %q\n", c.prog, t)
			c.failed = true
			return
		}
		if !ok {
			return
		}
	}
	if keyspace == "" {
		fmt.Fprintf(c.stderr, "%s: .multi census: a <keyspace> is required (e.g. .multi census sessions --bind ./m)\n", c.prog)
		c.failed = true
		return
	}

	sess, binding, err := c.multiSession(bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi census: %v\n", c.prog, err)
		c.failed = true
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		fmt.Fprintf(c.stderr, "%s: .multi census: aborting -- unresolved logical keyspace(s) above\n", c.prog)
		c.failed = true
		return
	}

	c.emitCensus(sess, keyspace, glue.CensusOptions{
		TypeField: typeField, TimeField: timeField, Depth: depth, Exclude: exclude,
	})
}

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
