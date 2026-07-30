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

// Census-mode cursor (DESIGN-census.md Phase 3): an incremental schema census. A
// `.multi cursor create NAME --mode census --keyspace <ks>` seeds an accumulated
// census; `peek`/`advance` census ONLY the records appended since the watermark, fold
// them into the accumulated census (the monoid), and emit the schema DRIFT
// (field_added / type_changed) — the drift alarm for free. The accumulated census and
// the watermark live in ONE cursor-state file, so `advance` commits both atomically
// (retiring the two-store double-count wall an external census-loop hits).

import (
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/n1k1/glue"
)

type censusCursorEnv struct {
	Cursor        string              `json:"cursor"`
	Mode          string              `json:"mode"`
	Status        string              `json:"status"` // pending | advanced | empty | error
	From          string              `json:"from"`
	To            string              `json:"to"`
	Advanced      bool                `json:"advanced"`
	WindowRecords int64               `json:"window_records"` // records censused this window
	Count         int                 `json:"count"`          // drift events
	Drift         []glue.CensusChange `json:"drift,omitempty"`
	Error         *cursorErr          `json:"error,omitempty"`
}

func (c *cli) censusOpts(a cursorArgs) glue.CensusOptions {
	return glue.CensusOptions{
		TypeField: a.censusType, TimeField: a.censusTime, Depth: a.censusDepth, Exclude: a.censusExclude,
	}
}

// cursorCensusCreate binds a census cursor to a keyspace and seeds the accumulated
// census (from the whole keyspace by default; --from now starts empty and only
// accumulates future records). The watermark is captured either way.
func (c *cli) cursorCensusCreate(a cursorArgs) {
	if a.keyspace == "" {
		c.cursorFail(a.name, "bad-args", fmt.Errorf(
			`builtin:census needs a keyspace, e.g. --queries "builtin:census?keyspace=<ks>"`))
		return
	}
	store, err := c.cursorStore(a.store)
	if err != nil {
		c.cursorFail(a.name, "no-bundle", err)
		return
	}
	if _, e := store.Load(a.name); e == nil {
		c.cursorFail(a.name, "exists", fmt.Errorf("cursor %q already exists (rm it first)", a.name))
		return
	}
	sess, binding, err := c.multiSession(a.bind)
	if err != nil {
		c.cursorFail(a.name, "open", err)
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		c.cursorFail(a.name, "source-unbound",
			fmt.Errorf("keyspace %q resolved to nothing (fail-loud; see stderr)", a.keyspace))
		return
	}

	// Full census now: validates the keyspace AND captures the current head watermark.
	res, cerr := sess.Census(a.keyspace, c.censusOpts(a))
	if cerr != nil {
		c.cursorFail(a.name, "census", cerr)
		return
	}

	fromNow := strings.EqualFold(a.from, "now")
	now := time.Now().UTC().Format(time.RFC3339)
	st := &glue.CursorState{
		Name: a.name, Bind: a.bind, Mode: "census", Keyspace: a.keyspace,
		Builtin:         a.builtinVersion, // e.g. "census@1" -- for future version-compat checks
		CensusTypeField: a.censusType, CensusTimeField: a.censusTime,
		CensusDepth: a.censusDepth, CensusExclude: a.censusExclude,
		Water: res.NewWater, CensusTotals: map[string]int64{},
		Description: a.desc, Created: now, Updated: now,
	}
	if !fromNow { // default: seed the accumulated census with all history
		st.Census = res.Rows
		st.CensusTotals = res.TypeTotals
		st.CensusRecords = res.Records
	}
	if err := store.Save(st); err != nil {
		c.cursorFail(a.name, "state-write", err)
		return
	}
	c.printJSON(struct {
		Created  string `json:"created"`
		OK       bool   `json:"ok"`
		Mode     string `json:"mode"`
		Keyspace string `json:"keyspace"`
		Cells    int    `json:"cells"`
		Records  int64  `json:"records"`
		From     string `json:"from"`
	}{Created: a.name, OK: true, Mode: "census", Keyspace: a.keyspace,
		Cells: len(st.Census), Records: st.CensusRecords, From: "census:0"})
}

// cursorCensusPeekAdvance censuses records appended since the watermark, folds them
// into the accumulated census, and emits the drift. peek recomputes without moving;
// advance commits the merged census + new watermark in one atomic write.
func (c *cli) cursorCensusPeekAdvance(a cursorArgs, st *glue.CursorState, store *glue.CursorStore, advance bool) {
	from := fmt.Sprintf("census:%d", st.CensusVersion)
	env := censusCursorEnv{Cursor: st.Name, Mode: "census", From: from, To: from}

	sess, binding, err := c.multiSession(st.Bind)
	if err != nil {
		env.Status, env.Error = "error", &cursorErr{Kind: "open", Message: err.Error()}
		c.printJSON(env)
		c.failed = true
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		env.Status = "error"
		env.Error = &cursorErr{Kind: "source-unbound", Message: "keyspace resolved to nothing (fail-loud; see stderr)"}
		c.printJSON(env)
		c.failed = true
		return
	}

	opts := glue.CensusOptions{
		TypeField: st.CensusTypeField, TimeField: st.CensusTimeField,
		Depth: st.CensusDepth, Exclude: st.CensusExclude, Since: st.Water,
	}
	window, cerr := sess.Census(st.Keyspace, opts)
	if cerr != nil {
		env.Status, env.Error = "error", &cursorErr{Kind: "census", Message: cerr.Error()}
		c.printJSON(env)
		c.failed = true
		return
	}

	drift := glue.CensusDrift(st.Census, window.Rows)
	changed := len(drift) > 0
	env.WindowRecords = window.Records
	env.Count = len(drift)

	if !advance {
		env.Advanced = false
		if changed {
			env.Status, env.To, env.Drift = "pending", fmt.Sprintf("census:%d", st.CensusVersion+1), drift
		} else {
			env.Status = "empty"
		}
		c.printJSON(env)
		return
	}

	// advance: fold the window into the accumulated census and commit both (census +
	// watermark) in one atomic state write.
	prior := &glue.CensusResult{Rows: st.Census, TypeTotals: st.CensusTotals, Records: st.CensusRecords}
	merged := glue.MergeCensus(prior, window)
	st.Census = merged.Rows
	st.CensusTotals = merged.TypeTotals
	st.CensusRecords = merged.Records
	st.Water = window.NewWater
	st.Updated = time.Now().UTC().Format(time.RFC3339)
	st.LastCount = len(drift)
	if changed {
		st.CensusVersion++
		st.TotalAdvances++
	}
	if err := store.Save(st); err != nil {
		env.Status, env.Error = "error", &cursorErr{Kind: "state-write", Message: err.Error()}
		c.printJSON(env)
		c.failed = true
		return
	}
	env.To = fmt.Sprintf("census:%d", st.CensusVersion)
	env.Advanced = changed
	if changed {
		env.Status = "advanced"
		if !a.quiet {
			env.Drift = drift
		}
	} else {
		env.Status = "empty"
	}
	c.printJSON(env)
}
