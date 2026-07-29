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

// `.multi cursor <verb>` — the CEP named-cursor surface (DESIGN-cep.md Phase 1).
// A cursor lives under `.multi` because it is meaningless without a query pack.
// Verbs:
//
//	create NAME --pack <dir> [--bind <m>] [--from now|start] [--desc ...]
//	                          bind + validate (compile + probe binding); no rows.
//	peek   NAME               pending delta; does NOT move the committed position.
//	advance NAME [--to <pos>] [--quiet]
//	                          commit (get + move); echoes the delta unless --quiet.
//	list                      inventory of cursors in the store.
//	show   NAME               one cursor's committed position + metadata.
//	rm     NAME               forget a cursor.
//
// Every verb prints ONE JSON envelope to stdout (compact when piped, indented at a
// TTY) — the shape an agent switches on. Diagnostics go to stderr.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/n1k1/glue"
)

// cursorArgs is the parsed flag set for the cursor verbs: a positional NAME plus
// the per-verb flags. Unlike parseMultiArgs it does NOT require --queries (a
// peek/advance is addressed by cursor name; --pack is create-only).
type cursorArgs struct {
	name    string   // the cursor NAME (first positional)
	pack    []string // --pack <dir> (repeatable / comma-list), create-only
	bind    string   // --bind <manifest>
	to      string   // --to <pos>, advance-only (the opaque position token)
	from    string   // --from now|start, create-only (default: now)
	desc    string   // --desc <text>, create-only
	store   string   // --cursor-store <dir> (override the default state dir)
	mode    string   // --mode append|diff, create-only (default: append)
	idField string   // --id-field <name>, create-only diff (default: id)
	quiet   bool     // --quiet, advance-only (ack only, no labelResults echo)
	prune   bool     // --prune, apply-only (destroy managed cursors not declared)
}

func parseCursorArgs(arg string) (cursorArgs, error) {
	var a cursorArgs
	toks := strings.Fields(arg)
	need := func(i *int, val *string, flag string) error {
		if *val != "" {
			return nil
		}
		*i++
		if *i >= len(toks) {
			return fmt.Errorf("%s needs a value", flag)
		}
		*val = toks[*i]
		return nil
	}
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if !strings.HasPrefix(t, "-") {
			if a.name == "" {
				a.name = t
				continue
			}
			return a, fmt.Errorf("unexpected extra argument %q", t)
		}
		key, val, hasEq := t, "", false
		if eq := strings.IndexByte(t, '='); eq >= 0 {
			key, val, hasEq = t[:eq], t[eq+1:], true
		}
		switch strings.TrimLeft(key, "-") {
		case "pack", "queries":
			if !hasEq {
				if err := need(&i, &val, "--pack"); err != nil {
					return a, err
				}
			}
			for _, d := range strings.Split(val, ",") {
				if d = strings.TrimSpace(d); d != "" {
					a.pack = append(a.pack, d)
				}
			}
		case "bind":
			if !hasEq {
				if err := need(&i, &val, "--bind"); err != nil {
					return a, err
				}
			}
			a.bind = val
		case "to":
			if !hasEq {
				if err := need(&i, &val, "--to"); err != nil {
					return a, err
				}
			}
			a.to = val
		case "from":
			if !hasEq {
				if err := need(&i, &val, "--from"); err != nil {
					return a, err
				}
			}
			a.from = val
		case "desc", "description":
			if !hasEq {
				if err := need(&i, &val, "--desc"); err != nil {
					return a, err
				}
			}
			a.desc = val
		case "cursor-store", "store":
			if !hasEq {
				if err := need(&i, &val, "--cursor-store"); err != nil {
					return a, err
				}
			}
			a.store = val
		case "mode":
			if !hasEq {
				if err := need(&i, &val, "--mode"); err != nil {
					return a, err
				}
			}
			a.mode = val
		case "id-field", "id":
			if !hasEq {
				if err := need(&i, &val, "--id-field"); err != nil {
					return a, err
				}
			}
			a.idField = val
		case "quiet":
			a.quiet = !hasEq || val == "true" || val == "1"
		case "prune":
			a.prune = !hasEq || val == "true" || val == "1"
		default:
			return a, fmt.Errorf("unknown flag %q", t)
		}
	}
	return a, nil
}

// cmdMultiCursor dispatches `.multi cursor <verb>`.
func (c *cli) cmdMultiCursor(arg string) {
	verb, rest := splitFirst(arg)
	switch strings.ToLower(verb) {
	case "create":
		c.cursorCreate(rest)
	case "peek":
		c.cursorPeekAdvance(rest, false)
	case "advance":
		c.cursorPeekAdvance(rest, true)
	case "list", "ls":
		c.cursorList(rest)
	case "show":
		c.cursorShow(rest)
	case "rm", "remove", "delete":
		c.cursorRemove(rest)
	case "plan":
		c.cursorReconcile(rest, false)
	case "apply":
		c.cursorReconcile(rest, true)
	case "", "help":
		c.cursorHelp()
	default:
		fmt.Fprintf(c.stderr, "unknown cursor verb %q; try .multi cursor help\n", verb)
		c.failed = true
	}
}

// ------------------------------------------------------------- the envelope

type cursorRow struct {
	Op          string          `json:"op"`               // append: "insert"; diff: insert|update|delete
	Id          string          `json:"id,omitempty"`     // diff: the doc identity
	Label       string          `json:"label"`            // which detector fired
	Fingerprint string          `json:"fingerprint"`      // dedup_key for agent-side dedup
	Result      json.RawMessage `json:"result,omitempty"` // append: the labelResult value
	Before      json.RawMessage `json:"before,omitempty"` // diff: prior doc (update/delete)
	After       json.RawMessage `json:"after,omitempty"`  // diff: current doc (insert/update)
}

type cursorErr struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

type cursorEnvelope struct {
	Cursor       string      `json:"cursor"`
	Pack         string      `json:"pack,omitempty"`
	Status       string      `json:"status"` // pending | advanced | empty | error
	From         string      `json:"from,omitempty"`
	To           string      `json:"to,omitempty"`
	Advanced     bool        `json:"advanced"`
	Count        int         `json:"count"`
	LabelResults []cursorRow `json:"labelResults,omitempty"`
	Error        *cursorErr  `json:"error,omitempty"`
}

// printJSON emits v as the single response envelope: indented at a TTY (the
// design's readable jsonc), compact one-line when piped (jsonlines-friendly).
func (c *cli) printJSON(v interface{}) {
	var b []byte
	if c.fancyTTY {
		b, _ = json.MarshalIndent(v, "", "  ")
	} else {
		b, _ = json.Marshal(v)
	}
	fmt.Fprintln(c.out, string(b))
}

// cursorStore roots the cursor state dir: --cursor-store if given, else the
// gitignorable <bundle>/.n1k1-state/cursors (DESIGN-cep.md § State & idempotency).
func (c *cli) cursorStore(override string) (*glue.CursorStore, error) {
	dir := override
	if dir == "" {
		if c.dir == "" {
			return nil, fmt.Errorf("no bundle open -- open a datastore directory first (.open <dir>)")
		}
		dir = filepath.Join(c.dir, ".n1k1-state", "cursors")
	}
	return glue.NewCursorStore(dir), nil
}

// loadPackPaths loads a cursor's pack from a comma-list of paths, each a *.sql++
// FILE (the GitOps one-file-one-cursor case) or a DIR of them (imperative --pack).
func loadPackPaths(paths []string) ([]glue.MultiQueryEntry, error) {
	var all []glue.MultiQueryEntry
	for _, p := range paths {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		es, err := glue.LoadPack(p)
		if err != nil {
			return nil, err
		}
		all = append(all, es...)
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("empty pack")
	}
	return all, nil
}

func fingerprint(label string, result json.RawMessage) string {
	h := sha256.New()
	h.Write([]byte(label))
	h.Write([]byte{0})
	h.Write(result)
	return hex.EncodeToString(h.Sum(nil))[:6]
}

func encodeWater(w map[string]int64) string {
	if len(w) == 0 {
		return ""
	}
	b, _ := json.Marshal(w) // encoding/json sorts map keys -> deterministic token
	return string(b)
}

func decodeWater(s string) (map[string]int64, error) {
	m := map[string]int64{}
	if strings.TrimSpace(s) == "" {
		return m, nil
	}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("--to %q is not a valid position token: %v", s, err)
	}
	return m, nil
}

func waterEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func toRows(lrs []glue.LabelResult) []cursorRow {
	rows := make([]cursorRow, 0, len(lrs))
	for _, lr := range lrs {
		rows = append(rows, cursorRow{
			Op:          "insert",
			Label:       lr.Label,
			Fingerprint: fingerprint(lr.Label, lr.Result),
			Result:      lr.Result,
		})
	}
	return rows
}

// ------------------------------------------------------------- verbs

// cursorCreate binds a cursor to a pack and validates it (compile + fail-loud
// binding probe), setting the start position. No rows. On any failure the cursor
// is NOT created.
func (c *cli) cursorCreate(arg string) {
	a, err := parseCursorArgs(arg)
	if err != nil {
		c.cursorFail("", "bad-args", err)
		return
	}
	if a.name == "" {
		c.cursorFail("", "bad-args", fmt.Errorf("cursor NAME is required"))
		return
	}
	if len(a.pack) == 0 {
		c.cursorFail(a.name, "bad-args", fmt.Errorf("--pack <dir> is required for create"))
		return
	}
	store, err := c.cursorStore(a.store)
	if err != nil {
		c.cursorFail(a.name, "no-bundle", err)
		return
	}
	if _, err := store.Load(a.name); err == nil {
		c.cursorFail(a.name, "exists", fmt.Errorf("cursor %q already exists (rm it first)", a.name))
		return
	}

	dets, err := loadPackPaths(a.pack)
	if err != nil {
		c.cursorFail(a.name, "pack-load", err)
		return
	}
	sess, binding, err := c.multiSession(a.bind)
	if err != nil {
		c.cursorFail(a.name, "open", err)
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		c.cursorFail(a.name, "source-unbound",
			fmt.Errorf("one or more logical keyspaces resolved to nothing (fail-loud; see stderr)"))
		return
	}

	mode := strings.ToLower(a.mode)
	if mode == "" {
		mode = "append"
	}
	if mode != "append" && mode != "diff" {
		c.cursorFail(a.name, "bad-args", fmt.Errorf("--mode must be append or diff, got %q", a.mode))
		return
	}
	fromStart := strings.EqualFold(a.from, "start") || strings.EqualFold(a.from, "beginning")

	now := time.Now().UTC().Format(time.RFC3339)
	st := &glue.CursorState{
		Name:        a.name,
		Pack:        strings.Join(a.pack, ","),
		Bind:        a.bind,
		PackID:      glue.PackID(a.name, dets),
		Mode:        mode,
		Description: a.desc,
		Created:     now,
		Updated:     now,
	}
	var fromTok string

	if mode == "append" {
		// Validate by compiling + running once; --from now yields the current head.
		res, rerr := sess.RunCursorPack(dets, nil)
		if rerr != nil {
			c.cursorFail(a.name, "compile", rerr)
			return
		}
		st.Water = res.NewWater
		if fromStart {
			st.Water = map[string]int64{} // replay everything on the first peek
		}
		fromTok = encodeWater(st.Water)
	} else { // diff
		idField := a.idField
		if idField == "" {
			idField = "id"
		}
		st.IdField = idField
		// Validate + capture the current state as the baseline snapshot.
		lrs, rerr := sess.RunPackFull(dets)
		if rerr != nil {
			c.cursorFail(a.name, "compile", rerr)
			return
		}
		snap, _ := glue.SnapshotFromResults(lrs, idField)
		if fromStart {
			snap = map[string]glue.SnapshotEntry{} // first peek reports all current rows as inserts
		}
		if err := store.SaveSnapshot(a.name, snap); err != nil {
			c.cursorFail(a.name, "state-write", err)
			return
		}
		fromTok = "snap:0"
	}

	// Imperative create is unmanaged (never pruned by `apply`), but carries a
	// SpecHash so a later `apply` declaring the same name can adopt it.
	st.SpecHash = glue.SpecHash(st.PackID, st.Mode, st.Bind, st.IdField, st.Description, st.Labels)
	st.Managed = false

	if err := store.Save(st); err != nil {
		c.cursorFail(a.name, "state-write", err)
		return
	}

	c.printJSON(struct {
		Created  string `json:"created"`
		OK       bool   `json:"ok"`
		Pack     string `json:"pack"`
		Compiles string `json:"compiles"`
		Mode     string `json:"mode"`
		From     string `json:"from"`
	}{
		Created:  a.name,
		OK:       true,
		Pack:     st.PackID,
		Compiles: "ok",
		Mode:     mode,
		From:     fromTok,
	})
}

// cursorPeekAdvance implements both peek (advance=false: non-moving) and advance
// (advance=true: commits). They share the run (the delta is computed the same
// way); only the commit + status differ.
func (c *cli) cursorPeekAdvance(arg string, advance bool) {
	a, err := parseCursorArgs(arg)
	if err != nil {
		c.cursorFail("", "bad-args", err)
		return
	}
	if a.name == "" {
		c.cursorFail("", "bad-args", fmt.Errorf("cursor NAME is required"))
		return
	}
	store, err := c.cursorStore(a.store)
	if err != nil {
		c.cursorFail(a.name, "no-bundle", err)
		return
	}
	st, err := store.Load(a.name)
	if err != nil {
		if err == glue.ErrCursorNotExist {
			c.cursorFail(a.name, "no-such-cursor",
				fmt.Errorf("no cursor %q (create it: .multi cursor create %s --pack <dir>)", a.name, a.name))
			return
		}
		c.cursorFail(a.name, "state-read", err)
		return
	}

	dets, err := loadPackPaths(strings.Split(st.Pack, ","))
	if err != nil {
		c.cursorFail(a.name, "pack-load", err)
		return
	}
	sess, binding, err := c.multiSession(st.Bind)
	if err != nil {
		c.cursorFail(a.name, "open", err)
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		from := c.positionToken(st)
		env := cursorEnvelope{Cursor: a.name, Pack: st.PackID, Status: "error",
			From: from, To: from,
			Error: &cursorErr{Kind: "source-unbound",
				Message: "one or more logical keyspaces resolved to nothing (fail-loud; see stderr)"}}
		c.printJSON(env)
		c.failed = true
		return
	}

	if st.Mode == "diff" {
		c.cursorDiffPeekAdvance(a, st, store, sess, dets, advance)
		return
	}

	committed := st.Water
	res, err := sess.RunCursorPack(dets, committed)
	if err != nil {
		env := cursorEnvelope{Cursor: a.name, Pack: st.PackID, Status: "error",
			From: encodeWater(committed), To: encodeWater(committed),
			Error: &cursorErr{Kind: "run", Message: err.Error()}}
		c.printJSON(env)
		c.failed = true
		return
	}

	rows := toRows(res.LabelResults)
	env := cursorEnvelope{
		Cursor: a.name, Pack: st.PackID,
		From:  encodeWater(committed),
		Count: len(rows),
	}

	if !advance {
		// peek: never moves; the next peek still returns this (at-least-once).
		env.To = encodeWater(res.NewWater)
		env.Advanced = false
		if len(rows) > 0 {
			env.Status = "pending"
			env.LabelResults = rows
		} else {
			env.Status = "empty"
		}
		c.printJSON(env)
		return
	}

	// advance: commit. --to lets the agent commit to the exact head it peeked
	// (the two-step); otherwise commit to the current head.
	newWater := res.NewWater
	if a.to != "" {
		w, derr := decodeWater(a.to)
		if derr != nil {
			c.cursorFail(a.name, "bad-args", derr)
			return
		}
		newWater = w
	}
	moved := !waterEqual(committed, newWater)

	st.Water = newWater
	st.Updated = time.Now().UTC().Format(time.RFC3339)
	st.LastCount = len(rows)
	if moved {
		st.TotalAdvances++
	}
	if err := store.Save(st); err != nil {
		c.cursorFail(a.name, "state-write", err)
		return
	}

	env.To = encodeWater(newWater)
	env.Advanced = moved
	if moved {
		env.Status = "advanced"
	} else {
		env.Status = "empty"
	}
	if !a.quiet && len(rows) > 0 {
		env.LabelResults = rows
	}
	c.printJSON(env)
}

// positionToken renders a cursor's committed position for display / envelopes:
// the `{container→offset}` water token for append, "snap:N" for diff.
func (c *cli) positionToken(st *glue.CursorState) string {
	if st.Mode == "diff" {
		return fmt.Sprintf("snap:%d", st.SnapVersion)
	}
	return encodeWater(st.Water)
}

// cursorDiffPeekAdvance is the diff-mode counterpart of the append peek/advance:
// it runs the pack over the FULL current state, diffs against the committed
// snapshot into Debezium {op,id,before,after} rows, and (on advance) replaces the
// snapshot + bumps the snap version.
func (c *cli) cursorDiffPeekAdvance(a cursorArgs, st *glue.CursorState, store *glue.CursorStore,
	sess *glue.Session, dets []glue.MultiQueryEntry, advance bool) {

	from := fmt.Sprintf("snap:%d", st.SnapVersion)

	prior, err := store.LoadSnapshot(st.Name)
	if err != nil {
		c.cursorFail(st.Name, "state-read", err)
		return
	}
	lrs, err := sess.RunPackFull(dets)
	if err != nil {
		c.printJSON(cursorEnvelope{Cursor: st.Name, Pack: st.PackID, Status: "error",
			From: from, To: from, Error: &cursorErr{Kind: "run", Message: err.Error()}})
		c.failed = true
		return
	}
	current, _ := glue.SnapshotFromResults(lrs, st.IdField)
	events := glue.DiffSnapshot(prior, current, st.IdField)
	rows := diffRows(events)
	changed := len(events) > 0

	env := cursorEnvelope{Cursor: st.Name, Pack: st.PackID, From: from, Count: len(rows)}

	if !advance {
		// peek: does NOT replace the snapshot; re-peek recomputes the same diff.
		if changed {
			env.To = fmt.Sprintf("snap:%d", st.SnapVersion+1)
			env.Status = "pending"
			env.LabelResults = rows
		} else {
			env.To = from
			env.Status = "empty"
		}
		env.Advanced = false
		c.printJSON(env)
		return
	}

	// advance: commit the current state as the new baseline snapshot.
	newVer := st.SnapVersion
	if changed {
		newVer++
	}
	if err := store.SaveSnapshot(st.Name, current); err != nil {
		c.cursorFail(st.Name, "state-write", err)
		return
	}
	st.SnapVersion = newVer
	st.Updated = time.Now().UTC().Format(time.RFC3339)
	st.LastCount = len(rows)
	if changed {
		st.TotalAdvances++
	}
	if err := store.Save(st); err != nil {
		c.cursorFail(st.Name, "state-write", err)
		return
	}

	env.To = fmt.Sprintf("snap:%d", newVer)
	env.Advanced = changed
	if changed {
		env.Status = "advanced"
	} else {
		env.Status = "empty"
	}
	if !a.quiet && len(rows) > 0 {
		env.LabelResults = rows
	}
	c.printJSON(env)
}

func diffRows(events []glue.ChangeEvent) []cursorRow {
	rows := make([]cursorRow, 0, len(events))
	for _, e := range events {
		rows = append(rows, cursorRow{
			Op: e.Op, Id: e.Id, Label: e.Label,
			Fingerprint: diffFingerprint(e),
			Before:      e.Before, After: e.After,
		})
	}
	return rows
}

// diffFingerprint is the dedup_key for a change event: hash over
// (label, op, id, the new-or-prior doc), so an agent dedupes the same change.
func diffFingerprint(e glue.ChangeEvent) string {
	h := sha256.New()
	h.Write([]byte(e.Label))
	h.Write([]byte{0})
	h.Write([]byte(e.Op))
	h.Write([]byte{0})
	h.Write([]byte(e.Id))
	h.Write([]byte{0})
	if e.After != nil {
		h.Write(e.After)
	} else {
		h.Write(e.Before)
	}
	return hex.EncodeToString(h.Sum(nil))[:6]
}

func (c *cli) cursorList(arg string) {
	a, err := parseCursorArgs(arg)
	if err != nil {
		c.cursorFail("", "bad-args", err)
		return
	}
	store, err := c.cursorStore(a.store)
	if err != nil {
		c.cursorFail("", "no-bundle", err)
		return
	}
	names, err := store.List()
	if err != nil {
		c.cursorFail("", "state-read", err)
		return
	}
	type listRow struct {
		Cursor    string `json:"cursor"`
		Pack      string `json:"pack"`
		Mode      string `json:"mode"`
		Committed string `json:"committed"`
		Advances  int    `json:"total_advances"`
	}
	out := make([]listRow, 0, len(names))
	for _, n := range names {
		st, lerr := store.Load(n)
		if lerr != nil {
			continue
		}
		out = append(out, listRow{Cursor: n, Pack: st.PackID, Mode: st.Mode,
			Committed: c.positionToken(st), Advances: st.TotalAdvances})
	}
	c.printJSON(out)
}

func (c *cli) cursorShow(arg string) {
	a, err := parseCursorArgs(arg)
	if err != nil {
		c.cursorFail("", "bad-args", err)
		return
	}
	if a.name == "" {
		c.cursorFail("", "bad-args", fmt.Errorf("cursor NAME is required"))
		return
	}
	store, err := c.cursorStore(a.store)
	if err != nil {
		c.cursorFail(a.name, "no-bundle", err)
		return
	}
	st, err := store.Load(a.name)
	if err != nil {
		if err == glue.ErrCursorNotExist {
			c.cursorFail(a.name, "no-such-cursor", fmt.Errorf("no cursor %q", a.name))
			return
		}
		c.cursorFail(a.name, "state-read", err)
		return
	}
	c.printJSON(struct {
		Cursor        string            `json:"cursor"`
		Pack          string            `json:"pack"`
		PackDir       string            `json:"pack_dir"`
		Bind          string            `json:"bind,omitempty"`
		Mode          string            `json:"mode"`
		IdField       string            `json:"id_field,omitempty"`
		Committed     string            `json:"committed"`
		Description   string            `json:"description,omitempty"`
		Labels        map[string]string `json:"labels,omitempty"`
		Created       string            `json:"created,omitempty"`
		Updated       string            `json:"updated,omitempty"`
		LastCount     int               `json:"last_count"`
		TotalAdvances int               `json:"total_advances"`
	}{
		Cursor: st.Name, Pack: st.PackID, PackDir: st.Pack, Bind: st.Bind,
		Mode: st.Mode, IdField: st.IdField, Committed: c.positionToken(st), Description: st.Description,
		Labels: st.Labels, Created: st.Created, Updated: st.Updated,
		LastCount: st.LastCount, TotalAdvances: st.TotalAdvances,
	})
}

func (c *cli) cursorRemove(arg string) {
	a, err := parseCursorArgs(arg)
	if err != nil {
		c.cursorFail("", "bad-args", err)
		return
	}
	if a.name == "" {
		c.cursorFail("", "bad-args", fmt.Errorf("cursor NAME is required"))
		return
	}
	store, err := c.cursorStore(a.store)
	if err != nil {
		c.cursorFail(a.name, "no-bundle", err)
		return
	}
	if err := store.Remove(a.name); err != nil {
		if err == glue.ErrCursorNotExist {
			c.cursorFail(a.name, "no-such-cursor", fmt.Errorf("no cursor %q", a.name))
			return
		}
		c.cursorFail(a.name, "state-write", err)
		return
	}
	c.printJSON(struct {
		Removed string `json:"removed"`
		OK      bool   `json:"ok"`
	}{Removed: a.name, OK: true})
}

// ------------------------------------------------------------- GitOps reconcile

// desiredCursor is one cursor declared by a *.sql++ file in a desired-state dir:
// name = the file stem, pack = the file itself, policy read from its front-matter.
type desiredCursor struct {
	name     string
	file     string
	mode     string
	bind     string
	idField  string
	from     string
	desc     string
	labels   map[string]string
	packID   string
	specHash string
	entries  []glue.MultiQueryEntry
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func parseLabels(s string) map[string]string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	m := map[string]string{}
	if json.Unmarshal([]byte(s), &m) != nil {
		return nil
	}
	return m
}

// buildDesired reads a desired-state dir: one *.sql++ file => one cursor (name =
// stem, pack = the file, policy from front-matter mode/bind/id-field/from/labels +
// description). Returns the cursors keyed + ordered by name.
func buildDesired(dir, globalBind string) (map[string]*desiredCursor, []string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.sql++"))
	if err != nil {
		return nil, nil, err
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, nil, fmt.Errorf("no *.sql++ files in %q", dir)
	}
	out := map[string]*desiredCursor{}
	var names []string
	for _, f := range files {
		body, rerr := os.ReadFile(f)
		if rerr != nil {
			return nil, nil, rerr
		}
		e, perr := glue.ParseMultiQueryEntry(f, string(body))
		if perr != nil {
			return nil, nil, fmt.Errorf("%s: %v", f, perr)
		}
		name := e.Label
		if _, dup := out[name]; dup {
			return nil, nil, fmt.Errorf("duplicate cursor name %q (two files share the stem)", name)
		}
		d := &desiredCursor{name: name, file: f, entries: []glue.MultiQueryEntry{e}}
		d.mode = strings.ToLower(firstNonEmpty(e.Meta["mode"], "append"))
		d.bind = firstNonEmpty(e.Meta["bind"], globalBind)
		d.idField = firstNonEmpty(e.Meta["id-field"], e.Meta["id_field"], "id")
		d.from = firstNonEmpty(e.Meta["from"], "now")
		d.desc = e.Description
		d.labels = parseLabels(e.Meta["labels"])
		d.packID = glue.PackID(name, d.entries)
		d.specHash = glue.SpecHash(d.packID, d.mode, d.bind, d.idField, d.desc, d.labels)
		out[name] = d
		names = append(names, name)
	}
	return out, names, nil
}

// validateDesired compiles a desired cursor's pack (fold-in `.multi lint`) and
// probes its binding — the plan-time / pre-apply check.
func (c *cli) validateDesired(d *desiredCursor) error {
	if d.mode != "append" && d.mode != "diff" {
		return fmt.Errorf("--mode must be append or diff, got %q", d.mode)
	}
	sess, binding, err := c.multiSession(d.bind)
	if err != nil {
		return err
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		return fmt.Errorf("source unbound (fail-loud; see stderr)")
	}
	if _, err := sess.MultiQueryCompile(d.entries); err != nil {
		return err
	}
	return nil
}

// provisionCursor creates a new managed cursor or updates an existing one in place
// from a desired spec. The committed position is PRESERVED when the mode is
// unchanged (the idempotency guarantee: re-apply never rewinds a cursor); a new
// cursor or a mode change re-baselines per the file's `from` (default now).
func (c *cli) provisionCursor(store *glue.CursorStore, d *desiredCursor, prior *glue.CursorState) error {
	sess, binding, err := c.multiSession(d.bind)
	if err != nil {
		return err
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		return fmt.Errorf("source unbound (fail-loud; see stderr)")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	fromStart := strings.EqualFold(d.from, "start") || strings.EqualFold(d.from, "beginning")
	preserve := prior != nil && prior.Mode == d.mode

	st := &glue.CursorState{}
	if prior != nil {
		*st = *prior // keep bookkeeping (created, advances) + position when preserving
	}
	st.Name = d.name
	st.Pack = d.file
	st.Bind = d.bind
	st.PackID = d.packID
	st.Mode = d.mode
	st.IdField = d.idField
	st.Description = d.desc
	st.Labels = d.labels
	st.SpecHash = d.specHash
	st.Managed = true
	st.Updated = now
	if st.Created == "" {
		st.Created = now
	}

	if d.mode == "append" {
		if !preserve {
			res, rerr := sess.RunCursorPack(d.entries, nil)
			if rerr != nil {
				return rerr
			}
			st.Water = res.NewWater
			if fromStart {
				st.Water = map[string]int64{}
			}
			st.SnapVersion = 0
		}
	} else { // diff
		if !preserve {
			lrs, rerr := sess.RunPackFull(d.entries)
			if rerr != nil {
				return rerr
			}
			snap, _ := glue.SnapshotFromResults(lrs, d.idField)
			if fromStart {
				snap = map[string]glue.SnapshotEntry{}
			}
			if serr := store.SaveSnapshot(d.name, snap); serr != nil {
				return serr
			}
			st.SnapVersion = 0
			st.Water = nil
		}
	}
	return store.Save(st)
}

// cursorReconcile implements `.multi cursor plan <dir>` (apply=false: diff only)
// and `.multi cursor apply <dir> [--prune]` (apply=true: execute). Terraform-shaped:
// each *.sql++ in <dir> is the desired state; unchanged cursors keep their position.
func (c *cli) cursorReconcile(arg string, apply bool) {
	a, err := parseCursorArgs(arg)
	if err != nil {
		c.cursorFail("", "bad-args", err)
		return
	}
	dir := a.name // the positional is the desired-state dir here, not a cursor name
	if dir == "" {
		c.cursorFail("", "bad-args", fmt.Errorf("a desired-state <dir> is required"))
		return
	}
	store, err := c.cursorStore(a.store)
	if err != nil {
		c.cursorFail("", "no-bundle", err)
		return
	}
	desired, names, err := buildDesired(dir, a.bind)
	if err != nil {
		c.cursorFail("", "desired-load", err)
		return
	}

	// Validate every declared cursor (fold in lint).
	var probs []map[string]string
	for _, n := range names {
		if verr := c.validateDesired(desired[n]); verr != nil {
			probs = append(probs, map[string]string{"cursor": n, "error": verr.Error()})
		}
	}

	live := map[string]*glue.CursorState{}
	liveNames, lerr := store.List()
	if lerr != nil {
		c.cursorFail("", "state-read", lerr)
		return
	}
	for _, n := range liveNames {
		if st, e := store.Load(n); e == nil {
			live[n] = st
		}
	}
	desiredHashes := map[string]string{}
	for n, d := range desired {
		desiredHashes[n] = d.specHash
	}

	if !apply {
		plan := glue.PlanReconcile(desiredHashes, live, true) // show prunable destroys
		c.printJSON(struct {
			Plan     glue.ReconcilePlan  `json:"plan"`
			Changes  int                 `json:"changes"`
			Prunable int                 `json:"prunable"`
			Errors   []map[string]string `json:"errors,omitempty"`
		}{Plan: plan, Changes: len(plan.Create) + len(plan.Update), Prunable: len(plan.Destroy), Errors: probs})
		if len(probs) > 0 {
			c.failed = true
		}
		return
	}

	// apply: fail-loud on ANY invalid file — no partial apply.
	if len(probs) > 0 {
		c.printJSON(struct {
			OK     bool                `json:"ok"`
			Errors []map[string]string `json:"errors"`
		}{OK: false, Errors: probs})
		c.failed = true
		return
	}

	plan := glue.PlanReconcile(desiredHashes, live, a.prune)
	var created, updated, destroyed []string
	var execErrs []map[string]string
	for _, n := range plan.Create {
		if e := c.provisionCursor(store, desired[n], nil); e != nil {
			execErrs = append(execErrs, map[string]string{"cursor": n, "error": e.Error()})
		} else {
			created = append(created, n)
		}
	}
	for _, n := range plan.Update {
		if e := c.provisionCursor(store, desired[n], live[n]); e != nil {
			execErrs = append(execErrs, map[string]string{"cursor": n, "error": e.Error()})
		} else {
			updated = append(updated, n)
		}
	}
	for _, n := range plan.Destroy {
		if e := store.Remove(n); e != nil {
			execErrs = append(execErrs, map[string]string{"cursor": n, "error": e.Error()})
		} else {
			destroyed = append(destroyed, n)
		}
	}
	c.printJSON(struct {
		Applied struct {
			Created   []string `json:"created"`
			Updated   []string `json:"updated"`
			Destroyed []string `json:"destroyed"`
			Unchanged []string `json:"unchanged"`
		} `json:"applied"`
		Prune  bool                `json:"prune"`
		OK     bool                `json:"ok"`
		Errors []map[string]string `json:"errors,omitempty"`
	}{
		Applied: struct {
			Created   []string `json:"created"`
			Updated   []string `json:"updated"`
			Destroyed []string `json:"destroyed"`
			Unchanged []string `json:"unchanged"`
		}{Created: created, Updated: updated, Destroyed: destroyed, Unchanged: plan.Unchanged},
		Prune: a.prune, OK: len(execErrs) == 0, Errors: execErrs,
	})
	if len(execErrs) > 0 {
		c.failed = true
	}
}

// cursorFail prints an error envelope to stdout (so an agent parsing stdout still
// gets structured JSON) and marks the command failed.
func (c *cli) cursorFail(name, kind string, err error) {
	c.printJSON(cursorEnvelope{
		Cursor:   name,
		Status:   "error",
		Advanced: false,
		Error:    &cursorErr{Kind: kind, Message: err.Error()},
	})
	c.failed = true
}

func (c *cli) cursorHelp() {
	fmt.Fprint(c.stderr, cursorHelpText)
}

const cursorHelpText = `.multi cursor <verb> — CEP named cursors (a durable "what's new since I last looked")

  create NAME --pack <dir> [--bind <m>] [--from now|start] [--mode append|diff]
              [--id-field <name>] [--desc <t>]
                       bind + validate a pack; set the start position. No rows.
                       --mode diff tracks a snapshot keyed by --id-field (default id)
                       and emits {op:insert|update|delete, id, before, after}.
  peek   NAME          the pending delta; does NOT move the cursor (re-peek is safe).
  advance NAME [--to <pos>] [--quiet]
                       commit (get + move). Echoes the delta unless --quiet.
                       --to <pos> commits the exact position peek reported (two-step).
  list                 the cursors in the store.
  show   NAME          one cursor's committed position + metadata.
  rm     NAME          forget a cursor.

  plan   <dir>         GitOps diff: each *.sql++ in <dir> = one cursor (name = file
                       stem, policy from front-matter mode/from/bind/id-field/labels);
                       show create/update/destroy/unchanged vs live (no changes).
  apply  <dir> [--prune]
                       reconcile the store to <dir>; UNCHANGED cursors keep their
                       position (idempotent). --prune destroys managed cursors no
                       longer declared (imperative cursors are never pruned).

  --cursor-store <dir> override the state dir (default <bundle>/.n1k1-state/cursors).

Safe agent loop: peek (look) -> act -> advance --to <pos> --quiet. Each response is
one JSON envelope; switch on "status" (pending | advanced | empty | error).
`
