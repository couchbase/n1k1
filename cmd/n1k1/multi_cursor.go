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
	"path/filepath"
	"strings"
	"time"

	"github.com/couchbase/n1k1/glue"
)

// cursorArgs is the parsed flag set for the cursor verbs: a positional NAME plus
// the per-verb flags. Unlike parseMultiArgs it does NOT require --queries (a
// peek/advance is addressed by cursor name; --pack is create-only).
type cursorArgs struct {
	name  string   // the cursor NAME (first positional)
	pack  []string // --pack <dir> (repeatable / comma-list), create-only
	bind  string   // --bind <manifest>
	to    string   // --to <pos>, advance-only (the opaque position token)
	from  string   // --from now|start, create-only (default: now)
	desc  string   // --desc <text>, create-only
	store string   // --cursor-store <dir> (override the default state dir)
	quiet bool     // --quiet, advance-only (ack only, no labelResults echo)
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
		case "quiet":
			a.quiet = !hasEq || val == "true" || val == "1"
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
	case "", "help":
		c.cursorHelp()
	default:
		fmt.Fprintf(c.stderr, "unknown cursor verb %q; try .multi cursor help\n", verb)
		c.failed = true
	}
}

// ------------------------------------------------------------- the envelope

type cursorRow struct {
	Op          string          `json:"op"`          // "insert" (append mode is always an insert)
	Label       string          `json:"label"`       // which detector fired
	Fingerprint string          `json:"fingerprint"` // dedup_key: hash(label + result), for agent-side dedup
	Result      json.RawMessage `json:"result"`      // the labelResult value
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

	dets, err := loadMultiQueryEntries(a.pack)
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

	// Validate by compiling + running the pack once. This both proves the pack is
	// runnable and (for --from now) yields the current head as the start position.
	res, err := sess.RunCursorPack(dets, nil)
	if err != nil {
		c.cursorFail(a.name, "compile", err)
		return
	}

	water := res.NewWater
	if strings.EqualFold(a.from, "start") || strings.EqualFold(a.from, "beginning") {
		water = map[string]int64{} // replay everything from the beginning on first peek
	}

	now := time.Now().UTC().Format(time.RFC3339)
	st := &glue.CursorState{
		Name:        a.name,
		Pack:        strings.Join(a.pack, ","),
		Bind:        a.bind,
		PackID:      glue.PackID(a.name, dets),
		Mode:        "append",
		Water:       water,
		Description: a.desc,
		Created:     now,
		Updated:     now,
	}
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
		Mode:     "append",
		From:     encodeWater(water),
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

	dets, err := loadMultiQueryEntries(strings.Split(st.Pack, ","))
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
		env := cursorEnvelope{Cursor: a.name, Pack: st.PackID, Status: "error",
			From: encodeWater(st.Water), To: encodeWater(st.Water),
			Error: &cursorErr{Kind: "source-unbound",
				Message: "one or more logical keyspaces resolved to nothing (fail-loud; see stderr)"}}
		c.printJSON(env)
		c.failed = true
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
			Committed: encodeWater(st.Water), Advances: st.TotalAdvances})
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
		Committed     string            `json:"committed"`
		Description   string            `json:"description,omitempty"`
		Labels        map[string]string `json:"labels,omitempty"`
		Created       string            `json:"created,omitempty"`
		Updated       string            `json:"updated,omitempty"`
		LastCount     int               `json:"last_count"`
		TotalAdvances int               `json:"total_advances"`
	}{
		Cursor: st.Name, Pack: st.PackID, PackDir: st.Pack, Bind: st.Bind,
		Mode: st.Mode, Committed: encodeWater(st.Water), Description: st.Description,
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

  create NAME --pack <dir> [--bind <m>] [--from now|start] [--desc <t>]
                       bind + validate a pack; set the start position. No rows.
  peek   NAME          the pending delta; does NOT move the cursor (re-peek is safe).
  advance NAME [--to <pos>] [--quiet]
                       commit (get + move). Echoes the delta unless --quiet.
                       --to <pos> commits the exact position peek reported (two-step).
  list                 the cursors in the store.
  show   NAME          one cursor's committed position + metadata.
  rm     NAME          forget a cursor.

  --cursor-store <dir> override the state dir (default <bundle>/.n1k1-state/cursors).

Safe agent loop: peek (look) -> act -> advance --to <pos> --quiet. Each response is
one JSON envelope; switch on "status" (pending | advanced | empty | error).
`
