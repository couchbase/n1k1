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
//	create NAME --queries <dir> [--bind <m>] [--from now|start] [--desc ...]
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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/n1k1/glue"
)

// cursorArgs is the parsed flag set for the cursor verbs: a positional NAME plus
// the per-verb flags. Unlike parseMultiArgs it does NOT require --queries (a
// peek/advance is addressed by cursor name; --queries is create-only).
type cursorArgs struct {
	name          string   // the cursor NAME (first positional)
	pack          []string // --queries <dir> (repeatable / comma-list), create-only
	bind          string   // --bind <manifest>
	to            string   // --to <pos>, advance-only (the opaque position token)
	toFile        string   // --to-file <path>, advance-only (read --to from a file: large positions)
	from          string   // --from now|start, create-only (default: now)
	desc          string   // --desc <text>, create-only
	store         string   // --cursor-store <dir> (override the default state dir)
	mode          string   // --mode append|diff, create-only (default: append)
	idField       string   // --id-field <name>, create-only diff (default: id)
	quiet         bool     // --quiet, advance-only (ack only, no labelResults echo)
	force         bool     // --force, advance --to only (commit a rewinding/unknown position anyway)
	allowDrift    bool     // --allow-drift, advance only (commit even though the query edited since create)
	positions     bool     // --positions, show-only (full position map, not a summary)
	only          []string // --only <node,...>, compose-only (emit rows for just these)
	terminal      bool     // --terminal, compose-only (emit rows for leaf nodes only)
	allowRejected bool     // --allow-rejected, compose-only (don't hard-fail on a rejected node)

	// census cursor (create --queries builtin:census?keyspace=..&...): filled from the
	// builtin:census ref's params, not from standalone flags.
	keyspace       string   // ?keyspace=<ks>
	censusType     string   // ?type-field=<f>
	censusTime     string   // ?time-field=<f>
	censusDepth    int      // ?depth=1|2
	censusExclude  []string // ?exclude=a,b
	builtinVersion string   // the resolved builtin ref (e.g. "census@1"), stamped into state
}

func parseCursorArgs(arg string) (cursorArgs, error) {
	var a cursorArgs
	toks := splitArgsQuoted(arg)
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
		case "queries":
			if !hasEq {
				if err := need(&i, &val, "--queries"); err != nil {
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
		case "to-file":
			if !hasEq {
				if err := need(&i, &val, "--to-file"); err != nil {
					return a, err
				}
			}
			a.toFile = val
		case "from":
			if !hasEq {
				if err := need(&i, &val, "--from"); err != nil {
					return a, err
				}
			}
			a.from = val
		case "desc":
			if !hasEq {
				if err := need(&i, &val, "--desc"); err != nil {
					return a, err
				}
			}
			a.desc = val
		case "cursor-store":
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
		case "id-field":
			if !hasEq {
				if err := need(&i, &val, "--id-field"); err != nil {
					return a, err
				}
			}
			a.idField = val
		case "quiet":
			a.quiet = !hasEq || val == "true" || val == "1"
		case "force":
			a.force = !hasEq || val == "true" || val == "1"
		case "allow-drift":
			a.allowDrift = !hasEq || val == "true" || val == "1"
		case "positions":
			a.positions = !hasEq || val == "true" || val == "1"
		case "only":
			if !hasEq {
				if err := need(&i, &val, "--only"); err != nil {
					return a, err
				}
			}
			for _, n := range strings.Split(val, ",") {
				if n = strings.TrimSpace(n); n != "" {
					a.only = append(a.only, n)
				}
			}
		case "terminal":
			a.terminal = !hasEq || val == "true" || val == "1"
		case "allow-rejected":
			a.allowRejected = !hasEq || val == "true" || val == "1"
		default:
			if repl, ok := renamedCursorFlag[strings.TrimLeft(key, "-")]; ok {
				return a, fmt.Errorf("flag %q was removed; use %s", key, repl)
			}
			return a, fmt.Errorf("unknown flag %q", t)
		}
	}
	return a, nil
}

// renamedCursorFlag maps a removed flag/alias (bare, no leading dashes) to its
// canonical replacement, so the hard cut errors with a pointer, not a bare
// "unknown flag" (naming overhaul Phase 1b).
var renamedCursorFlag = map[string]string{
	"pack":        "--queries",
	"store":       "--cursor-store",
	"description": "--desc",
	"id":          "--id-field",
	"verbose":     "--positions",
	// census cursor params moved into the builtin:census ref's query-string.
	"keyspace":   `--queries "builtin:census?keyspace=<ks>"`,
	"type-field": `builtin:census?type-field=<f>`,
	"time-field": `builtin:census?time-field=<f>`,
	"depth":      `builtin:census?depth=<n>`,
	"exclude":    `builtin:census?exclude=a,b`,
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
	case "list":
		c.cursorList(rest)
	case "show":
		c.cursorShow(rest)
	case "check":
		c.cursorCheck(rest)
	case "rm":
		c.cursorRemove(rest)
	case "ls", "remove", "delete":
		canon := map[string]string{"ls": "list", "remove": "rm", "delete": "rm"}[strings.ToLower(verb)]
		fmt.Fprintf(c.stderr, "cursor verb %q was removed; use %q\n", verb, canon)
		c.failed = true
	case "plan", "apply":
		fmt.Fprintf(c.stderr, "cursor %q was removed (the GitOps reconcile) -- manage cursors with "+
			"create/rm, or peek/advance to run them; a declarative reconcile returns with a serve/monitor runtime\n", verb)
		c.failed = true
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
	Label       string          `json:"label"`            // which query fired
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

	// ISSUE-13: an explicit `advance --to` that would silently rewind an append
	// cursor is refused (status=error) unless --force; either way the affected
	// containers are disclosed here — never a silent 456→1 wipe.
	Dropped []string `json:"dropped,omitempty"` // held (offset>0) but absent from --to -> would reset to byte 0
	Rewound []string `json:"rewound,omitempty"` // --to offset < committed -> would re-deliver
	Unknown []string `json:"unknown,omitempty"` // --to names a container the datastore lacks

	// ISSUE-17: the query file can be edited after create; `pack` is the baseline
	// (creation-time) id, QueriesCurrent is the id re-hashed from the query NOW. They
	// differ iff the query drifted. advance refuses on drift unless --allow-drift.
	QueriesCurrent string `json:"queries_current,omitempty"`
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
// gitignorable CWD-relative ./.n1k1-state/cursors (DESIGN-cep.md § State &
// idempotency — tfstate lives next to your config, NOT inside the resource). It
// deliberately does NOT default inside the datastore bundle: a bundle can be
// read-only or owned by another live process (e.g. ~/.claude/projects, scanned by
// Claude Code), where writing state either fails or corrupts that process's walks.
func (c *cli) cursorStore(override string) (*glue.CursorStore, error) {
	dir := override
	if dir == "" {
		dir = filepath.Join(".n1k1-state", "cursors")
	}
	return glue.NewCursorStore(dir), nil
}

// loadPackPaths loads a cursor's pack from a comma-list of paths, each a single
// *.sql++ FILE or a DIR of them.
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

// encodeWater renders a water position as an OPAQUE token safe to pass back on argv.
// ISSUE-13: the raw JSON `{"c":n}` has quotes that `.multi`'s quote-aware tokenizer
// strips, so `advance --to {peek.to}` failed to round-trip. base64url has no quotes or
// spaces, so it survives argv verbatim. Deterministic (json sorts map keys).
func encodeWater(w map[string]int64) string {
	if len(w) == 0 {
		return ""
	}
	b, _ := json.Marshal(w)
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeWater accepts the opaque base64url token encodeWater emits (the round-trip
// path) OR raw JSON `{"c":n}` (a hand-written --to-file, back-compat).
func decodeWater(s string) (map[string]int64, error) {
	m := map[string]int64{}
	s = strings.TrimSpace(s)
	if s == "" {
		return m, nil
	}
	if !strings.HasPrefix(s, "{") { // opaque token -> decode to the JSON first
		if raw, err := base64.RawURLEncoding.DecodeString(s); err == nil {
			s = string(raw)
		}
	}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("--to %q is not a valid position token: %v", s, err)
	}
	return m, nil
}

// validateWater compares a requested advance --to position against the committed
// watermark and the datastore head (every container the scan visited). It returns the
// containers that would be dropped (held at offset>0 but absent from the request ->
// reset to 0), rewound (request offset < committed), or unknown (named but not in the
// datastore). All three are silent-double-count / typo hazards for an append cursor.
func validateWater(committed, requested, datastore map[string]int64) (dropped, rewound, unknown []string) {
	for k, off := range committed {
		if off > 0 {
			if _, ok := requested[k]; !ok {
				dropped = append(dropped, k)
			}
		}
	}
	for k, off := range requested {
		if c, ok := committed[k]; ok && off < c {
			rewound = append(rewound, k)
		}
		if _, ok := datastore[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	sort.Strings(dropped)
	sort.Strings(rewound)
	sort.Strings(unknown)
	return dropped, rewound, unknown
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
	// census stopped being a --mode; it's a queries source (checked up front so the
	// migration error fires regardless of the --queries value).
	if strings.EqualFold(a.mode, "census") {
		c.cursorFail(a.name, "bad-args", fmt.Errorf(
			`census is a queries source now, not a --mode: cursor create %s --queries "builtin:census?keyspace=<ks>"`, a.name))
		return
	}
	// A census cursor is created over the `builtin:census` entity (params in its
	// query-string), not a *.sql++ pack — separate create path.
	if ref, ok := builtinCensusRef(a.pack); ok {
		a.keyspace = ref.params["keyspace"]
		a.censusType = ref.params["type-field"]
		a.censusTime = ref.params["time-field"]
		if d := ref.params["depth"]; d != "" {
			if n, e := strconv.Atoi(d); e == nil {
				a.censusDepth = n
			}
		}
		if ex := ref.params["exclude"]; ex != "" {
			for _, e := range strings.Split(ex, ",") {
				if e = strings.TrimSpace(e); e != "" {
					a.censusExclude = append(a.censusExclude, e)
				}
			}
		}
		a.builtinVersion = ref.name + "@" + ref.version
		c.cursorCensusCreate(a)
		return
	}
	if len(a.pack) == 0 {
		c.cursorFail(a.name, "bad-args", fmt.Errorf("--queries <dir> is required for create"))
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

	// ISSUE-14: a single-file cursor's query carries policy in its front-matter
	// (`from`, `mode`, `description`, ...). `cursor apply` used to honor those keys;
	// `create` replaced apply but never learned them, so a checked-in `from: start`
	// was silently dropped and the cursor baselined at NOW -- skipping the whole
	// corpus while reporting {"ok":true}. Honor them here (precedence: CLI flag >
	// front-matter > default), and report any front-matter key create does NOT
	// consume in the envelope's "ignored" list, so a dropped key is never silent.
	var fm map[string]string
	var fmDesc string
	if len(dets) == 1 { // front-matter is per-query; only meaningful for a single-file cursor
		fm = dets[0].Meta
		fmDesc = dets[0].Description
	}
	effFrom := a.from
	if effFrom == "" {
		effFrom = fm["from"]
	}
	effMode := a.mode
	if effMode == "" {
		effMode = fm["mode"]
	}
	effDesc := a.desc
	if effDesc == "" {
		effDesc = fmDesc
	}

	mode := strings.ToLower(effMode)
	if mode == "" {
		mode = "append"
	}
	if mode != "append" && mode != "diff" {
		c.cursorFail(a.name, "bad-args", fmt.Errorf("--mode must be append or diff, got %q", effMode))
		return
	}
	fromStart := strings.EqualFold(effFrom, "start") || strings.EqualFold(effFrom, "beginning")

	// ignored: front-matter keys create saw but did not translate into cursor state
	// (create honors from/mode above; description/source/tags/label are consumed
	// elsewhere). Also flags a CLI --id-field that append mode discards (ISSUE-14 §3).
	honoredFM := map[string]bool{"from": true, "mode": true}
	if mode == "diff" {
		honoredFM["id-field"], honoredFM["id_field"] = true, true
	}
	var ignored []string
	for k := range fm {
		if !honoredFM[k] {
			ignored = append(ignored, k)
		}
	}
	if a.idField != "" && mode == "append" {
		ignored = append(ignored, "id-field") // append tracks byte watermarks; no row identity
	}
	sort.Strings(ignored)

	now := time.Now().UTC().Format(time.RFC3339)
	st := &glue.CursorState{
		Name:        a.name,
		Pack:        strings.Join(a.pack, ","),
		Bind:        a.bind,
		PackID:      glue.PackID(a.name, dets),
		Mode:        mode,
		Description: effDesc,
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
		if idField == "" { // ISSUE-14: fall back to front-matter, then the default
			if idField = fm["id-field"]; idField == "" {
				idField = fm["id_field"]
			}
		}
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

	if err := store.Save(st); err != nil {
		c.cursorFail(a.name, "state-write", err)
		return
	}

	c.printJSON(struct {
		Created  string   `json:"created"`
		OK       bool     `json:"ok"`
		Pack     string   `json:"queries"`
		Compiles string   `json:"compiles"`
		Mode     string   `json:"mode"`
		From     string   `json:"from"`
		Ignored  []string `json:"ignored,omitempty"`
	}{
		Created:  a.name,
		OK:       true,
		Ignored:  ignored,
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
				fmt.Errorf("no cursor %q (create it: .multi cursor create %s --queries <dir>)", a.name, a.name))
			return
		}
		c.cursorFail(a.name, "state-read", err)
		return
	}

	// A census cursor has a keyspace, not a pack — its own peek/advance path.
	if st.Mode == "census" {
		c.cursorCensusPeekAdvance(a, st, store, advance)
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

	// ISSUE-17: the query file may have been edited since create. Re-hash it now; a
	// cursor that runs the NEW query against a watermark advanced under the OLD one
	// silently skips the records the new predicate would have matched behind the
	// watermark. peek surfaces the drift (queries_current); advance refuses it (the
	// committed position becomes meaningless under a different query) unless
	// --allow-drift. `plan` used to be the drift check and was removed in the rename.
	currentID := glue.PackID(st.Name, dets)
	drifted := currentID != st.PackID
	if advance && drifted && !a.allowDrift {
		c.cursorDriftRefuse(a.name, st.PackID, currentID)
		return
	}

	if st.Mode == "diff" {
		c.cursorDiffPeekAdvance(a, st, store, sess, dets, advance, currentID, drifted)
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
	if drifted {
		env.QueriesCurrent = currentID
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
	// (the two-step); --to-file reads that token from a file (a many-container
	// append position is large — tens of KB — and shouldn't ride argv). Otherwise
	// commit to the current head.
	toTok := a.to
	if a.toFile != "" {
		b, ferr := os.ReadFile(a.toFile)
		if ferr != nil {
			c.cursorFail(a.name, "bad-args", fmt.Errorf("--to-file %q: %v", a.toFile, ferr))
			return
		}
		toTok = strings.TrimSpace(string(b))
	}
	newWater := res.NewWater
	if toTok != "" {
		w, derr := decodeWater(toTok)
		if derr != nil {
			c.cursorFail(a.name, "bad-args", derr)
			return
		}
		newWater = w

		// ISSUE-13: an explicit position REPLACES the watermark wholesale. Validate it
		// against the committed position + the datastore head (res.NewWater visited every
		// container) before committing: a held container dropped from the position resets
		// to byte 0 and re-delivers its whole history (double-counting, not an error); a
		// smaller offset rewinds; an unknown container is almost certainly a typo. Refuse
		// unless --force, and disclose the affected containers either way.
		dropped, rewound, unknown := validateWater(committed, newWater, res.NewWater)
		if len(dropped)+len(rewound)+len(unknown) > 0 {
			if !a.force {
				env := cursorEnvelope{
					Cursor: a.name, Pack: st.PackID, Status: "error",
					From: encodeWater(committed), To: encodeWater(committed),
					Dropped: dropped, Rewound: rewound, Unknown: unknown,
					Error: &cursorErr{Kind: "unsafe-position", Message: fmt.Sprintf(
						"--to would rewind this append cursor (dropped=%d rewound=%d unknown=%d); "+
							"a dropped/rewound container re-delivers its history. Re-run with --force to commit anyway.",
						len(dropped), len(rewound), len(unknown))},
				}
				c.printJSON(env)
				c.failed = true
				return
			}
			// forced: proceed, but the rewind is disclosed in the envelope below.
			env.Dropped, env.Rewound, env.Unknown = dropped, rewound, unknown
		}
	}
	moved := !waterEqual(committed, newWater)

	st.Water = newWater
	st.PackID = currentID // ISSUE-17: adopt the current query as the baseline (no-op if unchanged; re-baselines an --allow-drift advance so it isn't permanently "drifted")
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

// positionToken renders a cursor's committed position as the OPAQUE from/to token
// an agent passes back to `advance --to`: the `{container→offset}` water token for
// append, "snap:N" for diff. (Display uses committedField instead.)
func (c *cli) positionToken(st *glue.CursorState) string {
	switch st.Mode {
	case "diff":
		return fmt.Sprintf("snap:%d", st.SnapVersion)
	case "census":
		return fmt.Sprintf("census:%d", st.CensusVersion)
	}
	return encodeWater(st.Water)
}

// committedField renders a cursor's committed position for HUMAN display in
// show/list: a nested object (never a double-encoded string). For an append cursor
// with many containers it summarizes ({containers, min, max}) unless `verbose`
// (--positions) asks for the full per-container map; a single-container cursor
// shows the map directly. Diff cursors show the compact "snap:N".
func committedField(st *glue.CursorState, verbose bool) interface{} {
	if st.Mode == "diff" {
		return fmt.Sprintf("snap:%d", st.SnapVersion)
	}
	if st.Mode == "census" {
		return fmt.Sprintf("census:%d", st.CensusVersion)
	}
	if verbose || len(st.Water) <= 1 {
		return st.Water
	}
	var lo, hi int64
	first := true
	for _, off := range st.Water {
		if first || off < lo {
			lo = off
		}
		if first || off > hi {
			hi = off
		}
		first = false
	}
	return map[string]interface{}{"containers": len(st.Water), "min": lo, "max": hi}
}

// cursorDiffPeekAdvance is the diff-mode counterpart of the append peek/advance:
// it runs the pack over the FULL current state, diffs against the committed
// snapshot into Debezium {op,id,before,after} rows, and (on advance) replaces the
// snapshot + bumps the snap version.
func (c *cli) cursorDiffPeekAdvance(a cursorArgs, st *glue.CursorState, store *glue.CursorStore,
	sess *glue.Session, dets []glue.MultiQueryEntry, advance bool, currentID string, drifted bool) {

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
	if drifted {
		env.QueriesCurrent = currentID
	}

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
	st.PackID = currentID // ISSUE-17: adopt the current query as the baseline (see append path)
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
		Cursor    string      `json:"cursor"`
		Pack      string      `json:"queries"`
		Mode      string      `json:"mode"`
		Committed interface{} `json:"committed"`
		Advances  int         `json:"total_advances"`
	}
	out := make([]listRow, 0, len(names))
	for _, n := range names {
		st, lerr := store.Load(n)
		if lerr != nil {
			continue
		}
		out = append(out, listRow{Cursor: n, Pack: st.PackID, Mode: st.Mode,
			Committed: committedField(st, a.positions), Advances: st.TotalAdvances})
	}
	c.printJSON(out)
}

// cursorCheck re-hashes every cursor's query and reports which have DRIFTED since
// create (ISSUE-17: the drift check `plan` used to provide, gone in the rename). Prints
// one row per cursor {cursor, baseline, current, drifted} and exits nonzero if any
// drifted, so CI can gate on it. A census/builtin cursor (no query file) never drifts
// here; a cursor whose source path is gone is reported drifted (can't verify -> loud).
func (c *cli) cursorCheck(arg string) {
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
	type checkRow struct {
		Cursor   string `json:"cursor"`
		Baseline string `json:"baseline"`
		Current  string `json:"current,omitempty"`
		Drifted  bool   `json:"drifted"`
		Error    string `json:"error,omitempty"`
	}
	out := make([]checkRow, 0, len(names))
	anyDrift := false
	for _, n := range names {
		st, lerr := store.Load(n)
		if lerr != nil {
			continue
		}
		row := checkRow{Cursor: n, Baseline: st.PackID, Current: st.PackID}
		if st.Mode != "census" && st.Pack != "" { // pack-backed cursor: re-hash its query
			dets, derr := loadPackPaths(strings.Split(st.Pack, ","))
			if derr != nil {
				row.Current, row.Error, row.Drifted = "", derr.Error(), true
			} else {
				row.Current = glue.PackID(st.Name, dets)
				row.Drifted = row.Current != row.Baseline
			}
		}
		if row.Drifted {
			anyDrift = true
		}
		out = append(out, row)
	}
	c.printJSON(out)
	if anyDrift {
		c.failed = true // nonzero exit so a CI/monitor can gate on drift
	}
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
		Cursor        string      `json:"cursor"`
		Pack          string      `json:"pack,omitempty"`
		PackDir       string      `json:"pack_dir,omitempty"`
		Keyspace      string      `json:"keyspace,omitempty"`     // census mode
		CensusCells   int         `json:"census_cells,omitempty"` // census mode
		CensusRecords int64       `json:"census_records,omitempty"`
		Bind          string      `json:"bind,omitempty"`
		Mode          string      `json:"mode"`
		IdField       string      `json:"id_field,omitempty"`
		Committed     interface{} `json:"committed"`
		Description   string      `json:"description,omitempty"`
		Created       string      `json:"created,omitempty"`
		Updated       string      `json:"updated,omitempty"`
		LastCount     int         `json:"last_count"`
		TotalAdvances int         `json:"total_advances"`
	}{
		Cursor: st.Name, Pack: st.PackID, PackDir: st.Pack,
		Keyspace: st.Keyspace, CensusCells: len(st.Census), CensusRecords: st.CensusRecords,
		Bind: st.Bind,
		Mode: st.Mode, IdField: st.IdField, Committed: committedField(st, a.positions), Description: st.Description,
		Created: st.Created, Updated: st.Updated,
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

// cursorDriftRefuse reports that the cursor's query changed since create (ISSUE-17), so
// an advance would commit a position taken under the old query. Names both hashes.
func (c *cli) cursorDriftRefuse(name, baseline, current string) {
	c.printJSON(cursorEnvelope{
		Cursor: name, Pack: baseline, QueriesCurrent: current, Status: "error",
		Error: &cursorErr{Kind: "pack-drift", Message: fmt.Sprintf(
			"the query changed since create (baseline %s, current %s); advancing would commit a "+
				"position taken under the old query. Re-create the cursor, or pass --allow-drift.",
			baseline, current)},
	})
	c.failed = true
}

func (c *cli) cursorHelp() {
	fmt.Fprint(c.stderr, cursorHelpText)
}

const cursorHelpText = `.multi cursor <verb> — CEP named cursors (a durable "what's new since I last looked")

Two planes: RUN a cursor (the frequent loop) = peek/advance; MANAGE which cursors exist
(occasional) = create/list/show/rm.

  create NAME --queries <dir> [--bind <m>] [--from now|start] [--mode append|diff]
              [--id-field <name>] [--desc <t>]
  create NAME --queries "builtin:census?keyspace=<ks>[&type-field=f&time-field=f&depth=1|2&exclude=a,b]"
              [--bind <m>] [--from now|start]
                       bind + validate; set the start position. No rows.
                       --mode diff tracks a snapshot keyed by --id-field (default id)
                       and emits {op:insert|update|delete, id, before, after}.
                       A cursor over builtin:census accumulates a schema census of the
                       keyspace; peek/advance fold new records + emit drift (field_added
                       / type_changed). The census + watermark commit atomically.
                       A single-file cursor HONORS the query's front-matter (from / mode
                       / description); precedence is CLI flag > front-matter > default. A
                       front-matter key create does not consume is listed in "ignored".
  peek   NAME          the pending delta; does NOT move the cursor (re-peek is safe).
  advance NAME [--to <pos> | --to-file <path>] [--quiet] [--force]
                       commit (get + move). Echoes the delta unless --quiet.
                       --to <pos> commits the exact position peek reported (two-step);
                       the token is OPAQUE (survives argv verbatim). --to-file reads it
                       from a file (positions can be large — an append position is a
                       per-container map). A --to that would REWIND an append cursor
                       (drop a held container, rewind an offset, or name a container the
                       datastore lacks) is refused (error kind "unsafe-position", the
                       containers disclosed) — pass --force to commit it anyway.
                       If the query was EDITED since create, peek surfaces
                       "queries_current" and advance refuses (error kind "pack-drift")
                       unless --allow-drift (which adopts the new query as the baseline).
  list                 the cursors in the store.
  show   NAME [--positions]
                       one cursor's committed position + metadata (description);
                       committed is summarized for many-container append cursors unless
                       --positions asks for the full per-container map.
  check                re-hash every cursor's query and report which DRIFTED since
                       create ({cursor, baseline, current, drifted}); exits nonzero if
                       any drifted, so a CI/monitor can gate on it.
  rm     NAME          forget a cursor.

  --cursor-store <dir> override the state dir (default ./.n1k1-state/cursors, CWD-
                       relative and gitignorable; n1k1 never writes inside the datastore
                       bundle, which may be read-only or owned by another process).

Safe agent loop: peek (look) -> act -> advance --to <pos> --quiet. Each response is
one JSON envelope; switch on "status" (pending | advanced | empty | error).
`
