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
//	            [--annotation k=v ...] [--annotations-file <f>] [--labels ...] [--source-ref <sha>]
//	                          bind + validate (compile + probe binding); no rows. Client
//	                          metadata (annotations/labels/source-ref) is stored verbatim,
//	                          echoed by show, and OUTSIDE spec_hash (a retag never resets).
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
	name          string            // the cursor NAME (first positional)
	pack          []string          // --queries <dir> (repeatable / comma-list), create-only
	bind          string            // --bind <manifest>
	to            string            // --to <pos>, advance-only (the opaque position token)
	toFile        string            // --to-file <path>, advance-only (read --to from a file: large positions)
	from          string            // --from now|start, create-only (default: now)
	desc          string            // --desc <text>, create-only
	annotFile     string            // --annotations-file <path>, create-only (JSON blob base; a file, since .multi's tokenizer strips the quotes raw JSON needs -- same reason as --to-file)
	annotationKV  []string          // --annotation k=v (repeatable), create-only (quote-free overlays onto the blob)
	labels        string            // --labels k=v,... | {json}, create-only (indexable tags)
	sourceRef     string            // --source-ref <sha>, create-only (else auto-captured from git)
	params        map[string]string // --param k=v (repeatable): pack query parameters -- create binds + stores them; peek/advance replay the STORED ones
	store         string            // --cursor-store <dir> (override the default state dir)
	mode          string            // --mode append|diff, create-only (default: append)
	idField       string            // --id-field <name>, create-only diff (default: id)
	quiet         bool              // --quiet, advance-only (ack only, no labelResults echo)
	pruneRotated  bool              // --prune-rotated, advance-only (drop rotated containers from the committed position)
	acceptTrunc   bool              // --accept-truncation, advance-only (acknowledge truncated containers; re-baseline each to its current extent)
	force         bool              // --force, advance --to only (commit a rewinding/unknown position anyway)
	allowDrift    bool              // --allow-drift, advance only (commit even though the query edited since create)
	expect        string            // --expect <committed_id>, advance only (compare-and-swap: refuse if moved)
	positions     bool              // --positions, show-only (full position map, not a summary)
	long          bool              // --long, list-only (every cursor's full field set — one machine-readable table)
	only          []string          // --only <node,...>, compose-only (emit rows for just these)
	terminal      bool              // --terminal, compose-only (emit rows for leaf nodes only)
	allowRejected bool              // --allow-rejected, compose-only (don't hard-fail on a rejected node)

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
		case "annotations-file":
			if !hasEq {
				if err := need(&i, &val, "--annotations-file"); err != nil {
					return a, err
				}
			}
			a.annotFile = val
		case "annotation":
			if !hasEq {
				if err := need(&i, &val, "--annotation"); err != nil {
					return a, err
				}
			}
			a.annotationKV = append(a.annotationKV, val)
		case "labels":
			if !hasEq {
				if err := need(&i, &val, "--labels"); err != nil {
					return a, err
				}
			}
			a.labels = val
		case "source-ref", "source_ref":
			if !hasEq {
				if err := need(&i, &val, "--source-ref"); err != nil {
					return a, err
				}
			}
			a.sourceRef = val
		case "param":
			if !hasEq {
				if err := need(&i, &val, "--param"); err != nil {
					return a, err
				}
			}
			pk, pv, pok := strings.Cut(val, "=")
			if pk = strings.TrimSpace(pk); !pok || pk == "" {
				return a, fmt.Errorf("--param %q must be key=value", val)
			}
			if a.params == nil {
				a.params = map[string]string{}
			}
			a.params[pk] = pv
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
		case "prune-rotated":
			a.pruneRotated = !hasEq || val == "true" || val == "1"
		case "accept-truncation":
			a.acceptTrunc = !hasEq || val == "true" || val == "1"
		case "force":
			a.force = !hasEq || val == "true" || val == "1"
		case "allow-drift":
			a.allowDrift = !hasEq || val == "true" || val == "1"
		case "expect":
			if !hasEq {
				if err := need(&i, &val, "--expect"); err != nil {
					return a, err
				}
			}
			a.expect = val
		case "positions":
			a.positions = !hasEq || val == "true" || val == "1"
		case "long":
			a.long = !hasEq || val == "true" || val == "1"
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
	Cursor string `json:"cursor"`
	// Pack is the cursor's content id; emitted as "queries" (ISSUE-15 §2a: one
	// vocabulary — `queries` is the id everywhere, `queries_path` the source path).
	Pack         string      `json:"queries,omitempty"`
	Status       string      `json:"status"` // pending | advanced | empty | error
	From         string      `json:"from,omitempty"`
	To           string      `json:"to,omitempty"`
	ToID         string      `json:"to_id,omitempty"` // digest of the position this op moves TO (peek: the pending head)
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

	// "Append-mostly, with whole-file rotation" (DESIGN-cep.md): committed containers
	// whose SOURCE violated append-only this scan. Rotated = held in the committed
	// position but nothing observed (file deleted/renamed, or now empty); truncated =
	// the observed extent fell below the committed offset (rewritten shorter — its
	// records below the old offset are skipped, since the watermark never rewinds).
	// Disclosure, not failure: a census/doctor can correlate a count drop with the
	// evidence leaving. `advance --prune-rotated` drops the rotated entries from the
	// committed position (PrunedRotated acks how many). Rewritten = the committed
	// boundary-record fingerprint (water_fp) no longer matches the record at that
	// offset — the file was replaced in place WITHOUT shrinking, the violation a
	// size check cannot see.
	// The disclosure keys (rotated/truncated/rewritten) say what was SEEN this scan;
	// the accepted_* keys say what an --accept-truncation advance ACTED ON (ISSUE-18
	// ask #4) — a caller can tell "I was told" from "I acknowledged" without reading
	// the sidecar.
	Rotated            []string `json:"rotated,omitempty"`
	Truncated          []string `json:"truncated,omitempty"`
	Rewritten          []string `json:"rewritten,omitempty"`
	AcceptedTruncation []string `json:"accepted_truncation,omitempty"`
	AcceptedRewritten  []string `json:"accepted_rewritten,omitempty"`
	PrunedRotated      int      `json:"pruned_rotated,omitempty"`

	// ISSUE-17: the query file can be edited after create; `pack` is the baseline
	// (creation-time) id, QueriesCurrent is the id re-hashed from the query NOW. They
	// differ iff the query drifted. advance refuses on drift unless --allow-drift.
	QueriesCurrent string `json:"queries_current,omitempty"`

	// ISSUE-15: a fixed-width digest of the COMMITTED position (see committedID) — the
	// machine-readable "did the position move?" surface + the handle for `advance
	// --expect <committed_id>` compare-and-swap.
	CommittedID string `json:"committed_id,omitempty"`
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
		return nil, fmt.Errorf("no queries in the source")
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
		c.cursorFail(a.name, "queries-load", err)
		return
	}
	// Bind the pack's query parameters NOW (--param over declared defaults) and store
	// the RESOLVED set: peek/advance replay exactly these values, so a later
	// front-matter default change can never silently move this cursor, and QueriesID
	// below hashes the rendered statements -- params are inside the delta identity.
	dets, resolvedParams, err := glue.ApplyParams(dets, a.params)
	if err != nil {
		c.cursorFail(a.name, "bad-args", err)
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
	honoredFM := map[string]bool{
		"from": true, "mode": true,
		"annotations": true, "labels": true, "source-ref": true, "source_ref": true,
	}
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
		QueriesPath: strings.Join(a.pack, ","),
		Bind:        a.bind,
		Queries:     glue.QueriesID(a.name, dets),
		HashScheme:  glue.QueriesHashScheme,
		Params:      resolvedParams,
		Mode:        mode,
		Description: effDesc,
		Created:     now,
		Updated:     now,
	}
	// Client-owned metadata (DESIGN-cep.md labels/annotations split): CLI flags win over
	// front-matter. These are OUTSIDE QueriesID/spec_hash, so stamping provenance never moves
	// the position -- see CursorState.Annotations. Populated before Save; consumed keys are
	// pulled out of `ignored` above via honoredFM.
	ann, aerr := buildAnnotations(a, fm)
	if aerr != nil {
		c.cursorFail(a.name, "bad-args", aerr)
		return
	}
	st.Annotations = ann
	st.Labels = buildLabels(a, fm)
	st.SourceRef = resolveSourceRef(a, fm)
	var fromTok string

	if mode == "append" {
		// Validate by compiling + running once; --from now yields the current head.
		res, rerr := sess.RunCursorPack(dets, nil, nil)
		if rerr != nil {
			c.cursorFail(a.name, "compile", rerr)
			return
		}
		st.Water = res.NewWater
		st.WaterFP = glue.WaterFPMerge(st.Water, res.Observed, res.ObservedFP, nil, nil)
		if fromStart {
			st.Water = map[string]int64{} // replay everything on the first peek
			st.WaterFP = nil
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
		Created   string   `json:"created"`
		OK        bool     `json:"ok"`
		Pack      string   `json:"queries"`
		Compiles  string   `json:"compiles"`
		Mode      string   `json:"mode"`
		From      string   `json:"from"`
		SourceRef string   `json:"source_ref,omitempty"`
		Ignored   []string `json:"ignored,omitempty"`
	}{
		Created:   a.name,
		OK:        true,
		Ignored:   ignored,
		Pack:      st.Queries,
		Compiles:  "ok",
		Mode:      mode,
		From:      fromTok,
		SourceRef: st.SourceRef,
	})
}

// buildAnnotations assembles the cursor's verbatim annotations blob: a base JSON object
// (--annotations-file's contents, else the front-matter `annotations:` value) with any
// repeated --annotation k=v pairs overlaid as strings. An invalid JSON base is a hard error,
// not a silent drop (the whole point is that declared provenance is never lost). Returns nil
// when nothing was supplied, so the sidecar key stays omitted.
func buildAnnotations(a cursorArgs, fm map[string]string) (map[string]interface{}, error) {
	out := map[string]interface{}{}
	raw := fm["annotations"]
	if a.annotFile != "" {
		b, err := os.ReadFile(a.annotFile)
		if err != nil {
			return nil, fmt.Errorf("--annotations-file: %w", err)
		}
		raw = string(b)
	}
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			return nil, fmt.Errorf("annotations is not a JSON object: %w", err)
		}
	}
	for _, kv := range a.annotationKV {
		k, v, ok := strings.Cut(kv, "=")
		if k = strings.TrimSpace(k); !ok || k == "" {
			return nil, fmt.Errorf("--annotation %q must be key=value", kv)
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// buildLabels resolves the cursor's indexable k=v tags (CLI --labels over front-matter),
// accepting BOTH `k=v, k2=v2` and a JSON object (ISSUE-03 #3).
func buildLabels(a cursorArgs, fm map[string]string) map[string]string {
	raw := a.labels
	if raw == "" {
		raw = fm["labels"]
	}
	return parseLabelSpec(raw)
}

func parseLabelSpec(s string) map[string]string {
	if s = strings.TrimSpace(s); s == "" {
		return nil
	}
	if strings.HasPrefix(s, "{") { // JSON-object form
		var m map[string]string
		if json.Unmarshal([]byte(s), &m) == nil && len(m) > 0 {
			return m
		}
		return nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if k = strings.TrimSpace(k); ok && k != "" {
			out[k] = strings.TrimSpace(v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// resolveSourceRef determines the cursor's git provenance ref: an explicit --source-ref (or
// front-matter source-ref/source_ref) wins; otherwise best-effort auto-capture of the
// queries source's git HEAD (ISSUE-03 #5 -- so the git↔position correlation lives IN the
// cursor instead of an external ledger). Empty when the source isn't a git repo.
func resolveSourceRef(a cursorArgs, fm map[string]string) string {
	if a.sourceRef != "" {
		return a.sourceRef
	}
	if v := fm["source-ref"]; v != "" {
		return v
	}
	if v := fm["source_ref"]; v != "" {
		return v
	}
	if len(a.pack) > 0 {
		dir := a.pack[0]
		if fi, err := os.Stat(dir); err == nil && !fi.IsDir() {
			dir = filepath.Dir(dir)
		}
		if ref, ok := glue.GitCommitOf(dir); ok {
			return ref
		}
	}
	return ""
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

	dets, err := loadPackPaths(strings.Split(st.QueriesPath, ","))
	if err != nil {
		c.cursorFail(a.name, "queries-load", err)
		return
	}
	// Params are FIXED at create (stored resolved in the sidecar) and replayed here —
	// a different value against a position advanced under the old one is the ISSUE-17
	// hazard. A --param on peek/advance is therefore a hard error, not an override.
	if len(a.params) > 0 {
		c.cursorFail(a.name, "bad-args", fmt.Errorf(
			"params are fixed at create (this cursor stored: %s); rm + create to change them",
			paramsSummary(st.Params)))
		return
	}
	dets, _, err = glue.ApplyParams(dets, st.Params)
	if err != nil {
		c.cursorFail(a.name, "queries-load", fmt.Errorf("replaying stored params: %v", err))
		return
	}
	sess, binding, err := c.multiSession(st.Bind)
	if err != nil {
		c.cursorFail(a.name, "open", err)
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		from := c.positionToken(st)
		env := cursorEnvelope{Cursor: a.name, Pack: st.Queries, Status: "error",
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
	// Drift = the stored id matches under NO known hash scheme (QueriesIDMatches) --
	// a plain == against the current scheme would fake drift on every cursor stamped
	// by an older binary whenever a normalization scheme changes; advance re-stamps
	// st.Queries to the current scheme below, migrating old sidecars forward.
	currentID := glue.QueriesID(st.Name, dets)
	drifted := glue.QueriesIDMatches(st.Queries, st.Name, dets) == 0
	if advance && drifted && !a.allowDrift {
		c.cursorDriftRefuse(a.name, st.Queries, currentID)
		return
	}

	// ISSUE-15: `advance --expect <committed_id>` is a compare-and-swap — refuse if the
	// committed position moved since the caller peeked (a concurrent runner, or a stale
	// retry). This is the principled guard behind the ISSUE-13 family: instead of
	// n1k1 guessing which positions are "silly", the caller asserts the base it acted on.
	if advance && a.expect != "" {
		if got := committedID(st); got != a.expect {
			c.cursorExpectRefuse(a.name, got, a.expect)
			return
		}
	}

	if st.Mode == "diff" {
		c.cursorDiffPeekAdvance(a, st, store, sess, dets, advance, currentID, drifted)
		return
	}

	committed := st.Water
	res, err := sess.RunCursorPack(dets, committed, st.WaterFP)
	if err != nil {
		env := cursorEnvelope{Cursor: a.name, Pack: st.Queries, Status: "error",
			From: encodeWater(committed), To: encodeWater(committed),
			Error: &cursorErr{Kind: "run", Message: err.Error()}}
		c.printJSON(env)
		c.failed = true
		return
	}

	rows := toRows(res.LabelResults)
	env := cursorEnvelope{
		Cursor: a.name, Pack: st.Queries,
		From:        encodeWater(committed),
		Count:       len(rows),
		CommittedID: committedID(st), // the CURRENTLY-committed position (peek: unchanged)
		// Source-side append violations (rotation/truncation/rewrite) are DISCLOSED on
		// every peek/advance — evidence leaving the corpus is an event, never silent.
		Rotated:   res.Rotated,
		Truncated: res.Truncated,
		Rewritten: res.Rewritten,
	}
	if drifted {
		env.QueriesCurrent = currentID
	}

	if !advance {
		// peek: never moves; the next peek still returns this (at-least-once).
		env.To = encodeWater(res.NewWater)
		env.ToID = positionDigest(env.To) // the id peek would advance TO (committed_id is FROM)
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

	// Fail-loud on truncation (DESIGN-cep.md, "append-mostly with whole-file
	// rotation" rung 2): a truncated container means the SOURCE violated mode:
	// append's contract, and committing past it silently entrenches the loss — worse,
	// under the never-rewinding max-merge the container stays dead until the file
	// regrows past its old offset (future appends below it are skipped too). Refuse,
	// position untouched, unless the caller acknowledges the discontinuity with
	// --accept-truncation (the --allow-drift shape) — which re-baselines each
	// truncated container to its CURRENT extent: rewritten content below the old
	// offset is not re-delivered, and future appends deliver again. Rotation does
	// NOT refuse: nothing mis-delivers, disclosure (+ --prune-rotated) suffices.
	if (len(res.Truncated) > 0 || len(res.Rewritten) > 0) && !a.acceptTrunc {
		kind := "source-truncated"
		if len(res.Rewritten) > 0 {
			kind = "source-rewritten" // fingerprint mismatch: replaced in place, size unchanged
		}
		env.Status = "error"
		env.To = encodeWater(committed)
		env.Error = &cursorErr{Kind: kind, Message: fmt.Sprintf(
			"%d committed container(s) violated append-only (%d truncated, %d rewritten in place -- "+
				"see \"truncated\"/\"rewritten\"); the committed position no longer describes them, and "+
				"records below each old offset are being skipped. Re-run with --accept-truncation to "+
				"acknowledge the discontinuity and re-baseline each at its current content (no "+
				"re-delivery of rewritten content; future appends deliver again).",
			len(res.Truncated)+len(res.Rewritten), len(res.Truncated), len(res.Rewritten))}
		c.printJSON(env)
		c.failed = true
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
					Cursor: a.name, Pack: st.Queries, Status: "error",
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
	// --accept-truncation: re-baseline each truncated container at the position this
	// scan observed (NewWater's max-merge kept the old, higher offset). Applied WITH
	// or WITHOUT an explicit --to (ISSUE-18): the documented safe loop is
	// `peek -> advance --to-file <to>`, and peek's token carries the max-merged,
	// never-rewound water — it CANNOT express the rewind, so "the token wins" here
	// silently skipped the re-baseline, cleared the disclosure, and every record
	// later appended below the stale mark vanished. Lower-only: a hand-crafted --to
	// that rewinds a truncated container even deeper (deliberate replay) is honored.
	if a.acceptTrunc {
		for _, k := range res.Truncated {
			if v, ok := res.Observed[k]; ok {
				if cur, held := newWater[k]; !held || cur > v {
					newWater[k] = v
				}
			}
		}
		// Post-condition (ISSUE-18 ask #2): an acknowledgment that leaves a truncated
		// container's mark above its observed content is strictly worse than the
		// refusal — the disclosure is consumed but the silence returns. Fail loud;
		// this should be unreachable, which is exactly why it must not be silent.
		for _, k := range res.Truncated {
			if v, ok := res.Observed[k]; !ok || newWater[k] > v {
				c.cursorFail(a.name, "accept-failed", fmt.Errorf(
					"internal: --accept-truncation left container %q committed above its observed content; position NOT committed", k))
				return
			}
		}
		env.AcceptedTruncation = res.Truncated
		env.AcceptedRewritten = res.Rewritten
	}

	// --prune-rotated: deliberately shrink the committed position by the containers
	// whose files rotated away (disclosed in env.Rotated). Without this the position
	// map grows with every file ever seen. The cost is disclosed in help: if a
	// same-named file later reappears, it is a NEW container and replays from byte 0.
	if a.pruneRotated {
		for _, k := range res.Rotated {
			if _, held := newWater[k]; held {
				delete(newWater, k)
				env.PrunedRotated++
			}
		}
	}
	moved := !waterEqual(committed, newWater)

	st.Water = newWater
	// Commit the boundary-record fingerprints beside the water: observed-this-scan
	// where the offset matches, carried forward where it didn't move, and BACKFILLED
	// opportunistically for a legacy sidecar (no flag day — the hash_scheme pattern).
	st.WaterFP = glue.WaterFPMerge(newWater, res.Observed, res.ObservedFP, committed, st.WaterFP)
	st.Queries = currentID                 // ISSUE-17: adopt the current query as the baseline (no-op if unchanged; re-baselines an --allow-drift advance so it isn't permanently "drifted")
	st.HashScheme = glue.QueriesHashScheme // migrate an old-scheme sidecar forward
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
	env.CommittedID = committedID(st) // now reflects the NEWLY-committed position
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

// committedID is a short, fixed-width digest of a cursor's COMMITTED position
// (ISSUE-15): a sha over the canonical position token. Unlike list's {containers,min,max}
// summary it changes iff the position changes — even a one-byte rewind of one middle
// container — so it is a safe compare-and-swap handle for `advance --expect` and the
// machine-readable "did anything move?" surface. Uniform across modes.
func committedID(st *glue.CursorState) string {
	var pos string
	switch st.Mode {
	case "diff":
		pos = fmt.Sprintf("snap:%d", st.SnapVersion)
	case "census":
		pos = fmt.Sprintf("census:%d", st.CensusVersion)
	default:
		pos = encodeWater(st.Water)
	}
	return positionDigest(pos)
}

// positionDigest is the fixed-width sha of a position token — the shared core of
// committedID and the peek `to_id` (the id a peek WOULD advance to).
func positionDigest(pos string) string {
	h := sha256.Sum256([]byte("cursor-pos\x00" + pos))
	return hex.EncodeToString(h[:])[:12]
}

// specHash extracts the content-hash tail of a pack id ("<name>@<sha>" -> "<sha>"),
// the field n1k1-for-ai's provenance ledger pins (ISSUE-15 ask 5). "" if no @.
func specHash(packID string) string {
	if i := strings.LastIndexByte(packID, '@'); i >= 0 {
		return packID[i+1:]
	}
	return ""
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
		c.printJSON(cursorEnvelope{Cursor: st.Name, Pack: st.Queries, Status: "error",
			From: from, To: from, Error: &cursorErr{Kind: "run", Message: err.Error()}})
		c.failed = true
		return
	}
	current, _ := glue.SnapshotFromResults(lrs, st.IdField)
	events := glue.DiffSnapshot(prior, current, st.IdField)
	rows := diffRows(events)
	changed := len(events) > 0

	env := cursorEnvelope{Cursor: st.Name, Pack: st.Queries, From: from, Count: len(rows), CommittedID: committedID(st)}
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
		env.ToID = positionDigest(env.To)
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
	st.Queries = currentID                 // ISSUE-17: adopt the current query as the baseline (see append path)
	st.HashScheme = glue.QueriesHashScheme // migrate an old-scheme sidecar forward
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
	env.CommittedID = committedID(st) // now reflects the newly-committed snapshot
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
	// listRow is the compact inventory; --long (ISSUE-15 §2b) adds every field a status
	// table needs, so it is one call over all cursors instead of N+1 sidecar reads.
	type listRow struct {
		Cursor      string      `json:"cursor"`
		Pack        string      `json:"queries"`
		SpecHash    string      `json:"spec_hash,omitempty"`
		Mode        string      `json:"mode"`
		Committed   interface{} `json:"committed"`
		CommittedID string      `json:"committed_id"`
		Advances    int         `json:"total_advances"`

		// --long only:
		QueriesPath string                 `json:"queries_path,omitempty"`
		Bind        string                 `json:"bind,omitempty"`
		IdField     string                 `json:"id_field,omitempty"`
		Schema      int                    `json:"schema,omitempty"`
		HashScheme  int                    `json:"hash_scheme,omitempty"`
		Description string                 `json:"description,omitempty"`
		Annotations map[string]interface{} `json:"annotations,omitempty"`
		Labels      map[string]string      `json:"labels,omitempty"`
		SourceRef   string                 `json:"source_ref,omitempty"`
		Params      map[string]string      `json:"params,omitempty"`
		Created     string                 `json:"created,omitempty"`
		Updated     string                 `json:"updated,omitempty"`
		LastCount   int                    `json:"last_count,omitempty"`
	}
	out := make([]listRow, 0, len(names))
	for _, n := range names {
		st, lerr := store.Load(n)
		if lerr != nil {
			continue
		}
		row := listRow{Cursor: n, Pack: st.Queries, SpecHash: specHash(st.Queries), Mode: st.Mode,
			Committed: committedField(st, a.positions), CommittedID: committedID(st),
			Advances: st.TotalAdvances}
		if a.long {
			row.QueriesPath, row.Bind, row.IdField = st.QueriesPath, st.Bind, st.IdField
			row.Schema, row.Description = glue.CursorSchemaVersion, st.Description
			row.HashScheme = st.HashScheme
			row.Annotations, row.Labels, row.SourceRef = st.Annotations, st.Labels, st.SourceRef
			row.Params = st.Params
			row.Created, row.Updated, row.LastCount = st.Created, st.Updated, st.LastCount
		}
		out = append(out, row)
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
		row := checkRow{Cursor: n, Baseline: st.Queries, Current: st.Queries}
		if st.Mode != "census" && st.QueriesPath != "" { // pack-backed cursor: re-hash its query
			dets, derr := loadPackPaths(strings.Split(st.QueriesPath, ","))
			if derr == nil {
				dets, _, derr = glue.ApplyParams(dets, st.Params) // replay the stored params
			}
			if derr != nil {
				row.Current, row.Error, row.Drifted = "", derr.Error(), true
			} else {
				// Any-scheme compare (not ==): a baseline stamped by an older binary
				// under an older normalization scheme is NOT drift (see QueriesIDMatches).
				row.Current = glue.QueriesID(st.Name, dets)
				row.Drifted = glue.QueriesIDMatches(row.Baseline, st.Name, dets) == 0
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
		Pack          string      `json:"queries"`                // the content id (ISSUE-15 §2a: one vocabulary)
		QueriesPath   string      `json:"queries_path,omitempty"` // the source dir/file
		SpecHash      string      `json:"spec_hash,omitempty"`    // the @sha tail of the id (ISSUE-15 ask 5)
		Keyspace      string      `json:"keyspace,omitempty"`     // census mode
		CensusCells   int         `json:"census_cells,omitempty"` // census mode
		CensusRecords int64       `json:"census_records,omitempty"`
		Bind          string      `json:"bind,omitempty"`
		Mode          string      `json:"mode"`
		IdField       string      `json:"id_field,omitempty"`
		Committed     interface{} `json:"committed"`
		CommittedID   string      `json:"committed_id"`
		Schema        int         `json:"schema"`                // sidecar/output schema version (ISSUE-15 §3)
		HashScheme    int         `json:"hash_scheme,omitempty"` // QueriesID normalization scheme that stamped `queries`
		Description   string      `json:"description,omitempty"`
		// Client-owned metadata, echoed verbatim (outside spec_hash) -- provenance lives here.
		Annotations   map[string]interface{} `json:"annotations,omitempty"`
		Labels        map[string]string      `json:"labels,omitempty"`
		SourceRef     string                 `json:"source_ref,omitempty"`
		Params        map[string]string      `json:"params,omitempty"` // resolved at create; replayed on every peek/advance
		Created       string                 `json:"created,omitempty"`
		Updated       string                 `json:"updated,omitempty"`
		LastCount     int                    `json:"last_count"`
		TotalAdvances int                    `json:"total_advances"`
	}{
		Cursor: st.Name, Pack: st.Queries, QueriesPath: st.QueriesPath, SpecHash: specHash(st.Queries),
		Keyspace: st.Keyspace, CensusCells: len(st.Census), CensusRecords: st.CensusRecords,
		Bind: st.Bind,
		Mode: st.Mode, IdField: st.IdField, Committed: committedField(st, a.positions),
		CommittedID: committedID(st), Schema: glue.CursorSchemaVersion, Description: st.Description,
		HashScheme:  st.HashScheme,
		Annotations: st.Annotations, Labels: st.Labels, SourceRef: st.SourceRef, Params: st.Params,
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

// cursorExpectRefuse reports an `advance --expect` compare-and-swap miss (ISSUE-15): the
// committed position moved since the caller captured `expected`, so the advance is refused.
func (c *cli) cursorExpectRefuse(name, got, expected string) {
	c.printJSON(cursorEnvelope{
		Cursor: name, Status: "error", CommittedID: got,
		Error: &cursorErr{Kind: "stale", Message: fmt.Sprintf(
			"cursor moved since you looked: committed_id is %s, --expect was %s. "+
				"peek again to re-read the position, then advance.", got, expected)},
	})
	c.failed = true
}

// cursorDriftRefuse reports that the cursor's query changed since create (ISSUE-17), so
// an advance would commit a position taken under the old query. Names both hashes.
func (c *cli) cursorDriftRefuse(name, baseline, current string) {
	c.printJSON(cursorEnvelope{
		Cursor: name, Pack: baseline, QueriesCurrent: current, Status: "error",
		Error: &cursorErr{Kind: "query-drift", Message: fmt.Sprintf(
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
              [--id-field <name>] [--desc <t>] [--param k=v ...]
              [--annotation k=v ...] [--annotations-file <f>] [--labels k=v,...] [--source-ref <sha>]
  create NAME --queries "builtin:census?keyspace=<ks>[&type-field=f&time-field=f&depth=1|2&exclude=a,b]"
              [--bind <m>] [--from now|start]
                       bind + validate; set the start position. No rows.
                       --mode diff tracks a snapshot keyed by --id-field (default id)
                       and emits {op:insert|update|delete, id, before, after}.
                       A cursor over builtin:census accumulates a schema census of the
                       keyspace; peek/advance fold new records + emit drift (field_added
                       / type_changed). The census + watermark commit atomically.
                       A single-file cursor HONORS the query's front-matter (from / mode
                       / description / annotations / labels / source-ref); precedence is
                       CLI flag > front-matter > default. A front-matter key create does
                       not consume is listed in "ignored".
                       Client metadata (echoed by show/list --long, NEVER folded into
                       spec_hash, so a retag never re-baselines the position): --annotation
                       k=v is a quote-free overlay (repeatable); --annotations-file is a
                       JSON blob (a file, since .multi's tokenizer strips the quotes raw
                       JSON needs — as with --to-file); --labels are indexable tags. The
                       home for provenance — e.g. --annotation git_sha=$(git rev-parse HEAD),
                       or --source-ref, which AUTO-captures the queries dir's git HEAD
                       (+"-dirty") when unset, so "which commit produced this position"
                       lives in the cursor instead of an external ledger.
  peek   NAME          the pending delta; does NOT move the cursor (re-peek is safe).
                       Both peek and advance DISCLOSE source-side append violations
                       ("append-mostly, with whole-file rotation"): "rotated" lists
                       committed containers no longer observed (file deleted, or now
                       empty), "truncated" those rewritten SHORTER than the committed
                       offset (their records below it are skipped -- the watermark
                       never rewinds, so nothing double-delivers), and "rewritten"
                       those replaced in place WITHOUT shrinking -- caught by the
                       boundary-record fingerprint (water_fp: the hash of the record
                       at each committed offset, kept beside the position; the one
                       violation a size check cannot see). Evidence leaving the
                       corpus is an event, never silent; a field that reads 0 NOW
                       may still exist in an accumulated census.
  advance NAME [--to <pos> | --to-file <path>] [--quiet] [--force]
               [--prune-rotated] [--accept-truncation]
                       commit (get + move). Echoes the delta unless --quiet.
                       --prune-rotated drops the rotated containers from the
                       committed position (acked as "pruned_rotated"; without it the
                       position map holds every container ever seen). Cost: if a
                       same-named file later reappears, it replays from byte 0.
                       A TRUNCATED or REWRITTEN container REFUSES the advance (error
                       kind "source-truncated" / "source-rewritten", position
                       untouched): the source violated append-only, and committing
                       past it would entrench the loss — a truncated container even
                       stays dead until the file regrows past its old offset.
                       --accept-truncation acknowledges the discontinuity (the
                       --allow-drift shape) and re-baselines each violating
                       container at its CURRENT content: nothing below the old
                       offset is re-delivered; future appends deliver again, and
                       the fingerprint re-stamps to the new content. The
                       re-baseline applies with or without --to/--to-file (a
                       peeked token carries the never-rewound water, so it cannot
                       express the rewind itself — ISSUE-18); a --to that rewinds
                       a truncated container even deeper is honored. The advance
                       acks what it acted on as "accepted_truncation" /
                       "accepted_rewritten", distinct from the disclosure keys.
                       (A census cursor stays disclosure-only: its fold is additive
                       and never rewinds, so blocking it would only lose more.)
                       --to <pos> commits the exact position peek reported (two-step);
                       the token is OPAQUE (survives argv verbatim). --to-file reads it
                       from a file (positions can be large — an append position is a
                       per-container map). A --to that would REWIND an append cursor
                       (drop a held container, rewind an offset, or name a container the
                       datastore lacks) is refused (error kind "unsafe-position", the
                       containers disclosed) — pass --force to commit it anyway.
                       If the query was EDITED since create, peek surfaces
                       "queries_current" and advance refuses (error kind "query-drift")
                       unless --allow-drift (which adopts the new query as the baseline).
                       --expect <committed_id> is a compare-and-swap: advance only if the
                       committed position still matches (peek reports committed_id);
                       refuses (error kind "stale") if another run moved it. Safe for
                       concurrent runners / stale retries.
  list [--long]        the cursors in the store (one JSON array). --long adds every
                       field a status table needs (queries_path, spec_hash, bind,
                       description, created/updated, ...) so it is ONE call, not an
                       N+1 sweep of the private sidecar files. Vocabulary: "queries" is
                       the content id everywhere, "queries_path" the source dir/file.
  show   NAME [--positions]
                       one cursor's committed position + metadata (description, and any
                       annotations / labels / source_ref set at create); committed is
                       summarized for many-container append cursors unless --positions
                       asks for the full per-container map.
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

// paramsSummary renders a cursor's stored params compactly for error messages.
func paramsSummary(params map[string]string) string {
	if len(params) == 0 {
		return "(none)"
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + params[k]
	}
	return strings.Join(parts, ", ")
}
