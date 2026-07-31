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

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestMultiCursorAppendLoop drives the full Phase-1 CLI loop over a growing jsonl
// keyspace: create --from start (replay), peek (pending), advance (commit), peek
// (empty), append, peek (the one new match), advance --quiet (ack), then
// show/list/rm.
func TestMultiCursorAppendLoop(t *testing.T) {
	root := t.TempDir()
	ksDir := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(ksDir, "events.jsonl")
	appendLines := func(lines ...string) {
		f, err := os.OpenFile(events, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		for _, ln := range lines {
			if _, err := f.WriteString(ln + "\n"); err != nil {
				t.Fatal(err)
			}
		}
		f.Close()
	}
	appendLines(`{"n":1,"sev":"ERROR"}`, `{"n":2,"sev":"INFO"}`, `{"n":3,"sev":"ERROR"}`)

	pack := writeMultiQueryEntries(t, map[string]string{
		"errs": "-- label: errs\n" + `SELECT e.n FROM events e WHERE e.sev = "ERROR"`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	store := t.TempDir()

	// run executes a `.multi cursor ...` command and returns the parsed envelope.
	run := func(cmd string) map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("cursor " + cmd + " --cursor-store " + store)
		line := strings.TrimSpace(out.String())
		if line == "" {
			t.Fatalf("cursor %q: no JSON output (stderr: %s)", cmd, errb.String())
		}
		var env map[string]interface{}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			t.Fatalf("cursor %q: bad JSON %q: %v", cmd, line, err)
		}
		return env
	}
	countOf := func(env map[string]interface{}) int {
		n, _ := env["count"].(float64)
		return int(n)
	}

	// create --from start: replay everything on the first peek.
	if env := run("create errs --queries " + pack + " --from start"); env["ok"] != true {
		t.Fatalf("create: not ok: %v", env)
	}

	// peek: both ERRORs are pending; the cursor did NOT move.
	env := run("peek errs")
	if env["status"] != "pending" || countOf(env) != 2 || env["advanced"] != false {
		t.Fatalf("peek(seed): want pending/2/advanced=false, got %v", env)
	}

	// re-peek is stable (non-advancing): still 2.
	if env := run("peek errs"); countOf(env) != 2 {
		t.Fatalf("re-peek: want 2 (non-advancing), got %v", env)
	}

	// advance: commit past both, echoing the delta.
	env = run("advance errs")
	if env["status"] != "advanced" || env["advanced"] != true || countOf(env) != 2 {
		t.Fatalf("advance: want advanced/true/2, got %v", env)
	}
	committedTo, _ := env["to"].(string)

	// peek now: nothing new.
	if env := run("peek errs"); env["status"] != "empty" || countOf(env) != 0 {
		t.Fatalf("peek(after advance): want empty/0, got %v", env)
	}

	// Append 1 ERROR + 1 WARN. Only the ERROR is a new match.
	appendLines(`{"n":4,"sev":"ERROR"}`, `{"n":5,"sev":"WARN"}`)

	env = run("peek errs")
	if env["status"] != "pending" || countOf(env) != 1 {
		t.Fatalf("peek(after append): want pending/1, got %v", env)
	}
	if env["from"].(string) != committedTo {
		t.Fatalf("peek: from should equal last committed 'to' %q, got %q", committedTo, env["from"])
	}
	lrs, _ := env["labelResults"].([]interface{})
	if len(lrs) != 1 {
		t.Fatalf("peek: want 1 labelResult, got %v", env["labelResults"])
	}
	row := lrs[0].(map[string]interface{})
	rb, _ := json.Marshal(row["result"])
	if string(rb) != `{"n":4}` {
		t.Fatalf("peek: delivered %s, want {\"n\":4}", rb)
	}
	if fp, _ := row["fingerprint"].(string); fp == "" {
		t.Fatalf("peek: labelResult missing fingerprint: %v", row)
	}

	// advance --quiet: commit, ack only (no labelResults echoed).
	env = run("advance errs --quiet")
	if env["status"] != "advanced" || env["advanced"] != true {
		t.Fatalf("advance --quiet: want advanced/true, got %v", env)
	}
	if _, has := env["labelResults"]; has {
		t.Fatalf("advance --quiet: labelResults should be suppressed, got %v", env["labelResults"])
	}

	// show: committed position is present (a nested object, not a string) + append mode.
	if env := run("show errs"); env["mode"] != "append" || env["committed"] == nil {
		t.Fatalf("show: want append mode + non-nil committed object, got %v", env)
	}

	// list: includes our cursor.
	out.Reset()
	c.cmdMulti("cursor list --cursor-store " + store)
	if !strings.Contains(out.String(), `"cursor":"errs"`) {
		t.Fatalf("list: missing errs: %s", out.String())
	}

	// rm: then peek reports no-such-cursor.
	if env := run("rm errs"); env["ok"] != true {
		t.Fatalf("rm: not ok: %v", env)
	}
	if env := run("peek errs"); env["status"] != "error" {
		t.Fatalf("peek after rm: want error, got %v", env)
	}
}

// TestMultiCursorCreateGuards: create requires a name + pack, refuses a duplicate,
// and a peek on an unknown cursor is a structured error (not a crash).
func TestMultiCursorCreateGuards(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "default", "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "default", "events", "e.jsonl"),
		[]byte(`{"n":1,"sev":"ERROR"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := writeMultiQueryEntries(t, map[string]string{
		"errs": "-- label: errs\n" + `SELECT e.n FROM events e WHERE e.sev = "ERROR"`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	store := t.TempDir()
	env := func() map[string]interface{} {
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}

	out.Reset()
	c.cmdMulti("cursor create --cursor-store " + store) // no name
	if env()["status"] != "error" {
		t.Fatalf("create no-name: want error, got %s", out.String())
	}

	out.Reset()
	c.cmdMulti("cursor create c1 --cursor-store " + store) // no pack
	if env()["status"] != "error" {
		t.Fatalf("create no-pack: want error, got %s", out.String())
	}

	out.Reset()
	c.cmdMulti("cursor create c1 --queries " + pack + " --cursor-store " + store)
	if env()["ok"] != true {
		t.Fatalf("create c1: want ok, got %s", out.String())
	}

	out.Reset()
	c.cmdMulti("cursor create c1 --queries " + pack + " --cursor-store " + store) // duplicate
	if e := env(); e["status"] != "error" || e["error"].(map[string]interface{})["kind"] != "exists" {
		t.Fatalf("create duplicate: want error/exists, got %s", out.String())
	}
}

// TestMultiCursorCreateFrontMatter guards ISSUE-14: a single-file cursor's query
// front-matter (from/description/mode) must be honored by `create` (cursor apply used
// to, and apply was removed in the rename). A dropped `from: start` silently baselined
// at NOW and skipped the whole corpus while reporting ok. Precedence: CLI > front-matter
// > default; a front-matter key create doesn't consume is reported in "ignored".
func TestMultiCursorCreateFrontMatter(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "default", "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "default", "events", "e.jsonl"),
		[]byte(`{"n":1}`+"\n"+`{"n":2}`+"\n"+`{"n":3}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A single-file cursor query declaring its baseline policy in front-matter.
	qfile := filepath.Join(t.TempDir(), "q.sql++")
	if err := os.WriteFile(qfile, []byte(
		"-- label: CC-X\n-- description: the marker query\n-- from: start\n-- managed: true\n"+
			"SELECT e.n FROM events e"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := t.TempDir()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	sidecar := func(name string) map[string]interface{} {
		b, err := os.ReadFile(filepath.Join(store, name+".json"))
		if err != nil {
			t.Fatalf("read sidecar %s: %v (stderr %s)", name, err, errb.String())
		}
		var m map[string]interface{}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	env := func() map[string]interface{} {
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}
	waterNonZero := func(m map[string]interface{}) int {
		n := 0
		if w, ok := m["water"].(map[string]interface{}); ok {
			for _, v := range w {
				if f, _ := v.(float64); f > 0 {
					n++
				}
			}
		}
		return n
	}

	// (A) create WITHOUT --from: front-matter `from: start` -> every watermark zero.
	out.Reset()
	c.cmdMulti("cursor create CC-X --queries " + qfile + " --cursor-store " + store)
	if env()["ok"] != true {
		t.Fatalf("create: %s (stderr %s)", out.String(), errb.String())
	}
	sc := sidecar("CC-X")
	if n := waterNonZero(sc); n != 0 {
		t.Errorf("front-matter from:start ignored: %d non-zero watermarks, want 0", n)
	}
	if sc["description"] != "the marker query" {
		t.Errorf("front-matter description dropped: got %v", sc["description"])
	}
	ignored, _ := env()["ignored"].([]interface{})
	if len(ignored) != 1 || ignored[0] != "managed" {
		t.Errorf("ignored: want [managed], got %v", env()["ignored"])
	}

	// (B) CLI --from now must OVERRIDE front-matter from:start -> non-zero watermarks.
	out.Reset()
	c.cmdMulti("cursor create CC-Y --queries " + qfile + " --from now --cursor-store " + store)
	if env()["ok"] != true {
		t.Fatalf("create --from now: %s", out.String())
	}
	if n := waterNonZero(sidecar("CC-Y")); n == 0 {
		t.Errorf("CLI --from now did not override front-matter from:start (watermarks all zero)")
	}
}

// TestMultiCursorAnnotations covers the cursor client-metadata passthrough (ISSUE-03:
// annotations/labels shipped on the removed `apply`/`plan` path and had to be re-wired onto
// `create`). It asserts: front-matter + CLI flags both populate; --annotation k=v overlays
// the --annotations blob; show/list --long echo all three verbatim including a nested object;
// --source-ref stamps provenance; the consumed front-matter keys leave the `ignored` list;
// and — the load-bearing invariant — metadata is OUTSIDE spec_hash, so two cursors over the
// SAME pack with DIFFERENT annotations share a spec_hash (a retag never re-baselines).
func TestMultiCursorAnnotations(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "default", "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "default", "events", "e.jsonl"),
		[]byte(`{"n":1}`+"\n"+`{"n":2}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A query that declares provenance annotations + labels in front-matter.
	qfile := filepath.Join(t.TempDir(), "q.sql++")
	if err := os.WriteFile(qfile, []byte(
		"-- label: CC-A\n"+
			`-- annotations: {"provenance": {"git_sha": "abc123", "prompt": "ROI watch"}}`+"\n"+
			"-- labels: team=devinfra, severity=normal\n"+
			"SELECT e.n FROM events e"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := t.TempDir()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	env := func() map[string]interface{} {
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}

	// (A) create — front-matter annotations + labels persist; nothing lands in `ignored`.
	out.Reset()
	c.cmdMulti("cursor create CC-A --queries " + qfile + " --source-ref feedface --cursor-store " + store)
	if env()["ok"] != true {
		t.Fatalf("create CC-A: %s (stderr %s)", out.String(), errb.String())
	}
	if ig, _ := env()["ignored"].([]interface{}); len(ig) != 0 {
		t.Errorf("front-matter annotations/labels reported as ignored: %v", ig)
	}
	if env()["source_ref"] != "feedface" {
		t.Errorf("--source-ref not echoed on create: %v", env()["source_ref"])
	}

	// (B) show echoes annotations (incl. the nested object), labels, and source_ref.
	out.Reset()
	c.cmdMulti("cursor show CC-A --cursor-store " + store)
	sh := env()
	ann, _ := sh["annotations"].(map[string]interface{})
	prov, _ := ann["provenance"].(map[string]interface{})
	if prov["git_sha"] != "abc123" || prov["prompt"] != "ROI watch" {
		t.Errorf("show did not round-trip nested annotations: %v", sh["annotations"])
	}
	lbl, _ := sh["labels"].(map[string]interface{})
	if lbl["team"] != "devinfra" || lbl["severity"] != "normal" {
		t.Errorf("show did not round-trip labels: %v", sh["labels"])
	}
	if sh["source_ref"] != "feedface" {
		t.Errorf("show source_ref: got %v", sh["source_ref"])
	}
	specA, _ := sh["spec_hash"].(string)

	// (C) --annotations-file (a JSON blob, since the tokenizer mangles raw-JSON argv) as the
	// base, REPLACING the front-matter blob, plus a quote-free --annotation k=v overlay and
	// CLI labels. Same pack as CC-A, so spec_hash must MATCH despite different metadata.
	annFile := filepath.Join(t.TempDir(), "prov.json")
	if err := os.WriteFile(annFile, []byte(`{"env":"prod"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	c.cmdMulti(`cursor create CC-B --queries ` + qfile +
		` --annotations-file ` + annFile + ` --annotation git_sha=zzz999 --labels team=platform --cursor-store ` + store)
	if env()["ok"] != true {
		t.Fatalf("create CC-B: %s (stderr %s)", out.String(), errb.String())
	}
	out.Reset()
	c.cmdMulti("cursor show CC-B --cursor-store " + store)
	shB := env()
	annB, _ := shB["annotations"].(map[string]interface{})
	if annB["env"] != "prod" || annB["git_sha"] != "zzz999" {
		t.Errorf("--annotations-file base + --annotation overlay: got %v", shB["annotations"])
	}
	if _, leaked := annB["provenance"]; leaked {
		t.Errorf("--annotations-file should replace the front-matter blob, not merge: %v", annB)
	}
	if lblB, _ := shB["labels"].(map[string]interface{}); lblB["team"] != "platform" {
		t.Errorf("CLI --labels: got %v", shB["labels"])
	}
	if specB, _ := shB["spec_hash"].(string); specB != specA || specA == "" {
		t.Errorf("spec_hash must be independent of annotations: CC-A=%q CC-B=%q", specA, specB)
	}
}

// TestMultiCursorHashSchemeUpgrade guards the spec_hash-across-versions fix: a sidecar
// whose `queries` id was stamped by an OLDER binary under an older normalization scheme
// (scheme 1 = ends-only TrimSpace; the n1k1-for-ai team hit exactly this when ISSUE-05's
// scheme 2 landed and every advance started refusing with a false query-drift) must
// peek clean, advance WITHOUT --allow-drift, be re-stamped to the current scheme -- and
// a real edit must still drift.
func TestMultiCursorHashSchemeUpgrade(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "default", "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "default", "events", "e.jsonl"),
		[]byte(`{"n":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// The query carries an interior blank line -- the shape whose hash scheme 2 moved.
	qfile := filepath.Join(t.TempDir(), "q.sql++")
	body := "-- label: CC-S\n-- a prose preamble\n\nSELECT e.n FROM events e"
	if err := os.WriteFile(qfile, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	store := t.TempDir()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	env := func() map[string]interface{} {
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}
	sidecarPath := filepath.Join(store, "CC-S.json")
	rewrite := func(mut func(m map[string]interface{})) {
		b, err := os.ReadFile(sidecarPath)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]interface{}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		mut(m)
		nb, _ := json.Marshal(m)
		if err := os.WriteFile(sidecarPath, nb, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	out.Reset()
	c.cmdMulti("cursor create CC-S --queries " + qfile + " --from start --cursor-store " + store)
	if env()["ok"] != true {
		t.Fatalf("create: %s (stderr %s)", out.String(), errb.String())
	}

	// Simulate the old binary: re-stamp the sidecar with the SCHEME-1 id, no scheme field.
	dets, err := glue.LoadPack(qfile)
	if err != nil {
		t.Fatal(err)
	}
	oldID := glue.QueriesIDUnderScheme("CC-S", dets, 1)
	curID := glue.QueriesID("CC-S", dets)
	if oldID == curID {
		t.Fatal("test premise broken: scheme 1 and 2 ids should differ for this query")
	}
	rewrite(func(m map[string]interface{}) {
		m["queries"] = oldID
		delete(m, "hash_scheme")
	})

	// peek: NOT drifted (no queries_current), and advance succeeds WITHOUT --allow-drift.
	out.Reset()
	c.cmdMulti("cursor peek CC-S --cursor-store " + store)
	if pv := env(); pv["queries_current"] != nil || pv["status"] == "error" {
		t.Fatalf("old-scheme sidecar peeked as drifted: %s", out.String())
	}
	out.Reset()
	c.failed = false
	c.cmdMulti("cursor advance CC-S --quiet --cursor-store " + store)
	if av := env(); av["status"] == "error" || c.failed {
		t.Fatalf("old-scheme sidecar refused advance: %s", out.String())
	}

	// The advance re-stamped the sidecar to the CURRENT scheme.
	b, _ := os.ReadFile(sidecarPath)
	var m map[string]interface{}
	json.Unmarshal(b, &m)
	if m["queries"] != curID {
		t.Fatalf("advance did not re-stamp to the current scheme id: got %v, want %s", m["queries"], curID)
	}
	if s, _ := m["hash_scheme"].(float64); int(s) != glue.QueriesHashScheme {
		t.Fatalf("advance did not stamp hash_scheme: got %v", m["hash_scheme"])
	}

	// A REAL edit still drifts: peek surfaces queries_current, advance refuses.
	if err := os.WriteFile(qfile, []byte(body+" WHERE e.n > 0"), 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	c.cmdMulti("cursor peek CC-S --cursor-store " + store)
	if env()["queries_current"] == nil {
		t.Fatalf("real edit not surfaced as drift: %s", out.String())
	}
	out.Reset()
	c.failed = false
	c.cmdMulti("cursor advance CC-S --quiet --cursor-store " + store)
	if ev, _ := env()["error"].(map[string]interface{}); ev == nil || ev["kind"] != "query-drift" {
		t.Fatalf("real edit must refuse advance with query-drift: %s", out.String())
	}
}

// TestMultiCursorRotationDisclosure covers the "append-mostly, with whole-file
// rotation" disclosure (DESIGN-cep.md): a committed container whose file is DELETED
// surfaces as "rotated" and one rewritten SHORTER than its watermark as "truncated"
// on every peek/advance — evidence loss is an event, never silent. The watermark
// never rewinds (no double-delivery), live containers keep delivering, and
// `advance --prune-rotated` deliberately drops rotated entries from the position.
func TestMultiCursorRotationDisclosure(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	aPath := filepath.Join(ks, "a.jsonl")
	bPath := filepath.Join(ks, "b.jsonl")
	if err := os.WriteFile(aPath, []byte(`{"n":1}`+"\n"+`{"n":2}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bPath, []byte(`{"n":3}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := writeMultiQueryEntries(t, map[string]string{"all": "-- label: all\nSELECT e.n FROM events e"})
	store := t.TempDir()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	env := func() map[string]interface{} {
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}
	strs := func(v interface{}) []string {
		arr, _ := v.([]interface{})
		var s []string
		for _, x := range arr {
			s = append(s, x.(string))
		}
		return s
	}

	// Baseline: create from start + advance commits watermarks for both containers.
	out.Reset()
	c.cmdMulti("cursor create ROT --queries " + pack + " --from start --cursor-store " + store)
	out.Reset()
	c.cmdMulti("cursor advance ROT --quiet --cursor-store " + store)
	if av := env(); av["status"] != "advanced" || av["rotated"] != nil || av["truncated"] != nil {
		t.Fatalf("clean advance: %s", out.String())
	}

	// Rotate b away; append to a. Peek must deliver the new record AND disclose b.
	if err := os.Remove(bPath); err != nil {
		t.Fatal(err)
	}
	f, _ := os.OpenFile(aPath, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"n":4}` + "\n")
	f.Close()
	out.Reset()
	c.cmdMulti("cursor peek ROT --cursor-store " + store)
	pv := env()
	if pv["status"] != "pending" || pv["count"] != float64(1) {
		t.Fatalf("peek after rotation should still deliver the live delta: %s", out.String())
	}
	rot := strs(pv["rotated"])
	if len(rot) != 1 || !strings.Contains(rot[0], "b.jsonl") {
		t.Fatalf("rotated should name b.jsonl: %v", pv["rotated"])
	}
	if pv["truncated"] != nil {
		t.Fatalf("nothing truncated yet: %s", out.String())
	}

	// Truncate a below its watermark (rewrite shorter). Peek: truncated names a,
	// rotated still names b, no rows (all below the never-rewinding watermark).
	if err := os.WriteFile(aPath, []byte(`{"n":9}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	c.cmdMulti("cursor peek ROT --cursor-store " + store)
	pv = env()
	if pv["count"] != float64(0) {
		t.Fatalf("truncation must not double-deliver: %s", out.String())
	}
	trunc := strs(pv["truncated"])
	if len(trunc) != 1 || !strings.Contains(trunc[0], "a.jsonl") {
		t.Fatalf("truncated should name a.jsonl: %v", pv["truncated"])
	}
	if rot = strs(pv["rotated"]); len(rot) != 1 || !strings.Contains(rot[0], "b.jsonl") {
		t.Fatalf("rotated disclosure must persist while the position holds b: %v", pv["rotated"])
	}

	// Fail-loud on truncation: a bare advance REFUSES (kind "source-truncated"),
	// position untouched -- the source violated append-only, and committing past it
	// would entrench the loss.
	out.Reset()
	c.failed = false
	c.cmdMulti("cursor advance ROT --quiet --prune-rotated --cursor-store " + store)
	av := env()
	if ev, _ := av["error"].(map[string]interface{}); ev == nil || ev["kind"] != "source-truncated" {
		t.Fatalf("truncated advance must refuse with source-truncated: %s", out.String())
	}
	if !c.failed {
		t.Fatalf("source-truncated refusal must set failure")
	}
	sidecar := func() map[string]interface{} {
		b, _ := os.ReadFile(filepath.Join(store, "ROT.json"))
		var sc map[string]interface{}
		json.Unmarshal(b, &sc)
		w, _ := sc["water"].(map[string]interface{})
		return w
	}
	if w := sidecar(); len(w) != 2 {
		t.Fatalf("refused advance must leave the position untouched: %v", w)
	}

	// --accept-truncation (+ --prune-rotated): acknowledges the discontinuity --
	// b pruned from the position, a RE-BASELINED at its current (smaller) extent so
	// future appends deliver again (under max-merge alone it would stay dead until
	// the file regrew past the old offset).
	out.Reset()
	c.failed = false
	c.cmdMulti("cursor advance ROT --quiet --prune-rotated --accept-truncation --cursor-store " + store)
	av = env()
	if av["status"] == "error" || c.failed {
		t.Fatalf("--accept-truncation advance should commit: %s", out.String())
	}
	if av["pruned_rotated"] != float64(1) {
		t.Fatalf("prune ack missing: %s", out.String())
	}
	water := sidecar()
	if len(water) != 1 {
		t.Fatalf("pruned water should hold only a.jsonl: %v", water)
	}
	// Watermark positions are record-START offsets: the truncated file's one record
	// starts at 0, so the re-baselined water is 0 (that record stays skipped —
	// admit needs pos > water — while an append at start 8 delivers).
	for k, v := range water {
		if !strings.Contains(k, "a.jsonl") || v.(float64) != 0 {
			t.Fatalf("truncated container not re-baselined at its observed position: %v", water)
		}
	}

	// After the acknowledgment, disclosures clear -- and a fresh append DELIVERS
	// (the re-baseline revived the container; the rewritten {"n":9} is NOT re-delivered).
	out.Reset()
	c.cmdMulti("cursor peek ROT --cursor-store " + store)
	if pv = env(); pv["rotated"] != nil || pv["truncated"] != nil || pv["count"] != float64(0) {
		t.Fatalf("disclosures should clear after prune+accept: %s", out.String())
	}
	f, _ = os.OpenFile(aPath, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"n":10}` + "\n")
	f.Close()
	out.Reset()
	c.cmdMulti("cursor peek ROT --cursor-store " + store)
	if pv = env(); pv["count"] != float64(1) {
		t.Fatalf("append after re-baseline must deliver (container was revived): %s", out.String())
	}
}

// TestMultiCursorRewriteFingerprint covers the tier-1 prefix fingerprint
// (DESIGN-cep.md rung 3): a file REWRITTEN in place without shrinking -- the one
// append violation a size check cannot see -- is detected by the committed
// boundary-record fingerprint (water_fp: the hash of the record starting at the
// committed offset), disclosed as "rewritten", refused on advance
// (kind "source-rewritten"), and acknowledged with --accept-truncation. Also: no
// false positive on identical bytes, and a legacy sidecar (no water_fp) skips
// verification until the next advance BACKFILLS it.
func TestMultiCursorRewriteFingerprint(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	aPath := filepath.Join(ks, "a.jsonl")
	orig := `{"n":1}` + "\n" + `{"n":2}` + "\n"
	if err := os.WriteFile(aPath, []byte(orig), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := writeMultiQueryEntries(t, map[string]string{"all": "-- label: all\nSELECT e.n FROM events e"})
	store := t.TempDir()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	env := func() map[string]interface{} {
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}
	sidecar := func() map[string]interface{} {
		b, _ := os.ReadFile(filepath.Join(store, "ROT.json"))
		var sc map[string]interface{}
		json.Unmarshal(b, &sc)
		return sc
	}

	// Baseline: create --from now captures water AND the boundary fingerprint.
	out.Reset()
	c.cmdMulti("cursor create ROT --queries " + pack + " --cursor-store " + store)
	if env()["ok"] != true {
		t.Fatalf("create: %s (stderr %s)", out.String(), errb.String())
	}
	if fp, _ := sidecar()["water_fp"].(map[string]interface{}); len(fp) != 1 {
		t.Fatalf("create should seed water_fp: %v", sidecar()["water_fp"])
	}

	// Identical bytes rewritten (a touch): NO false positive.
	if err := os.WriteFile(aPath, []byte(orig), 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	c.cmdMulti("cursor peek ROT --cursor-store " + store)
	if pv := env(); pv["rewritten"] != nil || pv["status"] != "empty" {
		t.Fatalf("identical rewrite must not trip the fingerprint: %s", out.String())
	}

	// Same-LENGTH different content: size says nothing changed; the fingerprint sees it.
	if err := os.WriteFile(aPath, []byte(`{"n":7}`+"\n"+`{"n":8}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	c.cmdMulti("cursor peek ROT --cursor-store " + store)
	pv := env()
	rew, _ := pv["rewritten"].([]interface{})
	if len(rew) != 1 || !strings.Contains(rew[0].(string), "a.jsonl") {
		t.Fatalf("in-place rewrite not detected: %s", out.String())
	}
	if pv["truncated"] != nil {
		t.Fatalf("same-length rewrite must not read as truncated: %s", out.String())
	}

	// advance refuses (source-rewritten, position untouched) until acknowledged.
	out.Reset()
	c.failed = false
	c.cmdMulti("cursor advance ROT --quiet --cursor-store " + store)
	if ev, _ := env()["error"].(map[string]interface{}); ev == nil || ev["kind"] != "source-rewritten" {
		t.Fatalf("rewritten advance must refuse with source-rewritten: %s", out.String())
	}
	out.Reset()
	c.failed = false
	c.cmdMulti("cursor advance ROT --quiet --accept-truncation --cursor-store " + store)
	if av := env(); av["status"] == "error" || c.failed {
		t.Fatalf("--accept-truncation should commit past a rewrite: %s", out.String())
	}

	// The fingerprint re-stamped to the NEW content: peek is clean, appends deliver.
	out.Reset()
	c.cmdMulti("cursor peek ROT --cursor-store " + store)
	if pv = env(); pv["rewritten"] != nil {
		t.Fatalf("fingerprint should re-stamp on accept: %s", out.String())
	}
	f, _ := os.OpenFile(aPath, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"n":9}` + "\n")
	f.Close()
	out.Reset()
	c.cmdMulti("cursor peek ROT --cursor-store " + store)
	if pv = env(); pv["count"] != float64(1) {
		t.Fatalf("append after acknowledged rewrite must deliver: %s", out.String())
	}

	// Legacy sidecar (no water_fp): verification skipped, next advance backfills.
	raw, _ := os.ReadFile(filepath.Join(store, "ROT.json"))
	var sc map[string]interface{}
	json.Unmarshal(raw, &sc)
	delete(sc, "water_fp")
	nb, _ := json.Marshal(sc)
	os.WriteFile(filepath.Join(store, "ROT.json"), nb, 0o644)
	out.Reset()
	c.cmdMulti("cursor peek ROT --cursor-store " + store)
	if pv = env(); pv["rewritten"] != nil {
		t.Fatalf("legacy sidecar must not report rewrites (no fingerprints): %s", out.String())
	}
	out.Reset()
	c.failed = false
	c.cmdMulti("cursor advance ROT --quiet --cursor-store " + store)
	if av := env(); av["status"] == "error" || c.failed {
		t.Fatalf("legacy advance should commit + backfill: %s", out.String())
	}
	if fp, _ := sidecar()["water_fp"].(map[string]interface{}); len(fp) != 1 {
		t.Fatalf("advance should backfill water_fp on a legacy sidecar: %v", sidecar()["water_fp"])
	}
}

// TestMultiCursorAdvanceToSafety guards ISSUE-13: (1) peek's `to` token must survive
// being fed back to `advance --to` through .multi's quote-aware tokenizer (it is an
// opaque base64 token, no quotes to strip); (2) an explicit --to that would silently
// rewind an append cursor (drop a held container, or name one the datastore lacks) is
// REFUSED with the watermark intact, unless --force, and the affected containers are
// disclosed either way.
func TestMultiCursorAdvanceToSafety(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "a.jsonl"), []byte(`{"n":1}`+"\n"+`{"n":2}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "b.jsonl"), []byte(`{"n":3}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := writeMultiQueryEntries(t, map[string]string{"all": "-- label: all\nSELECT e.n FROM events e"})
	store := t.TempDir()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	run := func(cmd string) map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("cursor " + cmd + " --cursor-store " + store)
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}

	if e := run("create AT --queries " + pack + " --from start"); e["ok"] != true {
		t.Fatalf("create: %v (stderr %s)", e, errb.String())
	}

	// (1) round-trip: peek.to fed back verbatim to advance --to (through the tokenizer).
	to, _ := run("peek AT")["to"].(string)
	if to == "" {
		t.Fatal("peek produced no 'to' token")
	}
	if strings.ContainsAny(to, `{}" `) {
		t.Errorf("peek.to is not an opaque token (has JSON chars that argv strips): %q", to)
	}
	if e := run("advance AT --to " + to + " --quiet"); e["status"] != "advanced" {
		t.Fatalf("round-trip advance --to failed: %v (err %v)", e["status"], e["error"])
	}

	// (2) an unsafe --to (drops the held a.jsonl, names a nonexistent file) is refused.
	bogus := encodeWater(map[string]int64{"nope.jsonl": 999})
	e := run("advance AT --to " + bogus)
	if e["status"] != "error" {
		t.Fatalf("unsafe --to: want error, got %v", e)
	}
	if kind := e["error"].(map[string]interface{})["kind"]; kind != "unsafe-position" {
		t.Errorf("unsafe --to: want kind=unsafe-position, got %v", kind)
	}
	if unknown, _ := e["unknown"].([]interface{}); len(unknown) != 1 || unknown[0] != "nope.jsonl" {
		t.Errorf("unsafe --to: want unknown=[nope.jsonl], got %v", e["unknown"])
	}
	// watermark must be intact after the refusal (no silent 456->1 wipe).
	b, _ := os.ReadFile(filepath.Join(store, "AT.json"))
	var sc map[string]interface{}
	json.Unmarshal(b, &sc)
	if w, _ := sc["water"].(map[string]interface{}); len(w) < 2 {
		t.Errorf("refused advance still mutated the watermark: %v", sc["water"])
	}

	// (3) --force commits the unsafe position AND discloses what it did.
	e = run("advance AT --to " + bogus + " --force")
	if e["status"] != "advanced" {
		t.Fatalf("--force: want advanced, got %v", e)
	}
	if dropped, _ := e["dropped"].([]interface{}); len(dropped) == 0 {
		t.Errorf("--force: expected disclosed 'dropped' containers, got %v", e)
	}
}

// TestMultiCursorPackDrift guards ISSUE-17: if the query file is edited after create,
// the cursor would silently run the new query against a watermark advanced under the
// old one. peek must surface queries_current; advance must refuse (pack-drift) unless
// --allow-drift (which re-baselines); `check` must report it and exit nonzero.
func TestMultiCursorPackDrift(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "e.jsonl"),
		[]byte(`{"n":1,"sev":"ERROR"}`+"\n"+`{"n":2,"sev":"WARN"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	qfile := filepath.Join(t.TempDir(), "q.sql++")
	writeQ := func(sev string) {
		if err := os.WriteFile(qfile, []byte("-- label: DR\nSELECT e.n FROM events e WHERE e.sev = \""+sev+"\""), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeQ("ERROR")
	store := t.TempDir()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	run := func(cmd string) map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("cursor " + cmd + " --cursor-store " + store)
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}

	if e := run("create DR --queries " + qfile + " --from start"); e["ok"] != true {
		t.Fatalf("create: %v (stderr %s)", e, errb.String())
	}
	writeQ("WARN") // edit the query out from under the cursor

	// peek surfaces the drift (pack != queries_current) but does not block.
	pe := run("peek DR")
	if pe["queries_current"] == nil || pe["queries_current"] == pe["queries"] {
		t.Errorf("peek did not surface drift: queries=%v queries_current=%v", pe["queries"], pe["queries_current"])
	}

	// advance refuses with pack-drift.
	ae := run("advance DR")
	if ae["status"] != "error" || ae["error"].(map[string]interface{})["kind"] != "query-drift" {
		t.Fatalf("advance on drift: want error/query-drift, got %v", ae)
	}

	// check reports the drift and sets a nonzero exit (c.failed).
	ce := runList(c, &out, &errb, "cursor check --cursor-store "+store)
	if len(ce) != 1 || ce[0]["drifted"] != true {
		t.Fatalf("check: want one drifted row, got %v", ce)
	}
	if !c.failed {
		t.Errorf("check: expected nonzero exit (c.failed) on drift")
	}

	// --allow-drift advances AND re-baselines, so a subsequent check is clean.
	if e := run("advance DR --allow-drift --quiet"); e["status"] != "advanced" {
		t.Fatalf("advance --allow-drift: want advanced, got %v", e)
	}
	ce = runList(c, &out, &errb, "cursor check --cursor-store "+store)
	if len(ce) != 1 || ce[0]["drifted"] != false {
		t.Errorf("check after --allow-drift: want not drifted (re-baselined), got %v", ce)
	}
}

// TestMultiCursorExpectCAS guards ISSUE-15: peek exposes committed_id (a digest of the
// committed position); `advance --expect <committed_id>` is a compare-and-swap that
// refuses (kind "stale") if the position moved since the caller peeked.
func TestMultiCursorExpectCAS(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "e.jsonl"), []byte(`{"n":1}`+"\n"+`{"n":2}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := writeMultiQueryEntries(t, map[string]string{"all": "-- label: all\nSELECT e.n FROM events e"})
	store := t.TempDir()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	run := func(cmd string) map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("cursor " + cmd + " --cursor-store " + store)
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}

	if e := run("create CAS --queries " + pack + " --from start"); e["ok"] != true {
		t.Fatalf("create: %v (stderr %s)", e, errb.String())
	}
	cid, _ := run("peek CAS")["committed_id"].(string)
	if cid == "" {
		t.Fatal("peek exposed no committed_id")
	}

	// advance --expect <current> succeeds and reports a NEW committed_id.
	e := run("advance CAS --expect " + cid + " --quiet")
	if e["status"] != "advanced" {
		t.Fatalf("advance --expect <current>: want advanced, got %v", e)
	}
	if e["committed_id"] == cid || e["committed_id"] == "" {
		t.Errorf("advance should report the new committed_id, got %v (was %v)", e["committed_id"], cid)
	}

	// reusing the now-stale committed_id is refused (compare-and-swap miss).
	e = run("advance CAS --expect " + cid)
	if e["status"] != "error" || e["error"].(map[string]interface{})["kind"] != "stale" {
		t.Fatalf("advance --expect <stale>: want error/stale, got %v", e)
	}
}

// TestMultiCursorListLongAndVocab guards the ISSUE-15 completion: one machine-readable
// surface (list --long is the whole status table in one call) with consistent vocabulary
// (queries=id, queries_path=path — the pack/pack_dir trap gone), spec_hash + schema
// fields, and peek's to_id.
func TestMultiCursorListLongAndVocab(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "e.jsonl"), []byte(`{"n":1}`+"\n"+`{"n":2}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := writeMultiQueryEntries(t, map[string]string{"all": "-- label: all\n-- description: d\nSELECT e.n FROM events e"})
	store := t.TempDir()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	obj := func(cmd string) map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("cursor " + cmd + " --cursor-store " + store)
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}

	if obj("create V --queries " + pack + " --from start")["ok"] != true {
		t.Fatalf("create: %s", errb.String())
	}

	// show: unified vocabulary + spec_hash + schema; no legacy pack/pack_dir keys.
	sh := obj("show V")
	if sh["queries"] == nil || sh["queries_path"] == nil || sh["spec_hash"] == nil {
		t.Errorf("show missing queries/queries_path/spec_hash: %v", sh)
	}
	if _, ok := sh["pack"]; ok {
		t.Errorf("show still emits legacy 'pack' key")
	}
	if _, ok := sh["pack_dir"]; ok {
		t.Errorf("show still emits legacy 'pack_dir' key")
	}
	if sh["schema"] == nil {
		t.Errorf("show missing schema version")
	}

	// peek: committed_id (from) + to_id (pending).
	pe := obj("peek V")
	if pe["committed_id"] == nil || pe["to_id"] == nil {
		t.Errorf("peek missing committed_id/to_id: %v", pe)
	}

	// list --long: one call, full field set including queries_path + spec_hash + description.
	rows := runList(c, &out, &errb, "cursor list --long --cursor-store "+store)
	if len(rows) != 1 {
		t.Fatalf("list --long: want 1 row, got %d", len(rows))
	}
	for _, k := range []string{"queries", "queries_path", "spec_hash", "committed_id", "description", "mode", "total_advances"} {
		if _, ok := rows[0][k]; !ok {
			t.Errorf("list --long missing %q: %v", k, rows[0])
		}
	}
}

// runList runs a cursor command that prints a JSON ARRAY envelope (list/check).
func runList(c *cli, out, errb *bytes.Buffer, cmd string) []map[string]interface{} {
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti(cmd)
	var rows []map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(out.String())), &rows)
	return rows
}

// TestMultiCursorDiffLoop drives the Phase-2 diff loop over a MUTABLE keyspace:
// create --mode diff --from now (baseline), peek (empty), mutate the state, peek
// (insert+update+delete), advance (commit new snapshot, snap:0→snap:1), peek
// (empty again).
func TestMultiCursorDiffLoop(t *testing.T) {
	root := t.TempDir()
	ksDir := filepath.Join(root, "default", "incidents")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	incidents := filepath.Join(ksDir, "incidents.jsonl")
	writeState := func(lines ...string) {
		body := ""
		for _, ln := range lines {
			body += ln + "\n"
		}
		if err := os.WriteFile(incidents, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeState(`{"id":1,"status":"open"}`, `{"id":2,"status":"open"}`)

	pack := writeMultiQueryEntries(t, map[string]string{
		"inc": "-- label: inc\n" + `SELECT e.id, e.status FROM incidents e`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	store := t.TempDir()
	run := func(cmd string) map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("cursor " + cmd + " --cursor-store " + store)
		var env map[string]interface{}
		if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &env); err != nil {
			t.Fatalf("cursor %q: bad JSON %q: %v (stderr %s)", cmd, out.String(), err, errb.String())
		}
		return env
	}

	// create diff --from now: baseline snapshot = current state.
	env := run("create incs --queries " + pack + " --mode diff --from now")
	if env["ok"] != true || env["mode"] != "diff" || env["from"] != "snap:0" {
		t.Fatalf("create diff: %v", env)
	}

	// peek: no change yet.
	if env := run("peek incs"); env["status"] != "empty" || env["count"].(float64) != 0 {
		t.Fatalf("peek(baseline): want empty/0, got %v", env)
	}

	// Mutate: #1 open→closed (update), #2 removed (delete), #3 added (insert).
	writeState(`{"id":1,"status":"closed"}`, `{"id":3,"status":"open"}`)

	env = run("peek incs")
	if env["status"] != "pending" || env["count"].(float64) != 3 || env["to"] != "snap:1" || env["advanced"] != false {
		t.Fatalf("peek(mutated): want pending/3/to=snap:1/advanced=false, got %v", env)
	}
	ops := map[string]map[string]interface{}{}
	for _, r := range env["labelResults"].([]interface{}) {
		row := r.(map[string]interface{})
		ops[row["op"].(string)] = row
	}
	if len(ops) != 3 || ops["insert"] == nil || ops["update"] == nil || ops["delete"] == nil {
		t.Fatalf("peek(mutated): want insert+update+delete, got %v", env["labelResults"])
	}
	// update carries before+after; delete carries before; insert carries after.
	upd := ops["update"]
	bb, _ := json.Marshal(upd["before"])
	ab, _ := json.Marshal(upd["after"])
	if string(bb) != `{"id":1,"status":"open"}` || string(ab) != `{"id":1,"status":"closed"}` {
		t.Fatalf("update before/after: %s → %s", bb, ab)
	}
	if _, has := ops["delete"]["after"]; has {
		t.Fatalf("delete should have no after: %v", ops["delete"])
	}
	if _, has := ops["insert"]["before"]; has {
		t.Fatalf("insert should have no before: %v", ops["insert"])
	}

	// re-peek is stable (non-advancing): still 3.
	if env := run("peek incs"); env["count"].(float64) != 3 {
		t.Fatalf("re-peek: want 3 (non-advancing), got %v", env)
	}

	// advance: commit the new snapshot, snap:0 → snap:1.
	env = run("advance incs")
	if env["status"] != "advanced" || env["advanced"] != true || env["to"] != "snap:1" {
		t.Fatalf("advance: want advanced/true/to=snap:1, got %v", env)
	}

	// peek: quiet again.
	if env := run("peek incs"); env["status"] != "empty" || env["count"].(float64) != 0 {
		t.Fatalf("peek(after advance): want empty/0, got %v", env)
	}

	// show reflects diff mode + committed snap:1.
	if env := run("show incs"); env["mode"] != "diff" || env["committed"] != "snap:1" {
		t.Fatalf("show: want diff/snap:1, got %v", env)
	}
}
