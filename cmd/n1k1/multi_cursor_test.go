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
