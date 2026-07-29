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

	// run executes a `.multi cursor ...` command and returns the parsed envelope.
	run := func(cmd string) map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("cursor " + cmd)
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
	if env := run("create errs --pack " + pack + " --from start"); env["ok"] != true {
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

	// show: committed position is non-empty and metadata is present.
	if env := run("show errs"); env["committed"].(string) == "" || env["mode"] != "append" {
		t.Fatalf("show: want non-empty committed + append mode, got %v", env)
	}

	// list: includes our cursor.
	out.Reset()
	c.cmdMulti("cursor list")
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
	env := func() map[string]interface{} {
		var e map[string]interface{}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &e)
		return e
	}

	out.Reset()
	c.cmdMulti("cursor create") // no name
	if env()["status"] != "error" {
		t.Fatalf("create no-name: want error, got %s", out.String())
	}

	out.Reset()
	c.cmdMulti("cursor create c1") // no pack
	if env()["status"] != "error" {
		t.Fatalf("create no-pack: want error, got %s", out.String())
	}

	out.Reset()
	c.cmdMulti("cursor create c1 --pack " + pack)
	if env()["ok"] != true {
		t.Fatalf("create c1: want ok, got %s", out.String())
	}

	out.Reset()
	c.cmdMulti("cursor create c1 --pack " + pack) // duplicate
	if e := env(); e["status"] != "error" || e["error"].(map[string]interface{})["kind"] != "exists" {
		t.Fatalf("create duplicate: want error/exists, got %s", out.String())
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
	run := func(cmd string) map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("cursor " + cmd)
		var env map[string]interface{}
		if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &env); err != nil {
			t.Fatalf("cursor %q: bad JSON %q: %v (stderr %s)", cmd, out.String(), err, errb.String())
		}
		return env
	}

	// create diff --from now: baseline snapshot = current state.
	env := run("create incs --pack " + pack + " --mode diff --from now")
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

// TestMultiCursorGitOps drives Phase-3 declarative reconcile: plan/apply a dir of
// *.sql++ monitors, idempotent re-apply preserves an advanced position, an edited
// file updates, and --prune destroys a managed-but-undeclared cursor WITHOUT
// touching an imperatively-created one.
func TestMultiCursorGitOps(t *testing.T) {
	root := t.TempDir()
	logs := filepath.Join(root, "default", "logs")
	inc := filepath.Join(root, "default", "incidents")
	for _, d := range []string{logs, inc} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	logsFile := filepath.Join(logs, "logs.jsonl")
	if err := os.WriteFile(logsFile, []byte(`{"sev":"ERROR","msg":"boom"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inc, "i.jsonl"), []byte(`{"id":1,"status":"open"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Desired-state dir: an append monitor + a diff monitor (policy in front-matter).
	monitors := t.TempDir()
	writeMon := func(name, body string) {
		if err := os.WriteFile(filepath.Join(monitors, name+".sql++"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeMon("log-errors", "-- label: log-errors\n"+`SELECT l.msg FROM logs l WHERE l.sev = "ERROR"`)
	writeMon("open-incidents", "-- mode: diff\n-- id-field: id\n"+`SELECT e.id, e.status FROM incidents e`)

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	run := func(cmd string) map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("cursor " + cmd)
		var env map[string]interface{}
		if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &env); err != nil {
			t.Fatalf("cursor %q: bad JSON %q: %v (stderr %s)", cmd, out.String(), err, errb.String())
		}
		return env
	}
	strs := func(v interface{}) []string {
		if v == nil {
			return nil
		}
		var out []string
		for _, x := range v.([]interface{}) {
			out = append(out, x.(string))
		}
		return out
	}

	// plan (fresh): two creates.
	pl := run("plan " + monitors)["plan"].(map[string]interface{})
	if len(strs(pl["create"])) != 2 {
		t.Fatalf("plan(fresh): want 2 creates, got %v", pl["create"])
	}

	// apply: both created.
	ap := run("apply " + monitors)["applied"].(map[string]interface{})
	if len(strs(ap["created"])) != 2 {
		t.Fatalf("apply: want 2 created, got %v", ap)
	}

	// plan again: idempotent — everything unchanged, zero changes.
	env := run("plan " + monitors)
	if env["changes"].(float64) != 0 || len(strs(env["plan"].(map[string]interface{})["unchanged"])) != 2 {
		t.Fatalf("plan(idempotent): want 0 changes / 2 unchanged, got %v", env)
	}

	// Advance log-errors past its seed, then grow the source.
	run("advance log-errors")
	committed := run("show log-errors")["committed"].(string)
	if err := os.WriteFile(logsFile, []byte(`{"sev":"ERROR","msg":"boom"}`+"\n"+`{"sev":"ERROR","msg":"again"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Re-apply (unchanged files): must PRESERVE the committed position, NOT rebaseline.
	run("apply " + monitors)
	if got := run("show log-errors")["committed"].(string); got != committed {
		t.Fatalf("re-apply rebased an unchanged cursor: committed %q -> %q", committed, got)
	}
	// Proof the position was preserved: peek still sees the appended ERROR as new.
	if env := run("peek log-errors"); env["count"].(float64) != 1 {
		t.Fatalf("preserved-position peek: want 1 new row, got %v", env)
	}

	// An imperatively-created cursor (unmanaged) must survive --prune. Uses a
	// single-entry pack (one keyspace) deliberately: a multi-keyspace fused pack
	// trips a pre-existing cbq-fork expression.Copy race under -race (tracked in the
	// concurrency memo; orthogonal to cursor logic), and this test stays race-clean.
	manualPack := writeMultiQueryEntries(t, map[string]string{
		"m": "-- label: m\n" + `SELECT l.msg FROM logs l WHERE l.sev = "ERROR"`,
	})
	run("create manual --pack " + manualPack)
	// Drop open-incidents from the desired set.
	if err := os.Remove(filepath.Join(monitors, "open-incidents.sql++")); err != nil {
		t.Fatal(err)
	}

	// plan: open-incidents is a prunable destroy; manual is not (unmanaged).
	env = run("plan " + monitors)
	if env["prunable"].(float64) != 1 {
		t.Fatalf("plan: want 1 prunable, got %v", env)
	}
	if got := strs(env["plan"].(map[string]interface{})["destroy"]); len(got) != 1 || got[0] != "open-incidents" {
		t.Fatalf("plan destroy: want [open-incidents], got %v", got)
	}

	// apply --prune: destroys open-incidents only.
	ap = run("apply " + monitors + " --prune")["applied"].(map[string]interface{})
	if d := strs(ap["destroyed"]); len(d) != 1 || d[0] != "open-incidents" {
		t.Fatalf("apply --prune: want destroyed [open-incidents], got %v", ap["destroyed"])
	}

	// manual (imperative) survived; open-incidents gone.
	names := map[string]bool{}
	out.Reset()
	c.cmdMulti("cursor list")
	var list []map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(out.String())), &list)
	for _, r := range list {
		names[r["cursor"].(string)] = true
	}
	if !names["manual"] || !names["log-errors"] || names["open-incidents"] {
		t.Fatalf("after prune: want {manual,log-errors} present, open-incidents gone; got %v", names)
	}
}
