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

// TestMultiCensusCursor drives the Phase-3 incremental census loop: create (seed) ->
// peek empty -> append a born field + a type change -> peek surfaces the drift
// (field_added + type_changed) without moving -> advance folds it (census + watermark
// commit together) -> peek empty -> show reflects the folded accumulated census.
func TestMultiCensusCursor(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(ks, "e.jsonl")
	appendLines := func(lines ...string) {
		f, err := os.OpenFile(events, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range lines {
			f.WriteString(l + "\n")
		}
		f.Close()
	}
	appendLines(
		`{"type":"assistant","timestamp":"2026-06-21T00:00:00Z","model":"opus"}`,
		`{"type":"assistant","timestamp":"2026-06-22T00:00:00Z","model":"opus"}`,
	)

	store := t.TempDir()
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
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

	if env := run("create schema --mode census --keyspace events"); env["ok"] != true || env["mode"] != "census" {
		t.Fatalf("create: %v", env)
	}
	if env := run("peek schema"); env["status"] != "empty" || env["count"].(float64) != 0 {
		t.Fatalf("peek(seed): want empty/0, got %v", env)
	}

	// A born field (effort) and a type change (model string -> array).
	appendLines(
		`{"type":"assistant","timestamp":"2026-07-17T00:00:00Z","model":"opus","effort":"high"}`,
		`{"type":"assistant","timestamp":"2026-07-18T00:00:00Z","model":["opus"]}`,
	)

	env := run("peek schema")
	if env["status"] != "pending" || env["advanced"] != false || env["count"].(float64) != 2 {
		t.Fatalf("peek(drift): want pending/advanced=false/2, got %v", env)
	}
	ops := map[string]string{} // path -> op
	for _, d := range env["drift"].([]interface{}) {
		m := d.(map[string]interface{})
		ops[m["path"].(string)] = m["op"].(string)
	}
	if ops["effort"] != "field_added" {
		t.Fatalf("effort should be field_added, got %v", ops)
	}
	if ops["model"] != "type_changed" {
		t.Fatalf("model should be type_changed, got %v", ops)
	}

	// advance: folds the window; census + watermark commit together.
	if env := run("advance schema"); env["status"] != "advanced" || env["advanced"] != true || env["to"] != "census:1" {
		t.Fatalf("advance: want advanced/true/census:1, got %v", env)
	}
	// re-peek: folded, nothing new.
	if env := run("peek schema"); env["status"] != "empty" || env["count"].(float64) != 0 {
		t.Fatalf("peek(after advance): want empty/0, got %v", env)
	}
	// show: accumulated census reflects all 4 records, committed census:1.
	env = run("show schema")
	if env["mode"] != "census" || env["committed"] != "census:1" || env["census_records"].(float64) != 4 {
		t.Fatalf("show: want census/census:1/4 records, got %v", env)
	}
}
