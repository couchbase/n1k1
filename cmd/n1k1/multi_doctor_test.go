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

// TestMultiDoctor covers the ISSUE-06 Phase-2 differentiator: doctor flags a
// detector that reads a field the corpus lacks (birth-in-error) and hard-fails,
// leaves a detector reading real fields clean, and lists corpus fields no detector
// references (the unexplored surface).
func TestMultiDoctor(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "e.jsonl"), []byte(
		`{"type":"assistant","model":"opus","effort":"high"}`+"\n"+
			`{"type":"assistant","model":"opus"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := writeMultiQueryEntries(t, map[string]string{
		"good":  "-- label: GOOD\n" + `SELECT e.model FROM events e WHERE e.effort = "high"`,
		"birth": "-- label: BIRTH\n" + `SELECT e.model FROM events e WHERE e.isAbortedMidStream = true`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	c.cmdMulti("doctor --queries " + pack)

	var env struct {
		Checks []struct {
			Detector string   `json:"detector"`
			Keyspace string   `json:"keyspace"`
			Absent   []string `json:"references_absent"`
		} `json:"checks"`
		Unreferenced map[string][]string `json:"unreferenced"`
		OK           bool                `json:"ok"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &env); err != nil {
		t.Fatalf("bad JSON %q: %v (stderr %s)", out.String(), err, errb.String())
	}

	byDet := map[string][]string{}
	for _, ch := range env.Checks {
		byDet[ch.Detector] = ch.Absent
	}
	if len(byDet["GOOD"]) != 0 {
		t.Fatalf("GOOD should have no absent refs, got %v", byDet["GOOD"])
	}
	if len(byDet["BIRTH"]) != 1 || byDet["BIRTH"][0] != "isAbortedMidStream" {
		t.Fatalf("BIRTH should flag isAbortedMidStream, got %v", byDet["BIRTH"])
	}
	if env.OK {
		t.Fatal("doctor should report ok=false when a detector references an absent field")
	}
	if !c.failed {
		t.Fatal("doctor should hard-fail on a birth-in-error")
	}
	// `type` exists in the corpus but no detector reads it -> unexplored surface.
	found := false
	for _, f := range env.Unreferenced["events"] {
		if f == "type" {
			found = true
		}
	}
	if !found {
		t.Fatalf("unreferenced should include 'type', got %v", env.Unreferenced["events"])
	}
}
