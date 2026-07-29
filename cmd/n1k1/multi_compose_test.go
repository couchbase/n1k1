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

// TestMultiCompose drives the Phase-4 CLI: a two-node DAG (primitive errors ->
// correlation incident) runs in topo order, the leaf reads the upstream pack, and
// a cycle is rejected.
func TestMultiCompose(t *testing.T) {
	root := t.TempDir()
	logs := filepath.Join(root, "default", "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	body := ""
	for _, ln := range []string{
		`{"host":"h1","sev":"ERROR","msg":"disk"}`,
		`{"host":"h1","sev":"ERROR","msg":"oom"}`,
		`{"host":"h2","sev":"ERROR","msg":"net"}`,
	} {
		body += ln + "\n"
	}
	if err := os.WriteFile(filepath.Join(logs, "logs.jsonl"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	dag := t.TempDir()
	writeNode := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dag, name+".sql++"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeNode("errors", "-- label: errors\n"+`SELECT l.host, l.msg FROM logs l WHERE l.sev = "ERROR"`)
	writeNode("incident", "-- label: incident\n-- needs: errors\n"+
		`SELECT e.result.host AS host, COUNT(*) AS n FROM pack_errors e GROUP BY e.result.host HAVING COUNT(*) >= 2`)

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	c.cmdMulti("compose " + dag)

	var env struct {
		Order []string `json:"order"`
		Nodes []struct {
			Node         string `json:"node"`
			Count        int    `json:"count"`
			LabelResults []struct {
				Label  string          `json:"label"`
				Result json.RawMessage `json:"result"`
			} `json:"labelResults"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &env); err != nil {
		t.Fatalf("bad JSON %q: %v (stderr %s)", out.String(), err, errb.String())
	}
	if strings.Join(env.Order, ",") != "errors,incident" {
		t.Fatalf("order: want errors,incident, got %v", env.Order)
	}
	byNode := map[string]int{}
	var incidentRow string
	for _, n := range env.Nodes {
		byNode[n.Node] = n.Count
		if n.Node == "incident" && len(n.LabelResults) == 1 {
			incidentRow = string(n.LabelResults[0].Result)
		}
	}
	if byNode["errors"] != 3 || byNode["incident"] != 1 {
		t.Fatalf("counts: want errors=3 incident=1, got %v", byNode)
	}
	if incidentRow != `{"host":"h1","n":2}` {
		t.Fatalf("incident row: got %q", incidentRow)
	}

	// A cycle is rejected (nothing emitted to stdout; error on stderr).
	cyc := t.TempDir()
	if err := os.WriteFile(filepath.Join(cyc, "p.sql++"), []byte("-- label: p\n-- needs: q\nSELECT 1 AS n FROM pack_q"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cyc, "q.sql++"), []byte("-- label: q\n-- needs: p\nSELECT 1 AS n FROM pack_p"), 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("compose " + cyc)
	if !c.failed || !strings.Contains(errb.String(), "cycle") {
		t.Fatalf("cycle: want failure mentioning cycle; failed=%v stderr=%q stdout=%q", c.failed, errb.String(), out.String())
	}
}

// TestMultiComposeSelection covers ISSUE-02 #3/#4: --queries is rejected, and
// --terminal / --only gate which nodes emit their (potentially huge) labelResults
// while every node still reports a count.
func TestMultiComposeSelection(t *testing.T) {
	root := t.TempDir()
	logs := filepath.Join(root, "default", "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logs, "l.jsonl"),
		[]byte(`{"host":"h1","sev":"ERROR"}`+"\n"+`{"host":"h1","sev":"ERROR"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dag := t.TempDir()
	mustWrite := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dag, name+".sql++"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite("errs", "-- label: errs\n"+`SELECT l.host FROM logs l WHERE l.sev = "ERROR"`)
	mustWrite("roll", "-- label: roll\n-- needs: errs\n"+
		`SELECT p.result.host AS host, COUNT(*) AS n FROM pack_errs p GROUP BY p.result.host`)

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	rows := func(cmd string) map[string]int { // node -> labelResults length
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("compose " + cmd)
		var env struct {
			Nodes []struct {
				Node         string        `json:"node"`
				Count        int           `json:"count"`
				LabelResults []interface{} `json:"labelResults"`
			} `json:"nodes"`
		}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &env)
		m := map[string]int{}
		for _, n := range env.Nodes {
			m[n.Node] = len(n.LabelResults)
		}
		return m
	}

	// default: both nodes emit their rows.
	if m := rows(dag); m["errs"] != 2 || m["roll"] != 1 {
		t.Fatalf("default: want errs=2,roll=1 rows, got %v", m)
	}
	// --terminal: only the leaf (roll) emits; errs is count-only.
	if m := rows(dag + " --terminal"); m["errs"] != 0 || m["roll"] != 1 {
		t.Fatalf("--terminal: want errs=0,roll=1, got %v", m)
	}
	// --only errs: only errs emits.
	if m := rows(dag + " --only errs"); m["errs"] != 2 || m["roll"] != 0 {
		t.Fatalf("--only errs: want errs=2,roll=0, got %v", m)
	}

	// --queries is rejected.
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("compose " + dag + " --queries /tmp/x")
	if !c.failed || !strings.Contains(errb.String(), "single <dir>") {
		t.Fatalf("--queries: want rejection, failed=%v stderr=%q", c.failed, errb.String())
	}
}
