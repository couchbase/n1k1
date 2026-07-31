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
		`SELECT e.result.host AS host, COUNT(*) AS n FROM node('errors') e GROUP BY e.result.host HAVING COUNT(*) >= 2`)

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	c.cmdMulti("compose --queries " + dag)

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
	if err := os.WriteFile(filepath.Join(cyc, "p.sql++"), []byte("-- label: p\n-- needs: q\nSELECT 1 AS n FROM node('q')"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cyc, "q.sql++"), []byte("-- label: q\n-- needs: p\nSELECT 1 AS n FROM node('p')"), 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("compose --queries " + cyc)
	if !c.failed || !strings.Contains(errb.String(), "cycle") {
		t.Fatalf("cycle: want failure mentioning cycle; failed=%v stderr=%q stdout=%q", c.failed, errb.String(), out.String())
	}
}

// TestMultiComposeSelection covers ISSUE-02 #3/#4: --queries is rejected, and
// --terminal / --only gate which nodes emit their (potentially huge) labelResults
// while every node still reports a count.
// TestMultiComposeMultiDir guards ISSUE-16: several --queries dirs merge into one DAG,
// so a rollup dir can `-- needs:` a node from a shared tier-1 dir without symlinking.
func TestMultiComposeMultiDir(t *testing.T) {
	root := t.TempDir()
	logs := filepath.Join(root, "default", "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logs, "logs.jsonl"),
		[]byte(`{"sev":"ERROR"}`+"\n"+`{"sev":"ERROR"}`+"\n"+`{"sev":"INFO"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tier1 := t.TempDir()
	rollups := t.TempDir()
	if err := os.WriteFile(filepath.Join(tier1, "errs.sql++"),
		[]byte("-- label: errs\n"+`SELECT l.sev FROM logs l WHERE l.sev = "ERROR"`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rollups, "count.sql++"),
		[]byte("-- label: count\n-- needs: errs\n"+`SELECT COUNT(*) AS n FROM node('errs') e`), 0o644); err != nil {
		t.Fatal(err)
	}

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	c.cmdMulti("compose --queries " + tier1 + " --queries " + rollups)
	if c.failed {
		t.Fatalf("multi-dir compose failed: %s", errb.String())
	}
	var env struct {
		Order []string `json:"order"`
		Nodes []struct {
			Node         string `json:"node"`
			LabelResults []struct {
				Result json.RawMessage `json:"result"`
			} `json:"labelResults"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &env); err != nil {
		t.Fatalf("bad JSON %q: %v (stderr %s)", out.String(), err, errb.String())
	}
	if strings.Join(env.Order, ",") != "errs,count" {
		t.Fatalf("order across dirs: want errs,count, got %v", env.Order)
	}
	for _, n := range env.Nodes {
		if n.Node == "count" && (len(n.LabelResults) != 1 || string(n.LabelResults[0].Result) != `{"n":2}`) {
			t.Fatalf("count node (rollup reading tier-1 node across dirs): got %v", n.LabelResults)
		}
	}
}

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
		`SELECT p.result.host AS host, COUNT(*) AS n FROM node('errs') p GROUP BY p.result.host`)

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	rows := func(cmd string) map[string]int { // node -> labelResults length
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti("compose --queries " + cmd)
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

	// A positional dir (the pre-Phase-1c form) is rejected, naming --queries.
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("compose " + dag)
	if !c.failed || !strings.Contains(errb.String(), "--queries") {
		t.Fatalf("positional dir: want rejection naming --queries, failed=%v stderr=%q", c.failed, errb.String())
	}
}

// TestMultiComposeRejectedNode covers ISSUE-09: a node whose SQL fails to parse must
// surface as status:"rejected" with a reason (not a silent count:0) and hard-fail by
// default; --allow-rejected downgrades to a soft continue.
func TestMultiComposeRejectedNode(t *testing.T) {
	root := t.TempDir()
	logs := filepath.Join(root, "default", "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logs, "l.jsonl"), []byte(`{"host":"h1","sev":"ERROR"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dag := t.TempDir()
	w := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dag, name+".sql++"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	w("errs", "-- label: errs\n"+`SELECT l.host FROM logs l WHERE l.sev = "ERROR"`)
	// `value` is a reserved word => this node fails to parse.
	w("roll", "-- label: roll\n-- needs: errs\n"+"SELECT p.result.host AS value FROM node('errs') p")

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	nodeStatus := func() (map[string]string, string) {
		var env struct {
			Nodes []struct {
				Node   string `json:"node"`
				Status string `json:"status"`
				Reason string `json:"reason"`
			} `json:"nodes"`
		}
		json.Unmarshal([]byte(strings.TrimSpace(out.String())), &env)
		m := map[string]string{}
		reason := ""
		for _, n := range env.Nodes {
			m[n.Node] = n.Status
			if n.Node == "roll" {
				reason = n.Reason
			}
		}
		return m, reason
	}

	// Default: the rejected node is surfaced AND hard-fails.
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("compose --queries " + dag)
	st, reason := nodeStatus()
	if st["roll"] != "rejected" {
		t.Fatalf("want roll status=rejected, got %v", st)
	}
	if !strings.Contains(reason, "reserved word") && !strings.Contains(reason, "value") {
		t.Fatalf("want a reason naming the reserved word, got %q", reason)
	}
	if !c.failed {
		t.Fatalf("a rejected node must hard-fail by default")
	}

	// --allow-rejected: still surfaced, but soft (no failure).
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("compose --queries " + dag + " --allow-rejected")
	st, _ = nodeStatus()
	if st["roll"] != "rejected" {
		t.Fatalf("--allow-rejected: still want status=rejected, got %v", st)
	}
	if c.failed {
		t.Fatalf("--allow-rejected must not hard-fail")
	}

	// A node over an UNBOUND logical keyspace (compose run without --bind) must reject
	// with a reason pointing at --bind -- not a bare "no keyspace" that reads as "my
	// query is wrong" (it used to surface as an indistinguishable count:0).
	dag2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dag2, "sess.sql++"), []byte(
		"-- label: sess\n"+`SELECT s.x FROM sessions s WHERE s.x = 1`), 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("compose --queries " + dag2)
	var env struct {
		Nodes []struct {
			Status string `json:"status"`
			Reason string `json:"reason"`
		} `json:"nodes"`
	}
	json.Unmarshal([]byte(strings.TrimSpace(out.String())), &env)
	if len(env.Nodes) != 1 || env.Nodes[0].Status != "rejected" {
		t.Fatalf("unbound keyspace: want one rejected node, got %s", out.String())
	}
	for _, want := range []string{"sessions", "--bind <manifest>", "sessions = <glob>"} {
		if !strings.Contains(env.Nodes[0].Reason, want) {
			t.Fatalf("unbound-keyspace reason missing %q; got %q", want, env.Nodes[0].Reason)
		}
	}
	if !c.failed {
		t.Fatalf("an unbound-keyspace node must hard-fail by default")
	}
}

// TestComposeNodeRefOutsideDAG: node('x') parses + routes as a table source, but
// errors clearly when used outside a compose DAG (no materialized node to read).
func TestComposeNodeRefOutsideDAG(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "t")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "t.jsonl"), []byte(`{"x":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := glue.OpenSession(root, defaultNamespace)
	if err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root, sess: sess}
	c.exec(`SELECT * FROM node('foo') AS r`)
	if !strings.Contains(errb.String()+out.String(), "no such node") &&
		!strings.Contains(errb.String()+out.String(), "compose") {
		t.Fatalf("node() outside a DAG should error clearly; out=%q err=%q", out.String(), errb.String())
	}
}
