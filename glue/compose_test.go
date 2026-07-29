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

package glue

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTopoOrder pins the DAG ordering: dependencies precede dependents, unknown
// deps and cycles error.
func TestTopoOrder(t *testing.T) {
	nodes := []ComposeNode{
		{Name: "c", Needs: []string{"a", "b"}},
		{Name: "b", Needs: []string{"a"}},
		{Name: "a"},
	}
	order, err := TopoOrder(nodes)
	if err != nil {
		t.Fatalf("TopoOrder: %v", err)
	}
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	if !(pos["a"] < pos["b"] && pos["b"] < pos["c"]) {
		t.Fatalf("order violates deps: %v", order)
	}

	if _, err := TopoOrder([]ComposeNode{{Name: "x", Needs: []string{"missing"}}}); err == nil {
		t.Fatal("unknown dep: expected error")
	}
	_, err = TopoOrder([]ComposeNode{
		{Name: "p", Needs: []string{"q"}},
		{Name: "q", Needs: []string{"p"}},
	})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle: expected cycle error, got %v", err)
	}
}

// TestComposeDAG is the Phase-4 end-to-end: a primitive detector pack feeds a
// correlation pack via a materialized pack_<name> keyspace.
func TestComposeDAG(t *testing.T) {
	dir := t.TempDir()
	ksDir := filepath.Join(dir, "default", "logs")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ksDir, "logs.jsonl"), []byte(joinLines([]string{
		`{"host":"h1","sev":"ERROR","msg":"disk"}`,
		`{"host":"h1","sev":"ERROR","msg":"oom"}`,
		`{"host":"h1","sev":"INFO","msg":"ok"}`,
		`{"host":"h2","sev":"ERROR","msg":"net"}`,
	})), 0o644); err != nil {
		t.Fatal(err)
	}

	mkEntry := func(path, body string) MultiQueryEntry {
		e, err := ParseMultiQueryEntry(path, body)
		if err != nil {
			t.Fatalf("ParseMultiQueryEntry(%s): %v", path, err)
		}
		return e
	}
	// Primitive: each ERROR -> a detection carrying its host.
	errs := mkEntry("errors.sql++", "-- label: errors\n"+`SELECT l.host, l.msg FROM logs l WHERE l.sev = "ERROR"`)
	// Correlation: a host with >=2 errors -> an incident, reading pack_errors.
	incident := mkEntry("incident.sql++", "-- label: incident\n"+
		`SELECT e.result.host AS host, COUNT(*) AS n FROM pack_errors e GROUP BY e.result.host HAVING COUNT(*) >= 2`)

	nodes := []ComposeNode{
		{Name: "incident", Needs: []string{"errors"}, Entries: []MultiQueryEntry{incident}},
		{Name: "errors", Entries: []MultiQueryEntry{errs}},
	}

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	res, err := sess.Compose(nodes)
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}

	if strings.Join(res.Order, ",") != "errors,incident" {
		t.Fatalf("order: want errors,incident, got %v", res.Order)
	}
	byNode := map[string]ComposeNodeResult{}
	for _, n := range res.Nodes {
		byNode[n.Node] = n
	}
	if byNode["errors"].Count != 3 {
		t.Fatalf("errors: want 3 detections, got %d", byNode["errors"].Count)
	}
	inc := byNode["incident"]
	if inc.Count != 1 {
		t.Fatalf("incident: want 1 (h1 has 2 errors, h2 has 1), got %d (%v)", inc.Count, inc.LabelResults)
	}
	if got := string(inc.LabelResults[0].Result); got != `{"host":"h1","n":2}` {
		t.Fatalf("incident row: got %s, want {\"host\":\"h1\",\"n\":2}", got)
	}
}
