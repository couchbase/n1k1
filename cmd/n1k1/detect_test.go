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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeCorpus writes each name->body entry as <dir>/<name>.sql++ and returns dir.
func writeCorpus(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name+".sql++"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// newLogsBundle builds a <root>/default/logs datastore of a few log docs and returns
// the root (the bundle dir a .detect command opens as c.dir).
func newLogsBundle(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "default", "logs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := []string{
		`{"sev":"ERROR","msg":"disk full","ts":3}`,
		`{"sev":"INFO","msg":"started","ts":1}`,
		`{"sev":"ERROR","msg":"timeout","ts":5}`,
		`{"sev":"WARN","msg":"slow","ts":2}`,
	}
	for i, d := range docs {
		if err := os.WriteFile(filepath.Join(dir, "l"+string(rune('0'+i))+".json"), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// TestDetectRun: a corpus of one fusable filter, one correlated (standalone), and one
// broken (rejected) detector. The fusable + standalone produce tagged findings; the
// coverage summary reports 1 fused / 1 standalone / 1 rejected (with the reason); the
// broken detector does not abort the run.
func TestDetectRun(t *testing.T) {
	root := newLogsBundle(t)
	corpus := writeCorpus(t, map[string]string{
		"errors":   `SELECT * FROM logs WHERE sev = "ERROR"`,
		"prev_ts":  `SELECT e.msg, (SELECT RAW r.ts FROM logs r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1)[0] AS prior_ts FROM logs e`,
		"broken_x": `SELECT * FROM logs WHERE`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdDetect("run --corpus " + corpus)

	stderr := errb.String()
	if !strings.Contains(stderr, "1 fused, 1 standalone, 1 rejected") {
		t.Errorf("coverage summary wrong; stderr:\n%s", stderr)
	}
	// The rejected detector is surfaced with its tag + a reason, and did not abort.
	if !strings.Contains(stderr, "broken_x") {
		t.Errorf("rejected detector broken_x not surfaced; stderr:\n%s", stderr)
	}
	// Findings for the fusable (errors) and standalone (prev_ts) detectors appear,
	// tagged. (2 ERROR rows fused + 4 standalone projection rows.)
	stdout := out.String()
	if !strings.Contains(stdout, `"tag":"errors"`) {
		t.Errorf("no fusable findings tagged errors; stdout:\n%s", stdout)
	}
	if !strings.Contains(stdout, `"tag":"prev_ts"`) {
		t.Errorf("no standalone findings tagged prev_ts; stdout:\n%s", stdout)
	}
	if c.failed {
		t.Errorf("a broken detector must not abort the run (c.failed=true); stderr:\n%s", stderr)
	}
}

// TestDetectLint: the report card shows the three classes, an always-wake fused
// detector gets the always-wake advice, a boxed one names its native alternative, and
// the corpus score line is present.
func TestDetectLint(t *testing.T) {
	root := newLogsBundle(t)
	corpus := writeCorpus(t, map[string]string{
		"errors":     `SELECT * FROM logs WHERE sev = "ERROR"`,           // fused, native, indexed
		"everything": `SELECT * FROM logs`,                               // fused, always-wake (no literal)
		"grouped":    `SELECT sev, COUNT(*) AS n FROM logs GROUP BY sev`, // standalone
		"broken_x":   `SELECT * FROM logs WHERE`,                         // rejected
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdDetect("lint --corpus " + corpus)

	stdout := out.String()
	for _, want := range []string{`"class":"fused"`, `"class":"standalone"`, `"class":"rejected"`} {
		if !strings.Contains(stdout, want) {
			t.Errorf("lint report missing %s; stdout:\n%s", want, stdout)
		}
	}
	// The no-WHERE fused detector always-wakes -> the discriminating-literal advice.
	if !strings.Contains(stdout, "always-wake") {
		t.Errorf("expected always-wake advice for the no-literal detector; stdout:\n%s", stdout)
	}
	// A native+indexed detector reports its required literal.
	if !strings.Contains(stdout, "ERROR") {
		t.Errorf("expected the ERROR literal for the indexed detector; stdout:\n%s", stdout)
	}
	// The corpus score line (on stderr) is present.
	if !strings.Contains(errb.String(), "score:") || !strings.Contains(errb.String(), "% fused") {
		t.Errorf("corpus score line missing; stderr:\n%s", errb.String())
	}
}

// TestDetectRunBind: a corpus written against a LOGICAL keyspace resolves via a
// manifest and runs; an unresolved logical keyspace fails loud (coverage surfaces the
// gap) rather than reporting a silently clean bundle.
func TestDetectRunBind(t *testing.T) {
	// A flat bundle of *.json at the root (the manifest globs them directly).
	root := t.TempDir()
	for i, d := range []string{
		`{"sev":"ERROR","msg":"oom"}`,
		`{"sev":"INFO","msg":"ok"}`,
	} {
		if err := os.WriteFile(filepath.Join(root, "app"+string(rune('0'+i))+".json"), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	corpus := writeCorpus(t, map[string]string{
		"oom": `SELECT * FROM indexer_log WHERE sev = "ERROR"`,
	})

	// (1) A manifest that resolves -> the run works.
	good := filepath.Join(t.TempDir(), "manifest")
	if err := os.WriteFile(good, []byte("indexer_log = *.json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdDetect("run --corpus " + corpus + " --bind " + good)
	if !strings.Contains(out.String(), `"tag":"oom"`) {
		t.Errorf("bound run produced no findings; stdout:\n%s\nstderr:\n%s", out.String(), errb.String())
	}
	if !strings.Contains(errb.String(), "resolved") {
		t.Errorf("binding coverage should report the resolved keyspace; stderr:\n%s", errb.String())
	}
	if c.failed {
		t.Errorf("a resolving bind must not fail; stderr:\n%s", errb.String())
	}

	// (2) A manifest that resolves to NO files -> fail loud (a gap), not clean.
	bad := filepath.Join(t.TempDir(), "manifest")
	if err := os.WriteFile(bad, []byte("indexer_log = nowhere/*.json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out2, errb2 bytes.Buffer
	c2 := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out2, stderr: &errb2}
	c2.cmdDetect("run --corpus " + corpus + " --bind " + bad)
	if !strings.Contains(errb2.String(), "UNRESOLVED") {
		t.Errorf("an unresolved logical keyspace must fail loud; stderr:\n%s", errb2.String())
	}
	if !c2.failed {
		t.Errorf("an unresolved binding must set c.failed (fail-loud), stderr:\n%s", errb2.String())
	}
	if strings.TrimSpace(out2.String()) != "" {
		t.Errorf("must NOT render a (falsely clean) findings table on a gap; stdout:\n%s", out2.String())
	}
}
