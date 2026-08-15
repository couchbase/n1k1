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

package test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestScanSkipsUnreadableContainer guards ISSUE-28: a container the listing names
// but the open can't reach (a dangling symlink -- readdir/glob still return it)
// SKIPS with a disclosure warning instead of aborting the whole scan; the
// -halt-on-unreadable switch (glue.ScanHaltUnreadable) restores the abort; and the
// index side agrees with the scan side about which files exist, so an index over
// the same keyspace builds and serves.
func TestScanSkipsUnreadableContainer(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation needs privileges on Windows")
	}
	root := t.TempDir()
	ksDir := filepath.Join(root, "default", "logs")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ksDir, "real.jsonl"),
		[]byte(`{"id":"r1","sev":"ERROR"}`+"\n"+`{"id":"r2","sev":"INFO"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/nonexistent/gone.jsonl", filepath.Join(ksDir, "ghost.jsonl")); err != nil {
		t.Fatal(err)
	}

	run := func(q string) ([]string, []string, error) {
		sess, err := glue.OpenSession(root, "default")
		if err != nil {
			t.Fatal(err)
		}
		defer sess.Close()
		res, err := sess.Run(q)
		if err != nil {
			return nil, nil, err
		}
		var rows, warns []string
		for _, r := range res.Rows {
			rows = append(rows, string(r))
		}
		for _, w := range res.Warnings {
			warns = append(warns, w.Error())
		}
		return rows, warns, nil
	}
	const q = `SELECT COUNT(1) AS n FROM logs`

	// (a) Default: skip the ghost, count the real rows, DISCLOSE the skip.
	rows, warns, err := run(q)
	if err != nil {
		t.Fatalf("scan should skip the dangling symlink, got error: %v", err)
	}
	if len(rows) != 1 || rows[0] != `{"n":2}` {
		t.Fatalf("rows = %v, want [{\"n\":2}] (the real container only)", rows)
	}
	disclosed := false
	for _, w := range warns {
		disclosed = disclosed || (strings.Contains(w, "SKIPPED unreadable") &&
			strings.Contains(w, "ghost.jsonl"))
	}
	if !disclosed {
		t.Fatalf("the skip must be disclosed by name; warnings = %v", warns)
	}

	// (b) -halt-on-unreadable: the pre-ISSUE-28 abort.
	defer func(prev bool) { glue.ScanHaltUnreadable = prev }(glue.ScanHaltUnreadable)
	glue.ScanHaltUnreadable = true
	if _, _, err := run(q); err == nil || !strings.Contains(err.Error(), "ghost.jsonl") {
		t.Fatalf("halt mode should abort naming the file, got: %v", err)
	}
	glue.ScanHaltUnreadable = false

	// (c) Resolver agreement: an index over the same keyspace builds (skipping the
	// ghost like the scan does) and serves the indexed predicate.
	if _, err := glue.CatalogAddIndexes(root, []byte(`{
		"indexes": [{"name": "bysev", "keyspace": "logs", "keys": ["sev"]}]
	}`)); err != nil {
		t.Fatalf("CatalogAddIndexes: %v", err)
	}
	rows, warns, err = run(`SELECT l.id AS id FROM logs l WHERE l.sev = "ERROR"`)
	if err != nil {
		t.Fatalf("indexed scan over a keyspace with a dangling symlink: %v", err)
	}
	if len(rows) != 1 || rows[0] != `{"id":"r1"}` {
		t.Fatalf("indexed rows = %v, want [{\"id\":\"r1\"}]", rows)
	}
	_ = warns // an index-served span may not touch the ghost at all; disclosure is scan-side
}
