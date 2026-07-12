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
	"testing"
)

// gateSession writes one <root>/default/logs keyspace with the given JSONL and opens it.
func gateSession(t *testing.T, logsJSONL string) *Session {
	t.Helper()
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "logs")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "l.jsonl"), []byte(logsJSONL), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return sess
}

// TestCorpusGateStandalone: a standalone (window) detector gated on a NECESSARY
// precondition (`sev = "ERROR"`) is SKIPPED when its keyspace has no matching row --
// its expensive sort/window never runs -- and runs normally when the gate is present.
// A detector with no gate is never skipped (opt-in), proving the skip is the gate's.
func TestCorpusGateStandalone(t *testing.T) {
	// grep -C1 style window flag: standalone (has OVER), findings only near an ERROR.
	stmt := `SELECT line FROM (SELECT line, MAX(CASE WHEN sev = "ERROR" THEN 1 ELSE 0 END) OVER (ORDER BY code ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS near FROM logs) sub WHERE sub.near = 1`
	gated := CorpusDetector{Label: "CTX", Stmt: stmt, Source: "logs", Gate: `sev = "ERROR"`}
	ungated := CorpusDetector{Label: "CTX", Stmt: stmt}

	withErr := `{"sev":"ERROR","code":1,"line":"boom"}` + "\n" + `{"sev":"INFO","code":2,"line":"ok"}` + "\n"
	noErr := `{"sev":"INFO","code":1,"line":"a"}` + "\n" + `{"sev":"WARN","code":2,"line":"b"}` + "\n"

	run := func(t *testing.T, logs string, d CorpusDetector) *CompiledCorpus {
		sess := gateSession(t, logs)
		cc, err := sess.CorpusCompile([]CorpusDetector{d})
		if err != nil {
			t.Fatalf("CorpusCompile: %v", err)
		}
		if len(cc.Standalone) != 1 {
			t.Fatalf("expected the window detector to be standalone; got %+v", cc.Standalone)
		}
		return cc
	}

	// (1) gate present in the keyspace -> not skipped, detector runs and finds context.
	cc := run(t, withErr, gated)
	f, err := cc.Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(cc.GatedSkipped) != 0 {
		t.Errorf("gate present: expected no skip, got GatedSkipped=%v", cc.GatedSkipped)
	}
	if len(f) != 2 { // both rows are within 1 line of the ERROR at code=1
		t.Errorf("gate present: expected 2 context findings, got %d (%+v)", len(f), f)
	}

	// (2) gate ABSENT from the keyspace -> the detector is skipped (its sort/window is
	// never run), producing zero findings, and the skip is recorded (not silent).
	cc = run(t, noErr, gated)
	f, err = cc.Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(f) != 0 {
		t.Errorf("gate absent: expected 0 findings, got %+v", f)
	}
	if len(cc.GatedSkipped) != 1 || cc.GatedSkipped[0] != "CTX" {
		t.Errorf("gate absent: expected GatedSkipped=[CTX], got %v", cc.GatedSkipped)
	}

	// (3) SAME no-ERROR keyspace, but NO gate -> never skipped (gating is opt-in), so the
	// skip in (2) is attributable to the gate, not to the empty result.
	cc = run(t, noErr, ungated)
	if _, err = cc.Run(); err != nil {
		t.Fatal(err)
	}
	if len(cc.GatedSkipped) != 0 {
		t.Errorf("no gate: expected no skip, got %v", cc.GatedSkipped)
	}
}
