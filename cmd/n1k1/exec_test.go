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
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// lineSpyWriter records each Write call separately, so a test can see that rows
// were emitted one at a time (the streaming shape) rather than in one buffered blob.
type lineSpyWriter struct{ writes []string }

func (w *lineSpyWriter) Write(p []byte) (int, error) {
	w.writes = append(w.writes, string(p))
	return len(p), nil
}

// TestExecStreamsJSONLines proves the jsonlines output mode STREAMS: each result row
// is emitted the moment it is produced (via Session.OnRow), one Write per row, rather
// than accumulating the whole result set and rendering after. (The no-accumulation
// memory guarantee itself -- Result.Rows stays nil under OnRow -- is covered by
// glue's Test... OnRow test; here we prove the CLI wires jsonlines to that path and
// the bytes are correct.)
func TestExecStreamsJSONLines(t *testing.T) {
	sess, err := glue.OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()

	spy := &lineSpyWriter{}
	var errb strings.Builder
	c := &cli{prog: "n1k1", sess: sess, out: spy, stderr: &errb, mode: "jsonlines"}

	c.exec(`SELECT d.a FROM [{"a":1},{"a":2},{"a":3}] AS d`)

	// One Write per row (streaming emits row-at-a-time), each a complete JSON line.
	if len(spy.writes) != 3 {
		t.Fatalf("jsonlines should stream 3 rows as 3 writes, got %d: %q", len(spy.writes), spy.writes)
	}
	want := []string{"{\"a\":1}\n", "{\"a\":2}\n", "{\"a\":3}\n"}
	for i, w := range want {
		if spy.writes[i] != w {
			t.Errorf("row %d write = %q, want %q", i, spy.writes[i], w)
		}
	}
	if c.outErr != nil {
		t.Errorf("unexpected outErr on a healthy writer: %v", c.outErr)
	}
}

// TestExecJSONLinesRecordsWriteError proves the streaming path records a downstream
// write failure (a closed output pipe) instead of ignoring it: OnRow sees the error,
// stops attempting further writes, and c.outErr is set for the footer to report.
func TestExecJSONLinesRecordsWriteError(t *testing.T) {
	sess, err := glue.OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()

	fw := &failingWriter{}
	var errb strings.Builder
	c := &cli{prog: "n1k1", sess: sess, out: fw, stderr: &errb, mode: "jsonlines"}

	c.exec(`SELECT d.a FROM [{"a":1},{"a":2},{"a":3}] AS d`)

	if c.outErr == nil {
		t.Fatal("a failing output writer should set c.outErr")
	}
	// After the first failure, no further writes are attempted (one try, not 3).
	if fw.attempts != 1 {
		t.Errorf("write attempts after first failure = %d, want 1", fw.attempts)
	}
	if !strings.Contains(errb.String(), "output write failed") {
		t.Errorf("footer should note the write failure; stderr = %q", errb.String())
	}
}

// failingWriter fails every Write (simulating a closed pipe / dead consumer).
type failingWriter struct{ attempts int }

func (w *failingWriter) Write(p []byte) (int, error) {
	w.attempts++
	return 0, errBrokenTestPipe
}

var errBrokenTestPipe = &pipeErr{}

type pipeErr struct{}

func (*pipeErr) Error() string { return "broken pipe" }

func TestOrderedJSONRow(t *testing.T) {
	// Keys keep the given order (not Go map order), so the box renderer's columns
	// stay in declaration order.
	got := string(orderedJSONRow(
		[2]interface{}{"zeta", 1},
		[2]interface{}{"alpha", "x"},
	))
	if got != `{"zeta":1,"alpha":"x"}` {
		t.Errorf("orderedJSONRow order = %s", got)
	}

	// A nil value is omitted entirely (so a column absent from every row vanishes).
	got = string(orderedJSONRow(
		[2]interface{}{"a", nil},
		[2]interface{}{"b", 5},
		[2]interface{}{"c", nil},
	))
	if got != `{"b":5}` {
		t.Errorf("orderedJSONRow nil-omit = %s", got)
	}

	if got := string(orderedJSONRow()); got != `{}` {
		t.Errorf("orderedJSONRow() = %s, want {}", got)
	}
}
