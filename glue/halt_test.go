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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// TestInterruptHaltsScan: Interrupt() (the engine side of Ctrl-C / a closed output
// pipe) stops a running scan early with base.ErrHalted, and the session stays usable.
func TestInterruptHaltsScan(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "big")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	const total = 20000
	var sb strings.Builder
	for i := 0; i < total; i++ {
		fmt.Fprintf(&sb, `{"n":%d}`+"\n", i)
	}
	if err := os.WriteFile(filepath.Join(ks, "b.jsonl"), []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	seen := 0
	sess.OnRow = func(row []byte) {
		seen++
		if seen == 100 { // simulate a Ctrl-C / closed pipe partway through the scan
			sess.Interrupt()
		}
	}
	_, err = sess.Run("SELECT b.n FROM big b")
	sess.OnRow = nil
	if !errors.Is(err, base.ErrHalted) {
		t.Fatalf("Run after Interrupt: err = %v, want base.ErrHalted", err)
	}
	// Cooperative (checked ~every 1024 rows), so it stops well short of all `total` rows.
	if seen >= total {
		t.Errorf("scan did not halt early: saw all %d rows", seen)
	}
	t.Logf("halted after %d of %d rows", seen, total)

	// The session is reusable: the next Run clears the stale interrupt.
	res, rerr := sess.Run("SELECT 1+1 AS s")
	if rerr != nil || len(res.Rows) != 1 || string(res.Rows[0]) != `{"s":2}` {
		t.Fatalf("session unusable after interrupt: err=%v rows=%v", rerr, res.Rows)
	}
}
