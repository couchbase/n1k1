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
	"strings"
	"testing"

	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"
)

// TestExplainSQLGroupedAndExpanded: `.multi explain --sql` groups fused queries under
// their shared scan (with a synopsis of the shared wake-gate), sections standalone
// queries, and shows a macro query's FINAL expanded SQL++ bracketed by BEGIN/END markers.
func TestExplainSQLGroupedAndExpanded(t *testing.T) {
	registerBuiltinMacros() // so @grep_context expands

	dets := []glue.CorpusDetector{
		{Label: "DISK", Stmt: `SELECT l.msg FROM logs l WHERE l.sev = "ERROR" AND l.msg LIKE "%disk%"`},
		{Label: "OOM", Stmt: `SELECT l.msg FROM logs l WHERE l.sev = "ERROR" AND l.msg LIKE "%oom%"`},
		{Label: "CTX", Stmt: `SELECT g.msg FROM @grep_context(logs, when => sev = "ERROR", before => 1, after => 1, order => ts) AS g WHERE g.near = 1`},
	}
	report := []glue.DetectorLint{
		{Label: "DISK", Class: glue.LintFused, Keyspace: "default:logs", Lane: "native", Literal: "ERROR", Indexed: true},
		{Label: "OOM", Class: glue.LintFused, Keyspace: "default:logs", Lane: "native", Literal: "ERROR", Indexed: true},
		{Label: "CTX", Class: glue.LintStandalone, Lane: "boxed", Reason: "window function (OVER ...) -- runs standalone"},
	}
	score := glue.CorpusScore{Total: 3, Fused: 2, Standalone: 1}

	var out bytes.Buffer
	(&cli{prog: "n1k1", out: &out, stderr: &out, style: cmd.Style{}}).renderCorpusExplainSQL(dets, report, score)
	s := out.String()

	want := []string{
		"shared scan · default:logs · 2",           // fused grouping header
		`wakes only rows matching any of: "ERROR"`, // shared-gate synopsis
		"standalone · own scan",                    // standalone section
		"-- as written (before expansion):",        // macro before→after
		"-- BEGIN expansion of @grep_context",
		"-- END expansion of @grep_context",
		"OVER(ORDER BY ts", // the expanded window SQL++
	}
	for _, w := range want {
		if !strings.Contains(s, w) {
			t.Errorf("--sql output missing %q\n---\n%s", w, s)
		}
	}
	// The fused group must precede the standalone section.
	if i, j := strings.Index(s, "shared scan"), strings.Index(s, "standalone ·"); i < 0 || j < 0 || i > j {
		t.Errorf("fused group should come before standalone (%d vs %d)", i, j)
	}
	// A non-macro query is shown once, with no BEGIN/END noise.
	if strings.Contains(s, "BEGIN expansion of @grep_context, ") {
		t.Errorf("unexpected multi-macro bracket")
	}
}
