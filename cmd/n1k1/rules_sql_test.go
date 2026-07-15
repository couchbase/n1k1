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
		{Label: "SOLO", Stmt: `SELECT r.id FROM reqs r WHERE r.ms > 1000`}, // fuse-eligible but alone
		{Label: "CTX", Stmt: `SELECT g.msg FROM @grep_context(logs, when => sev = "ERROR", before => 1, after => 1, order => ts) AS g WHERE g.near = 1`},
	}
	report := []glue.DetectorLint{
		{Label: "DISK", Class: glue.LintFused, Keyspace: "default:logs", Lane: "native", Literal: "ERROR", Indexed: true},
		{Label: "OOM", Class: glue.LintFused, Keyspace: "default:logs", Lane: "native", Literal: "ERROR", Indexed: true},
		{Label: "SOLO", Class: glue.LintFused, Keyspace: "default:reqs", Lane: "native", Indexed: false},
		{Label: "CTX", Class: glue.LintStandalone, Lane: "boxed", Reason: "window function (OVER ...) -- runs standalone"},
	}
	score := glue.CorpusScore{Total: 4, Fused: 3, Standalone: 1}

	var out bytes.Buffer
	(&cli{prog: "n1k1", out: &out, stderr: &out, style: cmd.Style{}}).renderCorpusExplainSQL(dets, report, score)
	s := out.String()

	want := []string{
		"SHARED SCAN · default:logs · 2 queries fuse into ONE pass", // real fusion (>=2)
		`wakes only rows matching any of: "ERROR"`,                  // shared-gate synopsis
		"the UNION ALL of these 2 branches",                         // fused-query framing
		"\nUNION ALL\n",                                             // the branch separator
		"fuse-eligible · each the only query on its keyspace",       // the solo section
		"standalone · own scan",                                     // standalone section
		"-- as written (before expansion):",                         // macro before→after
		"-- BEGIN expansion of @grep_context",
		"-- END expansion of @grep_context",
		"OVER(ORDER BY ts", // the expanded window SQL++
	}
	for _, w := range want {
		if !strings.Contains(s, w) {
			t.Errorf("--sql output missing %q\n---\n%s", w, s)
		}
	}
	// Section order: real shared scan, then fuse-eligible-alone, then standalone.
	shared := strings.Index(s, "SHARED SCAN")
	solo := strings.Index(s, "only query on its keyspace")
	standalone := strings.Index(s, "standalone ·")
	if !(shared >= 0 && shared < solo && solo < standalone) {
		t.Errorf("section order wrong: shared=%d solo=%d standalone=%d", shared, solo, standalone)
	}
}
