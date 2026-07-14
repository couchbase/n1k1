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

// The shipped @vectorize_field ingest macro (extensions/macros/vectorize_field.macro.js):
// embed a text field, INSERT INTO a .jsonl or columnar .parquet vec keyspace, then search.
// Guards the DESIGN-vectors.md example from rotting.

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestVectorizeFieldMacroIngest runs the @vectorize_field ingest example for BOTH target
// formats (the .jsonl and .parquet forms shown in the macro's header): each embeds `line`,
// materializes a vec keyspace, and is searchable -- and the .parquet one takes the
// columnar VECTOR_DISTANCE fast path.
func TestVectorizeFieldMacroIngest(t *testing.T) {
	src, err := os.ReadFile("../extensions/macros/vectorize_field.macro.js")
	if err != nil {
		t.Fatal(err)
	}
	if err := glue.RegisterJSMacro("vectorize_field", string(src)); err != nil {
		t.Fatalf("RegisterJSMacro(vectorize_field): %v", err)
	}

	dir := t.TempDir()
	logs := filepath.Join(dir, "default", "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	const n = 40
	var b []byte
	for i := 0; i < n; i++ {
		b = append(b, []byte(fmt.Sprintf(`{"id":%d,"line":"log message number %d"}`+"\n", i, i))...)
	}
	if err := os.WriteFile(filepath.Join(logs, "data.jsonl"), b, 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}
	ingest := func(target string) {
		q := "INSERT INTO `" + target + "` (KEY UUID(), VALUE self) " +
			`SELECT r.id, r.vec FROM @vectorize_field(logs, field => line, id => id, batch => 8, opts => {"dim":4}) AS r`
		res, e := sess.Run(q)
		if e != nil {
			t.Fatalf("INSERT INTO %s via @vectorize_field: %v", target, e)
		}
		if res.Count != n {
			t.Errorf("%s: embedded+inserted %d rows, want %d", target, res.Count, n)
		}
	}
	ingest("vj/data.jsonl")   // JSON Lines vec keyspace `vj`
	ingest("vp/data.parquet") // columnar Parquet vec keyspace `vp`

	search := `VECTOR_DISTANCE(t.vec, [0.1, 0.2, 0.3, 0.4], "cosine") AS d`
	// .jsonl search works (row lane).
	sj, _ := glue.OpenSession(dir, "default")
	if rows := runVecRows(t, sj, `SELECT t.id, `+search+` FROM vj t ORDER BY d ASC LIMIT 5`); len(rows) != 5 {
		t.Fatalf("jsonl search returned %d rows, want 5", len(rows))
	}
	// .parquet search takes the columnar fast path.
	sp, _ := glue.OpenSession(dir, "default")
	before := atomic.LoadInt64(&glue.VectorColumnarApplied)
	rows := runVecRows(t, sp, `SELECT t.id, `+search+` FROM vp t ORDER BY d ASC LIMIT 5`)
	if atomic.LoadInt64(&glue.VectorColumnarApplied)-before == 0 {
		t.Errorf("columnar VECTOR_DISTANCE did not fire over the @vectorize_field .parquet keyspace")
	}
	if len(rows) != 5 {
		t.Fatalf("parquet search returned %d rows, want 5: %v", len(rows), rows)
	}
}
