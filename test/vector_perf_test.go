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

// End-to-end vector performance (DESIGN-vectors.md): does the columnar win survive the
// REAL pipeline -- INSERT INTO parquet, then the fused vector-distance-columnar op +
// row-lane order-offset-limit -- vs the jsonl row lane and the parquet row lane? Small N
// by default; set N1K1_VEC_ROWS (e.g. 100000) + N1K1_VEC_DIM for the headline run.

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/n1k1/glue"
)

func TestVectorEndToEndPerf(t *testing.T) {
	rows := 3000
	if s := os.Getenv("N1K1_VEC_ROWS"); s != "" {
		rows, _ = strconv.Atoi(s)
	}
	dim := 384
	if s := os.Getenv("N1K1_VEC_DIM"); s != "" {
		dim, _ = strconv.Atoi(s)
	}
	const k = 10

	dir := t.TempDir()
	srcDir := filepath.Join(dir, "default", "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Source jsonl keyspace `src`: N docs {id, vec[dim]}, deterministic values.
	seed := uint64(0x9e3779b97f4a7c15)
	nextf := func() float64 {
		seed = seed*6364136223846793005 + 1442695040888963407
		return float64(seed>>40) / float64(1<<24) // in [0,1)
	}
	// genStart := time.Now()
	sf, err := os.Create(filepath.Join(srcDir, "data.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	bw := bufio.NewWriterSize(sf, 1<<20)
	q := make([]float64, dim)
	for i := 0; i < rows; i++ {
		bw.WriteString(`{"id":`)
		bw.WriteString(strconv.Itoa(i))
		bw.WriteString(`,"vec":[`)
		for j := 0; j < dim; j++ {
			if j > 0 {
				bw.WriteByte(',')
			}
			v := nextf()
			bw.WriteString(strconv.FormatFloat(v, 'g', 6, 64))
			if i == 0 {
				q[j] = v
			}
		}
		bw.WriteString("]}\n")
	}
	bw.Flush()
	sf.Close()
	// tGen := time.Since(genStart)

	// Query vector literal from row 0's vec.
	var ql []byte
	ql = append(ql, '[')
	for j, v := range q {
		if j > 0 {
			ql = append(ql, ',')
		}
		ql = strconv.AppendFloat(ql, v, 'g', 6, 64)
	}
	ql = append(ql, ']')
	query := func(ks string) string {
		return `SELECT t.id, VECTOR_DISTANCE(t.vec, ` + string(ql) + `, "cosine") AS d FROM ` +
			"`" + ks + "`" + ` t ORDER BY d ASC LIMIT ` + strconv.Itoa(k)
	}

	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}

	timeRun := func(label, q string, columnar bool) time.Duration {
		glue.DisableColumnarOptimize = !columnar
		defer func() { glue.DisableColumnarOptimize = false }()
		before := atomic.LoadInt64(&glue.VectorColumnarApplied)
		start := time.Now()
		res, err := sess.Run(q)
		el := time.Since(start)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		// fired := atomic.LoadInt64(&glue.VectorColumnarApplied) - before
		// t.Logf("  %-28s %8v  (rows=%d, columnar-fired=%d)", label, el.Round(time.Millisecond), len(res.Rows), fired)
		_ = before
		_ = res
		return el
	}

	// t.Logf("end-to-end vector perf: rows=%d dim=%d k=%d  (source jsonl gen %v)", rows, dim, k, tGen.Round(time.Millisecond))

	// 1) jsonl row lane (native VECTOR_DISTANCE) -- the pre-columnar baseline.
	// tJSONL := timeRun("jsonl row-lane", query("src"), false)

	// 2) INSERT INTO parquet (the write half).
	// insStart := time.Now()
	if _, err := sess.Run("INSERT INTO `out/data.parquet` (KEY UUID(), VALUE self) SELECT s.id, s.vec FROM src s"); err != nil {
		t.Fatal(err)
	}
	// tInsert := time.Since(insStart)
	// fi, _ := os.Stat(filepath.Join(dir, "default", "out", "data.parquet"))
	// t.Logf("  %-28s %8v  (%.1f MB)", "INSERT INTO parquet", tInsert.Round(time.Millisecond), float64(fi.Size())/1e6)

	// 3) parquet row lane (columnar OFF: materialize vec to JSON per row + native eval).
	sess2, _ := glue.OpenSession(dir, "default")
	sess = sess2
	tPqRow := timeRun("parquet row-lane", query("out"), false)

	// 4) parquet columnar (the fused vector-distance-columnar op).
	tPqCol := timeRun("parquet columnar", query("out"), true)

	// speed := func(base, x time.Duration) float64 { return float64(base) / float64(x) }
	// t.Logf("SUMMARY: columnar %v  vs  jsonl %v (%.1fx)  vs  parquet-row %v (%.1fx)",
	//	tPqCol.Round(time.Millisecond), tJSONL.Round(time.Millisecond), speed(tJSONL, tPqCol),
	//	tPqRow.Round(time.Millisecond), speed(tPqRow, tPqCol))
	_ = tPqRow
	_ = tPqCol
}
