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

// Command n1k1bench times n1k1's full parse->plan->execute (glue.Session.Run) over
// a dir: file datastore, symmetric with the fork's cmd/localbench for cbq -- so the
// cbq-vs-n1k1 harness compares the SAME phases (not ExecOp-only vs full-request).
//
// Usage: n1k1bench <datastore-root>            # queries one-per-line on stdin
//   REPS (env, default 15) warm reps per query, first min(5, REPS/3) dropped.
//   Prints per query:  RESULT \t <median-ms> \t <median-MB> \t <rows>
package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/n1k1/glue"
)

func main() {
	if len(os.Args) < 2 {
		fatal(fmt.Errorf("usage: n1k1bench <datastore-root>  (queries on stdin)"))
	}
	reps := envInt("REPS", 15)
	warmup := reps / 3
	if warmup > 5 {
		warmup = 5
	}

	sess, err := glue.OpenSession(os.Args[1], "default")
	if err != nil {
		fatal(err)
	}

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<24), 1<<24) // large: inline-array CTE queries can be big
	for sc.Scan() {
		q := strings.TrimSpace(sc.Text())
		if q == "" {
			continue
		}
		ms := make([]float64, 0, reps)
		mb := make([]float64, 0, reps)
		rows := 0
		var m0, m1 runtime.MemStats
		for i := 0; i < reps; i++ {
			runtime.ReadMemStats(&m0)
			st := time.Now()
			res, err := sess.Run(q) // full parse -> plan -> convert -> execute
			el := time.Since(st)
			runtime.ReadMemStats(&m1)
			if err != nil {
				fatal(fmt.Errorf("query %q: %v", q, err))
			}
			rows = res.Count
			ms = append(ms, float64(el.Nanoseconds())/1e6)
			mb = append(mb, float64(m1.TotalAlloc-m0.TotalAlloc)/(1024*1024))
		}
		fmt.Printf("RESULT\t%.3f\t%.3f\t%d\n", median(ms[warmup:]), median(mb[warmup:]), rows)
	}
	if err := sc.Err(); err != nil {
		fatal(err)
	}
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "n1k1bench:", err)
	os.Exit(1)
}
