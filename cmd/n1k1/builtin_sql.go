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

// Generic runner for embedded SQL++ builtins (cmd/n1k1/builtins): resolve the URI
// params against the file's declared params, render the template, run it, stream
// the rows. There are NO per-builtin Go codepaths — a builtin is one self-contained
// SQL++ statement (census.sql++ v1.1 does its read-time coverage join in SQL++, where
// v1.0 needed a Go-side merge), so adding a new builtin is just adding a well-tested
// *.sql++ file to cmd/n1k1/builtins/.

import (
	"fmt"

	builtinq "github.com/couchbase/n1k1/cmd/n1k1/builtins"
)

// runBuiltinSQL executes `--queries builtin:<name>.sql++?...` for any embedded SQL++
// builtin: render + run the single statement and emit its rows as NDJSON (row count
// to stderr). One codepath, no per-builtin hooks.
func (c *cli) runBuiltinSQL(args multiArgs, r queriesRef, q builtinq.Query) {
	resolved, err := q.Resolve(r.params)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sql, err := q.Render(resolved)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sess, binding, err := c.multiSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		fmt.Fprintf(c.stderr, "%s: .multi run --queries builtin:%s: aborting -- unresolved keyspace above\n", c.prog, q.Name)
		c.failed = true
		return
	}
	res, err := sess.Run(sql)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run --queries builtin:%s: %v\n", c.prog, q.Name, err)
		c.failed = true
		return
	}
	for _, row := range res.Rows {
		fmt.Fprintln(c.out, string(row))
	}
	fmt.Fprintf(c.stderr, "%s%d row(s) [builtin:%s@%s]\n",
		c.icon("📇 "), len(res.Rows), q.Name, q.Version)
}

// renderBuiltinSQL resolves + renders one embedded builtin with the given URI
// params (defaults applied, unknown/missing-required params error loudly). Used by
// `.multi show` to print exactly what would run.
func renderBuiltinSQL(name string, params map[string]string) (string, error) {
	q, ok := builtinq.Lookup(name)
	if !ok {
		return "", fmt.Errorf("no embedded builtin %q", name)
	}
	resolved, err := q.Resolve(params)
	if err != nil {
		return "", err
	}
	return q.Render(resolved)
}
