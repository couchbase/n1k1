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
// the rows. A builtin needing a POST-PROCESSING step beyond run-and-emit (e.g.
// census.sql++'s read-time coverage join against census_totals.sql++) registers an
// override in builtinSQLOverride — adding a new plain builtin is just adding a
// well-tested *.sql++ file to cmd/n1k1/builtins/.

import (
	"fmt"

	builtinq "github.com/couchbase/n1k1/cmd/n1k1/builtins"
)

// builtinSQLOverride maps a builtin name to a custom runner (registered via init in
// the builtin's own file). Absent -> the generic run-and-emit path below.
var builtinSQLOverride = map[string]func(*cli, multiArgs, queriesRef, builtinq.Query){}

// runBuiltinSQL executes `--queries builtin:<name>.sql++?...` for any embedded SQL++
// builtin: the generic path renders + runs the single statement and emits its rows
// as NDJSON (row count to stderr).
func (c *cli) runBuiltinSQL(args multiArgs, r queriesRef, q builtinq.Query) {
	if fn, ok := builtinSQLOverride[q.Name]; ok {
		fn(c, args, r, q)
		return
	}
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
