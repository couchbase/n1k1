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

// `.multi compose --queries <dir>...` — run a DAG of queries (DESIGN-cep.md Phase 4).
// Each *.sql++ file in the dir(s) is one node (name = label/stem); a node declares
// upstream deps via `-- needs: a, b` front-matter and reads them as `FROM node('a')`.
// Several --queries dirs merge into one DAG (ISSUE-16), so a rollup dir can draw on a
// shared tier-1 dir without symlinking. Nodes
// run in topological order; each node's labelResults are materialized so a downstream
// node reads them via `FROM node('<name>')`. Output is one JSON envelope: the topo
// order + every node's labelResults.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// buildComposeNodes reads one or more compose dirs: each *.sql++ file => one DAG node
// (name = front-matter label or stem, deps from `needs:` front-matter). Multiple dirs
// (ISSUE-16) let a DAG draw its nodes from several trees — e.g. the shared tier-1
// queries dir plus the roll-up dir — without symlinking files into one directory. A
// duplicate node label across dirs is an error (ambiguous).
func buildComposeNodes(dirs []string) ([]glue.ComposeNode, error) {
	var files []string
	for _, dir := range dirs {
		fs, err := filepath.Glob(filepath.Join(dir, "*.sql++"))
		if err != nil {
			return nil, err
		}
		files = append(files, fs...)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no *.sql++ files in %s", strings.Join(dirs, ", "))
	}
	seen := map[string]bool{}
	var nodes []glue.ComposeNode
	for _, f := range files {
		body, rerr := os.ReadFile(f)
		if rerr != nil {
			return nil, rerr
		}
		e, perr := glue.ParseMultiQueryEntry(f, string(body))
		if perr != nil {
			return nil, fmt.Errorf("%s: %v", f, perr)
		}
		if seen[e.Label] {
			return nil, fmt.Errorf("duplicate node %q (two files share the stem)", e.Label)
		}
		seen[e.Label] = true
		var needs []string
		for _, n := range strings.Split(e.Meta["needs"], ",") {
			if n = strings.TrimSpace(n); n != "" {
				needs = append(needs, n)
			}
		}
		nodes = append(nodes, glue.ComposeNode{Name: e.Label, Needs: needs, Entries: []glue.MultiQueryEntry{e}})
	}
	return nodes, nil
}

// cmdMultiCompose implements `.multi compose --queries <dir> [--bind <manifest>]`.
func (c *cli) cmdMultiCompose(arg string) {
	a, err := parseCursorArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi compose: %v\n", c.prog, err)
		c.failed = true
		return
	}
	if a.name != "" {
		fmt.Fprintf(c.stderr, "%s: .multi compose: pass the DAG dir as --queries <dir>, "+
			"not a positional argument (got %q)\n", c.prog, a.name)
		c.failed = true
		return
	}
	if len(a.pack) == 0 {
		fmt.Fprintf(c.stderr, "%s: .multi compose: at least one --queries <dir> is required "+
			"(a directory of *.sql++ nodes; several dirs merge into one DAG)\n", c.prog)
		c.failed = true
		return
	}
	nodes, err := buildComposeNodes(a.pack)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi compose: %v\n", c.prog, err)
		c.failed = true
		return
	}
	// Which nodes emit their labelResults: --only <list>, else --terminal (leaf
	// nodes = no downstream dependents), else all. Non-selected nodes report a count
	// only, so an upstream 75k-row query isn't a firehose in a rollup pipeline.
	emit := selectComposeNodes(nodes, a.only, a.terminal)
	sess, binding, err := c.multiSession(a.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi compose: %v\n", c.prog, err)
		c.failed = true
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		fmt.Fprintf(c.stderr, "%s: .multi compose: aborting -- unresolved logical keyspace(s) above\n", c.prog)
		c.failed = true
		return
	}

	res, err := sess.Compose(nodes)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi compose: %v\n", c.prog, err)
		c.failed = true
		return
	}

	type rowOut struct {
		Label  string          `json:"label"`
		Result json.RawMessage `json:"result"`
	}
	type nodeOut struct {
		Node         string   `json:"node"`
		Count        int      `json:"count"`
		Status       string   `json:"status,omitempty"` // "rejected" for a node that failed to parse
		Reason       string   `json:"reason,omitempty"`
		LabelResults []rowOut `json:"labelResults,omitempty"`
	}
	out := struct {
		Order []string  `json:"order"`
		Nodes []nodeOut `json:"nodes"`
	}{Order: res.Order}
	var rejected []string
	for _, n := range res.Nodes {
		no := nodeOut{Node: n.Node, Count: n.Count}
		if n.Rejected {
			no.Status = "rejected"
			no.Reason = reservedWordReason(n.Reason)
			rejected = append(rejected, n.Node)
		} else if emit[n.Node] {
			for _, lr := range n.LabelResults {
				no.LabelResults = append(no.LabelResults, rowOut{Label: lr.Label, Result: lr.Result})
			}
		}
		out.Nodes = append(out.Nodes, no)
	}
	c.printJSON(out)
	fmt.Fprintf(c.stderr, "%s%d node(s), order: %s\n", c.icon("🧩 "), len(res.Nodes), strings.Join(res.Order, " -> "))

	// A rejected node (invalid SQL) is never a clean zero -- surface each reason and,
	// for a committed/scheduled DAG, hard-fail by default so it can't feed a silent
	// count:0 into a downstream number (ISSUE-09). --allow-rejected opts into soft mode.
	for _, n := range res.Nodes {
		if n.Rejected {
			fmt.Fprintf(c.stderr, "  %s %s: %s\n", c.icon("✗"), n.Node, c.style.Yellow(reservedWordReason(n.Reason)))
		}
	}
	if len(rejected) > 0 && !a.allowRejected {
		fmt.Fprintf(c.stderr, "%s: .multi compose: aborting -- %d rejected node(s): %s "+
			"(a rejected node poisons everything downstream; --allow-rejected to continue)\n",
			c.prog, len(rejected), strings.Join(rejected, ", "))
		c.failed = true
	}
}

// selectComposeNodes decides which nodes emit their labelResults: --only <list>
// (exactly those), else --terminal (nodes no other node depends on), else all.
func selectComposeNodes(nodes []glue.ComposeNode, only []string, terminal bool) map[string]bool {
	emit := map[string]bool{}
	if len(only) > 0 {
		for _, n := range only {
			emit[n] = true
		}
		return emit
	}
	if terminal {
		depended := map[string]bool{}
		for _, nd := range nodes {
			for _, dep := range nd.Needs {
				depended[dep] = true
			}
		}
		for _, nd := range nodes {
			if !depended[nd.Name] {
				emit[nd.Name] = true
			}
		}
		return emit
	}
	for _, nd := range nodes {
		emit[nd.Name] = true
	}
	return emit
}
