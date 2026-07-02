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

// cli keyspace listing, identifier quoting, and .schema sampling.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

func (c *cli) cmdKeyspaces() {
	c.printKeyspaces(c.out)
}

// keyspaceNames lists the keyspaces the datastore actually exposes in the
// current namespace -- via the datastore interface, not the raw filesystem, so
// the listing reflects n1k1's flattening (e.g. a synthetic flat-root keyspace,
// or later catalog-defined keyspaces), not just literal subdirectories.
func (c *cli) keyspaceNames() ([]string, error) {
	ns, nerr := c.sess.Store.Datastore.NamespaceByName(c.ns)
	if nerr != nil {
		// Namespace missing usually just means an empty datastore -- report it as
		// "no keyspaces" (empty list) rather than an error, so the caller can show
		// a friendly hint instead of a scary message.
		return nil, nil
	}
	names, kerr := ns.KeyspaceNames()
	if kerr != nil {
		return nil, fmt.Errorf("listing keyspaces: %v", kerr)
	}
	sort.Strings(names)
	return names, nil
}

// needsBackticks reports whether a keyspace/identifier name must be wrapped in
// backticks to parse as SQL++. An unquoted identifier is [A-Za-z_][A-Za-z0-9_]*;
// anything else -- empty, a leading digit, or a '.', '-', space, etc. -- needs
// quoting. Filesystem-derived keyspace names (a flat-root basename or single-file
// stem like "2026-01") routinely hit this. Backticks around an already-valid
// identifier are harmless, so being conservative here is safe.
func needsBackticks(name string) bool {
	if name == "" {
		return true
	}
	for i, r := range name {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
			// always allowed
		case r >= '0' && r <= '9':
			if i == 0 {
				return true // an unquoted identifier can't start with a digit
			}
		default:
			return true // any other rune (., -, space, :, ...) forces backticks
		}
	}
	return false
}

// quoteIdent wraps name in backticks when SQL++ parsing requires it, escaping an
// embedded backtick by doubling it. A valid bare identifier is returned unchanged.
func quoteIdent(name string) string {
	if !needsBackticks(name) {
		return name
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// exampleFor returns a copy-pasteable SQL++ example for a keyspace, varying the
// template by position so a multi-keyspace listing shows a mix. The keyspace is
// backticked when SQL++ parsing requires it, so the sample is paste-ready.
func exampleFor(ks string, i int) string {
	tmpls := []string{
		"SELECT COUNT(*) FROM %s;",
		"SELECT * FROM %s LIMIT 3;",
		"SELECT * FROM %s LIMIT 5;",
	}
	return fmt.Sprintf(tmpls[i%len(tmpls)], quoteIdent(ks))
}

// printKeyspaces writes the keyspace listing (with a copy-pasteable example per
// keyspace) to w. Shown at interactive startup and by .tables/.keyspaces.
func (c *cli) printKeyspaces(w io.Writer) {
	names, err := c.keyspaceNames()
	if err != nil {
		fmt.Fprintf(w, "%v\n", err)
		return
	}
	if len(names) == 0 {
		fmt.Fprintf(w, "%sNo keyspaces here yet — you can still evaluate expressions, e.g. %s\n",
			c.icon("💡 "), c.style.Cyan("SELECT 1+2;"))
		fmt.Fprintf(w, "   Point at data with %s (a dir of JSON, or <namespace>/<keyspace> subdirs).\n",
			c.style.Bold(".open <dir>"))
		return
	}
	// Display names backticked when SQL++ needs it (e.g. "2026-01"), so the
	// listed keyspace matches how it must be typed; pad on the displayed form.
	disp := make([]string, len(names))
	width := 0
	for i, n := range names {
		disp[i] = quoteIdent(n)
		if len(disp[i]) > width {
			width = len(disp[i])
		}
	}
	noun := "keyspaces"
	if len(names) == 1 {
		noun = "keyspace"
	}
	// The namespace is almost always "default"; only mention it when it isn't.
	nsNote := ""
	if c.ns != "default" {
		nsNote = " in namespace " + c.style.Bold(c.ns)
	}
	fmt.Fprintf(w, "%s%d %s%s — copy/paste to try:\n",
		c.icon("📚 "), len(names), noun, nsNote)
	for i, n := range names {
		pad := disp[i] + strings.Repeat(" ", width-len(disp[i]))
		fmt.Fprintf(w, "  %s   %s\n", c.style.Cyan(pad), c.style.Dim(exampleFor(n, i)))
	}
}

func (c *cli) cmdSchema(keyspace string) {
	kss := []string{keyspace}
	if keyspace == "" {
		names, err := c.keyspaceNames()
		if err != nil {
			fmt.Fprintf(c.stderr, "%v\n", err)
			return
		}
		kss = names
	}
	for _, ks := range kss {
		shape, n, err := c.sampleSchema(ks, 50)
		if err != nil {
			fmt.Fprintf(c.stderr, "%s%s\n", c.icon("✗ "), c.style.Red("Error: "+err.Error()))
			continue
		}
		fmt.Fprintf(c.out, "%s  (sampled %d docs):\n", ks, n)
		keys := make([]string, 0, len(shape))
		for k := range shape {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			types := shape[k]
			sort.Strings(types)
			fmt.Fprintf(c.out, "  %-20s %s\n", k, strings.Join(types, "|"))
		}
	}
}

// sampleSchema infers a keyspace's shape (top-level key -> observed JSON type
// names) by running `SELECT <alias>.* FROM <ks> LIMIT n` through the session --
// the same resolution + decoding path real queries take. This keeps .schema in
// lockstep with what queries actually see: flat roots, single-file keyspaces,
// multi-record JSONL/CSV, and gzip all work, not just one-doc-per-file *.json
// (the old filesystem walk reported "0 docs" for every one of those). Returns the
// shape, the number of docs sampled, and any query error.
func (c *cli) sampleSchema(ks string, limit int) (map[string][]string, int, error) {
	// quoteIdent so keyspaces like "2026-01" parse; alias x so `x.*` projects the
	// document's fields unwrapped (SELECT * would nest them under the keyspace).
	stmt := fmt.Sprintf("SELECT x.* FROM %s AS x LIMIT %d", quoteIdent(ks), limit)
	res, err := c.sess.Run(stmt)
	if err != nil {
		return nil, 0, err
	}
	shape := map[string]map[string]bool{}
	for _, row := range res.Rows {
		var m map[string]interface{}
		if json.Unmarshal(row, &m) != nil {
			continue // a non-object row (e.g. a bare scalar) contributes no fields
		}
		for k, v := range m {
			if shape[k] == nil {
				shape[k] = map[string]bool{}
			}
			shape[k][jsonType(v)] = true
		}
	}
	out := make(map[string][]string, len(shape))
	for k, set := range shape {
		for t := range set {
			out[k] = append(out[k], t)
		}
	}
	return out, len(res.Rows), nil
}

func jsonType(v interface{}) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case float64:
		return "number"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}
