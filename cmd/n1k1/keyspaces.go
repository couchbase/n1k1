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
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"

	"github.com/couchbase/query/datastore"
)

func (c *cli) cmdKeyspaces() {
	c.printKeyspaces(c.out)
}

// keyspaceNames lists the keyspaces the datastore actually exposes in the
// default namespace -- via the datastore interface, not the raw filesystem, so
// the listing reflects n1k1's flattening (e.g. a synthetic flat-root keyspace,
// or later catalog-defined keyspaces), not just literal subdirectories.
func (c *cli) keyspaceNames() ([]string, error) {
	ns, nerr := c.sess.Store.Datastore.NamespaceByName(defaultNamespace)
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

// quotePath backticks each dotted segment of a field *path* that SQL++ needs
// quoted, so a nested path stays a path expression: "profile.first name" ->
// "profile.`first name`", "sku" -> "sku". Used for index key expressions (a
// whole-path quoteIdent would wrongly turn a.b into the single identifier `a.b`).
func quotePath(path string) string {
	segs := strings.Split(path, ".")
	for i, s := range segs {
		segs[i] = quoteIdent(s)
	}
	return strings.Join(segs, ".")
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
	// Alongside each, a FRAMING tag (IDEA-0007) says how the keyspace's files become
	// rows -- a recipe / structured format (query-ready multi-record) vs a whole-file
	// blob (one row per file) -- so a user can tell them apart without probing.
	disp := make([]string, len(names))
	framing := make([]string, len(names))
	width, fwidth := 0, 0
	anyBlob := false
	ns, _ := c.sess.Store.Datastore.NamespaceByName(defaultNamespace)
	for i, n := range names {
		disp[i] = quoteIdent(n)
		if len(disp[i]) > width {
			width = len(disp[i])
		}
		kf, ok := c.keyspaceFraming(ns, n)
		if ok {
			if kf.Kind == glue.KeyspaceTemp {
				framing[i] = kf.Label() // in-memory: no file count
			} else {
				unit := "files"
				if kf.Files == 1 {
					unit = "file"
				}
				framing[i] = fmt.Sprintf("%s · %d %s", kf.Label(), kf.Files, unit)
			}
			if kf.Kind == glue.KeyspaceBlob {
				anyBlob = true
			}
		}
		if len(framing[i]) > fwidth {
			fwidth = len(framing[i])
		}
	}
	noun := "keyspaces"
	if len(names) == 1 {
		noun = "keyspace"
	}
	fmt.Fprintf(w, "%s%d %s — copy/paste to try:\n",
		c.icon("📚 "), len(names), noun)
	for i, n := range names {
		pad := disp[i] + strings.Repeat(" ", width-len(disp[i]))
		fpad := framing[i] + strings.Repeat(" ", fwidth-len(framing[i]))
		fmt.Fprintf(w, "  %s   %s   %s\n",
			c.style.Cyan(pad), c.style.Dim(fpad), c.style.Dim(exampleFor(n, i)))
	}
	if anyBlob {
		fmt.Fprintf(w, "  %s\n", c.style.Dim(
			"whole-file = one row per file (a text blob); frame it into rows with a *.extract.js recipe."))
	}
	// IDEA-0012: a bundle dir hides its big raw logs (memcached.log, couchbase.log,
	// ...) -- they're present but no recipe frames them, so they're not keyspaces and
	// otherwise leave no trace. Surface them so the user knows the data is there.
	if unexposed := glue.UnexposedRecordFiles(c.dir); len(unexposed) > 0 {
		shown, extra := unexposed, 0
		if len(shown) > 6 {
			extra, shown = len(shown)-6, shown[:6]
		}
		list := strings.Join(shown, ", ")
		if extra > 0 {
			list += fmt.Sprintf(", +%d more", extra)
		}
		noun := "files"
		if len(unexposed) == 1 {
			noun = "file"
		}
		fmt.Fprintf(w, "  %s\n", c.style.Dim(fmt.Sprintf(
			"%d more %s here aren't keyspaces (no recipe frames them): %s", len(unexposed), noun, list)))
		fmt.Fprintf(w, "  %s\n", c.style.Dim(
			"→ query one directly (n1k1 <dir>/"+shown[0]+"), or add a *.extract.js recipe to frame it into rows."))
	}
}

// keyspaceFraming resolves a keyspace's record-framing summary (IDEA-0007) for the
// listing: how its files turn into rows (a recipe, a structured format, or a
// whole-file blob). Content-free (a file listing + registry match), so it's safe to
// call per keyspace on startup / .tables even over a huge log. ok is false when the
// keyspace can't be resolved (reported as a blank framing cell, never fatal).
func (c *cli) keyspaceFraming(ns datastore.Namespace, name string) (glue.KeyspaceFraming, bool) {
	if ns == nil {
		return glue.KeyspaceFraming{}, false
	}
	ks, kerr := ns.KeyspaceByName(name)
	if kerr != nil || ks == nil {
		return glue.KeyspaceFraming{}, false
	}
	kf, ferr := glue.KeyspaceFramingFor(ks)
	if ferr != nil {
		return glue.KeyspaceFraming{}, false
	}
	return kf, true
}

// dataLoc describes where n1k1 is currently reading from (the dir or file from
// the CLI arg or the last .open), for status output. Empty means no datastore.
func (c *cli) dataLoc() string {
	if c.dir == "" {
		return "(none — use .open <dir> to load one)"
	}
	return c.dir
}

// catalogPath is where .index create/suggest read & write index definitions for
// the current datastore (its <sidecar>/catalog.json), or a placeholder when no dir
// is known (e.g. an empty-store session). The sidecar dir is named after the
// program ("."+prog), matching glue.SidecarName().
func (c *cli) catalogPath() string {
	sidecar := glue.SidecarName() // "."+prog (main sets it); ".n1k1" by default
	if c.dir == "" {
		return "<dataRoot>/" + sidecar + "/catalog.json"
	}
	return filepath.Join(c.dir, sidecar, "catalog.json")
}

// exampleQuery builds a copy-pasteable example over a real current keyspace, or
// "" when none is available. The "default:" namespace prefix is omitted (it's
// optional, and n1k1 only uses the default namespace).
func (c *cli) exampleQuery() string {
	names, err := c.keyspaceNames()
	if err != nil || len(names) == 0 {
		return ""
	}
	return "SELECT * FROM " + quoteIdent(names[0]) + " LIMIT 5;"
}

// cmdSchema renders a keyspace's sampled shape as a box: one row per top-level
// field with its observed JSON type(s), distinct-value count, and a copy-pasteable
// SQL++ example that filters on the field using the values seen (= / IN / IS NOT
// MISSING). Always box|pretty regardless of the -mode output setting.
func (c *cli) cmdSchema(keyspace string) {
	keyspace = unquoteIdent(strings.TrimSpace(keyspace)) // accept `ns_server.error` like SQL (IDEA-0009)
	kss := []string{keyspace}
	if keyspace == "" {
		names, err := c.keyspaceNames()
		if err != nil {
			fmt.Fprintf(c.stderr, "%v\n", err)
			return
		}
		kss = names
	}
	// Remind which datastore this shape is from (easy to forget across sessions).
	fmt.Fprintf(c.out, "%sdatastore: %s\n", c.icon("📂 "), c.dataLoc())
	ns, _ := c.sess.Store.Datastore.NamespaceByName(defaultNamespace)
	for _, ks := range kss {
		stats, n, err := c.sampleSchema(ks, 50)
		if err != nil {
			fmt.Fprintf(c.stderr, "%s%s\n", c.icon("✗ "), c.style.Red("Error: "+tidyMsg(err.Error())))
			continue
		}
		// Framing tag (IDEA-0007): how the keyspace's files become rows -- so a
		// 1-doc "sample" from a whole-file blob reads differently from a real sample.
		tag := ""
		if kf, ok := c.keyspaceFraming(ns, ks); ok {
			tag = " [" + kf.Label() + "]"
		}
		fmt.Fprintf(c.out, "%s%s  (sampled %d docs):\n", ks, tag, n)

		fields := make([]string, 0, len(stats))
		for f := range stats {
			fields = append(fields, f)
		}
		sort.Strings(fields)

		rows := make([]json.RawMessage, 0, len(fields))
		for _, f := range fields {
			fs := stats[f]
			types := make([]string, 0, len(fs.types))
			for t := range fs.types {
				types = append(types, t)
			}
			sort.Strings(types)
			distinct := strconv.Itoa(len(fs.values))
			if fs.capped {
				distinct += "+"
			}
			rows = append(rows, orderedJSONRow(
				[2]interface{}{"field", f},
				[2]interface{}{"types", strings.Join(types, "|")},
				[2]interface{}{"distinct", distinct},
				[2]interface{}{"example", schemaExample(ks, f, fs)},
			))
		}
		c.renderSchemaBox(rows)
	}
}

// renderSchemaBox draws the schema rows with the box renderer, always pretty and
// independent of the session's -mode (so .schema reads as a table even when piped
// output would otherwise be jsonlines).
func (c *cli) renderSchemaBox(rows []json.RawMessage) {
	if len(rows) == 0 {
		return
	}
	termWidth := 0
	if c.maxWidth < 0 {
		termWidth = c.terminalWidth()
	}
	cmd.RenderBox(c.out, rows, c.maxWidth, c.maxRows, termWidth, "", c.style, true /* pretty */)
}

// Bounds for the distinct values .schema keeps per field: maxSchemaValues caps how
// many distinct scalar values are retained; maxSchemaIn is how many it will list in
// an IN [...] example before falling back to a single `= <value>`.
const (
	maxSchemaValues = 16
	maxSchemaIn     = 5
)

// fieldStat accumulates, over the .schema sample, one top-level field's observed
// JSON types plus a bounded set of distinct scalar values used to synthesize a
// WHERE example.
type fieldStat struct {
	types     map[string]bool
	values    []json.RawMessage // distinct non-null scalar values, first-seen order
	seen      map[string]bool   // dedup by the value's canonical JSON text
	capped    bool              // more distinct scalar values than maxSchemaValues
	nonScalar bool              // saw an object/array value (no useful = literal)
}

// observe records one occurrence of a field's value (v = decoded, raw = its JSON).
func (fs *fieldStat) observe(v interface{}, raw json.RawMessage) {
	if fs.types == nil {
		fs.types = map[string]bool{}
		fs.seen = map[string]bool{}
	}
	t := jsonType(v)
	fs.types[t] = true
	switch t {
	case "object", "array":
		fs.nonScalar = true
	case "null":
		// null isn't a useful equality literal (`= null` is never true); skip it.
	default: // string, number, bool -- candidate WHERE literals
		key := string(raw)
		if fs.seen[key] {
			return
		}
		if len(fs.values) >= maxSchemaValues {
			fs.capped = true
			return
		}
		fs.seen[key] = true
		fs.values = append(fs.values, raw)
	}
}

// schemaExample builds a copy-pasteable SQL++ query filtering on one field, using
// the distinct values sampled: `= v` for a single value, `IN [...]` for a few, a
// representative `= v` when there are many, and `IS NOT MISSING` for a field with
// no scalar values (object/array-valued, or only ever null).
func schemaExample(ks, field string, fs *fieldStat) string {
	qks, qf := quoteIdent(ks), quoteIdent(field)
	switch {
	case len(fs.values) == 0:
		return fmt.Sprintf("SELECT * FROM %s WHERE %s IS NOT MISSING;", qks, qf)
	case len(fs.values) == 1:
		return fmt.Sprintf("SELECT * FROM %s WHERE %s = %s;", qks, qf, fs.values[0])
	case len(fs.values) <= maxSchemaIn && !fs.capped:
		parts := make([]string, len(fs.values))
		for i, v := range fs.values {
			parts[i] = string(v)
		}
		return fmt.Sprintf("SELECT * FROM %s WHERE %s IN [%s];", qks, qf, strings.Join(parts, ", "))
	default:
		return fmt.Sprintf("SELECT * FROM %s WHERE %s = %s;", qks, qf, fs.values[0])
	}
}

// sampleSchema samples a keyspace by running `SELECT <alias>.* FROM <ks> LIMIT n`
// through the session -- the same resolution + decoding path real queries take, so
// .schema stays in lockstep with what queries actually see (flat roots, single-file
// keyspaces, multi-record JSONL/CSV, gzip -- not just one-doc-per-file *.json). It
// returns per-field stats (types + distinct values), the docs sampled, and any
// query error.
func (c *cli) sampleSchema(ks string, limit int) (map[string]*fieldStat, int, error) {
	// quoteIdent so keyspaces like "2026-01" parse; alias x so `x.*` projects the
	// document's fields unwrapped (SELECT * would nest them under the keyspace).
	stmt := fmt.Sprintf("SELECT x.* FROM %s AS x LIMIT %d", quoteIdent(ks), limit)
	res, err := c.sess.Run(stmt)
	if err != nil {
		return nil, 0, err
	}
	stats := map[string]*fieldStat{}
	for _, row := range res.Rows {
		var m map[string]interface{}
		if json.Unmarshal(row, &m) != nil {
			continue // a non-object row (e.g. a bare scalar) contributes no fields
		}
		for k, v := range m {
			fs := stats[k]
			if fs == nil {
				fs = &fieldStat{}
				stats[k] = fs
			}
			raw, _ := json.Marshal(v)
			fs.observe(v, raw)
		}
	}
	return stats, len(res.Rows), nil
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
