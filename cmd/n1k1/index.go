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

// cli .index command family (list/show/rebuild/suggest/create).
package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// eagerBuildIndexes builds all catalog-declared secondary indexes now when
// -index=eager, so the first query over the datastore pays no build cost. No-op
// in lazy/off mode or when the datastore has no indexes.
func (c *cli) eagerBuildIndexes() {
	if c.indexMode != "eager" || c.sess == nil || c.sess.Store == nil {
		return
	}
	prog := newIndexProgress(c.stderr, c.fancyTTY)
	err := glue.EagerBuildSecondaryIndexes(c.sess.Store.Datastore, prog.handle)
	prog.finish()
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: eager index build: %v\n", c.prog, err)
	}
}

// cmdIndex dispatches the .index command family: `.index [list]`, `.index show
// <name>`, `.index rebuild [<name>]`, `.index suggest`, `.index create`, `.index
// help`. (`.indexes` is an alias for `.index list`.)
func (c *cli) cmdIndex(arg string) {
	sub, rest := splitFirst(arg)
	switch strings.ToLower(sub) {
	case "", "list":
		c.cmdIndexList()
	case "show":
		c.cmdIndexShow(rest)
	case "rebuild":
		c.cmdIndexRebuild(rest)
	case "help", "example":
		c.cmdIndexHelp()
	case "suggest", "auto-plan":
		c.cmdIndexSuggest(rest)
	case "create":
		c.cmdIndexCreate(rest)
	default:
		fmt.Fprintf(c.stderr, "unknown subcommand %q; try .index help\n", sub)
	}
}

// cmdIndexCreate implements `.index create`: add index definition(s) to
// .n1k1/catalog.json and build them. Two input forms:
//
//	.index create <name> on <keyspace> (<expr>[, <expr>]) [where <expr>]
//	.index create {"indexes":[ ... ]}      (or a single {...}; e.g. `.index suggest` output)
//
// It writes the human catalog (explicit user intent), re-opens the session so the
// new index is advertised, then builds it (showing progress).
func (c *cli) cmdIndexCreate(arg string) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		fmt.Fprintln(c.stderr, "usage: .index create <name> on <keyspace> (<expr>[, <expr>]) [where <expr>]")
		fmt.Fprintln(c.stderr, "   or: .index create {\"indexes\":[ ... ]}   (e.g. paste .index suggest output)")
		return
	}
	if c.sess == nil || c.dir == "" {
		fmt.Fprintln(c.stderr, "no datastore open (open a <ns>/<keyspace> directory first)")
		return
	}
	if glue.IsFlatDatastore(c.sess.Store.Datastore) {
		fmt.Fprintf(c.stderr, "%s: secondary indexes need a <namespace>/<keyspace> datastore directory; "+
			"%q is a flat/single-file datastore, where they aren't supported yet (nothing written)\n", c.prog, c.dir)
		return
	}

	var fragment []byte
	if strings.HasPrefix(arg, "{") {
		fragment = []byte(arg)
	} else {
		f, err := parseCreateDSL(arg)
		if err != nil {
			fmt.Fprintf(c.stderr, "%s: .index create: %v\n", c.prog, err)
			return
		}
		fragment = f
	}

	added, err := glue.CatalogAddIndexes(c.dir, fragment)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .index create: %v\n", c.prog, err)
		return
	}

	// Re-open so the datastore re-wraps with the freshly-written catalog (it may
	// not have been index-wrapped before if this is the first index).
	sess, oerr := glue.OpenSession(c.dir, c.ns)
	if oerr != nil {
		fmt.Fprintf(c.stderr, "%s: reopen after create: %v\n", c.prog, oerr)
		return
	}
	c.sess = sess

	// Build each new index now (force, with progress) so it's ready to use.
	prog := newIndexProgress(c.stderr, c.fancyTTY)
	for _, name := range added {
		if berr := glue.RebuildSecondaryIndexes(c.sess.Store.Datastore, name, prog.handle); berr != nil {
			fmt.Fprintf(c.stderr, "%s: build %q: %v\n", c.prog, name, berr)
		}
	}
	prog.finish()
	fmt.Fprintf(c.stderr, "%screated %s\n", c.icon("✓ "), strings.Join(added, ", "))
}

// parseCreateDSL parses `<name> on <keyspace> (<expr>[, <expr>]) [where <expr>]`
// into a one-index catalog fragment. Keys are split on top-level commas.
func parseCreateDSL(s string) ([]byte, error) {
	open := strings.IndexByte(s, '(')
	if open < 0 {
		return nil, fmt.Errorf("expected '(' with the key expression(s)")
	}
	closeIdx := matchParen(s, open)
	if closeIdx < 0 {
		return nil, fmt.Errorf("unbalanced parentheses")
	}
	head := fieldsBacktickAware(s[:open])
	if len(head) != 3 || !strings.EqualFold(head[1], "on") {
		return nil, fmt.Errorf("expected: <name> on <keyspace> (<expr>...)")
	}
	// name and keyspace are identifiers -- unquote a backticked one (e.g. a
	// keyspace with spaces) to the plain name the catalog stores/looks up by.
	name, keyspace := unquoteIdent(head[0]), unquoteIdent(head[2])

	keys := splitTopLevelCommas(s[open+1 : closeIdx])
	if len(keys) == 0 {
		return nil, fmt.Errorf("need at least one key expression")
	}

	where := ""
	if tail := strings.TrimSpace(s[closeIdx+1:]); tail != "" {
		w, rest := splitFirst(tail)
		if !strings.EqualFold(w, "where") || strings.TrimSpace(rest) == "" {
			return nil, fmt.Errorf("trailing text after ')' must be: where <expr>")
		}
		where = strings.TrimSpace(rest)
	}

	def := struct {
		Name     string   `json:"name"`
		Keyspace string   `json:"keyspace"`
		Keys     []string `json:"keys"`
		Where    string   `json:"where,omitempty"`
	}{name, keyspace, keys, where}
	b, err := json.Marshal(struct {
		Indexes []interface{} `json:"indexes"`
	}{[]interface{}{def}})
	return b, err
}

// matchParen returns the index of the ')' matching the '(' at open, or -1.
func matchParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return i
			}
		}
	}
	return -1
}

// fieldsBacktickAware splits s on whitespace like strings.Fields, but treats a
// `...`-quoted span as a single token (so a backticked keyspace/name with spaces
// stays whole). A doubled backtick “ inside the quotes is a literal backtick and
// does not end the span. Backticks are kept in the returned tokens (see
// unquoteIdent).
func fieldsBacktickAware(s string) []string {
	var toks []string
	var cur []byte
	inTick := false
	flush := func() {
		if len(cur) > 0 {
			toks = append(toks, string(cur))
			cur = cur[:0]
		}
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '`':
			cur = append(cur, ch)
			if inTick && i+1 < len(s) && s[i+1] == '`' { // escaped `` -> literal
				cur = append(cur, '`')
				i++
			} else {
				inTick = !inTick
			}
		case !inTick && (ch == ' ' || ch == '\t'):
			flush()
		default:
			cur = append(cur, ch)
		}
	}
	flush()
	return toks
}

// unquoteIdent strips surrounding backticks from a token and un-doubles any
// escaped “ -> ` inside; a non-backticked token is returned unchanged.
func unquoteIdent(tok string) string {
	if len(tok) >= 2 && tok[0] == '`' && tok[len(tok)-1] == '`' {
		return strings.ReplaceAll(tok[1:len(tok)-1], "``", "`")
	}
	return tok
}

// splitTopLevelCommas splits on commas not nested inside () or [], trimming each
// part and dropping empties.
func splitTopLevelCommas(s string) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case ',':
			if depth == 0 {
				if p := strings.TrimSpace(s[start:i]); p != "" {
					out = append(out, p)
				}
				start = i + 1
			}
		}
	}
	if p := strings.TrimSpace(s[start:]); p != "" {
		out = append(out, p)
	}
	return out
}

// cmdIndexSuggest implements `.index suggest [<keyspace>]`: sample docs, score
// selective scalar/nested-no-array fields, and present the advised indexes two
// ways -- a catalog.json fragment (on stdout, for saving into .n1k1/catalog.json;
// each entry carries a "why" the catalog loader ignores) and the equivalent
// `.index create` commands (on stderr, to paste into the REPL). Headers + the
// commands go to stderr so stdout stays a clean, redirectable JSON fragment.
func (c *cli) cmdIndexSuggest(keyspace string) {
	if c.sess == nil || c.sess.Store == nil {
		fmt.Fprintln(c.stderr, "no datastore open")
		return
	}
	sugg, note, err := glue.SuggestIndexes(c.sess.Store, c.ns, keyspace, 0)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: index suggest: %v\n", c.prog, err)
		return
	}
	if len(sugg) == 0 {
		if note == "" {
			note = "no selective scalar or text fields found in the sample"
		}
		fmt.Fprintf(c.stderr, "no index suggestions (%s)\n", note)
		return
	}
	// Struct field order = JSON key order (encoding/json); "why" is ignored by the
	// catalog loader (unknown field), so the fragment pastes straight into catalog.json.
	// kind is emitted only for fts (gsi is the loader's default); keys is omitted for a
	// dynamic (whole-keyspace) fts index.
	type outDef struct {
		Name     string   `json:"name"`
		Keyspace string   `json:"keyspace"`
		Kind     string   `json:"kind,omitempty"`
		Keys     []string `json:"keys,omitempty"`
		Why      string   `json:"why"`
	}
	type outCat struct {
		Indexes []outDef `json:"indexes"`
	}
	cat := outCat{}
	for _, s := range sugg {
		def := outDef{Name: s.Name, Keyspace: s.Keyspace, Why: s.Why}
		if s.Kind == "fts" {
			def.Kind = "fts"
		}
		if s.Field != "" { // a dynamic fts index has no keys (indexes every field)
			if s.Kind == "fts" {
				def.Keys = []string{s.Field} // fts keys are literal field paths, not SQL++ exprs
			} else {
				// gsi keys are SQL++ key *expressions* (parsed by the catalog loader),
				// so a field whose name/segment has spaces etc. must be backticked.
				def.Keys = []string{quotePath(s.Field)}
			}
		}
		cat.Indexes = append(cat.Indexes, def)
	}
	b, _ := json.MarshalIndent(cat, "", "  ")
	// The "why" fields are harmless -- the catalog loader ignores unknown keys --
	// so the fragment can be saved as-is into the datastore's catalog.json.
	fmt.Fprintf(c.stderr, "%s%d suggestion(s). Option 1 -- save this fragment into %s:\n",
		c.icon("💡 "), len(sugg), c.catalogPath())
	fmt.Fprintln(c.out, string(b))

	// Option 2: the equivalent .index create commands, copy-pasteable straight
	// into the REPL. Kept on stderr (like the headers) so stdout stays a clean
	// catalog.json fragment for redirecting.
	fmt.Fprintf(c.stderr, "%sOption 2 -- paste these commands:\n", c.icon("💡 "))
	for _, s := range sugg {
		if s.Kind == "fts" {
			// The `<name> on <ks> (<exprs>)` DSL builds a gsi index; an fts index
			// goes via the JSON form (also accepted by .index create). A dynamic fts
			// (empty Field) carries no keys.
			d := map[string]interface{}{"name": s.Name, "keyspace": s.Keyspace, "kind": "fts"}
			if s.Field != "" {
				d["keys"] = []string{s.Field}
			}
			j, _ := json.Marshal(d)
			fmt.Fprintf(c.stderr, "  .index create %s\n", j)
			continue
		}
		// Backtick the keyspace/name (whitespace-delimited in the DSL) and the key
		// path segments, so a name/keyspace/field with spaces still parses.
		fmt.Fprintf(c.stderr, "  .index create %s on %s (%s)\n",
			quoteIdent(s.Name), quoteIdent(s.Keyspace), quotePath(s.Field))
	}
	if glue.IsFlatDatastore(c.sess.Store.Datastore) {
		fmt.Fprintf(c.stderr, "%snote: this is a flat/single-file datastore, where secondary indexes "+
			"aren't supported yet -- the above is advisory only (they need a <namespace>/<keyspace> layout).\n",
			c.icon("⚠️  "))
	}
}

// cmdIndexHelp prints the .index subcommand syntax plus a copy-pasteable
// catalog.json example (the definition format isn't otherwise discoverable).
func (c *cli) cmdIndexHelp() {
	fmt.Fprint(c.stderr, `.index commands:
  .index [list]            list secondary indexes with keys + built stats
  .index show <name>       show one index's definition + stats
  .index rebuild [<name>]  force-rebuild (all, or one), ignoring freshness
  .index suggest [<ks>]    advise candidate indexes from a doc sample (emits catalog JSON)
  .index create ...        add index def(s) to catalog.json and build them:
                             .index create <name> on <ks> (<expr>[, <expr>]) [where <expr>]
                             .index create {"indexes":[ ... ]}   (e.g. paste suggest output)
  .index help              this help

Index definitions live in <dataRoot>/.n1k1/catalog.json:
  {
    "indexes": [
      { "name": "ix_country", "keyspace": "customer", "keys": ["country"] },
      { "name": "ix_adult", "keyspace": "customer", "keys": ["age"], "where": "age >= 18" },
      { "name": "ft_docs", "keyspace": "docs", "kind": "fts" }
    ]
  }
A gsi (default) index's keys are N1QL expressions -- a field ("country") or nested
path ("personal_details.state"); list several for a composite index; optional
"where" makes it partial. A "kind":"fts" index is full-text (bleve, dynamic: indexes
every field), queried with SEARCH(keyspace, "text") or SEARCH(keyspace.field, "q").
After editing catalog.json, run '.index rebuild' (or just query -- lazy on first use).
`)
}

// indexInfos returns the datastore's secondary-index snapshots, or nil (printing a
// friendly reason) when there are none / no datastore.
func (c *cli) indexInfos() []glue.IndexInfo {
	if c.sess == nil || c.sess.Store == nil {
		fmt.Fprintln(c.stderr, "no datastore open")
		return nil
	}
	infos := glue.SecondaryIndexInfos(c.sess.Store.Datastore)
	if len(infos) == 0 {
		switch {
		case c.indexMode == "off":
			fmt.Fprintln(c.stderr, "secondary indexes disabled (-index=off)")
		case glue.IsFlatDatastore(c.sess.Store.Datastore):
			fmt.Fprintln(c.stderr, "secondary indexes aren't supported for this flat/single-file datastore "+
				"(they need a <namespace>/<keyspace> layout)")
		default:
			fmt.Fprintf(c.stderr, "no secondary indexes (create one with .index create, or declare them in %s)\n", c.catalogPath())
		}
	}
	return infos
}

// cmdIndexList implements `.index list`: one row per declared secondary index with
// its keys, WHERE, and (once built) entry count + on-disk size, rendered in the
// current output mode (a box at a TTY). Opens/builds any not-yet-built index to
// report live stats.
func (c *cli) cmdIndexList() {
	infos := c.indexInfos()
	if len(infos) == 0 {
		return // indexInfos already printed the reason
	}
	rows := make([]json.RawMessage, 0, len(infos))
	for _, ix := range infos {
		where, entries, size, status := interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)
		if ix.Where != "" {
			where = ix.Where
		}
		if ix.Built {
			entries = ix.Entries
			size = humanBytes(ix.SizeBytes)
		} else if ix.Err != "" {
			status = ix.Err
		} else {
			status = "not built"
		}
		keys := strings.Join(ix.Keys, ", ")
		if ix.Kind == "fts" && keys == "" {
			keys = "(all fields)"
		}
		rows = append(rows, orderedJSONRow(
			[2]interface{}{"index", ix.Namespace + ":" + ix.Keyspace + "." + ix.Name},
			[2]interface{}{"kind", ix.Kind},
			[2]interface{}{"keys", keys},
			[2]interface{}{"where", where},
			[2]interface{}{"entries", entries},
			[2]interface{}{"size", size},
			[2]interface{}{"status", status},
		))
	}
	c.renderRows(rows, "", false)
}

// cmdIndexShow implements `.index show <name>`: one index's full detail, rendered
// as a field/value table (so it boxes nicely at a TTY).
func (c *cli) cmdIndexShow(name string) {
	if name == "" {
		fmt.Fprintln(c.stderr, "usage: .index show <name>")
		return
	}
	for _, ix := range c.indexInfos() {
		if ix.Name != name {
			continue
		}
		keys := strings.Join(ix.Keys, ", ")
		if ix.Kind == "fts" && keys == "" {
			keys = "(all fields)"
		}
		pairs := [][2]string{
			{"name", ix.Name},
			{"keyspace", ix.Namespace + ":" + ix.Keyspace},
			{"kind", ix.Kind},
			{"keys", keys},
		}
		if ix.Where != "" {
			pairs = append(pairs, [2]string{"where", ix.Where})
		}
		if ix.Built {
			pairs = append(pairs,
				[2]string{"entries", strconv.Itoa(ix.Entries)},
				[2]string{"size", humanBytes(ix.SizeBytes)},
				[2]string{"path", ix.Path})
		} else {
			pairs = append(pairs, [2]string{"status", "not built (" + ix.Err + ")"})
		}
		rows := make([]json.RawMessage, 0, len(pairs))
		for _, p := range pairs {
			rows = append(rows, orderedJSONRow(
				[2]interface{}{"field", p[0]},
				[2]interface{}{"value", p[1]},
			))
		}
		c.renderRows(rows, "", false)
		return
	}
	fmt.Fprintf(c.stderr, "no such index %q (try .index list)\n", name)
}

// cmdIndexRebuild implements `.index rebuild [<name>]`: force-rebuild all catalog
// indexes (or the one named), ignoring the freshness signature -- the escape hatch
// when the coarse (file count, newest mtime) freshness check misses a change (e.g.
// an edit within the same mtime tick). Shows the same concurrent build progress as
// -index=eager.
func (c *cli) cmdIndexRebuild(name string) {
	if c.sess == nil || c.sess.Store == nil {
		fmt.Fprintln(c.stderr, "no datastore open")
		return
	}
	prog := newIndexProgress(c.stderr, c.fancyTTY)
	err := glue.RebuildSecondaryIndexes(c.sess.Store.Datastore, name, prog.handle)
	prog.finish()
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: index rebuild: %v\n", c.prog, err)
	}
}

// humanBytes renders a byte count compactly with a unit (e.g. 512B, 4.0KB,
// 1.2MB). Powers of 1024 (KB/MB/GB/TB here mean KiB/MiB/... -- the common `ls -h`
// convention).
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGT"[exp])
}
