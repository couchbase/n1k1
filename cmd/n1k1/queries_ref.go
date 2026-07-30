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

// A `--queries <ref>` value is a SCHEME-QUALIFIED source of a queries-entity, not
// only a local directory. This is the universal entity resolver (naming overhaul
// Phase 3): the SAME `--queries` flow feeds run/lint/cursor/compose whether the
// entity is authored `*.sql++` on disk or a native builtin like the census — so the
// census "dissolves" into the algebra rather than needing its own verb/flag/mode.
//
//	./dir            local path (default)          -> refFile
//	file:./dir       local path (explicit scheme)  -> refFile
//	builtin:census   a native builtin              -> refBuiltin
//	builtin:census@1?keyspace=logs&depth=2         versioned + parameterized builtin
//	registry:...     remote/precanned (FUTURE — deferred; needs a trust story)
//
// Path-vs-scheme rule: a token is a SCHEME ref only if it starts with a RECOGNIZED
// scheme prefix (`file:` / `builtin:`). Everything else is a local path — crucially
// including a Windows drive path like `C:\data` (`c:` is not a recognized scheme).

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

const (
	refFile    = "file"
	refBuiltin = "builtin"
)

// queriesRef is one resolved `--queries` token.
type queriesRef struct {
	kind string // refFile | refBuiltin

	path string // refFile: the local path (dir or a single *.sql++ file)

	name    string            // refBuiltin: the builtin name (e.g. "census")
	version string            // refBuiltin: pinned @version, or "" for the latest
	params  map[string]string // refBuiltin: ?k=v&... parameters (e.g. keyspace, depth)
}

// builtinVersions is the registry of known builtins -> the versions each supports,
// newest FIRST (index 0 is the latest). An unknown name or an unsupported @version is
// a loud error, so a pinned ref never silently runs a different builtin than asked.
//
// TODO(builtin-versioning): this only gates the REF today; a full story also
// (a) stamps the resolved version into any durable artifact a builtin produces (e.g.
// a census cursor records "built with census@1") so a later incompatible version can
// detect + refuse/rebase a stale artifact, and (b) documents each version's schema.
// See tmp/naming.md; recorded as a TODO in TODO.md.
var builtinVersions = map[string][]string{
	"census": {"1"},
}

// parseQueriesRef classifies one `--queries` token. See the file header for the rules.
func parseQueriesRef(tok string) (queriesRef, error) {
	if tok == "" {
		return queriesRef{}, fmt.Errorf("empty --queries value")
	}
	switch {
	case strings.HasPrefix(tok, refBuiltin+":"):
		return parseBuiltinRef(tok[len(refBuiltin)+1:])
	case strings.HasPrefix(tok, refFile+":"):
		return queriesRef{kind: refFile, path: tok[len(refFile)+1:]}, nil
	default:
		// Any other token (incl. a Windows `C:\...` path) is a local path.
		return queriesRef{kind: refFile, path: tok}, nil
	}
}

// parseBuiltinRef parses the body after "builtin:" -> name[@version][?k=v&...].
func parseBuiltinRef(body string) (queriesRef, error) {
	r := queriesRef{kind: refBuiltin, params: map[string]string{}}

	// Split off the ?query-string first.
	head := body
	if q := strings.IndexByte(body, '?'); q >= 0 {
		head = body[:q]
		for _, kv := range strings.Split(body[q+1:], "&") {
			if kv == "" {
				continue
			}
			k, v, _ := strings.Cut(kv, "=")
			if k = strings.TrimSpace(k); k != "" {
				r.params[k] = v
			}
		}
	}
	// Then the @version off the name.
	r.name, r.version, _ = strings.Cut(head, "@")
	r.name = strings.TrimSpace(r.name)
	if r.name == "" {
		return r, fmt.Errorf("builtin: needs a name, e.g. builtin:census")
	}
	vers, known := builtinVersions[r.name]
	if !known {
		return r, fmt.Errorf("unknown builtin %q (known: %s)", r.name, strings.Join(builtinNames(), ", "))
	}
	if r.version != "" && !contains(vers, r.version) {
		return r, fmt.Errorf("builtin %q has no version %q (supported: %s)",
			r.name, r.version, strings.Join(vers, ", "))
	}
	if r.version == "" {
		r.version = vers[0] // resolve "latest" to the concrete newest version
	}
	return r, nil
}

// runBuiltinQueries handles a `--queries builtin:<name>` entity in `.multi run`. It
// returns true if it took the command (a builtin was present, so cmdMultiRun stops),
// false when every --queries ref is a local path (fall through to the normal pack
// path). A builtin resolves to a native entity, not *.sql++, so it must be routed
// before the pack loader.
func (c *cli) runBuiltinQueries(args multiArgs) bool {
	refs := make([]queriesRef, 0, len(args.queries))
	anyBuiltin := false
	for _, q := range args.queries {
		r, err := parseQueriesRef(q)
		if err != nil {
			fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
			c.failed = true
			return true
		}
		anyBuiltin = anyBuiltin || r.kind == refBuiltin
		refs = append(refs, r)
	}
	if !anyBuiltin {
		return false
	}
	if len(refs) != 1 {
		fmt.Fprintf(c.stderr, "%s: .multi run: a builtin: entity must be the only --queries value "+
			"(mixing builtins with dirs is not supported yet)\n", c.prog)
		c.failed = true
		return true
	}
	r := refs[0]
	switch r.name {
	case "census":
		c.runBuiltinCensus(args, r)
	default:
		fmt.Fprintf(c.stderr, "%s: .multi run: builtin %q is not runnable\n", c.prog, r.name)
		c.failed = true
	}
	return true
}

// runBuiltinCensus executes `.multi run --queries builtin:census?keyspace=...` by
// delegating to the census engine (the census "dissolves" into the queries algebra —
// same entry point as any other queries entity). Params: keyspace (required),
// type-field, time-field, depth (default 2), exclude (comma-list).
func (c *cli) runBuiltinCensus(args multiArgs, r queriesRef) {
	keyspace := r.params["keyspace"]
	if keyspace == "" {
		fmt.Fprintf(c.stderr, "%s: .multi run --queries builtin:census: needs a keyspace, "+
			"e.g. builtin:census?keyspace=sessions\n", c.prog)
		c.failed = true
		return
	}
	opts := glue.CensusOptions{
		TypeField: r.params["type-field"],
		TimeField: r.params["time-field"],
		Depth:     2,
	}
	if d := r.params["depth"]; d != "" {
		if n, e := strconv.Atoi(d); e == nil {
			opts.Depth = n
		}
	}
	if ex := r.params["exclude"]; ex != "" {
		for _, e := range strings.Split(ex, ",") {
			if e = strings.TrimSpace(e); e != "" {
				opts.Exclude = append(opts.Exclude, e)
			}
		}
	}
	sess, binding, err := c.multiSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		fmt.Fprintf(c.stderr, "%s: .multi run --queries builtin:census: aborting -- unresolved keyspace above\n", c.prog)
		c.failed = true
		return
	}
	c.emitCensus(sess, keyspace, opts)
}

// builtinCensusRef reports whether queries is a single `builtin:census[...]` ref (and
// returns it parsed). Used by `cursor create` to route to the census-cursor path.
func builtinCensusRef(queries []string) (queriesRef, bool) {
	if len(queries) != 1 {
		return queriesRef{}, false
	}
	r, err := parseQueriesRef(queries[0])
	if err != nil || r.kind != refBuiltin || r.name != "census" {
		return queriesRef{}, false
	}
	return r, true
}

func builtinNames() []string {
	out := make([]string, 0, len(builtinVersions))
	for n := range builtinVersions {
		out = append(out, n)
	}
	return out
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
