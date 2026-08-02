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
//	builtin:census.sql++   an embedded SQL++ builtin  -> refBuiltin
//	builtin:census.sql++@v2.1?keyspace=logs&depth=2   versioned + parameterized builtin
//	registry:...     remote/precanned (FUTURE — deferred; needs a trust story)
//
// Path-vs-scheme rule: a token is a SCHEME ref only if it starts with a RECOGNIZED
// scheme prefix (`file:` / `builtin:`). Everything else is a local path — crucially
// including a Windows drive path like `C:\data` (`c:` is not a recognized scheme).

import (
	"fmt"
	"strings"

	builtinq "github.com/couchbase/n1k1/cmd/n1k1/builtins"
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
// Native builtins list their versions here; embedded SQL++ builtins
// (cmd/n1k1/builtins/*.sql++) contribute theirs at init from each file's
// `-- version:` front-matter — the ARTIFACT's version, not n1k1's, bumped when the
// file's meaning/output changes so a pinned ref detects incompatibility instead of
// silently running a different query. (This is the builtin-versioning TODO's part
// (a) for SQL++ builtins; a census cursor already stamps its resolved ref.)
var builtinVersions = map[string][]string{
	// (the native Go "census" was RETIRED -- census.sql++ / census_agg.agg.js are
	// the census now; the Go implementation survives only as the CI oracle. See
	// parseQueriesRef's retired-name hint and DESIGN-census.md.)
}

func init() {
	all, err := builtinq.All()
	if err != nil {
		return // surfaced loudly on first use by runBuiltinSQL/lookup paths
	}
	for _, q := range all {
		vers := []string{}
		if q.Version != "" {
			vers = append(vers, q.Version)
		}
		if q.Name == "census.sql++" {
			vers = append(vers, "1") // legacy alias: census.sql++ predates file versions
		}
		if len(vers) == 0 {
			vers = []string{"1"}
		}
		builtinVersions[q.Name] = vers
	}
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
		return r, fmt.Errorf("builtin: needs a name, e.g. builtin:census.sql++")
	}
	vers, known := builtinVersions[r.name]
	if !known {
		if r.name == "census" { // retired: fail loud with the migration, not "unknown".
			return r, fmt.Errorf("builtin:census (the native Go census) was retired -- use " +
				"builtin:census.sql++ (forkable SQL++; same params + first-id=1 for provenance) " +
				"or the bundled census_agg() JS aggregate (always available; fork " +
				"extensions/functions/builtin_census_agg.js); " +
				"the Go implementation lives on only as the CI oracle")
		}
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
	if q, ok := builtinq.Lookup(r.name); ok { // embedded SQL++ builtin (generic)
		c.runBuiltinSQL(args, r, q)
		return true
	}
	fmt.Fprintf(c.stderr, "%s: .multi run: builtin %q is not runnable\n", c.prog, r.name)
	c.failed = true
	return true
}

// retiredCensusRef reports whether queries is a single ref to the RETIRED native
// census builtin (`builtin:census[@v][?...]`), so the cursor-create path can fail
// with the migration message instead of treating it as a queries dir.
func retiredCensusRef(queries []string) bool {
	if len(queries) != 1 {
		return false
	}
	q := strings.TrimSpace(queries[0])
	if !strings.HasPrefix(q, "builtin:") {
		return false
	}
	head, _, _ := strings.Cut(strings.TrimPrefix(q, "builtin:"), "?")
	name, _, _ := strings.Cut(head, "@")
	return strings.TrimSpace(name) == "census"
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
