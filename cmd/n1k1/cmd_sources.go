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

// cmd_sources.go parses the CLI's positional data-source arguments (DESIGN-data.md §2
// Phase 1). One bare path stays the classic single datastore root; two-or-more paths,
// or any `name=path`, are multiple sources (each a keyspace) opened via
// glue.OpenSessionSources. The same parsing backs a multi-source `.open`.
package main

import (
	"strings"

	"github.com/couchbase/n1k1/glue"
	"github.com/couchbase/n1k1/records"
)

// parseSourceArgs classifies the positional args. It returns multi=true (and the
// parsed sources) when there are 2+ args or a single `name=path`; otherwise multi is
// false and the caller uses the classic single-root path (a bare dir/file keeps its
// conventional <ns>/<keyspace>, flat-root, single-file discovery).
func parseSourceArgs(args []string) (sources []glue.Source, multi bool) {
	if len(args) == 0 {
		return nil, false
	}
	if len(args) == 1 {
		if name, _ := splitSourceArg(args[0]); name == "" {
			return nil, false // a lone bare path -> classic single source
		}
	}
	out := make([]glue.Source, 0, len(args))
	for _, a := range args {
		name, path := splitSourceArg(a)
		out = append(out, glue.Source{Name: name, Path: path})
	}
	return out, true
}

// stdinSources decides how a piped-stdin `stdin` keyspace fits with the positional
// source args (the jq-like `FROM stdin` feature): stdin is wanted when there are NO args
// at all, or a `-` (or `name=-`) appears among them. Every other arg becomes a file
// source. Returns the file sources, the keyspace name for stdin (default "stdin", or the
// `name` of a `name=-`), and whether stdin is wanted. The caller applies this only when
// -c/-f is set and stdin is piped (so it never clashes with bare-stdin-is-statements).
func stdinSources(fargs []string) (fileSrcs []glue.Source, stdinName string, wantStdin bool) {
	stdinName = "stdin"
	wantStdin = len(fargs) == 0
	for _, a := range fargs {
		if n, p := splitSourceArg(a); p == "-" {
			wantStdin = true
			if n != "" {
				stdinName = n
			}
		} else {
			fileSrcs = append(fileSrcs, glue.Source{Name: n, Path: p})
		}
	}
	return fileSrcs, stdinName, wantStdin
}

// splitSourceArg splits a `name=path` source spec into its name and path. The name
// part is honored only when it's a plausible bare identifier -- non-empty and free of
// path separators and glob metacharacters -- so an `=` inside a path (rare) or a
// pattern isn't mistaken for a name; otherwise the whole arg is the path (name "").
func splitSourceArg(arg string) (name, path string) {
	if i := strings.IndexByte(arg, '='); i > 0 {
		cand := arg[:i]
		if !strings.ContainsAny(cand, `/\`) && !records.HasGlobMeta(cand) {
			return cand, arg[i+1:]
		}
	}
	return "", arg
}

// sourceNames returns each source's keyspace name (or its path when the name is
// derived), for a friendly "querying N data sources: …" startup line.
func sourceNames(sources []glue.Source) []string {
	out := make([]string, len(sources))
	for i, s := range sources {
		if s.Name != "" {
			out[i] = s.Name
		} else {
			out[i] = s.Path
		}
	}
	return out
}
