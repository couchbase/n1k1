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

import (
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
)

// version is the n1k1 build version. The Makefile injects `git describe` output
// at link time:
//
//	go build -ldflags "-X main.version=$(git describe --long --tags --always --dirty)"
//
// A plain `go build`/`go install` that doesn't set it leaves "dev"; the embedded
// VCS revision (below) still pins the exact source commit in that case.
var version = "dev"

// printVersion writes the build version plus the Go toolchain, the VCS stamp Go
// embeds automatically, and the full module dependency graph -- as actually
// built -- to w.
//
// The dependency versions/sums come from runtime/debug.ReadBuildInfo, which
// reflects the binary's real compiled-in module graph. It needs no `go mod tidy`
// (a good thing here: this repo pins couchbase/query to the n1k1-query fork via a
// go.mod `replace`, plus placeholder-version indirect deps that only resolve in a
// full repo-sync build) and it reports each `replace` target as "=> ...".
func printVersion(w io.Writer) {
	fmt.Fprintf(w, "%s %s\n", prog, version)
	fmt.Fprintf(w, "  go:        %s\n", runtime.Version())

	bi, ok := debug.ReadBuildInfo()
	if !ok {
		fmt.Fprintln(w, "  (no embedded build info)")
		return
	}

	// The vcs.* settings are stamped by the Go toolchain when building from a
	// VCS checkout; they identify the source commit even without the ldflags.
	var rev, vtime, modified string
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.time":
			vtime = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if rev != "" {
		dirty := ""
		if modified == "true" {
			dirty = " (+uncommitted changes)"
		}
		fmt.Fprintf(w, "  revision:  %s%s\n", rev, dirty)
	}
	if vtime != "" {
		fmt.Fprintf(w, "  built:     %s\n", vtime)
	}
	fmt.Fprintf(w, "  module:    %s %s\n", bi.Main.Path, bi.Main.Version)

	if len(bi.Deps) == 0 {
		return
	}
	fmt.Fprintf(w, "dependencies (%d, as built):\n", len(bi.Deps))
	for _, d := range bi.Deps {
		line := fmt.Sprintf("  %s %s", d.Path, d.Version)
		if d.Sum != "" {
			line += " " + d.Sum
		}
		// A go.mod `replace` (e.g. couchbase/query => n1k1-query) shows the module
		// that was actually compiled in, with its own version + sum.
		if r := d.Replace; r != nil {
			line += fmt.Sprintf("  =>  %s %s", r.Path, r.Version)
			if r.Sum != "" {
				line += " " + r.Sum
			}
		}
		fmt.Fprintln(w, line)
	}
}
