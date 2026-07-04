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

// Loading query extensions into the CLI, via the -ext startup flag and the
// .ext dot-command. The kind of each extension is auto-detected from its file
// extension (today: ".js" = a JavaScript scalar UDF). The native
// sparkline()/histogram() aggregates need NO loading (they are always
// available). See DESIGN-extensions.md and extensions/functions/.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// loadExtensions registers query extensions from a comma-separated list of
// directories and/or individual files. A directory is scanned for recognized
// extension files (glue.RegisterExtensionDir); a file is registered directly
// (glue.RegisterExtensionFile), its kind auto-detected from the extension.
// Returns the registered function names (in the order encountered).
func loadExtensions(spec string) ([]string, error) {
	var names []string
	for _, p := range strings.Split(spec, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		fi, err := os.Stat(p)
		if err != nil {
			return names, err
		}
		if fi.IsDir() {
			ns, err := glue.RegisterExtensionDir(p)
			names = append(names, ns...)
			if err != nil {
				return names, err
			}
			continue
		}
		name, err := glue.RegisterExtensionFile(p)
		if err != nil {
			return names, err
		}
		names = append(names, name)
	}
	return names, nil
}

// cmdExt implements the ".ext <path>[,<path>...]" dot-command: load extensions
// live in the REPL. With no argument it prints usage.
func (c *cli) cmdExt(arg string) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		fmt.Fprintln(c.stderr, "usage: .ext <dir-or-file>[,<dir-or-file>...]  (load query extensions; kind auto-detected, e.g. .js = JavaScript)")
		return
	}
	names, err := loadExtensions(arg)
	if len(names) > 0 {
		fmt.Fprintf(c.stderr, "registered extension function(s): %s\n", strings.Join(names, ", "))
	}
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .ext: %v\n", c.prog, err)
	}
}
