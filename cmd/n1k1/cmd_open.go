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

// cmd_open.go holds the datastore/output dot-command bodies: .open (switch datastore)
// and .output (redirect results to a file). Invoked by the registry in cmd_registry.go.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// cmdOpen switches the datastore. `.open <dir>` opens one root (classic); `.open
// <src>...` (2+ whitespace-separated paths, or any name=path) opens MULTIPLE data
// sources, each a keyspace; `.open @<file>` loads sources from a JSON/YAML/TOML config
// (DESIGN-data.md §2, glue.OpenSessionSources / LoadSources).
// ⚠ REPL word-splitting is on whitespace, so a source PATH containing spaces can't be
// given inline -- that (and richer per-source options) is what the @<file> config is for.
func (c *cli) cmdOpen(arg string) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		fmt.Fprintln(c.stderr, "usage: .open <dir>  |  .open <src>... (name=path/bare)  |  .open @<config.yaml>")
		return
	}

	var sess *glue.Session
	var err error
	var label string
	multi := false
	switch {
	case strings.HasPrefix(arg, "@"): // config file of sources
		file := strings.TrimSpace(arg[1:])
		var sources []glue.Source
		if sources, err = glue.LoadSources(file); err == nil {
			sess, err = glue.OpenSessionSources(sources, defaultNamespace)
		}
		multi, label = true, fmt.Sprintf("%d data sources from %s: %s", len(sources), file, strings.Join(sourceNames(sources), ", "))
	default:
		sources, isMulti := parseSourceArgs(strings.Fields(arg))
		if isMulti {
			multi = true
			sess, err = glue.OpenSessionSources(sources, defaultNamespace)
			label = fmt.Sprintf("%d data sources: %s", len(sources), strings.Join(sourceNames(sources), ", "))
		} else {
			sess, err = glue.OpenSession(arg, defaultNamespace)
			label = arg
		}
	}
	if err != nil {
		fmt.Fprintf(c.stderr, "cannot open %s: %v\n", label, err)
		return
	}

	c.sess.Close() // release the previous datastore's TEMP KEYSPACE spill files
	c.sess = sess
	if multi {
		c.dir = "" // no single root; the keyspaces are the named sources
	} else {
		c.dir = arg
	}
	c.eagerBuildIndexes() // re-apply -index=eager to the newly opened datastore
	fmt.Fprintf(c.stderr, "opened %s\n", label)
}

func (c *cli) cmdOutput(path string) {
	if c.outFile != nil {
		c.outFile.Close()
		c.outFile = nil
	}
	if path == "" {
		c.out = os.Stdout
		c.style.On = c.fancyTTY // restore styling for the terminal
		return
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(c.stderr, "cannot create %q: %v\n", path, err)
		return
	}
	c.outFile, c.out = f, f
	c.style.On = false // never write ANSI codes to a file
}
