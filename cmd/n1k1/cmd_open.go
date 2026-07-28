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

	"github.com/couchbase/n1k1/glue"
)

func (c *cli) cmdOpen(dir string) {
	if dir == "" {
		fmt.Fprintln(c.stderr, "usage: .open <dir>")
		return
	}
	sess, err := glue.OpenSession(dir, defaultNamespace)
	if err != nil {
		fmt.Fprintf(c.stderr, "cannot open %q: %v\n", dir, err)
		return
	}
	c.sess.Close() // release the previous datastore's TEMP KEYSPACE spill files
	c.sess, c.dir = sess, dir
	c.eagerBuildIndexes() // re-apply -index=eager to the newly opened datastore
	fmt.Fprintf(c.stderr, "opened %s\n", dir)
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
