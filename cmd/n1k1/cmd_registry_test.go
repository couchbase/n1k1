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
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestBuiltinCmdsUnique: every built-in token (including aliases) resolves to exactly
// one command, so the table can't accidentally shadow itself with a copy-paste dup.
func TestBuiltinCmdsUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, cm := range builtinCmds() {
		names := cm.Names()
		if len(names) == 0 {
			t.Errorf("command with no names: %#v", cm)
		}
		for _, n := range names {
			if !strings.HasPrefix(n, ".") {
				t.Errorf("command token %q must start with '.'", n)
			}
			if seen[n] {
				t.Errorf("duplicate command token %q", n)
			}
			seen[n] = true
		}
	}
	// Spot-check a few tokens (and an alias) all index into the dispatch map.
	_, byName := builtins()
	for _, n := range []string{".help", ".quit", ".exit", ".tables", ".keyspaces", ".indexes"} {
		if byName[n] == nil {
			t.Errorf("built-in %q not indexed for dispatch", n)
		}
	}
}

// TestDotUnknownCommand: an unknown dot-command reports itself and doesn't quit.
func TestDotUnknownCommand(t *testing.T) {
	var errb strings.Builder
	c := &cli{stderr: &errb}
	if quit := c.dot(".nope"); quit {
		t.Error(".nope returned quit=true")
	}
	if !strings.Contains(errb.String(), `unknown command ".nope"`) {
		t.Errorf("unknown-command message missing: %q", errb.String())
	}
}

// fakeCmd is a minimal plugin command for the registration seam test -- the shape a
// future *.cmd.js bridge would implement.
type fakeCmd struct {
	names []string
	ran   *string
	help  string
}

func (f fakeCmd) Names() []string { return f.names }
func (f fakeCmd) Run(c *cli, name, arg string) bool {
	*f.ran = name + " " + arg
	return false
}
func (f fakeCmd) Help(c *cli) string { return f.help }

// TestRegisterCmd exercises the plugin seam that the registry exists to enable: a
// runtime-registered command dispatches, shadows a built-in of the same name, and
// shows up in .help -- exactly as a loaded *.cmd.js would.
func TestRegisterCmd(t *testing.T) {
	var ran string
	var errb strings.Builder
	sess, err := glue.OpenSession(t.TempDir(), defaultNamespace) // printHelp's example needs a session
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	c := &cli{stderr: &errb, sess: sess}

	// A brand-new command dispatches with the exact token + arg.
	c.registerCmd(fakeCmd{names: []string{".hello"}, ran: &ran, help: ".hello                a plugin command"})
	if quit := c.dot(".hello world"); quit {
		t.Error(".hello returned quit=true")
	}
	if ran != ".hello world" {
		t.Errorf("plugin command not run with (name,arg); got %q", ran)
	}

	// Registering an existing token shadows the built-in for this cli.
	ran = ""
	c.registerCmd(fakeCmd{names: []string{".schema"}, ran: &ran, help: ""})
	if quit := c.dot(".schema x"); quit {
		t.Error(".schema override returned quit=true")
	}
	if ran != ".schema x" {
		t.Errorf("plugin did not shadow built-in .schema; got %q", ran)
	}

	// The plugin's help line appears in .help (built-ins still there too).
	errb.Reset()
	c.printHelp()
	out := errb.String()
	if !strings.Contains(out, ".hello                a plugin command") {
		t.Errorf(".help missing the plugin command line:\n%s", out)
	}
	if !strings.Contains(out, ".version") {
		t.Errorf(".help dropped the built-ins:\n%s", out)
	}
}
