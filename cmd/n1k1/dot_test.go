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
)

// TestDotVerbose exercises the .verbose level command: named levels
// (off/on/debug), a bare numeric level, no-arg (show only), and a bad arg.
func TestDotVerbose(t *testing.T) {
	cases := []struct {
		start int    // c.verbose before the command
		line  string // the dot line
		want  int    // c.verbose after
		out   string // substring expected on stderr
	}{
		{0, ".verbose on", 1, "on"},
		{0, ".verbose debug", 2, "debug"},
		{2, ".verbose off", 0, "off"},
		{0, ".verbose 3", 3, "debug"},    // numeric level >=2 reads as debug
		{1, ".verbose", 1, "on"},         // no arg: show only, unchanged
		{1, ".verbose nope", 1, "usage"}, // bad arg: unchanged, prints usage
	}
	for _, tc := range cases {
		var errb strings.Builder
		c := &cli{verbose: tc.start, stderr: &errb}
		if quit := c.dot(tc.line); quit {
			t.Errorf("%q returned quit=true", tc.line)
		}
		if c.verbose != tc.want {
			t.Errorf("%q: verbose = %d, want %d", tc.line, c.verbose, tc.want)
		}
		if !strings.Contains(errb.String(), tc.out) {
			t.Errorf("%q: output %q missing %q", tc.line, errb.String(), tc.out)
		}
	}
}
