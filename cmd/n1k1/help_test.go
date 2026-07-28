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
	"bytes"
	"testing"

	"github.com/couchbase/n1k1/cmd"
)

// TestCommandHelpTwoWays: a command guide is reachable two ways -- ".<command> help"
// prints the SAME text as ".help <topic>", so there's one source of truth per topic.
func TestCommandHelpTwoWays(t *testing.T) {
	cases := []struct{ cmdForm, topic string }{
		{".extensions help", "extensions"},
		{".ext help", "extensions"},
		{".keyspaces help", "keyspaces"},
		{".tables help", "keyspaces"},
		{".meta help", "meta"},
		{".macro help", "macro"},
		{".extract help", "extract"},
	}
	for _, tc := range cases {
		// One buffer for both streams, so it's agnostic to which stream a given guide
		// writes to (concept helps -> stderr, some command guides -> stdout).
		var viaCmd, viaHelp bytes.Buffer
		(&cli{prog: "n1k1", out: &viaCmd, stderr: &viaCmd, style: cmd.Style{}}).dot(tc.cmdForm)
		(&cli{prog: "n1k1", out: &viaHelp, stderr: &viaHelp, style: cmd.Style{}}).cmdHelp(tc.topic)
		if viaCmd.Len() == 0 {
			t.Errorf("%q produced no output", tc.cmdForm)
		}
		if viaCmd.String() != viaHelp.String() {
			t.Errorf("%q != .help %s\n-- via command:\n%s\n-- via .help:\n%s",
				tc.cmdForm, tc.topic, viaCmd.String(), viaHelp.String())
		}
	}
}
