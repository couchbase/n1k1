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

import "testing"

// TestSplitSourceArg: `name=path` is split only when the name is a plausible bare
// identifier; an `=` inside a path or pattern is left as part of the path.
func TestSplitSourceArg(t *testing.T) {
	cases := []struct {
		arg, name, path string
	}{
		{"drive=~/Drive/**", "drive", "~/Drive/**"},
		{"docs=/a/b", "docs", "/a/b"},
		{"/plain/path", "", "/plain/path"},
		{"./rel/dir", "", "./rel/dir"},
		{"data/*.csv", "", "data/*.csv"},       // glob, no name
		{"a/b=c", "", "a/b=c"},                 // '=' after a separator -> part of path
		{"pat*=x", "", "pat*=x"},               // glob metachar in name candidate -> not a name
		{"s3://bucket/t", "", "s3://bucket/t"}, // ':' is not '='
		{"=/leading/eq", "", "=/leading/eq"},   // empty name -> whole thing is the path
	}
	for _, tc := range cases {
		name, path := splitSourceArg(tc.arg)
		if name != tc.name || path != tc.path {
			t.Errorf("splitSourceArg(%q) = (%q,%q), want (%q,%q)", tc.arg, name, path, tc.name, tc.path)
		}
	}
}

// TestParseSourceArgs: one bare path stays classic (multi=false); 2+ args or any
// single name=path is multi-source.
func TestParseSourceArgs(t *testing.T) {
	if _, multi := parseSourceArgs(nil); multi {
		t.Error("no args -> not multi")
	}
	if _, multi := parseSourceArgs([]string{"./data"}); multi {
		t.Error("one bare path -> classic single source, not multi")
	}
	if srcs, multi := parseSourceArgs([]string{"docs=~/Documents"}); !multi || len(srcs) != 1 ||
		srcs[0].Name != "docs" || srcs[0].Path != "~/Documents" {
		t.Errorf("single name=path -> multi with one named source; got multi=%v %+v", multi, srcs)
	}
	srcs, multi := parseSourceArgs([]string{"a=~/x", "/y", "z/*.json"})
	if !multi || len(srcs) != 3 {
		t.Fatalf("three args -> multi with 3 sources; got multi=%v %+v", multi, srcs)
	}
	if srcs[0].Name != "a" || srcs[1].Name != "" || srcs[2].Name != "" {
		t.Errorf("names = %q/%q/%q, want a / (derived) / (derived)", srcs[0].Name, srcs[1].Name, srcs[2].Name)
	}
}
