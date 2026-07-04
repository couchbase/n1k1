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

func TestSplitFirst(t *testing.T) {
	cases := []struct {
		in, head, tail string
	}{
		{"foo bar baz", "foo", "bar baz"},
		{"  foo   bar  ", "foo", "bar"}, // leading/trailing space trimmed
		{"foo", "foo", ""},
		{"", "", ""},
		{".index\tsuggest", ".index", "suggest"}, // splits on tab too
	}
	for _, tc := range cases {
		h, tl := splitFirst(tc.in)
		if h != tc.head || tl != tc.tail {
			t.Errorf("splitFirst(%q) = (%q,%q), want (%q,%q)", tc.in, h, tl, tc.head, tc.tail)
		}
	}
}

func TestOnOff(t *testing.T) {
	if onOff(true) != "on" || onOff(false) != "off" {
		t.Errorf("onOff: got %q/%q", onOff(true), onOff(false))
	}
}

func TestVerboseLevelSet(t *testing.T) {
	cases := []struct {
		start   int    // level before Set
		arg     string // value passed to Set
		want    int    // level after
		wantErr bool
	}{
		{0, "true", 1, false},  // bare -v raises by one
		{1, "true", 2, false},  // repeat accumulates
		{0, "on", 1, false},    //
		{5, "off", 0, false},   //
		{0, "debug", 2, false}, //
		{0, "3", 3, false},     // numeric level
		{2, "DEBUG", 2, false}, // case-insensitive
		{0, "false", 0, false}, //
		{1, "nope", 1, true},   // bad -> error, level unchanged
		{1, "-1", 1, true},     // negative -> error
	}
	for _, tc := range cases {
		v := verboseLevel(tc.start)
		err := v.Set(tc.arg)
		if (err != nil) != tc.wantErr {
			t.Errorf("Set(%q) err=%v, wantErr=%v", tc.arg, err, tc.wantErr)
		}
		if int(v) != tc.want {
			t.Errorf("Set(%q) from %d -> %d, want %d", tc.arg, tc.start, int(v), tc.want)
		}
	}
	var vb verboseLevel
	if !vb.IsBoolFlag() {
		t.Error("verboseLevel should be a bool flag")
	}
}

func TestNormalizeVerbose(t *testing.T) {
	cases := []struct {
		in, want []string
	}{
		// space form rewritten to =-form
		{[]string{"-v", "3"}, []string{"-v=3"}},
		{[]string{"-v", "on", "dir"}, []string{"-v=on", "dir"}},
		{[]string{"-verbose", "debug"}, []string{"-verbose=debug"}},
		// bare -v not followed by a level token stays bare
		{[]string{"-v", "dir"}, []string{"-v", "dir"}},
		{[]string{"-v", "-c", "SELECT 1"}, []string{"-v", "-c", "SELECT 1"}},
		// repeats accumulate as bare flags
		{[]string{"-v", "-v", "-v"}, []string{"-v", "-v", "-v"}},
		// already =-form untouched
		{[]string{"-v=2", "dir"}, []string{"-v=2", "dir"}},
		// after "--", tokens pass through verbatim (a dir literally named "3")
		{[]string{"-v", "--", "3"}, []string{"-v", "--", "3"}},
	}
	for _, tc := range cases {
		got := normalizeVerbose(tc.in)
		if strings.Join(got, "\x00") != strings.Join(tc.want, "\x00") {
			t.Errorf("normalizeVerbose(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestVerboseName(t *testing.T) {
	cases := []struct {
		n    int
		want string // substring the name must contain
	}{
		{-1, "off"},
		{0, "off"},
		{1, "on"},
		{2, "debug"},
		{5, "debug"},
	}
	for _, tc := range cases {
		if got := verboseName(tc.n); !strings.Contains(got, tc.want) {
			t.Errorf("verboseName(%d) = %q, want substring %q", tc.n, got, tc.want)
		}
	}
}

func TestNeedsBackticks(t *testing.T) {
	no := []string{"events", "orders", "_meta", "a1", "Foo_Bar99"}
	for _, s := range no {
		if needsBackticks(s) {
			t.Errorf("needsBackticks(%q) = true, want false", s)
		}
	}
	// Empty, leading digit, and any non-identifier rune all force backticks --
	// these are the filesystem-derived keyspace names the flat-root/single-file
	// features routinely produce.
	yes := []string{"", "2026-01", "my-data", "access.log", "with space", "a:b", "1foo"}
	for _, s := range yes {
		if !needsBackticks(s) {
			t.Errorf("needsBackticks(%q) = false, want true", s)
		}
	}
}

func TestQuoteIdent(t *testing.T) {
	cases := map[string]string{
		"events":  "events",    // valid bare identifier, unchanged
		"2026-01": "`2026-01`", // leading digit + hyphen
		"my-data": "`my-data`",
		"a.b":     "`a.b`",
		"a`b":     "`a``b`", // embedded backtick doubled
	}
	for in, want := range cases {
		if got := quoteIdent(in); got != want {
			t.Errorf("quoteIdent(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestExampleForQuotes: generated sample SQL++ backticks a name that needs it.
func TestExampleForQuotes(t *testing.T) {
	if got := exampleFor("2026-01", 0); got != "SELECT COUNT(*) FROM `2026-01`;" {
		t.Errorf("exampleFor(2026-01,0) = %q", got)
	}
	if got := exampleFor("events", 1); got != "SELECT * FROM events LIMIT 3;" {
		t.Errorf("exampleFor(events,1) = %q", got)
	}
}
