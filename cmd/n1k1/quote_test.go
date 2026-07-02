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
