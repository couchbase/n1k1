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
