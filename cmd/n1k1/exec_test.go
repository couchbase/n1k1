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

func TestOrderedJSONRow(t *testing.T) {
	// Keys keep the given order (not Go map order), so the box renderer's columns
	// stay in declaration order.
	got := string(orderedJSONRow(
		[2]interface{}{"zeta", 1},
		[2]interface{}{"alpha", "x"},
	))
	if got != `{"zeta":1,"alpha":"x"}` {
		t.Errorf("orderedJSONRow order = %s", got)
	}

	// A nil value is omitted entirely (so a column absent from every row vanishes).
	got = string(orderedJSONRow(
		[2]interface{}{"a", nil},
		[2]interface{}{"b", 5},
		[2]interface{}{"c", nil},
	))
	if got != `{"b":5}` {
		t.Errorf("orderedJSONRow nil-omit = %s", got)
	}

	if got := string(orderedJSONRow()); got != `{}` {
		t.Errorf("orderedJSONRow() = %s, want {}", got)
	}
}
