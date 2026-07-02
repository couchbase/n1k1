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

// TestJsonType covers the type-name mapping .schema uses to describe fields
// (the value shapes come from encoding/json's decode of a JSON document).
func TestJsonType(t *testing.T) {
	cases := []struct {
		v    interface{}
		want string
	}{
		{nil, "null"},
		{true, "bool"},
		{float64(3), "number"},
		{"s", "string"},
		{[]interface{}{1, 2}, "array"},
		{map[string]interface{}{"a": 1}, "object"},
	}
	for _, tc := range cases {
		if got := jsonType(tc.v); got != tc.want {
			t.Errorf("jsonType(%T) = %q, want %q", tc.v, got, tc.want)
		}
	}
}
