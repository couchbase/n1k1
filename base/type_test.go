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

package base

import "testing"

// TestTypeNameVal pins TYPE_NAME's byte-lane logic: the JSON type name of a value,
// NAMING missing/null rather than propagating them, matching cbq value.Type.String().
func TestTypeNameVal(t *testing.T) {
	cases := []struct {
		in   Val
		want string
	}{
		{ValMissing, `"missing"`},
		{Val(nil), `"missing"`},
		{ValNull, `"null"`},
		{Val(`null`), `"null"`},
		{Val(`true`), `"boolean"`},
		{Val(`false`), `"boolean"`},
		{Val(`5`), `"number"`},
		{Val(`-3.14`), `"number"`},
		{Val(`"hi"`), `"string"`},
		{Val(`[1,2]`), `"array"`},
		{Val(`{"k":1}`), `"object"`},
	}
	for _, c := range cases {
		if got := string(TypeNameVal(c.in)); got != c.want {
			t.Errorf("TypeNameVal(%s): got %s, want %s", c.in, got, c.want)
		}
	}
}
