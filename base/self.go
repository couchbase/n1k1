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

import "encoding/json"

// SelfKeyToken returns the JSON object-key token for key -- the JSON-encoded
// key string followed by ':' (e.g. "sales" -> `"sales":`). It is computed once
// at expression setup time (off the hot path), so the per-row assembly in
// ValsSelfObject only appends precomputed bytes.
func SelfKeyToken(key string) []byte {
	kb, _ := json.Marshal(key)
	return append(kb, ':')
}

// ValsSelfObject assembles a JSON object from selected vals into out, returning
// the extended out. For each i, keyTokens[i] (a `"key":` token from
// SelfKeyToken) is appended followed by vals[indices[i]], in order and WITHOUT
// sorting keys. A selected val that is empty (ValMissing) is omitted entirely,
// mirroring ConvertVals.Convert's UnsetField-on-empty (a MISSING field does not
// appear in the object). indices[i] must be a valid index into vals.
//
// This is the non-boxing byte path for a whole-row `self` / SELECT *
// projection: it builds the row object straight from label bytes, skipping the
// Convert(box) -> Self -> WriteJSON round-trip and its per-row garbage. Key
// order is the projection's label order rather than cbq's sorted-key order;
// that is a byte-level (not value-level) difference -- re-parsing yields the
// same value, and n1k1's result comparison is key-order-insensitive (see the
// test harness canonJSON / rowsMatch).
func ValsSelfObject(keyTokens [][]byte, indices []int, vals Vals, out []byte) []byte {
	out = append(out, '{')

	first := true

	for i, idx := range indices {
		v := vals[idx]
		if len(v) == 0 {
			continue // MISSING field -> omit key (matches Convert's UnsetField).
		}

		if !first {
			out = append(out, ',')
		}
		first = false

		out = append(out, keyTokens[i]...)
		out = append(out, v...)
	}

	out = append(out, '}')

	return out
}
