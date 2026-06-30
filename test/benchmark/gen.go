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

// Package benchmark validates the performance techniques claimed in DESIGN.md
// -- garbage avoidance, push-based per-row cost, static-param expr opt, and
// rhmap spill-to-disk -- via Go benchmarks over synthetic local data. See
// README.md and ../../DESIGN-benchmark.md.
package benchmark

import (
	"strconv"
	"strings"
)

// GenJSONs returns a "jsonsData" payload: nDocs corpus-like "contact" JSON
// documents, one per line. The "g" group-key field cycles through `distinct`
// distinct values (so a GROUP BY g yields ~min(distinct, nDocs) groups); the
// other fields vary by index. Deterministic, for reproducible runs.
//
// Shape mirrors the suite corpus's contacts (name/type/gender/age/city/hobbies)
// plus a "g" field used as a tunable-cardinality grouping key.
func GenJSONs(nDocs, distinct int) string {
	if distinct < 1 {
		distinct = 1
	}

	genders := [2]string{"m", "f"}
	hobbies := [3]string{`["golf","surf"]`, `["reading"]`, `["chess","run","cook"]`}
	const cities = 50

	var b strings.Builder
	b.Grow(nDocs * 96) // ~96 bytes/doc.

	for i := 0; i < nDocs; i++ {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(`{"g":"k`)
		b.WriteString(strconv.Itoa(i % distinct))
		b.WriteString(`","name":"n`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`","type":"contact","gender":"`)
		b.WriteString(genders[i%2])
		b.WriteString(`","age":`)
		b.WriteString(strconv.Itoa(18 + i%60))
		b.WriteString(`,"city":"c`)
		b.WriteString(strconv.Itoa(i % cities))
		b.WriteString(`","hobbies":`)
		b.WriteString(hobbies[i%3])
		b.WriteByte('}')
	}

	return b.String()
}
