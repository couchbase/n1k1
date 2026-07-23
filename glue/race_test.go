//go:build n1ql && race

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

package glue

import "testing"

// skipZeroAllocUnderRace skips a testing.AllocsPerRun-based "0 allocs/row"
// assertion when the binary is built with -race: the race detector's shadow-
// memory instrumentation adds allocations to almost any code, so a strict
// zero-alloc expectation cannot hold under it (this is the -race build; the
// assertions are still exercised in a normal build). See the !race twin.
func skipZeroAllocUnderRace(t *testing.T) {
	t.Helper()
	t.Skip("testing.AllocsPerRun zero-alloc assertion doesn't hold under -race instrumentation")
}

// raceEnabled reports whether the binary was built with -race.
const raceEnabled = true
