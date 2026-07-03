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

// TestStageBatchPoolDelegation: when Ctx wires AllocBatch/RecycleBatch, the Stage
// uses that request-scoped pool (so batches survive Stage teardown); otherwise it
// falls back to the per-instance Recycled list.
func TestStageBatchPoolDelegation(t *testing.T) {
	// Request-scoped pool shared across (throwaway) Stage instances.
	var pool [][]Vals
	ctx := &Ctx{
		AllocBatch: func() []Vals {
			if n := len(pool); n > 0 {
				rv := pool[n-1]
				pool = pool[:n-1]
				return rv
			}
			return nil
		},
		RecycleBatch: func(b []Vals) { pool = append(pool, b) },
	}
	vars := &Vars{Ctx: ctx}

	// A batch recycled by one Stage instance is reused by the next -- the whole
	// point (the per-instance Recycled would lose it when the Stage is discarded).
	s1 := &Stage{Vars: vars}
	b := make([]Vals, 0, 4)
	s1.RecycleBatch(b)
	if len(pool) != 1 {
		t.Fatalf("RecycleBatch did not reach the Ctx pool: pool=%d", len(pool))
	}
	s2 := &Stage{Vars: vars} // a *different* instance
	if got := s2.AcquireBatch(); got == nil || cap(got) != cap(b) {
		t.Fatalf("AcquireBatch did not reuse the pooled batch across instances")
	}
	if len(pool) != 0 {
		t.Fatalf("AcquireBatch did not take from the pool: pool=%d", len(pool))
	}

	// No Ctx pool wired -> fall back to the per-instance Recycled.
	s3 := &Stage{}
	s3.RecycleBatch(make([]Vals, 0, 2))
	if len(s3.Recycled) != 1 {
		t.Fatalf("fallback: expected per-instance Recycled, got %d", len(s3.Recycled))
	}
	if s3.AcquireBatch() == nil {
		t.Fatalf("fallback: AcquireBatch should return the per-instance batch")
	}
}
