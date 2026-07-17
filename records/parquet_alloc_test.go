//go:build !js

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

package records

import (
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func aligned64(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	return uintptr(unsafe.Pointer(unsafe.SliceData(b)))%64 == 0
}

func TestPoolAllocatorBasics(t *testing.T) {
	p := &poolAllocator{base: memory.DefaultAllocator}

	// Allocate returns exactly `size` bytes, 64-byte aligned (arrow's contract).
	for _, size := range []int{1, 63, 64, 65, 1000, 1 << 20} {
		b := p.Allocate(size)
		if len(b) != size {
			t.Fatalf("Allocate(%d): len=%d", size, len(b))
		}
		if !aligned64(b) {
			t.Fatalf("Allocate(%d): not 64-byte aligned", size)
		}
		p.Free(b)
	}

	// Free then re-Allocate in the same class reuses the SAME backing buffer, and it
	// comes back zeroed (arrow relies on make()-style zeroed memory for bitmaps).
	b1 := p.Allocate(1000)
	for i := range b1 {
		b1[i] = 0xFF
	}
	d1 := unsafe.SliceData(b1)
	p.Free(b1)
	b2 := p.Allocate(1000) // same class (1024) -> should recycle b1's buffer
	if unsafe.SliceData(b2) != d1 {
		t.Errorf("expected Free+Allocate to recycle the same buffer")
	}
	for i, c := range b2 {
		if c != 0 {
			t.Fatalf("recycled buffer not zeroed at %d: %#x", i, c)
		}
	}
	p.Free(b2)
}

func TestPoolAllocatorReallocate(t *testing.T) {
	p := &poolAllocator{base: memory.DefaultAllocator}

	// Grow beyond capacity: contents preserved, new region long enough, still aligned.
	b := p.Allocate(100)
	for i := range b {
		b[i] = byte(i)
	}
	b = p.Reallocate(5000, b)
	if len(b) != 5000 {
		t.Fatalf("Reallocate len=%d, want 5000", len(b))
	}
	if !aligned64(b) {
		t.Fatalf("Reallocate: not 64-byte aligned")
	}
	for i := 0; i < 100; i++ {
		if b[i] != byte(i) {
			t.Fatalf("Reallocate lost data at %d: %d", i, b[i])
		}
	}
	// Shrink within capacity: no move.
	d := unsafe.SliceData(b)
	b = p.Reallocate(200, b)
	if unsafe.SliceData(b) != d || len(b) != 200 {
		t.Fatalf("Reallocate shrink should reuse in place")
	}
	p.Free(b)
}

func TestPoolAllocatorClassOf(t *testing.T) {
	cases := []struct {
		size, want int
	}{
		{1, minPoolClass}, {64, minPoolClass}, {65, 7}, {128, 7}, {129, 8},
		{1 << 26, 26}, {(1 << 26) + 1, maxPoolClass + 1}, // just over the top class -> unpooled
	}
	for _, c := range cases {
		if got := classOf(c.size); got != c.want {
			t.Errorf("classOf(%d) = %d, want %d", c.size, got, c.want)
		}
	}
}

func TestPoolAllocatorOversizeFallsBack(t *testing.T) {
	p := &poolAllocator{base: memory.DefaultAllocator}
	// Above the top class: served by the base allocator, still correct len + aligned, and
	// Free must not panic (dropped to GC, not pooled).
	big := 1<<26 + 1
	b := p.Allocate(big)
	if len(b) != big {
		t.Fatalf("oversize Allocate len=%d, want %d", len(b), big)
	}
	if !aligned64(b) {
		t.Fatalf("oversize Allocate not aligned")
	}
	p.Free(b) // must not panic nor pool
}
