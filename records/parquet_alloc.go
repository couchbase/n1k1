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
	"math/bits"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// poolAllocator is a size-classed, GC-cooperative arrow memory.Allocator that recycles
// the record-batch buffers arrow frees on batch.Release(). memory.DefaultAllocator
// (GoAllocator) just make()s a fresh buffer per allocation and drops it to GC on Free --
// so a multi-batch Parquet scan re-allocates the FULL column decode buffers for every
// 8192-row batch (measured ~51% of a lone-VARIANT scan's alloc_space). Since n1k1 copies
// each batch out (borrowVariantRows / NDJSON render) or holds exactly one batch at a time
// (NextColumns) before the next Read(), the freed buffers are genuinely dead and can be
// handed straight back to the next batch's Read().
//
// Buffers are bucketed into power-of-two size classes backed by sync.Pool, so the free
// lists shrink automatically under GC pressure -- the pool is a bounded arena (peak ≈
// one batch's live buffers), not an unbounded cache. Allocations larger than the top
// class fall through to the base GoAllocator (rare, and not worth a giant pooled bucket).
type poolAllocator struct {
	base   memory.Allocator
	pools  [maxPoolClass + 1]sync.Pool // pools[c] holds *[]byte with cap == 1<<c
}

const (
	minPoolClass = 6  // 64 bytes == arrow's 64-byte buffer alignment
	maxPoolClass = 26 // 64 MiB; above this, defer to the base allocator
)

// arrowAlloc is the process-shared pooled allocator wired into every Parquet reader.
// One shared instance lets buffers freed by one scan feed another (many small files, or
// a re-scanned file); sync.Pool's per-P sharding keeps concurrent scans mostly lock-free,
// and its GC hook bounds total retention.
var arrowAlloc = &poolAllocator{base: memory.DefaultAllocator}

// classOf returns the smallest size class (a power-of-two exponent) whose 1<<class is
// >= size, clamped at the minimum. It returns maxPoolClass+1 to signal "too big to pool".
func classOf(size int) int {
	if size <= 1<<minPoolClass {
		return minPoolClass
	}
	c := bits.Len(uint(size - 1)) // ceil(log2(size))
	if c > maxPoolClass {
		return maxPoolClass + 1
	}
	return c
}

func (p *poolAllocator) Allocate(size int) []byte {
	if size == 0 {
		return []byte{}
	}
	c := classOf(size)
	if c > maxPoolClass {
		return p.base.Allocate(size)
	}
	if v := p.pools[c].Get(); v != nil {
		b := *(v.(*[]byte)) // cap == 1<<c, 64-byte-aligned start (preserved across reuse)
		b = b[:size]
		// arrow's GoAllocator hands back make()'d (zeroed) memory, and arrow relies on it:
		// e.g. a validity bitmap only has its set bits written, the rest assumed 0. Recycled
		// buffers carry stale bytes, so re-zero to preserve that contract. (Still far cheaper
		// than make + GC: a memclr, no allocation.)
		clear(b)
		return b
	}
	return alignedMake(1<<c)[:size]
}

func (p *poolAllocator) Reallocate(size int, b []byte) []byte {
	if cap(b) >= size {
		return b[:size]
	}
	nb := p.Allocate(size)
	copy(nb, b)
	p.Free(b)
	return nb
}

func (p *poolAllocator) Free(b []byte) {
	c := cap(b)
	if c == 0 {
		return
	}
	// Pool only buffers whose capacity is exactly one of our classes -- i.e. buffers this
	// allocator produced. bits.TrailingZeros of a power of two is its exponent; a non-power
	// (or over-size) capacity means it came from the base allocator, so drop it to GC.
	exp := bits.TrailingZeros(uint(c))
	if exp < minPoolClass || exp > maxPoolClass || 1<<exp != c {
		return
	}
	full := b[:c:c]
	p.pools[exp].Put(&full)
}

// alignedMake returns a fresh []byte of len==cap==n whose first byte is 64-byte aligned,
// matching arrow's GoAllocator (arrow assumes 64-byte-aligned buffers for its SIMD-ish
// kernels). Over-allocate by 64 and shift to the next 64-byte boundary.
func alignedMake(n int) []byte {
	buf := make([]byte, n+64)
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	shift := int((64 - addr%64) % 64)
	return buf[shift : shift+n : shift+n]
}

var _ memory.Allocator = (*poolAllocator)(nil)
