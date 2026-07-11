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

package glue

// Shared-scan cache for correlation detectors -- the EXECUTION half of Part B of the
// shared sorted-stream substrate (DESIGN-mqo-sorted.md). K temporal-correlation detectors
// over the same keyspaces each scan (and DECODE / re-extract) those keyspaces separately.
// corpusScanCache captures each correlation keyspace's scan once into a spillable heap and
// replays it for every later scan with the SAME key -- across the group's detectors -- so
// the expensive record extraction (gzip / multiline / regex) happens once per (keyspace,
// scan-shape) per corpus run.
//
// It is a base.DatastorePipe installed on the session for the corpus run (reaching the
// standalone detectors' own s.Run scans, since PlanExec propagates s.Pipe): a
// datastore-scan-records of a known correlation keyspace is served from the cache;
// everything else delegates to the underlying provider unchanged, so it is transparent to
// WireASOFJoin and to non-correlation detectors. The cache key is the keyspace QN plus a
// faithful serialization of every scan pushdown (see scanCacheKey), so two scans share a
// heap ONLY when they yield identical rows: a FULL scan (build side of K merges) shares by
// QN; a project-columns scan (driving side EarlyProjection) shares among detectors that
// project it identically; an unrecognized pushdown is not cached. The per-scan capture is
// byte-BUDGETED (CorpusScanCacheBudgetBytes): a keyspace larger than the budget is
// abandoned (partial heap freed, re-scanned thereafter) rather than mirrored to disk in
// full. Differential-tested: findings are identical to running each detector standalone.

import (
	"path/filepath"
	"strings"

	"github.com/couchbase/rhmap/store"

	"github.com/couchbase/n1k1/base"
)

// newCorpusScanCache builds a shared-scan cache over the given keyspace QNs, spilling
// under dir, delegating uncached ops to inner (nil -> the file datastore).
func newCorpusScanCache(qns map[string]bool, dir string, inner base.DatastorePipe) *corpusScanCache {
	return &corpusScanCache{
		sharedQNs: qns,
		captured:  map[string]*store.Heap{},
		tooBig:    map[string]bool{},
		dir:       dir,
		inner:     inner,
		budget:    CorpusScanCacheBudgetBytes,
	}
}

// correlationKeyspaceQNs returns the set of keyspace names the correlation groups read
// (both sides of each signature = the first two "\x00"-separated fields; see
// analyzeCorrelationDetector). Caching applies to every correlation keyspace -- a group
// of >1 shares across detectors, and a single correlated subquery still re-scans its
// inner keyspace once per outer row, which the cache collapses to one.
func correlationKeyspaceQNs(groups map[string][]string) map[string]bool {
	if len(groups) == 0 {
		return nil
	}
	out := map[string]bool{}
	for sig := range groups {
		parts := strings.Split(sig, "\x00")
		if len(parts) >= 2 {
			out[parts[0]] = true
			out[parts[1]] = true
		}
	}
	return out
}

// CorpusScanCacheBudgetBytes caps how many bytes the shared-scan cache will spill for ONE
// cached scan. A keyspace whose capture exceeds this is abandoned (its partial heap freed,
// future scans re-scanned) rather than mirrored to disk in full -- so a multi-GB keyspace
// degrades to the no-sharing baseline instead of a giant spill. The heap spills to disk,
// so this bounds disk (not RAM); raise it to trade disk for avoided re-decode on large
// shared keyspaces. Package var so a caller can tune it before a corpus run.
var CorpusScanCacheBudgetBytes int64 = 256 << 20 // 256 MiB per cached scan.

// corpusScanCache is a caching DatastorePipe. It is single-goroutine within a corpus run
// (the standalone detectors run sequentially), so the maps need no locking.
type corpusScanCache struct {
	sharedQNs map[string]bool        // keyspace QNs to cache (correlation keyspaces)
	captured  map[string]*store.Heap // scan-key -> captured rows (lazy, corpus-dir backed)
	tooBig    map[string]bool        // scan-keys whose capture blew the budget (don't retry)
	dir       string                 // corpus-scoped spill dir (outlives per-detector Runs)
	inner     base.DatastorePipe     // underlying provider (nil -> the file datastore)
	budget    int64                  // max bytes to spill per cached scan
	seq       int                    // distinct heap path suffix

	captures  int // # scans captured (test observability)
	replays   int // # scans served from the cache (test observability)
	abandoned int // # scans abandoned over budget (test observability)
}

// Op serves one datastore leaf op: a cacheable full correlation-keyspace scan from the
// cache (capturing on first access), else delegates unchanged.
func (c *corpusScanCache) Op(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	key, ok := c.scanCacheKey(o, vars)
	if !ok || c.tooBig[key] {
		c.delegate(o, vars, yieldVals, yieldErr, path, pathNext) // uncacheable, or known over-budget.
		return
	}

	if h := c.captured[key]; h != nil {
		c.replays++
		replayHeap(h, yieldVals, yieldErr)
		return
	}

	// First access: capture the scan into a corpus-scoped heap WHILE serving this caller
	// (so the first scan isn't wasted). The heap spills to disk, so RAM is bounded; it
	// lives under the corpus dir, not the per-detector Run's tmpDir (which is removed when
	// that Run ends), so later Runs can replay it. If the capture exceeds the byte budget
	// we ABANDON it (free the partial heap, poison the key) and keep serving -- so a huge
	// keyspace falls back to re-scanning instead of a giant spill.
	c.seq++
	heap := c.newHeap(c.seq)
	var buf []byte
	var capErr error
	var capBytes int64
	over := false
	capYield := func(vals base.Vals) {
		if capErr == nil && !over {
			buf = base.ValsEncode(vals, buf[:0])
			capBytes += int64(len(buf))
			if capBytes > c.budget {
				over = true
				c.tooBig[key] = true
				c.abandoned++
				heap.Close() // drop the partial spill immediately.
			} else if e := heap.PushBytes(buf); e != nil {
				capErr = e
			}
		}
		yieldVals(vals)
	}
	c.delegate(o, vars, capYield, yieldErr, path, pathNext)
	if capErr == nil && !over {
		c.captured[key] = heap
		c.captures++
	} else if capErr != nil {
		heap.Close()
	}
}

// scanCacheKey returns the cache key for a records-scan of a shared correlation keyspace,
// or ok=false to bypass. The key is the keyspace QN PLUS a faithful serialization of every
// scan pushdown, so two scans share a heap ONLY when they yield identical rows. A FULL scan
// (keyspacer index only) keys on the QN alone -- so the build side of K merges shares. A
// scan carrying a project-columns pushdown (EarlyProjection on the driving side) keys on
// the QN + those columns -- so the driving side shares across detectors that project it the
// same way. Any UNRECOGNIZED pushdown -> ok=false (its rows might differ in a way the key
// wouldn't capture; correctness beats sharing).
func (c *corpusScanCache) scanCacheKey(o *base.Op, vars *base.Vars) (string, bool) {
	if o.Kind != "datastore-scan-records" || len(o.Params) == 0 {
		return "", false
	}
	qn := scanCacheQN(o, vars)
	if qn == "" || !c.sharedQNs[qn] {
		return "", false
	}
	var b strings.Builder
	b.WriteString(qn)
	for _, p := range o.Params[1:] {
		pp, ok := p.([]interface{})
		if !ok || len(pp) != 2 {
			return "", false // unknown pushdown shape.
		}
		k, _ := pp[0].(string)
		names, ok := pp[1].([]string)
		if k != "project-columns" || !ok {
			return "", false // only projection pushdown is key-serializable so far.
		}
		b.WriteString("\x00pc")
		for _, n := range names {
			b.WriteByte(0)
			b.WriteString(n)
		}
	}
	return b.String(), true
}

// delegate runs the op via the underlying provider (the wrapped pipe, or the file
// datastore) WITHOUT re-entering this cache -- DatastoreDispatch skips the Ctx.Pipe check.
func (c *corpusScanCache) delegate(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	if c.inner != nil {
		c.inner.Op(o, vars, yieldVals, yieldErr, path, pathNext)
		return
	}
	DatastoreDispatch(o, vars, yieldVals, yieldErr, path, pathNext)
}

// newHeap builds an append-only, order-preserving, spillable heap under the corpus dir
// (mirrors MakeVars' AllocHeap construction; PushBytes/Get(i) give insertion order --
// the same "appendable sequence" use as OpTempCapture).
func (c *corpusScanCache) newHeap(seq int) *store.Heap {
	prefix := filepath.Join(c.dir, "scancache", itoaCache(seq))
	return &store.Heap{
		Heap: &store.Chunks{PathPrefix: prefix, FileSuffix: ".heap", ChunkSizeBytes: 1024 * 1024},
		Data: &store.Chunks{PathPrefix: prefix, FileSuffix: ".data", ChunkSizeBytes: 16 * 1024 * 1024},
	}
}

// replayHeap yields the captured rows in insertion (scan) order, decoding into a reused
// buffer (the same borrowed-slice contract as OpTempYield).
func replayHeap(h *store.Heap, yieldVals base.YieldVals, yieldErr base.YieldErr) {
	var vals base.Vals
	for i := int64(0); i < h.CurItems; i++ {
		b, err := h.Get(i)
		if err != nil {
			yieldErr(err)
			return
		}
		vals = base.ValsDecode(b, vals[:0])
		yieldVals(vals)
	}
}

// scanCacheQN resolves the qualified keyspace name a records-scan op reads, or "".
func scanCacheQN(o *base.Op, vars *base.Vars) string {
	idx, ok := o.Params[0].(int)
	if !ok || idx < 0 || idx >= len(vars.Temps) {
		return ""
	}
	ks, ok := vars.Temps[idx].(keyspacer)
	if !ok {
		return ""
	}
	if k := ks.Keyspace(); k != nil {
		return k.QualifiedName()
	}
	return ""
}

// itoaCache is a tiny int->string for heap path suffixes (avoids strconv on a cold path).
func itoaCache(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
