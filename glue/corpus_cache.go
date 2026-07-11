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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/rhmap/store"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

// applyMemEnv lets a run tune the memory behavior without a rebuild:
// N1K1_MERGEJOIN_SPILL_BYTES caps the merge-join's resident build payloads,
// N1K1_SCANCACHE_BUDGET_BYTES caps a single shared-scan capture (both byte counts),
// N1K1_MERGEJOIN_STREAM_ASOF=0 turns off the two-stream ASOF co-advance (fall back to the
// materialized build -- for A/B / debugging).
func applyMemEnv() {
	if v := envBytes("N1K1_MERGEJOIN_SPILL_BYTES"); v >= 0 {
		engine.MergeJoinBuildSpillBytes = v
	}
	if v := envBytes("N1K1_SCANCACHE_BUDGET_BYTES"); v >= 0 {
		CorpusScanCacheBudgetBytes = v
	}
	if s := os.Getenv("N1K1_MERGEJOIN_STREAM_ASOF"); s == "0" || s == "false" {
		engine.MergeJoinStreamASOF = false
	}
}

// envBytes parses an int64 byte count from the env var, or -1 if unset/invalid.
func envBytes(name string) int64 {
	s := os.Getenv(name)
	if s == "" {
		return -1
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// printMemStats writes a one-block memory-behavior summary to stderr after a corpus run
// (gated by N1K1_MEM_STATS): how much the merge-join builds materialized and whether they
// spilled, and what the shared-scan cache captured / replayed / abandoned. This is the
// evidence for whether the build-spill and scan-cache budgets actually fire on a real
// bundle (and thus whether the bounded-band sweep-line is worth pursuing).
func (cc *CompiledCorpus) printMemStats() {
	mb := func(b int64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }
	if m := cc.MergeStats; m != nil {
		fmt.Fprintf(os.Stderr, "mem-stats: merge-join count=%d streamed=%d spilled=%d (budget %s) "+
			"build rows=%d bytes=%s peak-build=%s no-key-skipped=%d\n",
			m.JoinCount.Load(), m.JoinStreamed.Load(), m.JoinSpillCount.Load(),
			mb(engine.MergeJoinBuildSpillBytes), m.BuildRows.Load(), mb(m.BuildBytes.Load()),
			mb(m.BuildBytesPeak.Load()), m.NoKeySkipped.Load())
		fmt.Fprintf(os.Stderr, "mem-stats: merge-scan streamed=%d materialized=%d\n",
			m.ScanStreamed.Load(), m.ScanMaterialized.Load())
	}
	if cc.scanCache != nil {
		c := cc.scanCache
		fmt.Fprintf(os.Stderr, "mem-stats: scan-cache captured=%d (%s) replayed=%d "+
			"skipped-big=%d abandoned=%d (budget %s)\n",
			c.captures, mb(c.capturedBytes), c.replays, c.skippedBig, c.abandoned,
			mb(CorpusScanCacheBudgetBytes))
	} else {
		fmt.Fprintf(os.Stderr, "mem-stats: scan-cache not installed (no correlation groups)\n")
	}
}

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

// correlationKeyspaceQNs returns the keyspaces worth caching: those read by 2+ correlation
// detectors (both sides of each signature = the first two "\x00"-separated fields, counted
// once per detector in the group). Caching a keyspace used by only ONE detector -- a
// per-detector probe, say -- is pure waste: it spills to a heap that is never replayed. So
// only GENUINELY-SHARED keyspaces are cached; the shared build of K correlators is framed
// once, a lone probe is left to stream. (An ASOF-lowered detector scans each side once, so
// cross-detector reuse is the only win; a boxed correlated subquery's inner re-scan never
// reaches this pipe.)
func correlationKeyspaceQNs(groups map[string][]string) map[string]bool {
	if len(groups) == 0 {
		return nil
	}
	uses := map[string]int{} // keyspace -> # detectors reading it.
	for sig, tags := range groups {
		parts := strings.Split(sig, "\x00")
		if len(parts) < 2 {
			continue
		}
		for _, ks := range parts[:2] {
			uses[ks] += len(tags)
		}
	}
	out := map[string]bool{}
	for ks, n := range uses {
		if n >= 2 {
			out[ks] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// CorpusScanCacheBudgetBytes caps how many bytes the shared-scan cache will spill for ONE
// cached scan of a GENUINELY-SHARED keyspace (read by 2+ correlation detectors). NOTE: the
// spill chunks are mmap-backed, so a capture counts toward RSS -- this cache trades MEMORY
// for TIME (frame a shared keyspace once instead of per detector), so raise it only when
// the shared keyspace is big AND reused by enough detectors to pay for the resident
// capture. A keyspace estimated to exceed this is SKIPPED up front (see the size gate in
// Op / keyspaceRawBytes); a keyspace that slips past the estimate but overflows mid-capture
// is abandoned (partial heap freed) as a backstop.
var CorpusScanCacheBudgetBytes int64 = 256 << 20 // 256 MiB per cached scan.

// CorpusScanCacheSizeFactor is the assumed framing+encode EXPANSION of a keyspace's raw
// file bytes into the cache's encoded spill (measured ~1.9x for a real log keyspace;
// varies ~0.8-2.2x). The size gate skips caching when rawBytes*factor > budget, so it errs
// toward NOT spilling a keyspace that would overflow -- a missed share is cheaper than a
// wasted budget-sized spill. Raise it to be more conservative (skip more), lower to try
// caching more aggressively (risking a mid-capture abandon).
var CorpusScanCacheSizeFactor = 2.0

// corpusScanCache is a caching DatastorePipe. The two-stream ASOF merge-join co-advance
// runs its left probe and right build on SEPARATE goroutines, both hitting this shared
// pipe concurrently (a detector's probe + build keyspaces are distinct, so they touch
// distinct map keys but still write the maps/counters at the same time). mu guards all
// shared state; the scan / replay / capture-serving happen OUTSIDE the lock (only the
// short map + counter mutations are under it), so the two scans still run concurrently.
type corpusScanCache struct {
	sharedQNs map[string]bool    // keyspace QNs to cache (correlation keyspaces); read-only after ctor.
	inner     base.DatastorePipe // underlying provider (nil -> the file datastore); read-only.
	dir       string             // corpus-scoped spill dir (outlives per-detector Runs); read-only.
	budget    int64              // max bytes to spill per cached scan; read-only.

	mu       sync.Mutex             // guards everything below.
	captured map[string]*store.Heap // scan-key -> captured rows (lazy, corpus-dir backed).
	tooBig   map[string]bool        // scan-keys whose capture blew the budget (don't retry).
	seq      int                    // distinct heap path suffix.

	captures      int   // # scans captured (test observability)
	replays       int   // # scans served from the cache (test observability)
	abandoned     int   // # scans abandoned mid-capture over budget (test observability)
	skippedBig    int   // # scans skipped up front by the size-estimate gate (never spilled)
	capturedBytes int64 // total bytes captured into heaps (memory-stats evidence)
}

// Op serves one datastore leaf op: a cacheable full correlation-keyspace scan from the
// cache (capturing on first access), else delegates unchanged.
func (c *corpusScanCache) Op(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	key, ok := c.scanCacheKey(o, vars)
	if !ok {
		c.delegate(o, vars, yieldVals, yieldErr, path, pathNext) // uncacheable.
		return
	}

	// Decide (under the lock) whether to replay, or reserve a fresh heap slot to capture.
	c.mu.Lock()
	if c.tooBig[key] {
		c.mu.Unlock()
		c.delegate(o, vars, yieldVals, yieldErr, path, pathNext) // known over-budget: re-scan.
		return
	}
	h := c.captured[key]
	c.mu.Unlock()

	if h != nil {
		c.mu.Lock()
		c.replays++
		c.mu.Unlock()
		replayHeap(h, yieldVals, yieldErr)
		return
	}

	// Size-estimate GATE (first access, OPTIONAL/ADVISORY): if the keyspace offers a raw-size
	// hint (keyspaceSizeHinter) that, scaled by the assumed framing/encode expansion, already
	// exceeds the budget, SKIP caching up front -- don't spill up to the budget only to
	// abandon it (a wasteful RSS spike, since spill chunks are mmap-backed). A keyspace that
	// offers NO hint (raw < 0 -- a datasource that can't cheaply size itself) is simply not
	// gated: it falls through to attempt-and-maybe-abandon, which is always correct.
	if raw := keyspaceRawBytes(o, vars); raw >= 0 &&
		float64(raw)*CorpusScanCacheSizeFactor > float64(c.budget) {
		c.mu.Lock()
		c.tooBig[key] = true
		c.skippedBig++
		c.mu.Unlock()
		c.delegate(o, vars, yieldVals, yieldErr, path, pathNext)
		return
	}

	c.mu.Lock()
	c.seq++
	seq := c.seq
	c.mu.Unlock()

	// First access: capture the scan into a corpus-scoped heap WHILE serving this caller
	// (so the first scan isn't wasted). The heap spills to disk, so RAM is bounded; it
	// lives under the corpus dir, not the per-detector Run's tmpDir (which is removed when
	// that Run ends), so later Runs can replay it. If the capture exceeds the byte budget
	// we ABANDON it (free the partial heap, poison the key) and keep serving -- so a huge
	// keyspace falls back to re-scanning instead of a giant spill. capYield touches only
	// LOCALS (+ this call's own heap), so no lock is held on the hot path; the shared
	// map/counter updates happen once, under the lock, after the scan finishes.
	heap := c.newHeap(seq)
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
				heap.Close() // drop the partial spill immediately.
			} else if e := heap.PushBytes(buf); e != nil {
				capErr = e
			}
		}
		yieldVals(vals)
	}
	c.delegate(o, vars, capYield, yieldErr, path, pathNext)

	c.mu.Lock()
	switch {
	case over:
		c.tooBig[key] = true
		c.abandoned++
	case capErr == nil:
		c.captured[key] = heap
		c.captures++
		c.capturedBytes += capBytes
	default:
		heap.Close()
	}
	c.mu.Unlock()
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
	// Chunk files are <prefix>_chunk_N.<suffix>; their parent must exist. c.dir is a
	// dedicated MkdirTemp dir, so prefix directly under it -- NO extra subdir, which
	// (uncreated) previously made spill chunk files fail to open, silently dropping the
	// capture of any keyspace bigger than the in-memory chunk-0 (~16 MiB).
	prefix := filepath.Join(c.dir, itoaCache(seq))
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

// keyspaceSizeHinter is an OPTIONAL capability a datasource keyspace may implement so the
// scan-cache's size gate can estimate a capture's size cheaply (no scan) and skip an
// over-budget keyspace up front. It is purely advisory: a keyspace that does NOT implement
// it -- or returns < 0 ("unknown") -- is simply not gated, and the cache falls back to
// attempt-and-maybe-abandon, which is always correct. A future datasource (Parquet footer,
// remote catalog stats, ...) opts into the optimization just by implementing this; one that
// can't size itself needs to do nothing. The file datastore's *flatKeyspace implements it
// via os.Stat.
type keyspaceSizeHinter interface {
	RawSizeHintBytes() int64 // approximate raw bytes, or < 0 when unknown.
}

// keyspaceRawBytes asks the scan's keyspace for its optional raw-size hint, or -1 when the
// keyspace offers none. Advisory only (see keyspaceSizeHinter) -- never required for
// correctness.
func keyspaceRawBytes(o *base.Op, vars *base.Vars) int64 {
	idx, ok := o.Params[0].(int)
	if !ok || idx < 0 || idx >= len(vars.Temps) {
		return -1
	}
	ks, ok := vars.Temps[idx].(keyspacer)
	if !ok {
		return -1
	}
	if h, ok := ks.Keyspace().(keyspaceSizeHinter); ok {
		return h.RawSizeHintBytes()
	}
	return -1
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
