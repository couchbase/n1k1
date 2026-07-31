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

// CEP named cursors (DESIGN-cep.md, Phase 1). A cursor is a durable, named
// high-water position bound to a query pack: `create` binds+validates, `peek`
// reports the pending delta WITHOUT moving the position, `advance` commits it.
// This file owns the three pieces the engine needs:
//
//   1. CursorState / CursorStore -- the durable, tfstate-style local state (one
//      JSON file per cursor NAME, atomic write-temp-rename).
//   2. RecordScanFilter -- the `append` delta mechanism: a records.Source wrapper
//      (installed at the single KeyspaceRecordsOpen choke point in
//      DatastoreScanRecords) that skips records at/below the committed per-source
//      offset and tracks the new high-water. No engine/records hot-path change.
//   3. Session.RunCursorPack -- run a pack under a filter and return both the
//      (delta-only) labelResults and the recomputed high-water.
//
// The offset is the cursor's "don't re-emit" mechanism; the per-labelResult
// fingerprint (dedup_key, added by the CLI) is a SEPARATE concern -- it lets the
// agent dedupe its own side effects. n1k1 persists no seen-set in Phase 1.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/n1k1/records"
)

// CursorSchemaVersion is the version stamped into every persisted CursorState (and
// echoed by show/list) so a reader can tell which sidecar shape it is looking at
// (ISSUE-15 §3). The sidecar is an IMPLEMENTATION DETAIL -- `.multi cursor show`/`list`
// are the supported machine-readable surface -- but stamping a version makes the
// distinction explicit rather than a guess. Bump on an incompatible sidecar change.
//
// v2: the sidecar keys `pack`/`pack_id` were renamed to `queries_path`/`queries` to
// match the show/list vocabulary (ISSUE-15 §2a). UnmarshalJSON still reads the v1 keys,
// so a v1 sidecar loads unchanged and is rewritten as v2 on its next Save.
const CursorSchemaVersion = 2

// UnmarshalJSON loads a CursorState, accepting the pre-v2 sidecar keys `pack` (source
// path) and `pack_id` (content id) as fallbacks for `queries_path` / `queries`. This
// lets a v1 sidecar keep loading after the rename; Save rewrites it with the v2 keys.
func (st *CursorState) UnmarshalJSON(b []byte) error {
	type alias CursorState // avoid recursing into this method
	aux := struct {
		*alias
		OldPack   *string `json:"pack"`
		OldPackID *string `json:"pack_id"`
	}{alias: (*alias)(st)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if st.QueriesPath == "" && aux.OldPack != nil {
		st.QueriesPath = *aux.OldPack
	}
	if st.Queries == "" && aux.OldPackID != nil {
		st.Queries = *aux.OldPackID
	}
	return nil
}

// CursorState is one cursor's durable state -- an opaque, serializable,
// comparable value (so the same code works when the backend later becomes a
// served KV). Persisted as <store>/<name>.json.
type CursorState struct {
	Name string `json:"name"`
	// Schema is CursorSchemaVersion at write time (ISSUE-15 §3); 0 on a pre-versioning
	// sidecar. Stamped by Save.
	Schema int `json:"schema,omitempty"`
	// QueriesPath is the comma-joined query source dir(s)/file the cursor is bound to
	// (the sidecar key was `pack` before the ISSUE-15 vocabulary unification); Bind is
	// the optional keyspace binding string (as passed to `.multi run --bind`).
	QueriesPath string `json:"queries_path"`
	Bind        string `json:"bind,omitempty"`
	// Queries is "<name>@<sha>", the content id captured at create (the drift baseline
	// checked on peek). The sidecar key was `pack_id` before the unification.
	// HashScheme records which QueriesID normalization scheme stamped it (0 on a
	// sidecar predating scheme versioning); drift comparison accepts any known
	// scheme (QueriesIDMatches) and advance re-stamps to the current one, so an
	// n1k1 upgrade that changes normalization never manufactures drift.
	Queries    string `json:"queries,omitempty"`
	HashScheme int    `json:"hash_scheme,omitempty"`
	// Mode is the delta strategy: "append" (offset high-water, Phase 1), "diff"
	// (snapshot-keyed-by-id, Phase 2 — mutable / current-state sources), or "census"
	// (incremental schema census, Phase 3 — a keyspace, not a pack).
	Mode string `json:"mode"`

	// --- append mode ---
	// Water is the committed high-water: source-container key -> offset. Empty
	// means "from the very beginning" (every record is new). WaterFP is the parallel
	// boundary-record fingerprint map (container -> hash of the record STARTING at
	// its committed offset) — the tier-1 prefix fingerprint that detects a file
	// REWRITTEN in place without shrinking (the violation a size check cannot see).
	// Optional + parallel (not folded into Water's value type) so legacy sidecars
	// load unchanged and fingerprints backfill on the next advance. This is the file
	// shape of git's SHA-as-position: a committed position carrying content identity.
	Water   map[string]int64  `json:"water,omitempty"`
	WaterFP map[string]string `json:"water_fp,omitempty"`
	// Params are the pack's RESOLVED query parameters captured at create (defaults
	// baked in, --param overrides applied — glue.ApplyParams) and replayed on every
	// peek/advance/check, so a later front-matter DEFAULT change can never silently
	// move a live cursor, and the rendered statement QueriesID hashes always reflects
	// exactly these values. A param change is a new standing question: rm + create.
	Params map[string]string `json:"params,omitempty"`

	// --- diff mode ---
	// IdField is the result field whose value keys the snapshot (default "id"); a
	// result missing it can't be diffed (no stable identity) and is skipped.
	IdField string `json:"id_field,omitempty"`
	// SnapVersion is the committed snapshot generation, surfaced as the "snap:N"
	// position token; it bumps on each advance that changed the snapshot.
	SnapVersion int `json:"snap_version,omitempty"`

	// --- census mode (Phase 3) ---
	// Keyspace is the keyspace being censused; CensusTypeField/TimeField/Depth/Exclude
	// are the census options. Census is the ACCUMULATED census (folded incrementally),
	// stored HERE with Water so both commit in one atomic write (the two-store wall).
	Keyspace        string           `json:"keyspace,omitempty"`
	CensusTypeField string           `json:"census_type_field,omitempty"`
	CensusTimeField string           `json:"census_time_field,omitempty"`
	CensusDepth     int              `json:"census_depth,omitempty"`
	CensusExclude   []string         `json:"census_exclude,omitempty"`
	Census          []CensusRow      `json:"census,omitempty"`
	CensusTotals    map[string]int64 `json:"census_totals,omitempty"`
	CensusRecords   int64            `json:"census_records,omitempty"`
	CensusVersion   int              `json:"census_version,omitempty"` // "census:N" token; bumps on drift

	// Builtin is the resolved builtin-queries ref this cursor is over, if any
	// (e.g. "census@1") — stamped at create so a future, incompatible builtin version
	// can detect + refuse/rebase a stale cursor rather than fold mismatched data.
	Builtin string `json:"builtin,omitempty"`

	// Description is a free-form note set at create time (--desc).
	Description string `json:"description,omitempty"`

	// Annotations is an arbitrary client-owned JSON blob, stored + returned VERBATIM and
	// never interpreted (DESIGN-cep.md's labels-vs-annotations split): the home for
	// provenance (a git SHA, the authoring prompt/model), runbook links, and suppression
	// hints. Labels are indexable k=v tags. SourceRef is the git commit (with a "-dirty"
	// suffix on an uncommitted tree) of the queries source captured at create -- "which
	// commit produced this cursor's positions". All three are DELIBERATELY OUTSIDE the delta
	// identity (QueriesID / spec_hash covers only pack content + policy), so a retag/reword or a
	// provenance stamp never re-baselines the committed position (the "metadata edits must
	// not reset the cursor" rule).
	Annotations map[string]interface{} `json:"annotations,omitempty"`
	Labels      map[string]string      `json:"labels,omitempty"`
	SourceRef   string                 `json:"source_ref,omitempty"`

	// Bookkeeping surfaced by `show`.
	Created       string `json:"created,omitempty"`
	Updated       string `json:"updated,omitempty"`
	LastCount     int    `json:"last_count"`
	TotalAdvances int    `json:"total_advances"`
}

// CursorStore is a tfstate-style local directory of cursor state files
// (DESIGN-cep.md § State & idempotency). Default location is
// <datastore>/.n1k1-state/cursors, overridable via `--cursor-store`.
type CursorStore struct{ dir string }

// NewCursorStore roots a store at dir (created lazily on first Save).
func NewCursorStore(dir string) *CursorStore { return &CursorStore{dir: dir} }

// Dir is the store's root directory.
func (s *CursorStore) Dir() string { return s.dir }

// cursorNameOK rejects a name that would escape the store dir or collide with the
// temp suffix, keeping <name>.json a plain file directly under dir.
func cursorNameOK(name string) error {
	if name == "" {
		return fmt.Errorf("cursor name is required")
	}
	if strings.ContainsAny(name, "/\\") || name == "." || name == ".." ||
		strings.HasPrefix(name, ".") || strings.Contains(name, "\x00") {
		return fmt.Errorf("invalid cursor name %q", name)
	}
	return nil
}

func (s *CursorStore) path(name string) string {
	return filepath.Join(s.dir, name+".json")
}

// ErrCursorNotExist is returned by Load/Remove for an unknown cursor name.
var ErrCursorNotExist = os.ErrNotExist

// Load reads a cursor's state; returns ErrCursorNotExist if absent.
func (s *CursorStore) Load(name string) (*CursorState, error) {
	if err := cursorNameOK(name); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(s.path(name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrCursorNotExist
		}
		return nil, err
	}
	var st CursorState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("cursor %q: corrupt state: %v", name, err)
	}
	if st.Water == nil {
		st.Water = map[string]int64{}
	}
	return &st, nil
}

// Save atomically persists a cursor's state (write-temp-then-rename, so a crash
// mid-write never leaves a torn file -- the crash-safety the two-step advance
// relies on).
func (s *CursorStore) Save(st *CursorState) error {
	if err := cursorNameOK(st.Name); err != nil {
		return err
	}
	st.Schema = CursorSchemaVersion // stamp the current sidecar version on every write
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	out, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	out = append(out, '\n')
	// Single-process CLI: a fixed per-name temp suffix is safe (no concurrent
	// writer of the same cursor) and needs no rand/time (unavailable in some build
	// contexts anyway).
	tmp := s.path(st.Name) + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path(st.Name)); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// List returns the cursor names in the store, sorted.
func (s *CursorStore) List() ([]string, error) {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range ents {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".json") {
			continue
		}
		names = append(names, strings.TrimSuffix(n, ".json"))
	}
	sort.Strings(names)
	return names, nil
}

// Remove deletes a cursor's state (and its diff-mode snapshot sidecar, if any);
// returns ErrCursorNotExist if the state file is absent.
func (s *CursorStore) Remove(name string) error {
	if err := cursorNameOK(name); err != nil {
		return err
	}
	if err := os.Remove(s.path(name)); err != nil {
		if os.IsNotExist(err) {
			return ErrCursorNotExist
		}
		return err
	}
	os.Remove(s.snapPath(name)) // best-effort; absent for append cursors
	return nil
}

// snapPath is the diff-mode snapshot sidecar for a cursor.
func (s *CursorStore) snapPath(name string) string {
	return filepath.Join(s.dir, name+".snap.json")
}

// SnapshotEntry is one row of a diff-mode snapshot: the labelResult's Doc (so a
// later diff can emit `before`) tagged with the query Label that produced it.
type SnapshotEntry struct {
	Label string          `json:"label,omitempty"`
	Doc   json.RawMessage `json:"doc"`
}

// LoadSnapshot reads a cursor's prior diff snapshot (id -> entry). A missing file
// is an empty snapshot (nil, nil) — the "from the beginning" case.
func (s *CursorStore) LoadSnapshot(name string) (map[string]SnapshotEntry, error) {
	if err := cursorNameOK(name); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(s.snapPath(name))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]SnapshotEntry{}, nil
		}
		return nil, err
	}
	m := map[string]SnapshotEntry{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("cursor %q: corrupt snapshot: %v", name, err)
	}
	return m, nil
}

// SaveSnapshot atomically replaces a cursor's diff snapshot (write-temp-rename).
func (s *CursorStore) SaveSnapshot(name string, snap map[string]SnapshotEntry) error {
	if err := cursorNameOK(name); err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	out, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := s.snapPath(name) + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.snapPath(name)); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// ---------------------------------------------------------------- scan filter

// RecordScanFilter drives the `append` delta: given the committed per-container
// high-water `since`, it wraps a records.Source to skip already-seen records and
// track the max position seen per container (the recomputed water). It is
// installed once per scan at DatastoreScanRecords via GlueContext.scanFilter.
type RecordScanFilter struct {
	since   map[string]int64  // committed high-water (read-only), container -> offset
	sinceFP map[string]string // committed boundary-record fingerprints (read-only), container -> hash; nil = no verification
	mu      sync.Mutex
	seen    map[string]int64  // max position observed this run, container -> offset
	lastDoc map[string][]byte // COPY of the record at the observed max (the next boundary record); hashed once at scan end
	boundFP map[string]string // hash of the record observed AT the committed offset -- the rewrite check
}

// NewRecordScanFilter builds a filter for the committed high-water `since` (nil
// means "from the beginning" -- admit everything). sinceFP carries the committed
// boundary-record fingerprints (CursorState.WaterFP): the hash of the record that
// STARTS at each container's committed offset. It is the tier-1 "prefix fingerprint"
// (DESIGN-cep.md): if that one record no longer hashes the same -- or no record
// starts there at all -- the file was REWRITTEN in place (framing shifted) even
// though its size never shrank, the one violation a size check cannot see. nil (a
// legacy sidecar) skips verification; fingerprints backfill on the next advance.
func NewRecordScanFilter(since map[string]int64, sinceFP map[string]string) *RecordScanFilter {
	return &RecordScanFilter{since: since, sinceFP: sinceFP,
		seen: map[string]int64{}, lastDoc: map[string][]byte{}, boundFP: map[string]string{}}
}

// wrap returns a Source that yields only records past the committed high-water,
// observing every record's position so NewWater reflects the full source extent.
func (f *RecordScanFilter) wrap(inner records.Source) records.Source {
	return &filteringSource{inner: inner, f: f}
}

// admit reports whether a record at container position `pos` is new. An untracked
// container (a freshly-appeared file) admits ALL its records -- including the one
// at offset 0 -- while a tracked one admits strictly past its committed offset.
func (f *RecordScanFilter) admit(path string, pos int64) bool {
	if f.since == nil {
		return true
	}
	w, ok := f.since[path]
	if !ok {
		return true
	}
	return pos > w
}

// observe records the max position seen in a container (new + already-seen), so
// the recomputed water covers everything currently present. It also keeps a COPY of
// the record at the current max (the boundary record a future scan will verify --
// copy now, hash once at scan end: hashing every record would cost ~30% of a scan,
// a memcpy ~1/10th of that) and, when the committed fingerprints ask for it, hashes
// the one record found AT the committed offset (the rewrite check).
func (f *RecordScanFilter) observe(path string, pos int64, doc []byte) {
	f.mu.Lock()
	if cur, ok := f.seen[path]; !ok || pos > cur {
		f.seen[path] = pos
		f.lastDoc[path] = append(f.lastDoc[path][:0], doc...)
	}
	if f.sinceFP != nil {
		if _, want := f.sinceFP[path]; want && pos == f.since[path] {
			f.boundFP[path] = recordFP(doc)
		}
	}
	f.mu.Unlock()
}

// FingerprintWater returns the boundary-record fingerprint for each container
// observed this scan: the hash of the record at its max position -- what
// CursorState.WaterFP commits so the NEXT scan can verify the prefix survived.
func (f *RecordScanFilter) FingerprintWater() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(f.lastDoc))
	for k, doc := range f.lastDoc {
		out[k] = recordFP(doc)
	}
	return out
}

// recordFP is the boundary-record hash: FNV-1a 64 over the record's bytes, hex.
// Not cryptographic -- it guards against rotation/rewrite accidents, not adversaries
// (an adversary owns the files anyway). Stable across scans of identical bytes.
func recordFP(doc []byte) string {
	h := fnv.New64a()
	h.Write(doc)
	return strconv.FormatUint(h.Sum64(), 16)
}

// NewWater merges the committed high-water with what was observed this run: the
// per-container max of the two (a container present before but not scanned this
// run keeps its old offset; a container scanned this run advances to its max).
func (f *RecordScanFilter) NewWater() map[string]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int64, len(f.since)+len(f.seen))
	for k, v := range f.since {
		out[k] = v
	}
	for k, v := range f.seen {
		if cur, ok := out[k]; !ok || v > cur {
			out[k] = v
		}
	}
	return out
}

// ObservedWater is what this scan actually saw, per container — the position of the
// last record observed, with no merge into the committed water (unlike NewWater's
// max-merge, which never rewinds). It is how a caller re-baselines a TRUNCATED
// container at its current content (`advance --accept-truncation`): under max-merge
// alone a truncated container is dead until the file regrows past its old offset —
// every future append below it is skipped too.
func (f *RecordScanFilter) ObservedWater() map[string]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int64, len(f.seen))
	for k, v := range f.seen {
		out[k] = v
	}
	return out
}

// SourceAnomalies reports the committed containers whose source violated the
// append-only assumption this scan ("append-mostly, with whole-file rotation" —
// DESIGN-cep.md): rotated = held in the committed water but NOTHING observed this
// scan (file deleted/renamed, or now empty); truncated = observed but the extent
// fell BELOW the committed offset (rewritten shorter — its records below the old
// offset are being skipped, since the watermark never rewinds); rewritten = size
// says nothing shrank, but the committed boundary-record fingerprint no longer
// matches the record at that offset (or no record starts there) — the file was
// replaced in place, the case a size check cannot see. All are silent evidence loss
// unless disclosed, which is this method's whole purpose. Sorted.
//
// This trio is the file instance of a generic contract a cursor-followable source
// owes its committed position — "does the position still describe the source?" —
// and maps directly onto the planned git:// provider's checks: rotated ≙ ref
// deleted, truncated ≙ ref rewound, rewritten ≙ history rewritten (cursor SHA no
// longer an ancestor). The disclose → refuse → acknowledge cycle built here is the
// one a git ref rewind will ride.
func (f *RecordScanFilter) SourceAnomalies() (rotated, truncated, rewritten []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, w := range f.since {
		seen, ok := f.seen[k]
		switch {
		case !ok:
			rotated = append(rotated, k)
		case seen < w:
			truncated = append(truncated, k)
		default:
			if fp, has := f.sinceFP[k]; has {
				if got, saw := f.boundFP[k]; !saw || got != fp {
					rewritten = append(rewritten, k)
				}
			}
		}
	}
	sort.Strings(rotated)
	sort.Strings(truncated)
	sort.Strings(rewritten)
	return rotated, truncated, rewritten
}

type filteringSource struct {
	inner records.Source
	f     *RecordScanFilter
}

func (s *filteringSource) Next(rec *records.Record) (bool, error) {
	for {
		ok, err := s.inner.Next(rec)
		if !ok || err != nil {
			return ok, err
		}
		path, pos, hasPos := records.ParseRecordPos(rec.ID)
		if !hasPos {
			return true, nil // whole-file unit: no within-file position to filter on
		}
		s.f.observe(path, pos, rec.Doc)
		if s.f.admit(path, pos) {
			return true, nil
		}
		// Already seen (<= committed offset): skip and pull the next record.
	}
}

func (s *filteringSource) Close() error { return s.inner.Close() }

// ---------------------------------------------------------------- run a pack

// CursorRunResult is the outcome of running a pack under an append filter.
type CursorRunResult struct {
	LabelResults []LabelResult    // the delta only (records past the water)
	NewWater     map[string]int64 // recomputed high-water (the candidate `to`)
	// Rotated / Truncated / Rewritten disclose committed containers whose source
	// violated append-only this scan (see RecordScanFilter.SourceAnomalies) — surfaced
	// in the peek/advance envelope so evidence loss is an event, never silent.
	// Observed is the un-merged per-container position this scan saw
	// (RecordScanFilter.ObservedWater) — what `advance --accept-truncation`
	// re-baselines a truncated container to; ObservedFP the matching boundary-record
	// fingerprints (RecordScanFilter.FingerprintWater) — what WaterFPMerge commits.
	Rotated    []string
	Truncated  []string
	Rewritten  []string
	Observed   map[string]int64
	ObservedFP map[string]string
	Report     *MultiQueryRunReport
}

// RunCursorPack compiles and runs the pack `dets` with an `append` scan filter
// seeded at the committed high-water `since`, returning the delta labelResults and
// the recomputed high-water. Used by both `peek` (discards the water) and
// `advance` (commits it).
func (s *Session) RunCursorPack(dets []MultiQueryEntry, since map[string]int64,
	sinceFP map[string]string) (*CursorRunResult, error) {
	return s.runCursorPack(dets, NewRecordScanFilter(since, sinceFP))
}

// RunPackFull compiles and runs the pack `dets` over the FULL current state (no
// append filter) — the `diff`-mode read: every currently-matching row, to be
// diffed against a stored snapshot by the caller.
func (s *Session) RunPackFull(dets []MultiQueryEntry) ([]LabelResult, error) {
	res, err := s.runCursorPack(dets, nil)
	if err != nil {
		return nil, err
	}
	return res.LabelResults, nil
}

func (s *Session) runCursorPack(dets []MultiQueryEntry, filter *RecordScanFilter) (*CursorRunResult, error) {
	s.cursorFilter = filter // nil => DatastoreScanRecords wraps nothing (full read)
	defer func() { s.cursorFilter = nil }()

	cc, err := s.MultiQueryCompile(dets)
	if err != nil {
		return nil, err
	}
	lrs, report, err := cc.RunReport()
	if err != nil {
		return nil, err
	}
	res := &CursorRunResult{LabelResults: lrs, Report: report}
	if filter != nil {
		res.NewWater = filter.NewWater()
		res.Rotated, res.Truncated, res.Rewritten = filter.SourceAnomalies()
		res.Observed = filter.ObservedWater()
		res.ObservedFP = filter.FingerprintWater()
	}
	return res, nil
}

// WaterFPMerge computes the boundary-record fingerprints to commit alongside a new
// water: for each committed container, the fingerprint observed this scan when the
// container's committed offset IS this scan's observed position; else the old
// fingerprint carried forward when the offset didn't move (an unscanned/rotated
// container); else absent (an explicit --to offset nobody observed a record at —
// no fingerprint beats a wrong one). This is also the opportunistic BACKFILL: a
// legacy sidecar with no fingerprints gains them on its next advance, no flag day
// (the hash_scheme pattern).
func WaterFPMerge(newWater, observed map[string]int64, observedFP map[string]string,
	oldWater map[string]int64, oldFP map[string]string) map[string]string {
	out := map[string]string{}
	for k, w := range newWater {
		if ov, ok := observed[k]; ok && ov == w {
			if h, ok2 := observedFP[k]; ok2 {
				out[k] = h
				continue
			}
		}
		if ow, ok := oldWater[k]; ok && ow == w {
			if h, ok2 := oldFP[k]; ok2 {
				out[k] = h
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// LoadPack loads a pack from a path that is EITHER a single *.sql++ file (one
// entry — the GitOps "one file = one cursor" case) or a directory of them (the
// imperative --pack <dir> case), so peek/advance reload uniformly whichever a
// cursor was bound to.
func LoadPack(path string) ([]MultiQueryEntry, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return LoadMultiQueryEntries(path)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	e, err := ParseMultiQueryEntry(path, string(body))
	if err != nil {
		return nil, err
	}
	return []MultiQueryEntry{e}, nil
}

// QueriesID returns a stable "<name>@<sha>" content identity for a set of queries
// (the value CursorState.Queries stores, whose @sha tail is surfaced as spec_hash):
// name is the given label, sha a short hash over each entry's label + normalized SQL
// (order-independent), so unchanged queries keep their id and any edit changes it.
// Computed under the CURRENT hash scheme; see QueriesIDMatches for why comparisons
// must not use == against a stored id.
func QueriesID(name string, dets []MultiQueryEntry) string {
	return QueriesIDUnderScheme(name, dets, QueriesHashScheme)
}

// QueriesHashScheme is the CURRENT normalization scheme QueriesID hashes under.
// The sha is content-addressed over normalized SQL, and the normalization
// CONVENTIONS evolve (scheme 2, ISSUE-05 #4, made the hash blank-line-invariant --
// which re-hashed every already-stored id computed under scheme 1). Each change is
// a new scheme here, with the old normalizer kept below, so an n1k1 upgrade can
// never manufacture drift: comparisons accept a stored id that matches under ANY
// known scheme (QueriesIDMatches), and advance re-stamps to the current scheme.
const QueriesHashScheme = 2

// queriesHashNormalizers maps each known scheme to its statement normalizer.
// NEVER edit an entry in place -- a normalization improvement is a NEW scheme
// (append here, bump QueriesHashScheme), or every committed cursor sidecar and
// provenance ledger re-baselines and `advance` refuses with a false query-drift.
var queriesHashNormalizers = map[int]func(string) string{
	1: strings.TrimSpace, // pre-a64dcd7b: ends-only trim (interior blank lines counted)
	2: normalizeQuerySQL, // ISSUE-05 #4: drop blank lines + trailing per-line whitespace
}

func QueriesIDUnderScheme(name string, dets []MultiQueryEntry, scheme int) string {
	norm := queriesHashNormalizers[scheme]
	if norm == nil {
		norm = queriesHashNormalizers[QueriesHashScheme]
	}
	lines := make([]string, 0, len(dets))
	for _, d := range dets {
		lines = append(lines, d.Label+"\x00"+norm(d.Stmt))
	}
	sort.Strings(lines)
	h := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return name + "@" + hex.EncodeToString(h[:])[:8]
}

// QueriesIDMatches reports whether a STORED queries id still identifies dets --
// i.e. the id computed under ANY known hash scheme equals it. This is the drift
// comparison peek/advance/check must use instead of `stored == QueriesID(...)`:
// a sidecar stamped by an older binary carries an older scheme's hash, and only a
// real content edit (no scheme matches) is drift. Returns the matched scheme
// (0 when none), so a caller can re-stamp an old-scheme id to the current one.
func QueriesIDMatches(stored, name string, dets []MultiQueryEntry) (matched int) {
	if stored == "" {
		return 0
	}
	// Try the current scheme first -- the overwhelmingly common case.
	if stored == QueriesIDUnderScheme(name, dets, QueriesHashScheme) {
		return QueriesHashScheme
	}
	for scheme := range queriesHashNormalizers {
		if scheme != QueriesHashScheme && stored == QueriesIDUnderScheme(name, dets, scheme) {
			return scheme
		}
	}
	return 0
}

// normalizeQuerySQL canonicalizes a statement for identity hashing (QueriesID
// scheme 2): trailing per-line whitespace is stripped and blank lines dropped, so a
// cosmetic reformat — blank lines between a comment preamble and the SELECT, trailing
// spaces — doesn't change the queries id and churn spec_hash in a GitOps diff
// (ISSUE-05 #4). A full SQL re-parse would be overkill; the blank-line/trailing-space
// collapse covers the realistic reformat the doc comment ("normalized SQL") already
// promises. Frozen: a change here is a NEW scheme (see queriesHashNormalizers).
func normalizeQuerySQL(s string) string {
	var out []string
	for _, ln := range strings.Split(s, "\n") {
		if ln = strings.TrimRight(ln, " \t"); strings.TrimSpace(ln) != "" {
			out = append(out, ln)
		}
	}
	return strings.Join(out, "\n")
}

// -------------------------------------------------------------- diff / snapshot

// ChangeEvent is one Debezium-style change (DESIGN-cep.md § Delta strategies):
// an insert has After, a delete has Before, an update has both. Id is the doc's
// identity; Label is the query whose current/prior view produced it.
type ChangeEvent struct {
	Op     string          // "insert" | "update" | "delete"
	Id     string          // the doc identity (from the id field)
	Label  string          // the query that matched
	Before json.RawMessage // prior doc (update / delete)
	After  json.RawMessage // current doc (insert / update)
}

// SnapshotFromResults keys a run's labelResults by doc identity for diffing: the
// value of the `idField` field in each result (default "id"). A result missing a
// usable id can't be diffed (no stable identity) and is counted in `skipped`
// rather than silently dropped. Rows are keyed by (label, id) so two queries
// that both match the same doc keep independent change streams; the reported Id
// stays the bare doc identity. On a same-key collision within one query, the
// last row wins.
func SnapshotFromResults(lrs []LabelResult, idField string) (snap map[string]SnapshotEntry, skipped int) {
	if idField == "" {
		idField = "id"
	}
	snap = make(map[string]SnapshotEntry, len(lrs))
	for _, lr := range lrs {
		id, ok := extractIdField(lr.Result, idField)
		if !ok {
			skipped++
			continue
		}
		snap[lr.Label+"\x00"+id] = SnapshotEntry{Label: lr.Label, Doc: lr.Result}
	}
	return snap, skipped
}

// DiffSnapshot compares a prior snapshot with the current one and returns the
// change events, sorted by (label,id) for deterministic output. Keys present only
// in current => insert; only in prior => delete; in both with differing Doc bytes
// => update; byte-identical => no event.
func DiffSnapshot(prior, current map[string]SnapshotEntry, idField string) []ChangeEvent {
	var events []ChangeEvent
	keys := make([]string, 0, len(prior)+len(current))
	seen := map[string]bool{}
	for k := range current {
		keys = append(keys, k)
		seen[k] = true
	}
	for k := range prior {
		if !seen[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		cur, inCur := current[k]
		old, inOld := prior[k]
		id := k
		if i := strings.IndexByte(k, 0); i >= 0 {
			id = k[i+1:] // strip the "<label>\x00" prefix back to the bare doc id
		}
		switch {
		case inCur && !inOld:
			events = append(events, ChangeEvent{Op: "insert", Id: id, Label: cur.Label, After: cur.Doc})
		case !inCur && inOld:
			events = append(events, ChangeEvent{Op: "delete", Id: id, Label: old.Label, Before: old.Doc})
		case inCur && inOld && !bytesEqualJSON(old.Doc, cur.Doc):
			events = append(events, ChangeEvent{Op: "update", Id: id, Label: cur.Label, Before: old.Doc, After: cur.Doc})
		}
	}
	return events
}

func bytesEqualJSON(a, b json.RawMessage) bool {
	return string(a) == string(b)
}

// extractIdField pulls the identity value of `field` from a result object: a
// string value is used verbatim; any other JSON value uses its compact encoding
// (so numeric ids like 42 key stably). ok is false when the result isn't an
// object or the field is absent/null.
func extractIdField(result json.RawMessage, field string) (string, bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(result, &obj); err != nil {
		return "", false
	}
	raw, ok := obj[field]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, true // it was a JSON string
	}
	return string(raw), true // numeric / other: use the compact JSON form
}
