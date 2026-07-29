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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/couchbase/n1k1/records"
)

// CursorState is one cursor's durable state -- an opaque, serializable,
// comparable value (so the same code works when the backend later becomes a
// served KV). Persisted as <store>/<name>.json.
type CursorState struct {
	Name string `json:"name"`
	// Pack is the comma-joined query-pack dir(s) the cursor is bound to; Bind is
	// the optional keyspace binding string (as passed to `.multi run --bind`).
	Pack string `json:"pack"`
	Bind string `json:"bind,omitempty"`
	// PackID is "<name>@<sha>" captured at create time (a drift check on peek).
	PackID string `json:"pack_id,omitempty"`
	// Mode is the delta strategy: "append" (offset high-water, Phase 1) or "diff"
	// (snapshot-keyed-by-id, Phase 2 — for mutable / current-state-only sources).
	Mode string `json:"mode"`

	// --- append mode ---
	// Water is the committed high-water: source-container key -> offset. Empty
	// means "from the very beginning" (every record is new).
	Water map[string]int64 `json:"water,omitempty"`

	// --- diff mode ---
	// IdField is the result field whose value keys the snapshot (default "id"); a
	// result missing it can't be diffed (no stable identity) and is skipped.
	IdField string `json:"id_field,omitempty"`
	// SnapVersion is the committed snapshot generation, surfaced as the "snap:N"
	// position token; it bumps on each advance that changed the snapshot.
	SnapVersion int `json:"snap_version,omitempty"`

	// Metadata (k8s labels-vs-annotations split): Labels are for selection/
	// grouping; Annotations is free-form provenance the client attaches.
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations json.RawMessage   `json:"annotations,omitempty"`

	// GitOps reconcile (Phase 3): SpecHash fingerprints the declared configuration
	// (pack content + policy) so `apply` tells an unchanged cursor (preserve its
	// position) from a drifted one; Managed marks a cursor created/adopted by
	// `apply` (only managed cursors are eligible for `--prune`).
	SpecHash string `json:"spec_hash,omitempty"`
	Managed  bool   `json:"managed,omitempty"`

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
// later diff can emit `before`) tagged with the detector Label that produced it.
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
	since map[string]int64 // committed high-water (read-only), container -> offset
	mu    sync.Mutex
	seen  map[string]int64 // max position observed this run, container -> offset
}

// NewRecordScanFilter builds a filter for the committed high-water `since` (nil
// means "from the beginning" -- admit everything).
func NewRecordScanFilter(since map[string]int64) *RecordScanFilter {
	return &RecordScanFilter{since: since, seen: map[string]int64{}}
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
// the recomputed water covers everything currently present.
func (f *RecordScanFilter) observe(path string, pos int64) {
	f.mu.Lock()
	if cur, ok := f.seen[path]; !ok || pos > cur {
		f.seen[path] = pos
	}
	f.mu.Unlock()
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
		s.f.observe(path, pos)
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
	Report       *MultiQueryRunReport
}

// RunCursorPack compiles and runs the pack `dets` with an `append` scan filter
// seeded at the committed high-water `since`, returning the delta labelResults and
// the recomputed high-water. Used by both `peek` (discards the water) and
// `advance` (commits it).
func (s *Session) RunCursorPack(dets []MultiQueryEntry, since map[string]int64) (*CursorRunResult, error) {
	return s.runCursorPack(dets, NewRecordScanFilter(since))
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
	}
	return res, nil
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

// SpecHash is the reconcile fingerprint of a cursor's DELTA IDENTITY — the pack
// content (packID), delta mode, binding, and id-field. It deliberately EXCLUDES
// description / labels / annotations: per DESIGN-cep.md the delta-identity is
// "(query+binding SHA, source-fingerprint)", and "a retag/reword is a no-op for
// state". So a metadata-only edit leaves SpecHash unchanged (the committed
// position is never re-baselined by a comment change); `apply` refreshes such
// metadata in place separately (see the metadata-drift pass).
func SpecHash(packID, mode, bind, idField string) string {
	h := sha256.New()
	for _, s := range []string{packID, mode, bind, idField} {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

// ReconcilePlan is the create/update/destroy/unchanged partition a GitOps `plan`
// computes and `apply` executes (each list holds cursor names, sorted).
type ReconcilePlan struct {
	Create    []string `json:"create"`
	Update    []string `json:"update"`
	Destroy   []string `json:"destroy"`
	Unchanged []string `json:"unchanged"`
}

// PlanReconcile diffs the declared cursors (name -> SpecHash) against the live
// store. A declared name absent from live => create; present with a different
// SpecHash => update; equal => unchanged (preserve position). A live cursor absent
// from `desired` is a destroy candidate, but only surfaces when prune is set AND
// the cursor is Managed (so an imperative `.multi cursor create` is never pruned).
func PlanReconcile(desired map[string]string, live map[string]*CursorState, prune bool) ReconcilePlan {
	var p ReconcilePlan
	for name, hash := range desired {
		switch cur := live[name]; {
		case cur == nil:
			p.Create = append(p.Create, name)
		case cur.SpecHash == hash:
			p.Unchanged = append(p.Unchanged, name)
		default:
			p.Update = append(p.Update, name)
		}
	}
	if prune {
		for name, st := range live {
			if _, ok := desired[name]; !ok && st.Managed {
				p.Destroy = append(p.Destroy, name)
			}
		}
	}
	sort.Strings(p.Create)
	sort.Strings(p.Update)
	sort.Strings(p.Destroy)
	sort.Strings(p.Unchanged)
	return p
}

// PackID returns a stable "<name>@<sha>" identity for a pack: name is the given
// label, sha a short hash over each entry's id + normalized SQL (order-
// independent), so an unchanged pack keeps its id and any edit changes it.
func PackID(name string, dets []MultiQueryEntry) string {
	lines := make([]string, 0, len(dets))
	for _, d := range dets {
		lines = append(lines, d.Label+"\x00"+normalizePackSQL(d.Stmt))
	}
	sort.Strings(lines)
	h := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return name + "@" + hex.EncodeToString(h[:])[:8]
}

// normalizePackSQL canonicalizes a statement for identity hashing (PackID/SpecHash):
// trailing per-line whitespace is stripped and blank lines dropped, so a cosmetic
// reformat — blank lines between a comment preamble and the SELECT, trailing spaces —
// doesn't change the pack id and churn spec_hash in a GitOps diff (ISSUE-05 #4). A
// full SQL re-parse would be overkill; the blank-line/trailing-space collapse covers
// the realistic reformat the doc comment ("normalized SQL") already promises.
func normalizePackSQL(s string) string {
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
// identity; Label is the detector whose current/prior view produced it.
type ChangeEvent struct {
	Op     string          // "insert" | "update" | "delete"
	Id     string          // the doc identity (from the id field)
	Label  string          // the detector that matched
	Before json.RawMessage // prior doc (update / delete)
	After  json.RawMessage // current doc (insert / update)
}

// SnapshotFromResults keys a run's labelResults by doc identity for diffing: the
// value of the `idField` field in each result (default "id"). A result missing a
// usable id can't be diffed (no stable identity) and is counted in `skipped`
// rather than silently dropped. Rows are keyed by (label, id) so two detectors
// that both match the same doc keep independent change streams; the reported Id
// stays the bare doc identity. On a same-key collision within one detector, the
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
