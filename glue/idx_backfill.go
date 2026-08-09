//go:build n1ql && !wasm

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

// Paged BACKWARDS backfill of a time-scoped index -- DESIGN-indexing.md
// "Newest-first / partial index builds", rung 2. A rung-1 index (indexDef.Since)
// covers containers modified at/after its declared cutoff; backfill walks that
// boundary -- the FLOOR -- down toward the beginning, newest containers first, one
// resumable page at a time. Container-aligned throughout (whole files, chosen by
// mtime), so record ids and the catch-up watermark machinery need nothing new:
//
//   - The indexed set is ALWAYS exactly {containers with mtime >= floor}: a page is
//     the next-newest K not-yet-indexed containers (extended to include mtime TIES,
//     or the invariant would split a timestamp between indexed and not).
//   - Each page commits atomically WITH its new floor (and the page's container
//     watermarks merged into the stored water, so later appends to a backfilled
//     container catch up incrementally like any other): a killed backfill resumes
//     exactly where the last page committed.
//   - The floor is MONOTONE DECREASING; when no containers remain below it, the
//     stored floor becomes 0 = COMPLETE -- the index now covers everything, the
//     scan-time disclosure warning stops, and out-of-scope churn stops existing.
//
// The floor lives beside the watermark: bolt meta key "floor" for gsi (atomic with
// the page's entries); the instDir/floor FILE for fts (written after the batch --
// a crash between leaves the floor one page HIGH, and re-indexing that page is
// idempotent: bleve Index() is an upsert). Hybrid serving (index ∪ scan-below-floor)
// is the remaining rung-2 piece -- until then a partially-backfilled index keeps
// the rung-1 disclosure contract, with the warning naming the CURRENT floor.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/couchbase/query/datastore"

	"github.com/couchbase/n1k1/records"
)

// siFloorKey is the meta-bucket key holding the stored floor (see floorFormat).
const siFloorKey = "floor"

// floorInfo renders an index's current floor for IndexInfo (.index list/show):
// "" for an unscoped index, "complete" for a scoped one backfilled all the way,
// else the floor timestamp.
func floorInfo(def *indexDef, floor time.Time) string {
	if def == nil || def.Since == "" {
		return ""
	}
	if floor.IsZero() {
		return "complete"
	}
	return floor.UTC().Format(time.RFC3339)
}

// floorNanos converts an effective floor to its atomic/int64 form (0 = complete).
func floorNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// getIndexFloor reads the index's effective scope boundary: the stored floor when
// one exists (a backfill has run -- possibly to COMPLETE, stored "0"), else the
// def's declared Since (a rung-1 index, or an unscoped one, where it is zero).
func getIndexFloor(db *bolt.DB, def *indexDef) time.Time {
	floor := def.sinceT
	_ = db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte(siMetaBucket))
		if mb == nil {
			return nil
		}
		if b := mb.Get([]byte(siFloorKey)); b != nil {
			if t, ok := floorParse(b); ok {
				floor = t
			}
		}
		return nil
	})
	return floor
}

// IndexBackfillResult reports one BackfillIndex invocation.
type IndexBackfillResult struct {
	Name       string
	Containers int    // containers indexed by this invocation
	Docs       int    // records folded by this invocation
	Floor      string // the new floor (RFC3339), "" when complete
	Complete   bool   // the index now covers the whole keyspace
	Remaining  int    // containers still below the new floor (0 when complete)
}

// BackfillIndex lowers a time-scoped index's floor by up to `pages` containers
// (pages <= 0 = all remaining), newest-first, committing each invocation
// atomically -- see the file comment. Routes gsi/fts by the def's kind.
func BackfillIndex(ds datastore.Datastore, name string, pages int,
	onDoc func(int)) (*IndexBackfillResult, error) {
	sds, ok := ds.(*siDatastore)
	if !ok {
		return nil, fmt.Errorf("backfill: no secondary-index catalog on this datastore")
	}
	var def *indexDef
	for _, d := range sds.cat.Indexes {
		if d.Name == name {
			def = d
			break
		}
	}
	if def == nil {
		return nil, fmt.Errorf("backfill: no index %q in the catalog", name)
	}
	if def.Since == "" {
		return nil, fmt.Errorf("backfill: index %q is not time-scoped (no since=; it already covers everything)", name)
	}
	ks, err := sds.wrappedKeyspace(def.Namespace, def.Keyspace)
	if err != nil {
		return nil, err
	}
	if def.isFTS() {
		return backfillFTS(ks, def, pages, onDoc)
	}
	return backfillSI(ks, def, pages, onDoc)
}

// backfillCandidates lists the containers still BELOW floor (mtime < floor),
// sorted newest-first, with their mtimes. Uses the unfiltered walk listing (the
// same eligibility rules as any scan).
func backfillCandidates(srcDir string, floor time.Time) (files []string, mtimes map[string]time.Time, err error) {
	opts := ScanWalkOptions
	opts.PathPrefix = ""
	all, err := records.WalkFiles(srcDir, opts)
	if err != nil {
		return nil, nil, err
	}
	mtimes = map[string]time.Time{}
	for _, f := range all {
		fi, serr := os.Stat(f)
		if serr != nil {
			continue // raced away; the next page sees the truth
		}
		if fi.ModTime().Before(floor) {
			files = append(files, f)
			mtimes[f] = fi.ModTime()
		}
	}
	sort.Slice(files, func(i, j int) bool { // newest first; path tiebreak for determinism
		ti, tj := mtimes[files[i]], mtimes[files[j]]
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		return files[i] < files[j]
	})
	return files, mtimes, nil
}

// backfillPage selects this invocation's page: the newest `pages` candidates,
// EXTENDED to include every candidate tied with the last one's mtime (the indexed
// set must stay exactly {mtime >= newFloor}). It returns the page, the new floor
// (zero when the page exhausts the candidates = complete), and how many
// candidates remain below it.
func backfillPage(files []string, mtimes map[string]time.Time, pages int) (page []string, newFloor time.Time, remaining int) {
	if len(files) == 0 {
		return nil, time.Time{}, 0
	}
	if pages <= 0 || pages >= len(files) {
		return files, time.Time{}, 0
	}
	cut := mtimes[files[pages-1]]
	n := pages
	for n < len(files) && mtimes[files[n]].Equal(cut) {
		n++ // ties ride along
	}
	if n >= len(files) {
		return files, time.Time{}, 0
	}
	return files[:n], cut, len(files) - n
}

// backfillSI indexes one page of below-floor containers into the bolt index and
// commits entries + merged watermarks + the lowered floor + the re-scoped source
// signature in ONE transaction.
func backfillSI(ks *siKeyspace, def *indexDef, pages int, onDoc func(int)) (*IndexBackfillResult, error) {
	si, err := openSecondaryIndex(ks, def, nil, false)
	if err != nil {
		return nil, err
	}
	ns := ks.Namespace().Name()
	srcDir := filepath.Join(ks.sds.root, ns, ks.Name())

	floor := getIndexFloor(si.db, def)
	res := &IndexBackfillResult{Name: def.Name}
	if floor.IsZero() {
		res.Complete = true
		return res, nil // already complete
	}

	files, mtimes, err := backfillCandidates(srcDir, floor)
	if err != nil {
		return nil, err
	}
	page, newFloor, remaining := backfillPage(files, mtimes, pages)
	res.Floor = ""
	if !newFloor.IsZero() {
		res.Floor = newFloor.UTC().Format(time.RFC3339)
	}
	res.Complete = newFloor.IsZero()
	res.Remaining = remaining
	res.Containers = len(page)

	// Fold the page's records (outside the write tx; the scan dominates), observing
	// per-container watermarks so future appends to these containers catch up.
	ctx := NewGlueContext(time.Now())
	filter := NewRecordScanFilter(nil, nil)
	scan := filter.wrap(records.WalkPrelisted(srcDir, page, indexWalkOptions(time.Time{})))
	var entries [][]byte
	var keyBuf []byte
	var rec records.Record
	for {
		ok, err := scan.Next(&rec)
		if err != nil {
			return nil, fmt.Errorf("backfill %q, next: %w", def.Name, err)
		}
		if !ok {
			break
		}
		res.Docs++
		if onDoc != nil && res.Docs%512 == 0 {
			onDoc(res.Docs)
		}
		entry, ok, err := indexEntryForDoc(ctx, def, &rec, &keyBuf)
		if err != nil {
			return nil, fmt.Errorf("backfill %q: %w", def.Name, err)
		}
		if ok {
			entries = append(entries, entry)
		}
	}
	if onDoc != nil {
		onDoc(res.Docs)
	}

	sig, err := sourceSignature(srcDir, newFloor)
	if err != nil {
		return nil, err
	}
	err = si.db.Update(func(tx *bolt.Tx) error {
		eb, err := tx.CreateBucketIfNotExists([]byte(siEntriesBucket))
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := eb.Put(e, nil); err != nil {
				return err
			}
		}
		mb, err := tx.CreateBucketIfNotExists([]byte(siMetaBucket))
		if err != nil {
			return err
		}
		water, fp, err := getIndexWater(mb)
		if err != nil {
			return err
		}
		if water == nil {
			water = map[string]int64{}
		}
		if fp == nil {
			fp = map[string]string{}
		}
		for k, v := range filter.NewWater() { // page containers are new to the map
			water[k] = v
		}
		for k, v := range filter.FingerprintWater() {
			fp[k] = v
		}
		if err := putIndexWater(mb, water, fp); err != nil {
			return err
		}
		if err := mb.Put([]byte(siFloorKey), []byte(floorFormat(newFloor))); err != nil {
			return err
		}
		return mb.Put([]byte(siSigKey), []byte(sig))
	})
	if err != nil {
		return nil, err
	}
	si.floorNanos.Store(floorNanos(newFloor))
	return res, nil
}

// backfillFTS is backfillSI's bleve sibling: the page's docs + merged watermarks
// commit in ONE batch; the floor file and sig file are written after (a crash
// between re-indexes the page -- idempotent, bleve Index() is an upsert).
func backfillFTS(ks *siKeyspace, def *indexDef, pages int, onDoc func(int)) (*IndexBackfillResult, error) {
	fi, err := openFTSIndex(ks, def, nil, false)
	if err != nil {
		return nil, err
	}
	ns := ks.Namespace().Name()
	srcDir := filepath.Join(ks.sds.root, ns, ks.Name())
	instDir := filepath.Dir(fi.bleveDir)

	floor := readFTSFloor(instDir, def)
	res := &IndexBackfillResult{Name: def.Name}
	if floor.IsZero() {
		res.Complete = true
		return res, nil
	}

	files, mtimes, err := backfillCandidates(srcDir, floor)
	if err != nil {
		return nil, err
	}
	page, newFloor, remaining := backfillPage(files, mtimes, pages)
	res.Floor = ""
	if !newFloor.IsZero() {
		res.Floor = newFloor.UTC().Format(time.RFC3339)
	}
	res.Complete = newFloor.IsZero()
	res.Remaining = remaining
	res.Containers = len(page)

	filter := NewRecordScanFilter(nil, nil)
	scan := filter.wrap(records.WalkPrelisted(srcDir, page, indexWalkOptions(time.Time{})))
	batch := fi.idx.NewBatch()
	var rec records.Record
	for {
		ok, err := scan.Next(&rec)
		if err != nil {
			return nil, fmt.Errorf("fts backfill %q, next: %w", def.Name, err)
		}
		if !ok {
			break
		}
		var doc interface{}
		if json.Unmarshal(rec.Doc, &doc) != nil {
			continue // skip undecodable docs (same as the full build)
		}
		if err := batch.Index(string(rec.ID), doc); err != nil {
			return nil, fmt.Errorf("fts backfill %q, index: %w", def.Name, err)
		}
		res.Docs++
		if onDoc != nil && res.Docs%512 == 0 {
			onDoc(res.Docs)
		}
	}
	if onDoc != nil {
		onDoc(res.Docs)
	}

	water, fp, err := getBleveWater(fi.idx)
	if err != nil {
		return nil, err
	}
	if water == nil {
		water = map[string]int64{}
	}
	if fp == nil {
		fp = map[string]string{}
	}
	for k, v := range filter.NewWater() {
		water[k] = v
	}
	for k, v := range filter.FingerprintWater() {
		fp[k] = v
	}
	if err := setBleveWater(batch, water, fp); err != nil {
		return nil, err
	}
	if err := fi.idx.Batch(batch); err != nil {
		return nil, err
	}
	if err := writeFTSFloor(instDir, newFloor); err != nil {
		return nil, err
	}
	sig, err := sourceSignature(srcDir, newFloor)
	if err != nil {
		return nil, err
	}
	if err := writeFTSSig(instDir, sig); err != nil {
		return nil, err
	}
	fi.floorNanos.Store(floorNanos(newFloor))
	return res, nil
}
