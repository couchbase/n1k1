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

// End-to-end: a directory that IS (or CONTAINS) an Apache Iceberg table becomes a
// queryable keyspace, so `SELECT * FROM <table>` runs through the normal session path
// (flat.go maybeIcebergTable -> KeyspaceRecordsOpen -> records.OpenIcebergTable). The
// fixture is built with iceberg-go (mirrors records/iceberg_test.go) since Iceberg metadata
// embeds absolute paths, so a committed testdata fixture wouldn't be portable.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	itable "github.com/apache/iceberg-go/table"

	"github.com/couchbase/n1k1/records"
)

// fsIcebergCat is a minimal filesystem CatalogIO: a commit applies the updates to the CURRENT
// metadata and writes the next metadata.json (no metastore/DB). It refreshes its cached table
// each commit so multiple AppendTable calls (successive snapshots) chain correctly.
type fsIcebergCat struct {
	tbl *itable.Table
	loc string
	ver int
	fsF itable.FSysF
	id  itable.Identifier
}

func (c *fsIcebergCat) LoadTable(ctx context.Context, id itable.Identifier) (*itable.Table, error) {
	return c.tbl, nil
}
func (c *fsIcebergCat) CommitTable(ctx context.Context, id itable.Identifier, reqs []itable.Requirement, updates []itable.Update) (itable.Metadata, string, error) {
	c.ver++
	newLoc := filepath.Join(c.loc, "metadata", fmt.Sprintf("%05d.metadata.json", c.ver))
	newMeta, err := itable.UpdateTableMetadata(c.tbl.Metadata(), updates, newLoc)
	if err != nil {
		return nil, "", err
	}
	b, err := json.Marshal(newMeta)
	if err != nil {
		return nil, "", err
	}
	if err := os.WriteFile(strings.TrimPrefix(newLoc, "file://"), b, 0o644); err != nil {
		return nil, "", err
	}
	if c.fsF != nil {
		c.tbl = itable.New(c.id, newMeta, newLoc, c.fsF, c) // next append bases off this.
	}
	return newMeta, newLoc, nil
}

// writeIcebergTable builds an Iceberg table {id int64, msg string} named `name` under dir,
// with `msgs` rows (msgs[i]=="" -> NULL msg).
func writeIcebergTable(t *testing.T, dir, name string, msgs []string) {
	t.Helper()
	ctx := context.Background()
	loc := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Join(loc, "metadata"), 0o755); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(loc, "data"), 0o755)

	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "msg", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta0, err := itable.NewMetadata(sch, iceberg.UnpartitionedSpec, itable.UnsortedSortOrder, loc, nil)
	if err != nil {
		t.Fatal("NewMetadata:", err)
	}
	loc0 := filepath.Join(loc, "metadata", "00000.metadata.json")
	b0, err := json.Marshal(meta0)
	if err != nil {
		t.Fatal("marshal meta0:", err)
	}
	if err := os.WriteFile(loc0, b0, 0o644); err != nil {
		t.Fatal(err)
	}

	fsF := iceio.LoadFSFunc(nil, loc)
	cat := &fsIcebergCat{loc: loc}
	cat.tbl = itable.New(itable.Identifier{"default", name}, meta0, loc0, fsF, cat)

	arrowSchema, err := itable.SchemaToArrowSchema(sch, nil, true, false)
	if err != nil {
		t.Fatal("SchemaToArrowSchema:", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bld.Release()
	idB := bld.Field(0).(*array.Int64Builder)
	msgB := bld.Field(1).(*array.StringBuilder)
	for i, m := range msgs {
		idB.Append(int64(i))
		if m == "" {
			msgB.AppendNull()
		} else {
			msgB.Append(m)
		}
	}
	rec := bld.NewRecord()
	defer rec.Release()
	atbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer atbl.Release()

	if _, err := cat.tbl.AppendTable(ctx, atbl, 1024, nil); err != nil {
		t.Fatal("AppendTable:", err)
	}
}

// TestIcebergKeyspaceQuery points a session at a directory holding an Iceberg table and
// runs SQL++ over it: the table shows up as a keyspace by its basename, and both a
// full-row and a projected/filtered query return the committed rows.
func TestIcebergKeyspaceQuery(t *testing.T) {
	root := t.TempDir()
	writeIcebergTable(t, root, "events", []string{"disk full", "", "oom killed"})

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rowsOf := func(stmt string) []string {
		t.Helper()
		res, err := s.Run(stmt)
		if err != nil {
			t.Fatalf("Run(%q): %v", stmt, err)
		}
		got := make([]string, len(res.Rows))
		for i, r := range res.Rows {
			got[i] = string(r)
		}
		sort.Strings(got)
		return got
	}

	// Full scan of the table keyspace (named after the table dir basename).
	all := rowsOf("SELECT e.id, e.msg FROM events AS e")
	want := []string{
		`{"id":0,"msg":"disk full"}`,
		`{"id":1,"msg":null}`, // NULL msg -> explicit JSON null.
		`{"id":2,"msg":"oom killed"}`,
	}
	if len(all) != len(want) {
		t.Fatalf("SELECT * rows = %v, want %v", all, want)
	}
	for i := range want {
		if all[i] != want[i] {
			t.Errorf("row %d = %s, want %s", i, all[i], want[i])
		}
	}

	// A predicate + projection still runs through the same scan.
	if g := rowsOf("SELECT e.msg FROM events AS e WHERE e.id = 2"); len(g) != 1 ||
		g[0] != `{"msg":"oom killed"}` {
		t.Errorf(`WHERE id=2 = %v, want [{"msg":"oom killed"}]`, g)
	}

	// COUNT(*) over the table.
	if g := rowsOf("SELECT COUNT(*) AS n FROM events"); len(g) != 1 || g[0] != `{"n":3}` {
		t.Errorf(`COUNT(*) = %v, want [{"n":3}]`, g)
	}
}

// runIceberg opens a session over root and returns a helper that runs a statement and
// returns its rows sorted.
func runIceberg(t *testing.T, root string) (func(string) []string, func()) {
	t.Helper()
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	rowsOf := func(stmt string) []string {
		t.Helper()
		res, err := s.Run(stmt)
		if err != nil {
			t.Fatalf("Run(%q): %v", stmt, err)
		}
		got := make([]string, len(res.Rows))
		for i, r := range res.Rows {
			got[i] = string(r)
		}
		sort.Strings(got)
		return got
	}
	return rowsOf, func() { s.Close() }
}

// writeIcebergTSTable builds a table {id int64, ts timestamp} with the given RFC3339 (no-tz)
// timestamps, one row each.
func writeIcebergTSTable(t *testing.T, dir, name string, stamps []string) {
	t.Helper()
	ctx := context.Background()
	loc := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Join(loc, "metadata"), 0o755); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(loc, "data"), 0o755)

	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
	)
	meta0, err := itable.NewMetadata(sch, iceberg.UnpartitionedSpec, itable.UnsortedSortOrder, loc, nil)
	if err != nil {
		t.Fatal("NewMetadata:", err)
	}
	loc0 := filepath.Join(loc, "metadata", "00000.metadata.json")
	b0, _ := json.Marshal(meta0)
	if err := os.WriteFile(loc0, b0, 0o644); err != nil {
		t.Fatal(err)
	}
	fsF := iceio.LoadFSFunc(nil, loc)
	cat := &fsIcebergCat{loc: loc}
	cat.tbl = itable.New(itable.Identifier{"default", name}, meta0, loc0, fsF, cat)

	arrowSchema, err := itable.SchemaToArrowSchema(sch, nil, true, false)
	if err != nil {
		t.Fatal("SchemaToArrowSchema:", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bld.Release()
	tsB := bld.Field(1).(*array.TimestampBuilder)
	for i, sVal := range stamps {
		bld.Field(0).(*array.Int64Builder).Append(int64(i))
		ts, err := arrow.TimestampFromString(sVal, arrow.Microsecond)
		if err != nil {
			t.Fatalf("bad timestamp %q: %v", sVal, err)
		}
		tsB.Append(ts)
	}
	rec := bld.NewRecord()
	defer rec.Release()
	atbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer atbl.Release()
	if _, err := cat.tbl.AppendTable(ctx, atbl, 1024, nil); err != nil {
		t.Fatal("AppendTable:", err)
	}
}

// TestIcebergTemporalPushdown: a timestamp range WHERE is pushed into the scan (temporal
// columns are the common partition key) and returns correct rows. Timestamps render as
// ISO-8601, so the engine's string comparison is chronologically correct.
func TestIcebergTemporalPushdown(t *testing.T) {
	root := t.TempDir()
	writeIcebergTSTable(t, root, "events",
		[]string{"2024-06-01T00:00:00", "2024-06-15T00:00:00", "2024-07-01T00:00:00"})
	rowsOf, done := runIceberg(t, root)
	defer done()

	before := atomic.LoadInt64(&records.IcebergRowFilterApplied)
	got := rowsOf("SELECT e.id FROM events AS e WHERE e.ts >= '2024-06-10T00:00:00'")
	want := []string{`{"id":1}`, `{"id":2}`}
	if len(got) != len(want) {
		t.Fatalf("temporal range rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %s, want %s", i, got[i], want[i])
		}
	}
	if atomic.LoadInt64(&records.IcebergRowFilterApplied) <= before {
		t.Error("temporal predicate was not pushed into the iceberg-go scan")
	}
}

// appendIcebergIDs commits one snapshot appending {id} rows for each id.
func appendIcebergIDs(t *testing.T, ctx context.Context, cat *fsIcebergCat, as *arrow.Schema, ids []int64) {
	t.Helper()
	bld := array.NewRecordBuilder(memory.DefaultAllocator, as)
	for _, id := range ids {
		bld.Field(0).(*array.Int64Builder).Append(id)
	}
	rec := bld.NewRecord()
	atbl := array.NewTableFromRecords(as, []arrow.Record{rec})
	_, err := cat.tbl.AppendTable(ctx, atbl, 1024, nil)
	atbl.Release()
	rec.Release()
	bld.Release()
	if err != nil {
		t.Fatal("AppendTable:", err)
	}
}

// writeIcebergTwoSnapshots builds an {id int64} table with TWO snapshots (ids 0,1 then 2,3)
// and returns the FIRST snapshot's id, so a time-travel query can read the older state.
func writeIcebergTwoSnapshots(t *testing.T, dir, name string) int64 {
	t.Helper()
	ctx := context.Background()
	loc := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Join(loc, "metadata"), 0o755); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(loc, "data"), 0o755)

	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	meta0, err := itable.NewMetadata(sch, iceberg.UnpartitionedSpec, itable.UnsortedSortOrder, loc, nil)
	if err != nil {
		t.Fatal("NewMetadata:", err)
	}
	loc0 := filepath.Join(loc, "metadata", "00000.metadata.json")
	b0, _ := json.Marshal(meta0)
	if err := os.WriteFile(loc0, b0, 0o644); err != nil {
		t.Fatal(err)
	}
	fsF := iceio.LoadFSFunc(nil, loc)
	id := itable.Identifier{"default", name}
	cat := &fsIcebergCat{loc: loc, fsF: fsF, id: id}
	cat.tbl = itable.New(id, meta0, loc0, fsF, cat)

	arrowSchema, err := itable.SchemaToArrowSchema(sch, nil, true, false)
	if err != nil {
		t.Fatal("SchemaToArrowSchema:", err)
	}
	appendIcebergIDs(t, ctx, cat, arrowSchema, []int64{0, 1})
	snap1 := cat.tbl.Metadata().CurrentSnapshot().SnapshotID
	appendIcebergIDs(t, ctx, cat, arrowSchema, []int64{2, 3})
	return snap1
}

// TestIcebergTimeTravel: a `<table>@<snapshot-id>` keyspace reads that past snapshot, and
// `<table>@<timestamp>` reads the snapshot current at that time (far-future => latest).
func TestIcebergTimeTravel(t *testing.T) {
	root := t.TempDir()
	snap1 := writeIcebergTwoSnapshots(t, root, "events")

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	count := func(stmt string) (string, error) {
		res, err := s.Run(stmt)
		if err != nil {
			return "", err
		}
		if len(res.Rows) != 1 {
			return "", fmt.Errorf("%q: %d rows, want 1", stmt, len(res.Rows))
		}
		return string(res.Rows[0]), nil
	}

	// Current: both snapshots -> 4 rows.
	if g, err := count("SELECT COUNT(*) AS n FROM events"); err != nil || g != `{"n":4}` {
		t.Fatalf("current = %v (err %v), want 4", g, err)
	}

	// By snapshot id: only the first snapshot -> 2 rows. COUNT(*) is answered from the
	// snapshot's manifest metadata (no data scan).
	if g, err := count(fmt.Sprintf("SELECT COUNT(*) AS n FROM `events@%d`", snap1)); err != nil ||
		g != `{"n":2}` {
		t.Fatalf("@snapshot1 = %v (err %v), want 2", g, err)
	}
	// A row-returning query over the same snapshot exercises the data-scan path, so the
	// snapshot selector reaches iceberg-go's ToArrowRecords (counter bumps).
	before := atomic.LoadInt64(&records.IcebergSnapshotApplied)
	if res, err := s.Run(fmt.Sprintf("SELECT e.id FROM `events@%d` AS e", snap1)); err != nil ||
		len(res.Rows) != 2 {
		t.Fatalf("@snapshot1 rows = %v (err %v), want 2", res.Rows, err)
	}
	if atomic.LoadInt64(&records.IcebergSnapshotApplied) <= before {
		t.Error("snapshot selection was not applied to the iceberg-go scan")
	}

	// As-of a far-FUTURE timestamp -> latest snapshot -> 4 rows.
	if g, err := count("SELECT COUNT(*) AS n FROM `events@2099-01-01T00:00:00Z`"); err != nil ||
		g != `{"n":4}` {
		t.Fatalf("@future = %v (err %v), want 4", g, err)
	}

	// As-of a far-PAST timestamp (before any snapshot) -> iceberg-go finds none -> error.
	if _, err := count("SELECT COUNT(*) AS n FROM `events@2000-01-01T00:00:00Z`"); err == nil {
		t.Error("@past should error (no snapshot that old), got success")
	}
}

// writeIcebergAmounts builds an {id int64, amt double} table (one snapshot).
func writeIcebergAmounts(t *testing.T, dir, name string, amts []float64) {
	t.Helper()
	ctx := context.Background()
	loc := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Join(loc, "metadata"), 0o755); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(loc, "data"), 0o755)

	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "amt", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)
	meta0, err := itable.NewMetadata(sch, iceberg.UnpartitionedSpec, itable.UnsortedSortOrder, loc, nil)
	if err != nil {
		t.Fatal("NewMetadata:", err)
	}
	loc0 := filepath.Join(loc, "metadata", "00000.metadata.json")
	b0, _ := json.Marshal(meta0)
	if err := os.WriteFile(loc0, b0, 0o644); err != nil {
		t.Fatal(err)
	}
	fsF := iceio.LoadFSFunc(nil, loc)
	cat := &fsIcebergCat{loc: loc}
	cat.tbl = itable.New(itable.Identifier{"default", name}, meta0, loc0, fsF, cat)

	arrowSchema, err := itable.SchemaToArrowSchema(sch, nil, true, false)
	if err != nil {
		t.Fatal("SchemaToArrowSchema:", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bld.Release()
	amtB := bld.Field(1).(*array.Float64Builder)
	for i, a := range amts {
		bld.Field(0).(*array.Int64Builder).Append(int64(i))
		amtB.Append(a)
	}
	rec := bld.NewRecord()
	defer rec.Release()
	atbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer atbl.Release()
	if _, err := cat.tbl.AppendTable(ctx, atbl, 1024, nil); err != nil {
		t.Fatal("AppendTable:", err)
	}
}

// TestIcebergColumnarAgg: an ungrouped aggregation over an Iceberg table takes the vectorized
// columnar path (NextColumns -- no JSON transpose), and its results match the row path.
func TestIcebergColumnarAgg(t *testing.T) {
	root := t.TempDir()
	writeIcebergAmounts(t, root, "sales", []float64{10, 20, 30, 40}) // ids 0..3.
	rowsOf, done := runIceberg(t, root)
	defer done()

	before := atomic.LoadInt64(&AggColumnarApplied)
	if g := rowsOf("SELECT SUM(e.amt) AS s FROM sales AS e"); len(g) != 1 || g[0] != `{"s":100}` {
		t.Errorf("SUM(amt) = %v, want {\"s\":100}", g)
	}
	if atomic.LoadInt64(&AggColumnarApplied) <= before {
		t.Error("ungrouped SUM over Iceberg did not take the columnar (NextColumns) path")
	}

	// SUM over a WHERE (the vectorized masked-predicate path reads both columns via NextColumns).
	if g := rowsOf("SELECT SUM(e.amt) AS s FROM sales AS e WHERE e.id >= 2"); len(g) != 1 ||
		g[0] != `{"s":70}` {
		t.Errorf("SUM(amt) WHERE id>=2 = %v, want {\"s\":70}", g)
	}

	// AVG + SUM(id) also columnar.
	if g := rowsOf("SELECT AVG(e.amt) AS a, SUM(e.id) AS s FROM sales AS e"); len(g) != 1 ||
		g[0] != `{"a":25,"s":6}` {
		t.Errorf("AVG(amt),SUM(id) = %v, want {\"a\":25,\"s\":6}", g)
	}

	// Parity with the row path.
	DisableColumnarOptimize = true
	defer func() { DisableColumnarOptimize = false }()
	if g := rowsOf("SELECT SUM(e.amt) AS s FROM sales AS e WHERE e.id >= 2"); len(g) != 1 ||
		g[0] != `{"s":70}` {
		t.Errorf("row-path SUM = %v, want {\"s\":70}", g)
	}
}

// TestIcebergMetadataAgg: COUNT(*), MIN and MAX over a delete-free Iceberg table are answered
// from the manifest stats alone (the agg-metadata path -- no data scan), and match the values
// a real scan produces.
func TestIcebergMetadataAgg(t *testing.T) {
	root := t.TempDir()
	writeIcebergAmounts(t, root, "sales", []float64{10, 20, 30, 40}) // ids 0..3.
	rowsOf, done := runIceberg(t, root)
	defer done()

	before := atomic.LoadInt64(&AggMetadataApplied)
	if g := rowsOf("SELECT COUNT(*) AS n FROM sales"); len(g) != 1 || g[0] != `{"n":4}` {
		t.Errorf("COUNT(*) = %v, want 4", g)
	}
	if g := rowsOf("SELECT MIN(e.amt) AS lo, MAX(e.amt) AS hi FROM sales AS e"); len(g) != 1 ||
		g[0] != `{"lo":10,"hi":40}` {
		t.Errorf("MIN/MAX(amt) = %v, want lo 10 hi 40", g)
	}
	if g := rowsOf("SELECT MIN(e.id) AS lo, MAX(e.id) AS hi FROM sales AS e"); len(g) != 1 ||
		g[0] != `{"lo":0,"hi":3}` {
		t.Errorf("MIN/MAX(id) = %v, want lo 0 hi 3", g)
	}
	if atomic.LoadInt64(&AggMetadataApplied) <= before {
		t.Error("COUNT/MIN/MAX over Iceberg did not take the metadata path")
	}

	// Parity: the same answers from a real scan.
	DisableColumnarOptimize = true
	defer func() { DisableColumnarOptimize = false }()
	if g := rowsOf("SELECT COUNT(*) AS n FROM sales"); len(g) != 1 || g[0] != `{"n":4}` {
		t.Errorf("row-path COUNT(*) = %v, want 4", g)
	}
	if g := rowsOf("SELECT MIN(e.amt) AS lo, MAX(e.amt) AS hi FROM sales AS e"); len(g) != 1 ||
		g[0] != `{"lo":10,"hi":40}` {
		t.Errorf("row-path MIN/MAX(amt) = %v, want lo 10 hi 40", g)
	}
}

// TestIcebergProjectionPushdown: a query reading a subset of columns pushes WithSelectedFields
// into the iceberg-go scan (IcebergProjectionApplied bumps) and still returns correct rows.
func TestIcebergProjectionPushdown(t *testing.T) {
	root := t.TempDir()
	writeIcebergTable(t, root, "events", []string{"disk full", "slow query", "oom killed"})
	rowsOf, done := runIceberg(t, root)
	defer done()

	before := atomic.LoadInt64(&records.IcebergProjectionApplied)
	got := rowsOf("SELECT e.id FROM events AS e") // reads only `id`, not `msg`.
	want := []string{`{"id":0}`, `{"id":1}`, `{"id":2}`}
	if len(got) != len(want) {
		t.Fatalf("projected rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %s, want %s", i, got[i], want[i])
		}
	}
	if atomic.LoadInt64(&records.IcebergProjectionApplied) <= before {
		t.Error("projection was not pushed into the iceberg-go scan")
	}
}

// TestIcebergPredicatePushdown: numeric and string WHEREs push WithRowFilter into the scan
// (IcebergRowFilterApplied bumps) and return the same rows as with pushdown disabled.
func TestIcebergPredicatePushdown(t *testing.T) {
	root := t.TempDir()
	writeIcebergTable(t, root, "events", []string{"disk full", "slow query", "oom killed"})
	rowsOf, done := runIceberg(t, root)
	defer done()

	// Numeric predicate.
	before := atomic.LoadInt64(&records.IcebergRowFilterApplied)
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.id = 2"); len(g) != 1 ||
		g[0] != `{"id":2}` {
		t.Errorf(`WHERE id=2 = %v, want [{"id":2}]`, g)
	}
	if atomic.LoadInt64(&records.IcebergRowFilterApplied) <= before {
		t.Error("numeric predicate was not pushed into the iceberg-go scan")
	}

	// String equality predicate.
	before = atomic.LoadInt64(&records.IcebergRowFilterApplied)
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.msg = 'oom killed'"); len(g) != 1 ||
		g[0] != `{"id":2}` {
		t.Errorf(`WHERE msg='oom killed' = %v, want [{"id":2}]`, g)
	}
	if atomic.LoadInt64(&records.IcebergRowFilterApplied) <= before {
		t.Error("string predicate was not pushed into the iceberg-go scan")
	}

	// Range predicate (cbq normalizes id >= 1 to a LT/LE-swapped form).
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.id >= 1"); len(g) != 2 {
		t.Errorf(`WHERE id>=1 = %v, want 2 rows`, g)
	}
}

// TestIcebergRicherPredicates: IN / != / IS [NOT] NULL all return correct rows, and at least
// one of these richer shapes is genuinely pushed into the scan.
func TestIcebergRicherPredicates(t *testing.T) {
	root := t.TempDir()
	writeIcebergTable(t, root, "events", []string{"disk full", "", "oom killed"}) // row 1 msg is NULL.
	rowsOf, done := runIceberg(t, root)
	defer done()

	before := atomic.LoadInt64(&records.IcebergRowFilterApplied)

	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.msg IN ['disk full', 'oom killed']"); len(g) != 2 ||
		g[0] != `{"id":0}` || g[1] != `{"id":2}` {
		t.Errorf("IN list = %v, want id 0 and 2", g)
	}
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.id != 1"); len(g) != 2 ||
		g[0] != `{"id":0}` || g[1] != `{"id":2}` {
		t.Errorf("!= = %v, want id 0 and 2", g)
	}
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.msg IS NULL"); len(g) != 1 ||
		g[0] != `{"id":1}` {
		t.Errorf("IS NULL = %v, want id 1", g)
	}
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.msg IS NOT NULL"); len(g) != 2 ||
		g[0] != `{"id":0}` || g[1] != `{"id":2}` {
		t.Errorf("IS NOT NULL = %v, want id 0 and 2", g)
	}
	// A pushable predicate mixed with an unpushable one (a UDF-ish expr) still pushes the
	// pushable conjunct (AND drops the rest).
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.id >= 1 AND UPPER(e.msg) = 'OOM KILLED'"); len(g) != 1 ||
		g[0] != `{"id":2}` {
		t.Errorf("mixed AND = %v, want id 2", g)
	}

	if atomic.LoadInt64(&records.IcebergRowFilterApplied) <= before {
		t.Error("no richer predicate was pushed into the iceberg-go scan")
	}
}

// TestIcebergNestedBoolean: genuinely nested boolean -- `(a AND b) OR c` and a De-Morgan
// `NOT((a OR b))` -- return correct rows and are pushed into the scan.
func TestIcebergNestedBoolean(t *testing.T) {
	root := t.TempDir()
	writeIcebergTable(t, root, "events", []string{"disk full", "", "oom killed"}) // id1 msg NULL.
	rowsOf, done := runIceberg(t, root)
	defer done()

	before := atomic.LoadInt64(&records.IcebergRowFilterApplied)

	// (id >= 2 AND msg = 'oom killed') OR id = 0  ->  id 0 and 2.
	if g := rowsOf("SELECT e.id FROM events AS e WHERE (e.id >= 2 AND e.msg = 'oom killed') OR e.id = 0"); len(g) != 2 ||
		g[0] != `{"id":0}` || g[1] != `{"id":2}` {
		t.Errorf("(a AND b) OR c = %v, want id 0 and 2", g)
	}
	// De Morgan: NOT(id = 1 OR msg IS NULL)  ==  id != 1 AND msg IS NOT NULL  ->  id 0 and 2.
	if g := rowsOf("SELECT e.id FROM events AS e WHERE NOT (e.id = 1 OR e.msg IS NULL)"); len(g) != 2 ||
		g[0] != `{"id":0}` || g[1] != `{"id":2}` {
		t.Errorf("NOT(a OR b) = %v, want id 0 and 2", g)
	}
	// Nested with an unpushable leaf under an inner AND: the AND drops it, the OR still pushes.
	if g := rowsOf("SELECT e.id FROM events AS e WHERE (e.id = 0 AND UPPER(e.msg) = 'DISK FULL') OR e.id = 2"); len(g) != 2 ||
		g[0] != `{"id":0}` || g[1] != `{"id":2}` {
		t.Errorf("(a AND weird) OR c = %v, want id 0 and 2", g)
	}

	if atomic.LoadInt64(&records.IcebergRowFilterApplied) <= before {
		t.Error("no nested boolean was pushed into the iceberg-go scan")
	}
}

// TestIcebergLikePrefix: `field LIKE 'prefix%'` pushes as StartsWith and returns correct
// rows; non-prefix LIKE (leading/interior wildcard, `_`) still returns correct rows (just
// not pushed); NOT LIKE 'prefix%' works via De Morgan.
func TestIcebergLikePrefix(t *testing.T) {
	root := t.TempDir()
	writeIcebergTable(t, root, "events", []string{"disk full", "disk slow", "oom killed"})
	rowsOf, done := runIceberg(t, root)
	defer done()

	before := atomic.LoadInt64(&records.IcebergRowFilterApplied)

	// Prefix LIKE -> StartsWith: "disk full", "disk slow".
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.msg LIKE 'disk%'"); len(g) != 2 ||
		g[0] != `{"id":0}` || g[1] != `{"id":1}` {
		t.Errorf("LIKE 'disk%%' = %v, want id 0 and 1", g)
	}
	if atomic.LoadInt64(&records.IcebergRowFilterApplied) <= before {
		t.Error("LIKE prefix was not pushed as StartsWith")
	}

	// NOT LIKE prefix -> NotStartsWith: only "oom killed".
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.msg NOT LIKE 'disk%'"); len(g) != 1 ||
		g[0] != `{"id":2}` {
		t.Errorf("NOT LIKE 'disk%%' = %v, want id 2", g)
	}

	// Non-prefix patterns aren't pushed but must still be correct.
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.msg LIKE '%killed'"); len(g) != 1 ||
		g[0] != `{"id":2}` {
		t.Errorf("LIKE '%%killed' = %v, want id 2", g)
	}
	if g := rowsOf("SELECT e.id FROM events AS e WHERE e.msg LIKE 'disk_full'"); len(g) != 1 ||
		g[0] != `{"id":0}` {
		t.Errorf("LIKE 'disk_full' = %v, want id 0", g)
	}
}

// TestIcebergPushdownParity: pushed results must equal unpushed results (pushdown is a pure
// optimization; the engine's filter/projection still runs).
func TestIcebergPushdownParity(t *testing.T) {
	root := t.TempDir()
	writeIcebergTable(t, root, "events", []string{"disk full", "slow query", "oom killed", ""})

	stmts := []string{
		"SELECT e.id, e.msg FROM events AS e WHERE e.id = 2",
		"SELECT e.id FROM events AS e WHERE e.id >= 1 AND e.msg = 'oom killed'",
		"SELECT e.msg FROM events AS e WHERE e.id = 0 OR e.id = 2",
		"SELECT COUNT(*) AS n FROM events AS e WHERE e.id < 2",
		"SELECT e.id FROM events AS e WHERE e.msg IN ['disk full', 'oom killed']",
		"SELECT e.id FROM events AS e WHERE e.id != 1",
		"SELECT e.id FROM events AS e WHERE e.msg IS NULL",
		"SELECT e.id FROM events AS e WHERE e.msg IS NOT NULL",
		"SELECT e.id FROM events AS e WHERE e.id NOT IN [0, 1]",
		"SELECT e.id FROM events AS e WHERE e.id >= 1 AND UPPER(e.msg) = 'OOM KILLED'",
		"SELECT e.id FROM events AS e WHERE (e.id >= 2 AND e.msg = 'oom killed') OR e.id = 0",
		"SELECT e.id FROM events AS e WHERE NOT (e.id = 1 OR e.msg IS NULL)",
		"SELECT e.id FROM events AS e WHERE (e.id = 0 AND UPPER(e.msg) = 'DISK FULL') OR e.id = 2",
		"SELECT e.id FROM events AS e WHERE e.msg LIKE 'oom%'",
		"SELECT e.id FROM events AS e WHERE e.msg NOT LIKE 'disk%'",
		"SELECT e.id FROM events AS e WHERE e.msg LIKE '%killed' OR e.id = 0",
	}
	for _, stmt := range stmts {
		rp, doneP := runIceberg(t, root)
		pushed := rp(stmt)
		doneP()

		DisableRowFilterPushdown = true
		DisableColumnProjection = true
		ru, doneU := runIceberg(t, root)
		unpushed := ru(stmt)
		doneU()
		DisableRowFilterPushdown = false
		DisableColumnProjection = false

		if len(pushed) != len(unpushed) {
			t.Fatalf("%q: pushed %v != unpushed %v", stmt, pushed, unpushed)
		}
		for i := range pushed {
			if pushed[i] != unpushed[i] {
				t.Errorf("%q row %d: pushed %s != unpushed %s", stmt, i, pushed[i], unpushed[i])
			}
		}
	}
}

// TestIcebergTableDirIsKeyspace points the session directly AT the table dir (rather than a
// parent): the basename still becomes the keyspace.
func TestIcebergTableDirIsKeyspace(t *testing.T) {
	root := t.TempDir()
	writeIcebergTable(t, root, "logs", []string{"a", "b"})

	s, err := OpenSession(filepath.Join(root, "logs"), "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	res, err := s.Run("SELECT COUNT(*) AS n FROM logs")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Rows) != 1 || string(res.Rows[0]) != `{"n":2}` {
		t.Errorf("COUNT(*) = %v, want [{\"n\":2}]", res.Rows)
	}
}
