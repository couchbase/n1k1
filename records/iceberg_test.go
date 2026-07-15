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

// Read-only Iceberg spike (DESIGN-data.md §7): create a tiny Iceberg table (a filesystem
// CatalogIO + iceberg-go's AppendTable writes the Parquet data file + Avro manifests +
// commits a snapshot), then read it back through OpenIcebergTable -> the shared Arrow-batch
// transpose. Proves the read path works cgo-free with no catalog server.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	itable "github.com/apache/iceberg-go/table"
)

// fsIcebergCat is a minimal filesystem CatalogIO for the fixture: a commit applies the
// updates to the CURRENT metadata and writes the next metadata.json (no metastore/DB). It
// refreshes its cached table each commit so several AppendTable calls (one per partition)
// chain correctly.
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
	c.tbl = itable.New(c.id, newMeta, newLoc, c.fsF, c) // next append bases off this.
	return newMeta, newLoc, nil
}

// writeIcebergFixture builds an Iceberg table of {id int64, msg string} with `msgs` rows
// (msgs[i]=="" -> a NULL msg) and returns its committed metadata location.
func writeIcebergFixture(t *testing.T, dir string, msgs []string) string {
	t.Helper()
	ctx := context.Background()
	loc := filepath.Join(dir, "tbl")
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
	id := itable.Identifier{"default", "t"}
	cat := &fsIcebergCat{loc: loc, fsF: fsF, id: id}
	cat.tbl = itable.New(id, meta0, loc0, fsF, cat)

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

	committed, err := cat.tbl.AppendTable(ctx, atbl, 1024, nil)
	if err != nil {
		t.Fatal("AppendTable:", err)
	}
	return committed.MetadataLocation()
}

func TestIcebergSourceReadsRows(t *testing.T) {
	dir := t.TempDir()
	metaLoc := writeIcebergFixture(t, dir, []string{"disk full", "", "oom killed"})

	src, err := OpenIcebergTable(metaLoc, "")
	if err != nil {
		t.Fatalf("OpenIcebergTable: %v", err)
	}
	defer src.Close()

	var got []map[string]interface{}
	for {
		var rec Record
		ok, err := src.Next(&rec)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		var m map[string]interface{}
		if err := json.Unmarshal(rec.Doc, &m); err != nil {
			t.Fatalf("row not JSON: %q (%v)", rec.Doc, err)
		}
		got = append(got, m)
	}

	if len(got) != 3 {
		t.Fatalf("read %d rows, want 3: %v", len(got), got)
	}
	// id is an int64 column, msg a string; row 1's msg was NULL -> JSON null.
	if got[0]["id"] != float64(0) || got[0]["msg"] != "disk full" {
		t.Errorf("row 0 = %v, want {id:0, msg:disk full}", got[0])
	}
	if got[2]["msg"] != "oom killed" {
		t.Errorf("row 2 msg = %v, want oom killed", got[2]["msg"])
	}
	if v, ok := got[1]["msg"]; !ok || v != nil {
		t.Errorf("row 1 msg = %v (present=%v), want JSON null", v, ok)
	}
}

// writePartitionedFixture builds a table {id int64, region string} partitioned by identity
// on region, writing ONE data file per region (a separate AppendTable per partition, since
// iceberg-go infers a file's partition value from its stats -- so each file must hold a
// single region). Returns the metadata location.
func writePartitionedFixture(t *testing.T, dir string, regions []string) string {
	t.Helper()
	if raceEnabled {
		// iceberg-go v0.4.0's partitionedFanoutWriter races internally during a partitioned
		// AppendTable (its own write-path goroutines, not n1k1); the READ path we exercise is
		// race-clean. Skip building partitioned fixtures under -race.
		t.Skip("iceberg-go partitioned AppendTable has an internal data race under -race")
	}
	ctx := context.Background()
	loc := filepath.Join(dir, "tbl")
	if err := os.MkdirAll(filepath.Join(loc, "metadata"), 0o755); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(loc, "data"), 0o755)

	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 2, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
	})
	meta0, err := itable.NewMetadata(sch, &spec, itable.UnsortedSortOrder, loc, nil)
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
	id := itable.Identifier{"default", "t"}
	cat := &fsIcebergCat{loc: loc, fsF: fsF, id: id}
	cat.tbl = itable.New(id, meta0, loc0, fsF, cat)

	arrowSchema, err := itable.SchemaToArrowSchema(sch, nil, true, false)
	if err != nil {
		t.Fatal("SchemaToArrowSchema:", err)
	}
	for ri, region := range regions {
		bld := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		bld.Field(0).(*array.Int64Builder).Append(int64(ri))
		bld.Field(1).(*array.StringBuilder).Append(region)
		rec := bld.NewRecord()
		atbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
		_, err := cat.tbl.AppendTable(ctx, atbl, 1024, nil) // one file per region.
		atbl.Release()
		rec.Release()
		bld.Release()
		if err != nil {
			t.Fatalf("AppendTable(%s): %v", region, err)
		}
	}
	return cat.tbl.MetadataLocation()
}

// writeDayPartitionedFixture builds a table {id int64, ts timestamp} partitioned by day(ts),
// one data file per timestamp (a separate AppendTable each, so each file is a single day).
func writeDayPartitionedFixture(t *testing.T, dir string, stamps []string) string {
	t.Helper()
	if raceEnabled {
		t.Skip("iceberg-go partitioned AppendTable has an internal data race under -race")
	}
	ctx := context.Background()
	loc := filepath.Join(dir, "tbl")
	if err := os.MkdirAll(filepath.Join(loc, "metadata"), 0o755); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(loc, "data"), 0o755)

	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 2, FieldID: 1000, Name: "ts_day", Transform: iceberg.DayTransform{},
	})
	meta0, err := itable.NewMetadata(sch, &spec, itable.UnsortedSortOrder, loc, nil)
	if err != nil {
		t.Fatal("NewMetadata:", err)
	}
	loc0 := filepath.Join(loc, "metadata", "00000.metadata.json")
	b0, _ := json.Marshal(meta0)
	if err := os.WriteFile(loc0, b0, 0o644); err != nil {
		t.Fatal(err)
	}
	fsF := iceio.LoadFSFunc(nil, loc)
	id := itable.Identifier{"default", "t"}
	cat := &fsIcebergCat{loc: loc, fsF: fsF, id: id}
	cat.tbl = itable.New(id, meta0, loc0, fsF, cat)

	arrowSchema, err := itable.SchemaToArrowSchema(sch, nil, true, false)
	if err != nil {
		t.Fatal("SchemaToArrowSchema:", err)
	}
	for i, sVal := range stamps {
		bld := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		bld.Field(0).(*array.Int64Builder).Append(int64(i))
		ts, err := arrow.TimestampFromString(sVal, arrow.Microsecond)
		if err != nil {
			t.Fatalf("bad timestamp %q: %v", sVal, err)
		}
		bld.Field(1).(*array.TimestampBuilder).Append(ts)
		rec := bld.NewRecord()
		atbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
		_, err = cat.tbl.AppendTable(ctx, atbl, 1024, nil)
		atbl.Release()
		rec.Release()
		bld.Release()
		if err != nil {
			t.Fatalf("AppendTable(%s): %v", sVal, err)
		}
	}
	return cat.tbl.MetadataLocation()
}

// TestIcebergDayPartitionPruning proves TEMPORAL partition pruning (the headline case): a
// table partitioned by day(ts) with one file per day, where a `ts >= <cutoff>` filter from
// our converter makes iceberg-go's inclusive partition projection drop the earlier day files.
func TestIcebergDayPartitionPruning(t *testing.T) {
	dir := t.TempDir()
	metaLoc := writeDayPartitionedFixture(t, dir,
		[]string{"2024-06-01T00:00:00", "2024-06-15T00:00:00", "2024-07-01T00:00:00"})

	ctx := context.Background()
	fsF := iceio.LoadFSFunc(nil, metaLoc)
	tbl, err := itable.NewFromLocation(ctx, itable.Identifier{"iceberg", "t"}, metaLoc, fsF, nil)
	if err != nil {
		t.Fatal(err)
	}
	if all, err := tbl.Scan().PlanFiles(ctx); err != nil || len(all) != 3 {
		t.Fatalf("unfiltered scan planned %d files (err %v), want 3", len(all), err)
	}

	s := &icebergSource{ctx: ctx, tbl: tbl}
	expr, ok := s.predicateToIceberg(ScanPredicate{
		Clause: ScanClause{Field: "ts", Op: "ge", Const: "2024-06-10T00:00:00"},
	})
	if !ok {
		t.Fatal("predicateToIceberg returned ok=false for a timestamp range")
	}
	pruned, err := tbl.Scan(itable.WithRowFilter(expr)).PlanFiles(ctx)
	if err != nil {
		t.Fatal("PlanFiles(pruned):", err)
	}
	if len(pruned) != 2 {
		t.Fatalf("ts>='2024-06-10' scan planned %d files, want 2 (day-partition pruning failed)", len(pruned))
	}
}

// TestIcebergInPruning: an IN-list predicate produced by our converter prunes to just the
// matching partitions (region IN ['eu','ap'] on a 3-partition table -> 2 files).
func TestIcebergInPruning(t *testing.T) {
	dir := t.TempDir()
	metaLoc := writePartitionedFixture(t, dir, []string{"us", "eu", "ap"})

	ctx := context.Background()
	fsF := iceio.LoadFSFunc(nil, metaLoc)
	tbl, err := itable.NewFromLocation(ctx, itable.Identifier{"iceberg", "t"}, metaLoc, fsF, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &icebergSource{ctx: ctx, tbl: tbl}
	expr, ok := s.predicateToIceberg(ScanPredicate{
		Clause: ScanClause{Field: "region", Op: "in", Consts: []interface{}{"eu", "ap"}},
	})
	if !ok {
		t.Fatal("predicateToIceberg returned ok=false for an IN list")
	}
	pruned, err := tbl.Scan(itable.WithRowFilter(expr)).PlanFiles(ctx)
	if err != nil {
		t.Fatal("PlanFiles:", err)
	}
	if len(pruned) != 2 {
		t.Fatalf("region IN ['eu','ap'] planned %d files, want 2", len(pruned))
	}
}

// TestIcebergLikeRangePruning: `region LIKE 'e%'` becomes the range `region >= 'e' AND
// region < 'f'`, which prunes a region-partitioned table to just the "eu" file (1 of 3).
func TestIcebergLikeRangePruning(t *testing.T) {
	dir := t.TempDir()
	metaLoc := writePartitionedFixture(t, dir, []string{"us", "eu", "ap"})

	ctx := context.Background()
	fsF := iceio.LoadFSFunc(nil, metaLoc)
	tbl, err := itable.NewFromLocation(ctx, itable.Identifier{"iceberg", "t"}, metaLoc, fsF, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &icebergSource{ctx: ctx, tbl: tbl}
	expr, ok := s.predicateToIceberg(ScanPredicate{Bool: "and", Children: []ScanPredicate{
		{Clause: ScanClause{Field: "region", Op: "ge", Const: "e"}},
		{Clause: ScanClause{Field: "region", Op: "lt", Const: "f"}},
	}})
	if !ok {
		t.Fatal("predicateToIceberg returned ok=false for a string range")
	}
	pruned, err := tbl.Scan(itable.WithRowFilter(expr)).PlanFiles(ctx)
	if err != nil {
		t.Fatal("PlanFiles:", err)
	}
	if len(pruned) != 1 {
		t.Fatalf("region LIKE 'e%%' range planned %d files, want 1", len(pruned))
	}
}

// TestIcebergNestedPruning proves a genuinely nested boolean prunes: on a region-partitioned
// table, `(region='eu' AND id>=0) OR region='ap'` projects onto the partition column to
// `region='eu' OR region='ap'` and plans just those 2 of 3 files.
func TestIcebergNestedPruning(t *testing.T) {
	dir := t.TempDir()
	metaLoc := writePartitionedFixture(t, dir, []string{"us", "eu", "ap"})

	ctx := context.Background()
	fsF := iceio.LoadFSFunc(nil, metaLoc)
	tbl, err := itable.NewFromLocation(ctx, itable.Identifier{"iceberg", "t"}, metaLoc, fsF, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &icebergSource{ctx: ctx, tbl: tbl}
	pred := ScanPredicate{Bool: "or", Children: []ScanPredicate{
		{Bool: "and", Children: []ScanPredicate{
			{Clause: ScanClause{Field: "region", Op: "eq", Const: "eu"}},
			{Clause: ScanClause{Field: "id", Op: "ge", Const: float64(0)}},
		}},
		{Clause: ScanClause{Field: "region", Op: "eq", Const: "ap"}},
	}}
	expr, ok := s.predicateToIceberg(pred)
	if !ok {
		t.Fatal("predicateToIceberg returned ok=false for a nested boolean")
	}
	pruned, err := tbl.Scan(itable.WithRowFilter(expr)).PlanFiles(ctx)
	if err != nil {
		t.Fatal("PlanFiles:", err)
	}
	if len(pruned) != 2 {
		t.Fatalf("(region=eu AND id>=0) OR region=ap planned %d files, want 2", len(pruned))
	}
}

// TestIcebergPartitionPruning proves that a predicate produced by our converter
// (predicateToIceberg) prunes partitions: on a table with one data file per region, an
// `region = 'eu'` filter makes iceberg-go plan FEWER files than an unfiltered scan.
func TestIcebergPartitionPruning(t *testing.T) {
	dir := t.TempDir()
	metaLoc := writePartitionedFixture(t, dir, []string{"us", "eu", "ap"})

	ctx := context.Background()
	fsF := iceio.LoadFSFunc(nil, metaLoc)
	tbl, err := itable.NewFromLocation(ctx, itable.Identifier{"iceberg", "t"}, metaLoc, fsF, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Baseline: an unfiltered scan plans all 3 partition files.
	allFiles, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal("PlanFiles(all):", err)
	}
	if len(allFiles) != 3 {
		t.Fatalf("unfiltered scan planned %d files, want 3", len(allFiles))
	}

	// The filter our source would build for `region = 'eu'`.
	s := &icebergSource{ctx: ctx, tbl: tbl}
	expr, ok := s.predicateToIceberg(ScanPredicate{
		Clause: ScanClause{Field: "region", Op: "eq", Const: "eu"},
	})
	if !ok {
		t.Fatal("predicateToIceberg returned ok=false")
	}
	pruned, err := tbl.Scan(itable.WithRowFilter(expr)).PlanFiles(ctx)
	if err != nil {
		t.Fatal("PlanFiles(pruned):", err)
	}
	if len(pruned) != 1 {
		t.Fatalf("region='eu' scan planned %d files, want 1 (partition pruning failed)", len(pruned))
	}

	// And end-to-end through the source, the rows are exactly the eu partition.
	if err := s.SetRowFilter(ScanPredicate{
		Clause: ScanClause{Field: "region", Op: "eq", Const: "eu"},
	}); err != nil {
		t.Fatal("SetRowFilter:", err)
	}
	var rows []map[string]interface{}
	for {
		var rec Record
		okRow, err := s.Next(&rec)
		if err != nil {
			t.Fatal("Next:", err)
		}
		if !okRow {
			break
		}
		var m map[string]interface{}
		if err := json.Unmarshal(rec.Doc, &m); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, m)
	}
	s.Close()
	if len(rows) != 1 || rows[0]["region"] != "eu" {
		t.Fatalf("filtered rows = %v, want one eu row", rows)
	}
}
