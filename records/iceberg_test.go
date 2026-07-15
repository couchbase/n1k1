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
// updates to the base metadata and writes the new metadata.json (no metastore/DB).
type fsIcebergCat struct {
	tbl *itable.Table
	loc string
	ver int
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
	cat := &fsIcebergCat{loc: loc}
	cat.tbl = itable.New(itable.Identifier{"default", "t"}, meta0, loc0, fsF, cat)

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
