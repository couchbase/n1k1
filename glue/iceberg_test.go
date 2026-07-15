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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	itable "github.com/apache/iceberg-go/table"
)

// fsIcebergCat is a minimal filesystem CatalogIO: a commit applies the updates to the base
// metadata and writes the next metadata.json (no metastore/DB).
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
