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

// CLI FROM s3:// keyspace resolution (DESIGN-data.md §8): pointing n1k1 at an object-store
// Iceberg table metadata location exposes it as a keyspace named after the table dir.
// End-to-end read of real object bytes needs a mock S3 (dep-blocked here -- see
// records/iceberg_s3_test.go); this covers the Store/keyspace construction + error paths.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// writeGlueTestParquet writes a {id int64, name string} parquet: (1,alice) (2,carol) (3,dan).
func writeGlueTestParquet(t *testing.T, path string) {
	t.Helper()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"alice", "carol", "dan"}, nil)
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pqarrow.WriteTable(tbl, f, 3, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatal(err)
	}
}

func rowStrings(rows []json.RawMessage) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r)
	}
	return out
}

func TestObjectStoreIcebergStore(t *testing.T) {
	const loc = "s3://my-bucket/warehouse/db/orders/metadata/00003-abc.metadata.json"
	st, err := FileStore(loc)
	if err != nil {
		t.Fatalf("FileStore(%q): %v", loc, err)
	}

	ns, nerr := st.Datastore.NamespaceByName("default")
	if nerr != nil {
		t.Fatalf("NamespaceByName(default): %v", nerr)
	}

	// The table dir's last segment is the keyspace name.
	names, _ := ns.KeyspaceNames()
	if len(names) != 1 || names[0] != "orders" {
		t.Fatalf("keyspace names = %v, want [orders]", names)
	}

	ks, kerr := ns.KeyspaceByName("orders")
	if kerr != nil {
		t.Fatalf("KeyspaceByName(orders): %v", kerr)
	}
	it, ok := ks.(interface{ IcebergMetadata() string })
	if !ok || it.IcebergMetadata() != loc {
		t.Fatalf("keyspace IcebergMetadata() = %v (ok=%v), want %q", ks, ok, loc)
	}

	// Time-travel `<table>@<snapshot>` resolves to a cloned keyspace over the same metadata.
	tt, tterr := ns.KeyspaceByName("orders@12345")
	if tterr != nil {
		t.Fatalf("KeyspaceByName(orders@12345): %v", tterr)
	}
	if it2, ok := tt.(interface{ IcebergMetadata() string }); !ok || it2.IcebergMetadata() != loc {
		t.Errorf("time-travel keyspace metadata = %v (ok=%v), want %q", tt, ok, loc)
	}
}

// TestObjectStoreIcebergStoreBareDir: pointing at a bare object-store table DIR (no explicit
// metadata.json) resolves the current metadata by listing metadata/ and builds the keyspace.
// A canned ListObjectsV2 endpoint stands in for S3 (no mock-S3 dep).
func TestObjectStoreIcebergStoreBareDir(t *testing.T) {
	const listXML = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bkt</Name><Prefix>warehouse/orders/metadata/</Prefix><KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>warehouse/orders/metadata/00001-a.metadata.json</Key><LastModified>2019-01-01T00:00:00.000Z</LastModified><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>warehouse/orders/metadata/00002-b.metadata.json</Key><LastModified>2019-01-01T00:00:00.000Z</LastModified><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(listXML))
			return
		}
		http.Error(w, "NoSuchKey", http.StatusNotFound)
	}))
	defer srv.Close()

	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_ENDPOINT_URL", srv.URL)
	t.Setenv("AWS_S3_ENDPOINT", "")

	st, err := FileStore("s3://bkt/warehouse/orders") // bare dir, no metadata.json
	if err != nil {
		t.Fatalf("FileStore(bare dir): %v", err)
	}
	ns, _ := st.Datastore.NamespaceByName("default")
	ks, kerr := ns.KeyspaceByName("orders")
	if kerr != nil {
		t.Fatalf("KeyspaceByName(orders): %v", kerr)
	}
	it, ok := ks.(interface{ IcebergMetadata() string })
	want := "s3://bkt/warehouse/orders/metadata/00002-b.metadata.json"
	if !ok || it.IcebergMetadata() != want {
		t.Fatalf("resolved metadata = %v (ok=%v), want %q", ks, ok, want)
	}
}

// TestObjectStoreParquetQuery: `FROM s3://.../x.parquet` runs a real SELECT end to end.
// A local parquet is written, its bytes served over a range-honoring httptest S3 endpoint,
// and a projection + COUNT(*) query runs through the planner -> remote scan -> rows.
func TestObjectStoreParquetQuery(t *testing.T) {
	// Build a tiny parquet {id int64, name string} and capture its bytes.
	dir := t.TempDir()
	path := filepath.Join(dir, "people.parquet")
	writeGlueTestParquet(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/data/people.parquet") {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			return
		}
		if rng := r.Header.Get("Range"); rng != "" {
			var a, b int
			fmt.Sscanf(rng, "bytes=%d-%d", &a, &b)
			if b >= len(data) {
				b = len(data) - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(b-a+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[a : b+1])
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_ENDPOINT_URL", srv.URL)
	t.Setenv("AWS_S3_ENDPOINT", "")

	s, err := OpenSession("s3://bkt/data/people.parquet", "default")
	if err != nil {
		t.Fatalf("OpenSession(remote parquet): %v", err)
	}
	defer s.Close()

	// Keyspace is named after the file stem ("people").
	res, err := s.Run("SELECT p.id, p.name FROM people AS p WHERE p.id = 2")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Rows) != 1 || string(res.Rows[0]) != `{"id":2,"name":"carol"}` {
		t.Fatalf("projected/filtered rows = %v, want [{\"id\":2,\"name\":\"carol\"}]", rowStrings(res.Rows))
	}

	cnt, err := s.Run("SELECT COUNT(*) AS n FROM people")
	if err != nil {
		t.Fatalf("Run COUNT: %v", err)
	}
	if len(cnt.Rows) != 1 || string(cnt.Rows[0]) != `{"n":3}` {
		t.Fatalf("COUNT(*) = %v, want [{\"n\":3}]", rowStrings(cnt.Rows))
	}
}

func TestObjectStoreIcebergStoreBareDirNoCreds(t *testing.T) {
	// A bare gs:// table dir now DOES attempt current-metadata listing (GCS/Azure parity);
	// without any GCS credentials in this offline test it fails cleanly rather than resolving.
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	if _, err := FileStore("gs://n1k1-nonexistent-bucket/warehouse/db/orders"); err == nil {
		t.Fatal("expected an error for a bare gs:// table dir with no credentials, got nil")
	}
}
