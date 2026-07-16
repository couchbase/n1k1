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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
  <Contents><Key>warehouse/orders/metadata/00001-a.metadata.json</Key><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>warehouse/orders/metadata/00002-b.metadata.json</Key><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>
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

func TestObjectStoreIcebergStoreRejectsUnlistable(t *testing.T) {
	// Current-metadata auto-resolution by listing is S3-only in v1; a bare gs:// table dir
	// (no explicit metadata.json) is rejected with guidance rather than silently failing.
	_, err := FileStore("gs://my-bucket/warehouse/db/orders")
	if err == nil {
		t.Fatal("expected an error for a bare gs:// table dir, got nil")
	}
	if !strings.Contains(err.Error(), "explicit") {
		t.Errorf("error should guide toward an explicit metadata location; got: %v", err)
	}
}
