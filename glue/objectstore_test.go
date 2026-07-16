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

func TestObjectStoreIcebergStoreRejectsNonMetadata(t *testing.T) {
	// A bare table-dir URI (no metadata.json) is rejected with guidance -- current-metadata
	// auto-resolution over object stores (which needs listing) is a follow-up.
	_, err := FileStore("s3://my-bucket/warehouse/db/orders")
	if err == nil {
		t.Fatal("expected an error for a bare object-store table dir, got nil")
	}
	if !strings.Contains(err.Error(), "metadata JSON") {
		t.Errorf("error should guide toward a metadata JSON location; got: %v", err)
	}
}
