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

// Object-store (S3) wiring test for the Iceberg source (DESIGN-data.md §8). A full
// in-process mock S3 (gofakes3) can't be vendored in this standalone worktree -- the
// go.mod pins several couchbase sibling modules at an unresolvable zero pseudo-version
// (they're local-path replaced only in the repo-sync build), so `go get` can't load the
// build list. Instead this proves n1k1's real remote wiring up to the HTTP boundary: an
// httptest endpoint that RECORDS requests, so we can assert OpenIcebergTable(s3://...) with
// env/props credentials dispatches to iceberg-go's S3 backend and issues a path-style GET
// for the right bucket/key against the configured endpoint. (End-to-end read of real
// object bytes is covered once a mock-S3 dep can be vendored; see §8 phasing.)

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestOpenIcebergTableS3Dispatch(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.Method+" "+r.URL.Path)
		mu.Unlock()
		// Not a real S3 response -- we only need the request to have REACHED us. Returning
		// an error makes iceberg-go's metadata read fail fast (so the test doesn't hang on
		// SDK retries with a real body).
		http.Error(w, "NoSuchKey", http.StatusNotFound)
	}))
	defer srv.Close()

	props := map[string]string{
		propS3Region:      "us-east-1",
		propS3AccessKeyID: "test",
		propS3SecretKey:   "test",
		propS3Endpoint:    srv.URL, // point the S3 client at our recorder (path-style default)
	}
	const bucket = "n1k1-test-bucket"
	metaLoc := "s3://" + bucket + "/tbl/metadata/00001.metadata.json"

	// Expected to fail (our endpoint serves 404), but the wiring must have dispatched to S3
	// and hit our endpoint -- NOT the LocalFS path (which would give a filesystem error and
	// never touch the network).
	_, err := OpenIcebergTableProps(metaLoc, "", props)
	if err == nil {
		t.Fatal("expected an error reading a non-existent object, got nil")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(paths) == 0 {
		t.Fatalf("S3 endpoint received no requests -- wiring did not dispatch to the S3 backend (err: %v)", err)
	}
	// Path-style addressing puts the bucket + object key in the URL path.
	var sawKey bool
	for _, p := range paths {
		if strings.Contains(p, bucket) && strings.Contains(p, "tbl/metadata/00001.metadata.json") {
			sawKey = true
		}
	}
	if !sawKey {
		t.Errorf("no request for the expected path-style bucket/key; got %v", paths)
	}
}

// TestResolveObjectStoreIcebergMetadata: ResolveObjectStoreIcebergMetadata lists a bare
// table dir's metadata/ prefix over S3 and returns the current metadata location. A stdlib
// httptest endpoint serves a canned ListObjectsV2 XML (no mock-S3 dep -- the AWS SDK parses
// it), proving the list -> pick -> compose chain end to end. gs:// is rejected (S3-only v1).
func TestResolveObjectStoreIcebergMetadata(t *testing.T) {
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

	got, err := ResolveObjectStoreIcebergMetadata("s3://bkt/warehouse/orders")
	if err != nil {
		t.Fatalf("ResolveObjectStoreIcebergMetadata: %v", err)
	}
	if want := "s3://bkt/warehouse/orders/metadata/00002-b.metadata.json"; got != want {
		t.Errorf("resolved = %q, want %q", got, want)
	}

	if _, err := ResolveObjectStoreIcebergMetadata("gs://bkt/warehouse/orders"); err == nil {
		t.Error("gs:// bare dir should be rejected (listing is S3-only in v1)")
	}
}
