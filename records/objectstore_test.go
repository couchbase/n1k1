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

import "testing"

// TestIsObjectStoreURI: object-store schemes classify as remote; local paths, file://,
// and Windows drive letters classify as local.
func TestIsObjectStoreURI(t *testing.T) {
	remote := []string{
		"s3://bucket/tbl/metadata/00001.metadata.json",
		"S3://Bucket/x", "s3a://b/x", "s3n://b/x",
		"gs://b/x", "gcs://b/x", "abfs://c@a.dfs.core.windows.net/x", "wasbs://c@a/x",
	}
	local := []string{
		"/var/data/tbl", "./rel/tbl", "tbl/metadata/x.json",
		"file:///var/data/tbl", "C:\\data\\tbl", "c:/data/tbl", "",
	}
	for _, l := range remote {
		if !IsObjectStoreURI(l) {
			t.Errorf("IsObjectStoreURI(%q) = false, want true", l)
		}
	}
	for _, l := range local {
		if IsObjectStoreURI(l) {
			t.Errorf("IsObjectStoreURI(%q) = true, want false", l)
		}
	}
}

// TestObjectStoreProps: a local path yields nil props (LocalFS); an s3:// URI maps the
// standard AWS environment variables onto iceberg-go's io property keys.
func TestObjectStoreProps(t *testing.T) {
	if p := ObjectStoreProps("/local/tbl"); p != nil {
		t.Errorf("local path props = %v, want nil", p)
	}
	if p := ObjectStoreProps("file:///local/tbl"); p != nil {
		t.Errorf("file:// props = %v, want nil", p)
	}

	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIA_TEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "shh")
	t.Setenv("AWS_SESSION_TOKEN", "tok")
	t.Setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9000")

	p := ObjectStoreProps("s3://bucket/tbl/metadata/00001.metadata.json")
	want := map[string]string{
		propS3Region:       "us-west-2",
		propS3AccessKeyID:  "AKIA_TEST",
		propS3SecretKey:    "shh",
		propS3SessionToken: "tok",
		propS3Endpoint:     "http://127.0.0.1:9000",
	}
	for k, v := range want {
		if p[k] != v {
			t.Errorf("props[%q] = %q, want %q", k, p[k], v)
		}
	}
}

// TestObjectStorePropsRegionFallback: AWS_DEFAULT_REGION is used when AWS_REGION is unset,
// and absent creds simply leave the key unset (the AWS default chain applies at runtime).
func TestObjectStorePropsRegionFallback(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "eu-central-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_S3_ENDPOINT", "")

	p := ObjectStoreProps("s3://b/x")
	if p[propS3Region] != "eu-central-1" {
		t.Errorf("region fallback = %q, want eu-central-1", p[propS3Region])
	}
	if _, ok := p[propS3AccessKeyID]; ok {
		t.Errorf("absent creds should leave %q unset, got %q", propS3AccessKeyID, p[propS3AccessKeyID])
	}
}

// TestSplitIcebergMetadataLocation: derive (tableDir, keyspace name) from a metadata
// location of the conventional .../<table>/metadata/<file>.metadata.json shape.
func TestSplitIcebergMetadataLocation(t *testing.T) {
	cases := []struct {
		loc, wantDir, wantName string
		wantOK                 bool
	}{
		{"s3://bucket/warehouse/db/orders/metadata/00003-a1b2.metadata.json",
			"s3://bucket/warehouse/db/orders", "orders", true},
		{"s3://bkt/t/metadata/v5.metadata.json", "s3://bkt/t", "t", true},
		{"/local/warehouse/orders/metadata/00001.metadata.json",
			"/local/warehouse/orders", "orders", true},
		// No "/metadata/" segment -> not derivable.
		{"s3://bucket/orders/00003.metadata.json", "", "", false},
		// Table at bucket root -> name would be the "bucket" host garbage; reject.
		{"s3://bucket/metadata/x.metadata.json", "", "", false},
	}
	for _, c := range cases {
		dir, name, ok := SplitIcebergMetadataLocation(c.loc)
		if ok != c.wantOK || dir != c.wantDir || name != c.wantName {
			t.Errorf("Split(%q) = (%q,%q,%v), want (%q,%q,%v)",
				c.loc, dir, name, ok, c.wantDir, c.wantName, c.wantOK)
		}
	}
}

// TestNormalizeObjectStoreLocation: a bucket-less object-store URI is rejected; a local
// path and a well-formed URI pass through unchanged.
func TestNormalizeObjectStoreLocation(t *testing.T) {
	if _, ok := normalizeObjectStoreLocation("s3:///no-bucket/key"); ok {
		t.Error("s3:///no-bucket should be rejected (no host)")
	}
	if _, ok := normalizeObjectStoreLocation("s3://bucket/tbl/metadata/x.json"); !ok {
		t.Error("well-formed s3:// URI should pass")
	}
	if _, ok := normalizeObjectStoreLocation("/local/path/tbl"); !ok {
		t.Error("local path should pass")
	}
}
