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

// Object-store (S3 / GCS / Azure) location helpers for the remote scan path
// (DESIGN-data.md §8). Kept dependency-free (stdlib only) so it links in EVERY build,
// including wasm -- the heavy iceberg-go/arrow read path stays in the !js files, but
// URI classification and the credential/props map are pure string work.
//
// The props map uses iceberg-go's io backend property keys directly (s3.endpoint,
// s3.region, ...); the same map shape feeds iceio.LoadFSFunc. v1 sources credentials
// from the standard AWS environment variables so the usual conventions "just work"
// with no new config schema; when they're absent the AWS default credential chain
// (instance role, shared config) still applies.

import (
	"net/url"
	"os"
	"strings"
)

// Object-store property keys (mirror iceberg-go/io so we needn't import it here).
const (
	propS3Region       = "s3.region"
	propS3AccessKeyID  = "s3.access-key-id"
	propS3SecretKey    = "s3.secret-access-key"
	propS3SessionToken = "s3.session-token"
	propS3Endpoint     = "s3.endpoint"
)

// objectStoreSchemes are the URI schemes routed to a remote object-store backend
// (matching iceberg-go/io's LoadFS dispatch). A local filesystem path (no scheme, or
// file://) is NOT here and keeps the LocalFS path.
var objectStoreSchemes = map[string]bool{
	"s3": true, "s3a": true, "s3n": true, // Amazon S3 (+ S3-compatible: MinIO, R2, ...)
	"gs": true, "gcs": true, // Google Cloud Storage
	"abfs": true, "abfss": true, "wasb": true, "wasbs": true, // Azure
}

// IsObjectStoreURI reports whether location names a remote object store (s3://, gs://,
// abfs://, ...) rather than a local filesystem path. A bare path or file:// is local.
func IsObjectStoreURI(location string) bool {
	scheme := uriScheme(location)
	return scheme != "" && objectStoreSchemes[scheme]
}

// uriScheme returns the lowercased URI scheme of location, or "" if it has none (a
// plain filesystem path). It tolerates Windows drive letters (C:\...) -- a single-letter
// "scheme" is treated as a local path, not a URI.
func uriScheme(location string) string {
	i := strings.Index(location, "://")
	if i <= 1 { // no "://", or a 1-char prefix (Windows drive) => local
		return ""
	}
	return strings.ToLower(location[:i])
}

// ObjectStoreProps builds the iceberg-go io properties map for an object-store
// location from the process environment, or nil for a local path (so the LocalFS
// backend is used unchanged). Only S3-family schemes are configured in v1; a GCS/Azure
// location returns an empty (non-nil) map so its backend falls back to its own default
// credential discovery.
//
// S3 env mapping (standard AWS names): AWS_REGION/AWS_DEFAULT_REGION -> s3.region,
// AWS_ACCESS_KEY_ID -> s3.access-key-id, AWS_SECRET_ACCESS_KEY -> s3.secret-access-key,
// AWS_SESSION_TOKEN -> s3.session-token, AWS_ENDPOINT_URL/AWS_S3_ENDPOINT -> s3.endpoint
// (the endpoint override that points a client at MinIO or another S3-compatible server;
// path-style addressing is iceberg-go's default, which such servers expect).
func ObjectStoreProps(location string) map[string]string {
	scheme := uriScheme(location)
	if !objectStoreSchemes[scheme] {
		return nil // local path: nil props => LocalFS
	}
	props := map[string]string{}
	if !strings.HasPrefix(scheme, "s3") {
		return props // gs/abfs: let the backend's own default discovery handle creds
	}
	putEnv(props, propS3Region, "AWS_REGION", "AWS_DEFAULT_REGION")
	putEnv(props, propS3AccessKeyID, "AWS_ACCESS_KEY_ID")
	putEnv(props, propS3SecretKey, "AWS_SECRET_ACCESS_KEY")
	putEnv(props, propS3SessionToken, "AWS_SESSION_TOKEN")
	putEnv(props, propS3Endpoint, "AWS_ENDPOINT_URL", "AWS_S3_ENDPOINT")
	return props
}

// putEnv sets props[key] to the first non-empty environment variable among envNames.
func putEnv(props map[string]string, key string, envNames ...string) {
	for _, name := range envNames {
		if v := os.Getenv(name); v != "" {
			props[key] = v
			return
		}
	}
}

// normalizeObjectStoreLocation is a light validity check: an object-store URI must have
// a host (bucket). It returns the input unchanged (callers pass it straight to
// iceberg-go); the parse only guards against an obviously malformed URL early.
func normalizeObjectStoreLocation(location string) (string, bool) {
	if !IsObjectStoreURI(location) {
		return location, true // local path: nothing to check here
	}
	u, err := url.Parse(location)
	if err != nil || u.Host == "" {
		return location, false
	}
	return location, true
}
