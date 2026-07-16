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
	"strconv"
	"strings"
)

// Object-store property keys (mirror iceberg-go/io so we needn't import it here).
const (
	propS3Region       = "s3.region"
	propS3AccessKeyID  = "s3.access-key-id"
	propS3SecretKey    = "s3.secret-access-key"
	propS3SessionToken = "s3.session-token"
	propS3Endpoint     = "s3.endpoint"
	propS3ForceVirtual = "s3.force-virtual-addressing"
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
	// Real AWS (no custom endpoint): force virtual-hosted addressing, since path-style
	// (iceberg-go's default) 301-redirects on a region mismatch and AWS is deprecating it.
	// A custom endpoint (MinIO et al.) keeps iceberg-go's path-style default. This prop is
	// honored by BOTH iceberg-go's own S3 client and n1k1's (s3UsePathStyle).
	if props[propS3Endpoint] == "" {
		props[propS3ForceVirtual] = "true"
	}
	return props
}

// awsNoSignRequest reports whether AWS_NO_SIGN_REQUEST is set to a truthy value, requesting
// anonymous/unsigned S3 access for public buckets (mirrors the AWS CLI's --no-sign-request).
func awsNoSignRequest() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("AWS_NO_SIGN_REQUEST"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// s3UsePathStyle decides S3 addressing: path-style for a custom endpoint (MinIO and other
// S3-compatible servers require it), virtual-hosted for real AWS (path-style there
// 301-redirects on a region mismatch and is being deprecated). The
// s3.force-virtual-addressing property overrides either way ("true" => virtual-hosted).
func s3UsePathStyle(endpoint, forceVirtual string) bool {
	usePathStyle := endpoint != ""
	if forceVirtual != "" {
		if b, err := strconv.ParseBool(forceVirtual); err == nil {
			usePathStyle = !b
		}
	}
	return usePathStyle
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

// SplitIcebergMetadataLocation splits an Iceberg table metadata location of the
// conventional shape ".../<table>/metadata/<file>.metadata.json" into the table directory
// (".../<table>") and a keyspace name (the table dir's last path segment). Works for any
// URI or path (scheme-agnostic) since it operates on the "/metadata/" delimiter. ok is
// false when there's no "/metadata/" segment or no derivable name -- the caller then errors
// with guidance. Used to name the synthetic keyspace for an object-store Iceberg table
// (DESIGN-data.md §8); the local path uses the directory basename instead.
func SplitIcebergMetadataLocation(metadataLoc string) (tableDir, name string, ok bool) {
	i := strings.LastIndex(metadataLoc, "/metadata/")
	if i <= 0 {
		return "", "", false
	}
	tableDir = metadataLoc[:i]
	// For a URI, require a path segment beyond the host -- else "s3://bucket/metadata/x"
	// would name the keyspace after the bucket host, not a table.
	if s := strings.Index(tableDir, "://"); s >= 0 && !strings.Contains(tableDir[s+3:], "/") {
		return "", "", false
	}
	name = tableDir[strings.LastIndex(tableDir, "/")+1:]
	if name == "" {
		return "", "", false
	}
	return tableDir, name, true
}

// pickCurrentMetadataName chooses the current Iceberg metadata file from the base names
// found under a table's metadata/ prefix, mirroring the local records.IcebergTableMetadata
// logic: honor version-hint.text (a bare version number -> "v<n>.metadata.json" /
// "<n>.metadata.json", or a full filename) when it names a present file, else take the
// lexicographically-greatest "*.metadata.json" (Iceberg's zero-padded "NNNNN-<uuid>" / "vNNNNN"
// naming sorts by version). ok is false when no "*.metadata.json" is present. Pure/testable;
// the object-store listing lives in ResolveObjectStoreIcebergMetadata.
func pickCurrentMetadataName(baseNames []string, versionHint string) (name string, ok bool) {
	inSet := map[string]bool{}
	best := ""
	for _, f := range baseNames {
		if strings.HasSuffix(f, ".metadata.json") {
			inSet[f] = true
			if f > best {
				best = f
			}
		}
	}
	if vh := strings.TrimSpace(versionHint); vh != "" {
		cands := []string{"v" + vh + ".metadata.json", vh + ".metadata.json"}
		if strings.HasSuffix(vh, ".metadata.json") {
			cands = append([]string{vh}, cands...)
		}
		for _, c := range cands {
			if inSet[c] {
				return c, true
			}
		}
		// version-hint present but its file is missing: fall back to the greatest.
	}
	if best == "" {
		return "", false
	}
	return best, true
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
