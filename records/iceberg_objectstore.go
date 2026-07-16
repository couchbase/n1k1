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

// Object-store current-metadata resolution (DESIGN-data.md §8): given a bare
// s3://bucket/db/table (or gs://, abfs://) dir URI, LIST its metadata/ prefix and pick the
// current *.metadata.json (the object-store analog of the local IcebergTableMetadata).
// iceberg-go's blob IO doesn't expose listing on its interface, but its concrete backend
// embeds a *gocloud.dev/blob.Bucket -- so we reach List/ReadAll through a structural
// assertion on the IO we already build (objectStoreFSFunc), using iceberg-go's own
// correctly-credentialed bucket. One path serves S3, GCS, and Azure.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	iceio "github.com/apache/iceberg-go/io"
	iceutils "github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"gocloud.dev/blob"
)

// objectStoreFSFunc returns the iceberg-go FS-factory for a table location. It is
// iceio.LoadFSFunc, except that for anonymous S3 access (AWS_NO_SIGN_REQUEST, no explicit
// key) it injects an anonymous aws.Config into the context iceberg-go uses to build its S3
// client (createS3Bucket consults utils.GetAwsConfig(ctx) first) -- so a public bucket's
// data files read with no credentials, matching n1k1's own client. Addressing (virtual-hosted
// on real AWS) rides the s3.force-virtual-addressing prop that ObjectStoreProps sets.
func objectStoreFSFunc(loc string, props map[string]string) func(context.Context) (iceio.IO, error) {
	base := iceio.LoadFSFunc(props, loc)
	if !strings.HasPrefix(uriScheme(loc), "s3") || !awsNoSignRequest() || props[iceio.S3AccessKeyID] != "" {
		return base
	}
	return func(ctx context.Context) (iceio.IO, error) {
		cfg, err := iceio.ParseAWSConfig(ctx, props)
		if err != nil {
			return base(ctx) // ParseAWSConfig rarely fails; fall back to the default path
		}
		cfg.Credentials = aws.AnonymousCredentials{}
		return base(iceutils.WithAwsConfig(ctx, cfg))
	}
}

// blobLister is the subset of gocloud.dev/blob.Bucket's API needed to enumerate + read a
// metadata/ prefix. iceberg-go's blob-backed IO embeds a *blob.Bucket, so its concrete type
// satisfies this (promoted methods) even though the io.IO interface doesn't declare them.
type blobLister interface {
	List(*blob.ListOptions) *blob.ListIterator
	ReadAll(context.Context, string) ([]byte, error)
}

// ResolveObjectStoreIcebergMetadata resolves the CURRENT metadata JSON location for an
// Iceberg table given a bare object-store table-dir URI (e.g. "s3://bucket/db/orders",
// "gs://bucket/db/orders", "abfs://fs@acct.dfs.core.windows.net/db/orders"). It lists
// "<dir>/metadata/", reads version-hint.text when present, and returns
// "<dir>/metadata/<current>.metadata.json". Credentials/endpoint come from the environment
// (ObjectStoreProps) + iceberg-go's backend defaults, identical to the read path.
func ResolveObjectStoreIcebergMetadata(dirLoc string) (string, error) {
	if !IsObjectStoreURI(dirLoc) {
		return "", fmt.Errorf("not an object-store location: %q", dirLoc)
	}
	u, err := url.Parse(dirLoc)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("malformed object-store location %q", dirLoc)
	}
	// Bucket-relative key prefix (the gocloud bucket is already scoped to the bucket/container).
	prefix := strings.Trim(u.Path, "/")
	metaPrefix := "metadata/"
	if prefix != "" {
		metaPrefix = prefix + "/metadata/"
	}

	ctx := context.Background()
	fsio, err := objectStoreFSFunc(dirLoc, ObjectStoreProps(dirLoc))(ctx)
	if err != nil {
		return "", fmt.Errorf("open object store for %q: %w", dirLoc, err)
	}
	lister, ok := fsio.(blobLister)
	if !ok {
		return "", fmt.Errorf("current-metadata listing is not supported for %q "+
			"(pass the explicit .../metadata/<file>.metadata.json location)", dirLoc)
	}

	var bases []string
	hasVersionHint := false
	it := lister.List(&blob.ListOptions{Prefix: metaPrefix})
	for {
		obj, lerr := it.Next(ctx)
		if errors.Is(lerr, io.EOF) {
			break
		}
		if lerr != nil {
			return "", fmt.Errorf("listing %q: %w", dirLoc, lerr)
		}
		if obj.IsDir {
			continue
		}
		base := strings.TrimPrefix(obj.Key, metaPrefix)
		if base == "" || strings.Contains(base, "/") {
			continue // a nested key (shouldn't occur without a delimiter)
		}
		if base == "version-hint.text" {
			hasVersionHint = true
		}
		if strings.HasSuffix(base, ".metadata.json") {
			bases = append(bases, base)
		}
	}

	versionHint := ""
	if hasVersionHint {
		if b, rerr := lister.ReadAll(ctx, metaPrefix+"version-hint.text"); rerr == nil {
			versionHint = string(b)
		}
	}

	name, ok := pickCurrentMetadataName(bases, versionHint)
	if !ok {
		return "", fmt.Errorf("no *.metadata.json found under %s/metadata/ (not an Iceberg table?)", strings.TrimRight(dirLoc, "/"))
	}
	return strings.TrimRight(dirLoc, "/") + "/metadata/" + name, nil
}
