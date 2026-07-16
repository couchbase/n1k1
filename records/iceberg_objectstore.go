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
// s3://bucket/warehouse/db/table dir URI, LIST its metadata/ prefix and pick the current
// *.metadata.json (the object-store analog of the local IcebergTableMetadata). iceberg-go's
// blob IO exposes no listing, so this uses aws-sdk-go-v2's S3 client directly -- configured
// via iceberg-go's own ParseAWSConfig + the same endpoint/path-style rules, so credentials
// and MinIO-style endpoints behave identically to the read path. S3-family only in v1
// (gs/abfs bare dirs must pass an explicit metadata JSON).

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ResolveObjectStoreIcebergMetadata resolves the CURRENT metadata JSON location for an
// Iceberg table given a bare object-store table-dir URI (e.g.
// "s3://bucket/warehouse/db/orders"). It lists "<dir>/metadata/", reads version-hint.text
// when present, and returns "<dir>/metadata/<current>.metadata.json". S3-family schemes
// only; a gs:// / abfs:// dir returns an error guiding the caller to pass an explicit
// metadata location. Credentials/endpoint come from the environment (ObjectStoreProps).
func ResolveObjectStoreIcebergMetadata(dirLoc string) (string, error) {
	if scheme := uriScheme(dirLoc); !strings.HasPrefix(scheme, "s3") {
		return "", fmt.Errorf("auto-resolving the current Iceberg metadata over %q is not supported; "+
			"pass the explicit .../metadata/<file>.metadata.json location", scheme)
	}
	u, err := url.Parse(dirLoc)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("malformed object-store location %q", dirLoc)
	}
	bucket := u.Host
	prefix := strings.Trim(u.Path, "/") // "warehouse/db/orders" (empty => table at bucket root)
	metaPrefix := "metadata/"
	if prefix != "" {
		metaPrefix = prefix + "/metadata/"
	}

	ctx := context.Background()
	props := ObjectStoreProps(dirLoc)
	client, err := newS3ClientForList(ctx, props)
	if err != nil {
		return "", err
	}

	var bases []string
	hasVersionHint := false
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(metaPrefix),
	})
	for pager.HasMorePages() {
		page, perr := pager.NextPage(ctx)
		if perr != nil {
			return "", fmt.Errorf("listing %q: %w", dirLoc, perr)
		}
		for _, obj := range page.Contents {
			base := strings.TrimPrefix(aws.ToString(obj.Key), metaPrefix)
			if base == "" || strings.Contains(base, "/") {
				continue // the prefix "directory" marker, or a nested key
			}
			if base == "version-hint.text" {
				hasVersionHint = true
			}
			if strings.HasSuffix(base, ".metadata.json") {
				bases = append(bases, base)
			}
		}
	}

	versionHint := ""
	if hasVersionHint {
		if b, gerr := s3GetObject(ctx, client, bucket, metaPrefix+"version-hint.text"); gerr == nil {
			versionHint = string(b)
		}
	}

	name, ok := pickCurrentMetadataName(bases, versionHint)
	if !ok {
		return "", fmt.Errorf("no *.metadata.json found under %s/metadata/ (not an Iceberg table?)", strings.TrimRight(dirLoc, "/"))
	}
	return strings.TrimRight(dirLoc, "/") + "/metadata/" + name, nil
}

// newS3ClientForList builds an aws-sdk-go-v2 S3 client from iceberg-go's ParseAWSConfig plus
// the same endpoint + path-style rules createS3Bucket uses, so listing sees the exact
// credentials/endpoint the read path does.
func newS3ClientForList(ctx context.Context, props map[string]string) (*s3.Client, error) {
	cfg, err := iceio.ParseAWSConfig(ctx, props)
	if err != nil {
		return nil, err
	}
	// Anonymous/unsigned access for public buckets (AWS CLI's --no-sign-request): with no
	// credentials the SDK would otherwise fail trying to resolve some. Honor AWS_NO_SIGN_REQUEST
	// only when no explicit access key was provided.
	if awsNoSignRequest() && props[iceio.S3AccessKeyID] == "" {
		cfg.Credentials = aws.AnonymousCredentials{}
	}
	endpoint := props[iceio.S3EndpointURL]
	usePathStyle := s3UsePathStyle(endpoint, props[iceio.S3ForceVirtualAddressing])
	return s3.NewFromConfig(*cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = usePathStyle
	}), nil
}

func s3GetObject(ctx context.Context, client *s3.Client, bucket, key string) ([]byte, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}
