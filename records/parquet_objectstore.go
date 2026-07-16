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

// Standalone remote Parquet (DESIGN-data.md §8): FROM s3://bucket/path/x.parquet reads a
// single Parquet object over the network. Parquet is a random-access format (footer at the
// end, then per-column-chunk byte ranges), so arrow-go's reader -- which only needs an
// io.ReaderAt+io.Seeker -- fetches ONLY the footer + projected column chunks via S3 ranged
// GetObject; the whole object is never downloaded. The projection/predicate pushdown and the
// batch->rows machinery above this are the SAME as the local parquetSource.

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// OpenParquetSourceRemote opens a single Parquet object in an object store (s3://...) as a
// Source, reading it through an S3-range-backed io.ReaderAt so only the bytes the query
// needs transfer. S3-family schemes only; credentials/endpoint come from the environment
// (ObjectStoreProps), identical to the Iceberg read path.
func OpenParquetSourceRemote(loc, idPrefix string) (Source, error) {
	if scheme := uriScheme(loc); !strings.HasPrefix(scheme, "s3") {
		return nil, fmt.Errorf("remote Parquet over %q is not supported (S3-family only)", scheme)
	}
	u, err := url.Parse(loc)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("malformed object-store location %q", loc)
	}
	bucket := u.Host
	key := strings.TrimPrefix(u.Path, "/")

	ctx := context.Background()
	client, err := newS3ClientForList(ctx, ObjectStoreProps(loc))
	if err != nil {
		return nil, err
	}
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("stat %q: %w", loc, err)
	}
	size := aws.ToInt64(head.ContentLength)
	if size <= 0 {
		return nil, fmt.Errorf("remote Parquet %q has unknown/zero size", loc)
	}

	r := &s3ReaderAt{ctx: ctx, client: client, bucket: bucket, key: key, size: size}
	pf, err := file.NewParquetReader(r)
	if err != nil {
		return nil, fmt.Errorf("open remote Parquet %q: %w", loc, err)
	}
	pr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		pf.Close()
		return nil, err
	}
	return &parquetSource{pf: pf, pr: pr, idPrefix: idPrefix}, nil
}

// s3ReaderAt is a parquet.ReaderAtSeeker (io.ReaderAt + io.Seeker + io.Reader) over an S3
// object: ReadAt issues a ranged GetObject (bytes=off-end), so arrow-go pulls only the
// footer + selected column chunks. size (from HeadObject) backs SeekEnd. Not safe for
// concurrent use -- a single scan reads sequentially/randomly from one goroutine.
type s3ReaderAt struct {
	ctx    context.Context
	client *s3.Client
	bucket string
	key    string
	size   int64
	pos    int64 // for Read/Seek
}

func (r *s3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.size {
		return 0, io.EOF
	}
	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}
	rng := fmt.Sprintf("bytes=%d-%d", off, end)
	out, err := r.client.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket), Key: aws.String(r.key), Range: aws.String(rng),
	})
	if err != nil {
		return 0, err
	}
	defer out.Body.Close()
	n, err := io.ReadFull(out.Body, p[:end-off+1])
	if err == nil && n < len(p) {
		// The clamped range delivered fewer bytes than the caller's buffer (a read that
		// runs into EOF): the io.ReaderAt contract requires a non-nil error then.
		err = io.EOF
	}
	return n, err
}

func (r *s3ReaderAt) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *s3ReaderAt) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, fmt.Errorf("s3ReaderAt.Seek: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("s3ReaderAt.Seek: negative position %d", abs)
	}
	r.pos = abs
	return abs, nil
}
