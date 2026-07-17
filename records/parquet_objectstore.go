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

// Standalone remote Parquet (DESIGN-data.md §8): FROM s3://bucket/path/x.parquet (or gs://,
// abfs://) reads a single Parquet object over the network. Parquet is a random-access format
// (footer at the end, then per-column-chunk byte ranges), so arrow-go's reader -- which only
// needs an io.ReaderAt+io.Seeker -- fetches ONLY the footer + projected column chunks; the
// whole object is never downloaded. We read through iceberg-go's own object-store IO (the
// same one it uses for its data files): its blobOpenFile is a ReaderAt+Seeker whose ReadAt
// issues a ranged GET, so S3/GCS/Azure all work via one path -- reusing every credential,
// endpoint, addressing, and anonymous (AWS_NO_SIGN_REQUEST) rule already wired for Iceberg
// (objectStoreFSFunc). The projection/predicate pushdown and batch->rows machinery above
// this are the SAME as the local parquetSource.

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// OpenParquetSourceRemote opens a single Parquet object in an object store (s3://, gs://,
// abfs://) as a Source, reading it through iceberg-go's IO so only the bytes the query needs
// transfer. Credentials/endpoint/addressing come from the environment (ObjectStoreProps),
// identical to the Iceberg read path; parquetSource.Close closes the underlying blob file
// (arrow's Reader.Close closes its io.Closer source).
func OpenParquetSourceRemote(loc, idPrefix string) (Source, error) {
	if !IsObjectStoreURI(loc) {
		return nil, fmt.Errorf("remote Parquet needs an object-store URI (s3://, gs://, abfs://), got %q", loc)
	}
	ctx := context.Background()
	fsio, err := objectStoreFSFunc(loc, ObjectStoreProps(loc))(ctx)
	if err != nil {
		return nil, fmt.Errorf("open object store for %q: %w", loc, err)
	}
	f, err := fsio.Open(loc) // iceberg-go File: io.ReadSeekCloser + io.ReaderAt (ranged GETs)
	if err != nil {
		return nil, fmt.Errorf("open remote Parquet %q: %w", loc, err)
	}
	// Buffered streaming: read column-chunk data through an io.SectionReader (page by page)
	// instead of pulling the ENTIRE chunk up front. Over the network that's the difference
	// between fetching a few pages for a LIMIT and downloading the whole (possibly huge)
	// column chunk -- e.g. SELECT * ... LIMIT 10 on a wide remote file.
	rprops := parquet.NewReaderProperties(memory.DefaultAllocator)
	rprops.BufferedStreamEnabled = true
	pf, err := file.NewParquetReader(f, file.WithReadProps(rprops)) // f satisfies parquet.ReaderAtSeeker
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("open remote Parquet %q: %w", loc, err)
	}
	// Pool only the pqarrow output arrays; the decode allocator stays default -- string/binary
	// output aliases decode buffers zero-copy, so recycling them would corrupt live columns.
	pr, err := pqarrow.NewFileReader(pf, arrowReadProps(), arrowAlloc)
	if err != nil {
		pf.Close() // closes f via arrow's Reader.Close (io.Closer)
		return nil, err
	}
	return &parquetSource{pf: pf, pr: pr, idPrefix: idPrefix}, nil
}
