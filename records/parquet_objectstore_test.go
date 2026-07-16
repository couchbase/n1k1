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

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// s3ObjectServer is a minimal S3-object endpoint over one object's bytes: HEAD returns the
// size, GET honors a Range header (206 + Content-Range) so a ReaderAt fetches sub-slices.
// It records whether any ranged GET happened (proving streaming, not a single full download).
func s3ObjectServer(t *testing.T, keySuffix string, data []byte) (*httptest.Server, *int32) {
	t.Helper()
	var rangedGets int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, keySuffix) {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)
			return
		}
		if rng := r.Header.Get("Range"); rng != "" {
			var a, b int
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &a, &b); err != nil || a < 0 || a >= len(data) {
				http.Error(w, "InvalidRange", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if b >= len(data) {
				b = len(data) - 1
			}
			atomic.AddInt32(&rangedGets, 1)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(b-a+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[a : b+1])
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	return srv, &rangedGets
}

// TestOpenParquetSourceRemote: a single Parquet object is read over an S3-range-backed
// io.ReaderAt end to end -- a stdlib httptest endpoint serves the real parquet bytes via
// HEAD + ranged GET (no mock-S3 dep), so this exercises s3ReaderAt + arrow-go's reader +
// the shared batch->rows renderer. Rows and values must match, and reads must be ranged.
func TestOpenParquetSourceRemote(t *testing.T) {
	// A real small parquet on disk, then its bytes served over the mock endpoint.
	path := filepath.Join(t.TempDir(), "x.parquet")
	writeColTestParquet(t, path, 100, 5) // id 100..104, price id+0.5
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	srv, rangedGets := s3ObjectServer(t, "/data/x.parquet", data)
	defer srv.Close()

	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_ENDPOINT_URL", srv.URL)
	t.Setenv("AWS_S3_ENDPOINT", "")

	src, err := OpenParquetSourceRemote("s3://bkt/data/x.parquet", "")
	if err != nil {
		t.Fatalf("OpenParquetSourceRemote: %v", err)
	}
	defer src.Close()

	var got []map[string]interface{}
	for {
		var rec Record
		ok, nerr := src.Next(&rec)
		if nerr != nil {
			t.Fatalf("Next: %v", nerr)
		}
		if !ok {
			break
		}
		var m map[string]interface{}
		if jerr := json.Unmarshal(rec.Doc, &m); jerr != nil {
			t.Fatalf("row not JSON: %q (%v)", rec.Doc, jerr)
		}
		got = append(got, m)
	}

	if len(got) != 5 {
		t.Fatalf("read %d rows, want 5: %v", len(got), got)
	}
	if got[0]["id"] != float64(100) || got[0]["price"] != 100.5 {
		t.Errorf("row 0 = %v, want {id:100, price:100.5}", got[0])
	}
	if got[4]["id"] != float64(104) {
		t.Errorf("row 4 id = %v, want 104", got[4]["id"])
	}
	if atomic.LoadInt32(rangedGets) == 0 {
		t.Error("expected ranged GET(s) (streaming), saw none")
	}
}

// TestOpenParquetSourceRemoteRejectsNonS3: gs:// / local paths aren't the S3 remote path.
func TestOpenParquetSourceRemoteRejectsNonS3(t *testing.T) {
	if _, err := OpenParquetSourceRemote("gs://bkt/x.parquet", ""); err == nil {
		t.Error("gs:// remote parquet should be rejected (S3-only)")
	}
}
