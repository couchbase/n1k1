//go:build js

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

import "fmt"

// newParquetSource is unavailable in the wasm/js build: apache/arrow-go's
// Parquet reader does not compile for GOOS=js. Native builds get the real
// source in parquet.go. See DESIGN-col.md and the wasm build-tag guards.
func newParquetSource(path, idPrefix string) (Source, error) {
	return nil, fmt.Errorf("records: parquet is not supported in the wasm/js build: %s", path)
}

// OpenParquetSourceRemote is unavailable in the wasm/js build (see newParquetSource).
func OpenParquetSourceRemote(loc, idPrefix string) (Source, error) {
	return nil, fmt.Errorf("records: remote parquet is not supported in the wasm/js build: %s", loc)
}
