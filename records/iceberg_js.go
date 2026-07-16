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

// OpenIcebergTable is unavailable in the wasm build (arrow-go's Parquet reader + iceberg-go
// don't build for GOOS=js); mirrors parquet_js.go.
func OpenIcebergTable(metadataLocation, idPrefix string) (Source, error) {
	return nil, fmt.Errorf("records: Iceberg tables are not supported in the wasm build")
}

// OpenIcebergTableProps mirrors OpenIcebergTable in the wasm build (unsupported).
func OpenIcebergTableProps(metadataLocation, idPrefix string, props map[string]string) (Source, error) {
	return nil, fmt.Errorf("records: Iceberg tables are not supported in the wasm build")
}
