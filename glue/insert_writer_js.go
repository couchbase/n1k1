//go:build n1ql && js

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

package glue

// The browser (GOOS=js/wasm) build has no Arrow/Parquet writer (arrow-go's Parquet
// code doesn't build for wasm, matching records/parquet_js.go), so INSERT supports only
// JSON Lines targets there.

import (
	"fmt"
	"path/filepath"
	"strings"
)

func newInsertWriter(path, seedFrom, mode string) (insertWriter, error) {
	if strings.EqualFold(filepath.Ext(path), ".parquet") {
		return nil, fmt.Errorf("INSERT INTO %q: .parquet targets are not supported in the wasm build", path)
	}
	return newJSONLWriter(path, seedFrom)
}
