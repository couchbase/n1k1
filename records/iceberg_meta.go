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

// Iceberg table DETECTION -- pure-Go (no arrow/iceberg-go), so it links in every build
// incl. wasm. The heavy read path (OpenIcebergTable, iceberg.go) is !js; the wasm stub
// (iceberg_js.go) errors.

import (
	"os"
	"path/filepath"
	"strings"
)

// IcebergTableMetadata reports whether dir is an Iceberg table -- it has a `metadata/`
// subdirectory containing at least one `*.metadata.json` -- and returns the CURRENT
// metadata file's path. The current version comes from `metadata/version-hint.text` when
// present (the Hadoop-catalog convention: a bare version number), else the
// lexicographically-greatest `*.metadata.json` (Iceberg's zero-padded `NNNNN-<uuid>` /
// `vNNNNN` naming sorts by version, so max == newest).
func IcebergTableMetadata(dir string) (string, bool) {
	mdir := filepath.Join(dir, "metadata")
	if fi, err := os.Stat(mdir); err != nil || !fi.IsDir() {
		return "", false
	}
	if b, err := os.ReadFile(filepath.Join(mdir, "version-hint.text")); err == nil {
		v := strings.TrimSpace(string(b))
		for _, cand := range []string{"v" + v + ".metadata.json", v + ".metadata.json"} {
			p := filepath.Join(mdir, cand)
			if _, err := os.Stat(p); err == nil {
				return p, true
			}
		}
	}
	entries, err := os.ReadDir(mdir)
	if err != nil {
		return "", false
	}
	best := ""
	for _, e := range entries {
		if n := e.Name(); strings.HasSuffix(n, ".metadata.json") && n > best {
			best = n
		}
	}
	if best == "" {
		return "", false
	}
	return filepath.Join(mdir, best), true
}
