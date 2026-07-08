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

// Glob support for inline keyspace-name globs (DESIGN-data.md "Mode 2b" --
// `FROM `./data/**/*.json``). Pure-Go, no doublestar dependency: `**` matches
// across directory boundaries, `*`/`?`/`[...]` match within a path segment
// (stdlib filepath.Match semantics per segment). Glue resolves the three base-dir
// forms (./ or ../ = CWD, / = absolute, bare = datastore-root) to an ABSOLUTE
// pattern before calling GlobFiles.

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// HasGlobMeta reports whether s contains glob metacharacters (`*`, `?`, `[`), so
// a keyspace name should be treated as a glob pattern rather than a literal name.
// (`**` contains `*`, so it is covered.)
func HasGlobMeta(s string) bool {
	return strings.ContainsAny(s, "*?[")
}

// GlobFiles resolves an ABSOLUTE doublestar glob to the sorted absolute paths of
// matching, decodable record files (each passing opts' format/compression filter
// -- the -formats lockdown), plus the walk base: the longest metacharacter-free
// leading directory, used as the dir-relative synthetic-ID root by WalkPrelisted.
func GlobFiles(absGlob string, opts WalkOptions) (base string, files []string, err error) {
	base = GlobBase(absGlob)
	err = filepath.Walk(base, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if info.IsDir() {
			return nil
		}
		if opts.eligible(path) && globMatch(absGlob, path) {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return base, nil, err
	}
	sort.Strings(files)
	return base, files, nil
}

// GlobBase returns the longest leading run of pattern's path segments that contain
// no glob metacharacter -- the directory to walk. Pattern is absolute, so the first
// segment is empty (the leading separator) and is preserved.
func GlobBase(pattern string) string {
	sep := string(filepath.Separator)
	var kept []string
	for _, seg := range strings.Split(pattern, sep) {
		if HasGlobMeta(seg) {
			break
		}
		kept = append(kept, seg)
	}
	base := strings.Join(kept, sep)
	if base == "" {
		base = sep
	}
	return base
}

// globMatch reports whether an absolute path matches an absolute doublestar
// pattern. Both are split on the separator and matched segment-by-segment; a `**`
// segment matches zero or more whole segments, everything else uses filepath.Match.
func globMatch(pattern, path string) bool {
	sep := string(filepath.Separator)
	return matchSegments(strings.Split(pattern, sep), strings.Split(path, sep))
}

func matchSegments(pat, name []string) bool {
	for len(pat) > 0 {
		if pat[0] == "**" {
			if len(pat) == 1 {
				return true // a trailing ** matches any remaining segments
			}
			// ** matches zero or more segments: try every suffix of name.
			for i := 0; i <= len(name); i++ {
				if matchSegments(pat[1:], name[i:]) {
					return true
				}
			}
			return false
		}
		if len(name) == 0 {
			return false
		}
		ok, err := filepath.Match(pat[0], name[0])
		if err != nil || !ok {
			return false
		}
		pat, name = pat[1:], name[1:]
	}
	return len(name) == 0
}
