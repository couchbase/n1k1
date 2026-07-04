//go:build n1ql

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

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/algebra"
)

// ext.go is the entry point for n1k1's extension layer (DESIGN-extensions.md):
//   - Native, zero-garbage extension AGGREGATES (sparkline, histogram) are wired
//     into the cbq parser at package init; their computation lives in
//     base/agg_ext.go via the base.Agg protocol.
//   - Drop-in scalar-function extensions are loaded from files/dirs by
//     RegisterExtensionFile / RegisterExtensionDir, which dispatch by file
//     extension (today: ".js" JavaScript UDFs; WASM etc. later). Loading is
//     OPT-IN -- an embedder (or the CLI's -ext flag / .ext command) calls it
//     explicitly, since executing user code in-process is a real attack surface
//     (see the Caveats in DESIGN-extensions.md).

func init() {
	// Extension aggregates. The name here MUST match a base.AggCatalog entry
	// (base/agg_ext.go) so conv.go's VisitGroup can route computation to the
	// native handler. Property ALLOWS_REGULAR = usable in GROUP BY and as a bare
	// aggregate over the implicit single group.
	for _, name := range []string{"sparkline", "histogram"} {
		if _, ok := base.AggCatalog[name]; !ok {
			// Defensive: a name registered with the parser but absent from the
			// engine catalog would parse then fail to execute. Skip to surface
			// the mismatch as an "unknown aggregate" rather than a silent gap.
			continue
		}
		registerExtAggregate(name, algebra.AGGREGATE_ALLOWS_REGULAR)
	}
}

// ExtensionInfo describes one currently-loaded extension function (for listing).
type ExtensionInfo struct {
	Name   string // the SQL++ function name
	Kind   string // e.g. "javascript"
	Source string // originating dir/file path, or "(inline)"
}

// extLoaded tracks the currently-loaded extension functions by name, so the CLI
// can list and unload them. Registering records here; UnloadExtension removes.
// (Distinct from extOurs in ext_goja.go, which persists across unload to keep
// reload from tripping the builtin-shadow guard.)
var extLoaded = map[string]ExtensionInfo{}

// extensionLoaders maps a (lower-case) file extension to the loader that turns
// such a file into a registered function, and the kind label it records. This is
// the single place to add a new extension kind (e.g. ".wasm") as the roadmap
// advances -- callers stay generic.
var extensionLoaders = map[string]struct {
	kind string
	load func(name, path string) error
}{
	".js": {"javascript", func(name, path string) error {
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return registerJSFunc(name, string(src)) // JavaScript scalar UDF.
	}},
}

// RegisterExtensionFile registers a single extension file as a scalar function
// whose SQL++ name is the file's base name (minus its extension). The kind is
// auto-detected from the file extension (today ".js" = JavaScript); an
// unrecognized extension is an error. Returns the registered function name.
func RegisterExtensionFile(path string) (string, error) {
	base := filepath.Base(path)

	// "<name>.agg.js" is a JS AGGREGATE (3-callback protocol; see ext_jsagg.go),
	// checked before the generic ".js" scalar loader since it also ends in ".js".
	if lower := strings.ToLower(base); strings.HasSuffix(lower, ".agg.js") {
		name := strings.TrimSuffix(lower, ".agg.js")
		src, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		if err := RegisterJSAggregate(name, string(src)); err != nil {
			return "", err
		}
		extLoaded[name] = ExtensionInfo{Name: name, Kind: "javascript-aggregate", Source: path}
		return name, nil
	}

	ext := strings.ToLower(filepath.Ext(path))
	loader, ok := extensionLoaders[ext]
	if !ok {
		return "", fmt.Errorf("RegisterExtensionFile %q: unsupported extension %q", path, ext)
	}
	name := strings.ToLower(strings.TrimSuffix(base, filepath.Ext(path)))
	if err := loader.load(name, path); err != nil {
		return "", err
	}
	extLoaded[name] = ExtensionInfo{Name: name, Kind: loader.kind, Source: path}
	return name, nil
}

// RegisterExtensionDir scans dir (non-recursively) and registers every file
// whose extension is a recognized extension kind, skipping the rest (READMEs,
// etc.). The directory IS the catalog (DESIGN-extensions.md); `git pull` to
// update. Returns the registered names (sorted). Opt-in, per the security note
// above.
func RegisterExtensionDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("RegisterExtensionDir %q: %w", dir, err)
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, ok := extensionLoaders[strings.ToLower(filepath.Ext(e.Name()))]; !ok {
			continue // not a recognized extension file
		}
		name, err := RegisterExtensionFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return names, err
		}
		names = append(names, name)
	}

	sort.Strings(names)
	return names, nil
}

// RegisterJSFunc registers a single JavaScript scalar UDF from inline source:
// source must define a function whose name equals name, which then resolves as
// name(args) in SQL++. The programmatic counterpart of dropping a "<name>.js"
// file into an extension directory. Safe to call at startup before parsing; not
// safe to call concurrently with query parsing.
func RegisterJSFunc(name, source string) error {
	if err := registerJSFunc(name, source); err != nil {
		return err
	}
	name = strings.ToLower(name)
	extLoaded[name] = ExtensionInfo{Name: name, Kind: "javascript", Source: "(inline)"}
	return nil
}

// ListExtensions returns the currently-loaded extension functions, sorted by
// name. (The always-on sparkline/histogram aggregates are not "loaded" and are
// not included.)
func ListExtensions() []ExtensionInfo {
	out := make([]ExtensionInfo, 0, len(extLoaded))
	for _, info := range extLoaded {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// UnloadExtension disables a loaded extension function: the name is replaced in
// the parser's registry with a stub that errors when called (cbq's registry has
// no delete, so the name still parses), and it is dropped from the loaded set. A
// later Register* of the same name re-enables it. Returns an error if the name
// is not currently loaded.
func UnloadExtension(name string) error {
	name = strings.ToLower(name)
	if _, ok := extLoaded[name]; !ok {
		return fmt.Errorf("extension %q is not loaded", name)
	}
	unregisterJSFunc(name)
	delete(extLoaded, name)
	return nil
}
