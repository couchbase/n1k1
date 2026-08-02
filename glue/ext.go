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
	"strconv"
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
	for _, name := range []string{"sparkline", "histogram", "min_by", "max_by"} {
		if _, ok := base.AggCatalog[name]; !ok {
			// Defensive: a name registered with the parser but absent from the
			// engine catalog would parse then fail to execute. Skip to surface
			// the mismatch as an "unknown aggregate" rather than a silent gap.
			continue
		}
		registerExtAggregate(name, algebra.AGGREGATE_ALLOWS_REGULAR)
	}

	// MULTI_MATCHES: a multi-query corpus as a composable, array-returning FROM source
	// (multi_matches.go). Always-on (like the aggregates above), no grammar change --
	// cbq resolves multi_matches(...) as a scalar function and VisitExpressionScan
	// routes a FROM multi_matches(...) through the materializing expr-scan.
	registerMultiMatchesFunc()

	// VECTORIZE_BATCH(batch, opts): batched text->vector embedding (DESIGN-vectors.md
	// Phase 0). Always-on, no grammar change; offline/deterministic by default (only
	// reaches the network with an explicit endpoint opt). Search rides cbq's existing
	// pure-Go VECTOR_DISTANCE -- no registration needed for that.
	registerVectorizeBatchFunc()
}

// ExtensionInfo describes one currently-loaded extension function (for listing).
type ExtensionInfo struct {
	Name   string // the SQL++ function name
	Kind   string // e.g. "javascript"
	Source string // originating dir/file path, or "(inline)"
}

// extLoaded tracks the currently-loaded extension functions by name, so the CLI
// can list and unload them. Registering records here; UnloadExtension removes.
// (Distinct from extOurs in ext_jsvm.go, which persists across unload to keep
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
		// Single-function scalar UDF keyed off the filename stem. A multi-export MODULE
		// (a file that sets exports.functions) is handled explicitly in
		// RegisterExtensionFile so each function is recorded individually.
		return registerJSFunc(name, string(src))
	}},
}

// RegisterExtensionFile registers a single extension file as a scalar function
// whose SQL++ name is the file's base name (minus its extension). The kind is
// auto-detected from the file extension (today ".js" = JavaScript); an
// unrecognized extension is an error. Returns the registered function name.
func RegisterExtensionFile(path string) (string, error) {
	base := filepath.Base(path)

	// "<name>.extract.js" is a JS EXTRACT PLUGIN (describe() -> ExtractSpec, native
	// per-row SpecApply; see ext_extract_jsvm.go), checked before the generic ".js"
	// scalar loader since it also ends in ".js". It registers a records.ExtractPlugin, not a
	// SQL function, so it is tracked separately (extractPluginsLoaded, not extLoaded).
	if lower := strings.ToLower(base); strings.HasSuffix(lower, ".extract.js") {
		name := strings.TrimSuffix(lower, ".extract.js")
		src, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		if err := RegisterJSExtractPlugin(name, string(src)); err != nil {
			return "", err
		}
		// Record the originating path (RegisterJSExtractPlugin logged "(inline)").
		if n := len(extractPluginsLoaded); n > 0 && extractPluginsLoaded[n-1].Name == name {
			extractPluginsLoaded[n-1].Source = path
		}
		return name, nil
	}

	// "<name>.macro.js" is a JS PRE-PARSE MACRO (expand(args,ctx) -> SQL++ text; see
	// ext_macro_jsvm.go), checked before the generic ".js" scalar loader since it
	// also ends in ".js". It registers into the macro registry (macro.go), not a
	// SQL function, so it is tracked there (ListMacros), not in extLoaded.
	if lower := strings.ToLower(base); strings.HasSuffix(lower, ".macro.js") {
		name := strings.TrimSuffix(lower, ".macro.js")
		src, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		if err := RegisterJSMacro(name, string(src)); err != nil {
			return "", err
		}
		if e := macroRegistry[strings.ToLower(name)]; e != nil {
			e.source = path // RegisterJSMacro recorded "(inline)".
		}
		return name, nil
	}

	// "<name>.stream.js" is a JS STREAMING TABLE-VALUED SOURCE (emit protocol; see
	// ext_stream_jsvm.go), checked before the generic ".js" scalar loader.
	if lower := strings.ToLower(base); strings.HasSuffix(lower, ".stream.js") {
		name := strings.TrimSuffix(lower, ".stream.js")
		src, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		if err := RegisterJSStream(name, string(src)); err != nil {
			return "", err
		}
		extLoaded[name] = ExtensionInfo{Name: name, Kind: "javascript-stream", Source: path}
		noteBuiltinShadow(name, path)
		return name, nil
	}

	// "<name>.agg.js" is a JS AGGREGATE (3-callback protocol; see ext_agg_jsvm.go),
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
		noteBuiltinShadow(name, path)
		return name, nil
	}

	// A generic "<name>.js" that sets exports.functions is a MULTI-EXPORT MODULE: register
	// its whole family and record EACH function individually (so `.extensions
	// list/show/examples/test/unload` work per function, all sharing this file as source).
	// A plain single-function .js falls through to the generic loader below.
	if lower := strings.ToLower(base); strings.HasSuffix(lower, ".js") {
		src, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		if looksLikeJSModule(string(src)) {
			name := strings.TrimSuffix(lower, ".js")
			if _, err := registerJSModule(name, string(src), path); err != nil {
				return "", err
			}
			return name, nil
		}
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
	noteBuiltinShadow(name, path)
	return name, nil
}

// RegisterExtensionDir scans dir (non-recursively) and registers every file
// whose extension is a recognized extension kind, skipping the rest (READMEs,
// etc.). The directory IS the catalog (DESIGN-extensions.md); `git pull` to
// update. Returns the registered names (sorted). Opt-in, per the security note
// above.
//
// Files are registered in **sorted filename order** (os.ReadDir is already
// name-sorted; we re-sort explicitly so the guarantee is the code's, not the
// OS's). Because all loaded JS shares one runtime scope, this gives authors
// deterministic collision control: when two files define the same top-level
// name, the alphabetically-later file wins (last definition wins in JS), so a
// `zz_overrides.js` reliably shadows an earlier `base.js`.
// RegisterExtensionGlob registers every recognized extension file matching a filepath
// glob, in sorted order (like RegisterExtensionDir, but pattern-based). It lets an
// embedder pull in the shipped builtin modules by naming convention, e.g.
// RegisterExtensionGlob("extensions/functions/builtin_*.js"). A non-extension match
// is skipped. Returns the registered names (per file; a module returns its bundle stem).
func RegisterExtensionGlob(pattern string) ([]string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("RegisterExtensionGlob %q: %w", pattern, err)
	}
	sort.Strings(matches) // deterministic load order -> later file wins on name clash
	var names []string
	for _, m := range matches {
		if _, ok := extensionLoaders[strings.ToLower(filepath.Ext(m))]; !ok {
			continue // not a recognized extension file
		}
		name, err := RegisterExtensionFile(m)
		if err != nil {
			return names, err
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func RegisterExtensionDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("RegisterExtensionDir %q: %w", dir, err)
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, ok := extensionLoaders[strings.ToLower(filepath.Ext(e.Name()))]; !ok {
			continue // not a recognized extension file
		}
		files = append(files, e.Name())
	}
	sort.Strings(files) // deterministic load order -> later file wins on name clash

	var names []string
	for _, f := range files {
		name, err := RegisterExtensionFile(filepath.Join(dir, f))
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
	info, ok := extLoaded[name]
	if !ok {
		return fmt.Errorf("extension %q is not loaded", name)
	}
	unregisterJSFunc(name)
	forgetExtExamples(info.Kind, name)
	delete(extLoaded, name)
	return nil
}

// --- built-in shadowing visibility (ISSUE-23) ---
//
// Bundled (embedded, auto-registered) extensions and user -ext loads share one
// name registry, and shadowing a bundled name IS the documented fork workflow
// (copy builtin_census_agg.js out, edit, -ext it). The hazard is silence: same
// name, same declared version, different answers, and the bundled entry used to
// vanish from `.extensions list` entirely. So the override stays allowed and
// becomes VISIBLE: registration records which names the built-ins provide,
// notes every later non-built-in registration of such a name, and the CLI
// surfaces both (a load-time stderr line + list annotations on both rows).

// extBuiltinProvider records, per function name, the built-in module bundle
// that provides it (populated only for "(built-in)" module registrations).
var extBuiltinProvider = map[string]string{}

// extShadowNotices accumulates human-readable shadow events since the last
// drain (ExtShadowNotices); the CLI prints them to stderr after each load.
var extShadowNotices []string

// noteBuiltinShadow records that a non-built-in registration of name shadows a
// bundled built-in, with a version-aware wording: a fork declaring an OLDER
// version than the bundle is probably stale (a fix shipped that the fork
// predates); a newer one is probably deliberate.
func noteBuiltinShadow(name, newSource string) {
	bundle, ok := extBuiltinProvider[strings.ToLower(name)]
	if !ok || newSource == "(built-in)" {
		return
	}
	bunVer := ""
	if src, ok := ModuleSource(bundle); ok {
		bunVer = JSFrontMatter(src)["version"]
	}
	newVer := ""
	if b, err := os.ReadFile(newSource); err == nil {
		newVer = JSFrontMatter(string(b))["version"]
	}
	msg := fmt.Sprintf("extension %q (%s) shadows the bundled built-in %s", name, newSource, bundle)
	if bunVer != "" {
		msg += "@" + bunVer
	}
	switch cmp := versionCompare(newVer, bunVer); {
	case newVer == "" || bunVer == "":
		// nothing comparable; leave the bare notice.
	case cmp < 0:
		msg += fmt.Sprintf(" -- yours declares %s, OLDER than the bundled %s: likely a stale fork missing bundled fixes", newVer, bunVer)
	case cmp > 0:
		msg += fmt.Sprintf(" (yours declares %s, newer -- assuming a deliberate fork)", newVer)
	default:
		msg += fmt.Sprintf(" -- SAME declared version %s but possibly different behavior; bump your fork's // version: when you change it", newVer)
	}
	extShadowNotices = append(extShadowNotices, msg)
}

// ExtShadowNotices drains the shadow events recorded since the last call.
func ExtShadowNotices() []string {
	s := extShadowNotices
	extShadowNotices = nil
	return s
}

// ShadowedBuiltins returns bundle -> ExtensionInfo of the CURRENT (shadowing)
// provider, for every built-in-provided name whose active registration is not
// the built-in anymore -- so `.extensions list` can keep the shadowed bundle
// visible and annotate the shadowing row.
func ShadowedBuiltins() map[string]ExtensionInfo {
	out := map[string]ExtensionInfo{}
	for name, bundle := range extBuiltinProvider {
		if info, ok := extLoaded[name]; ok && info.Source != "(built-in)" {
			out[bundle] = info
		}
	}
	return out
}

// versionCompare compares two "v1.2.3"-style artifact versions numerically per
// dot-segment ("v" prefix optional). 0 when equal or either is unparseable.
func versionCompare(a, b string) int {
	pa, oka := versionParse(a)
	pb, okb := versionParse(b)
	if !oka || !okb {
		return 0
	}
	for i := 0; i < len(pa) || i < len(pb); i++ {
		va, vb := 0, 0
		if i < len(pa) {
			va = pa[i]
		}
		if i < len(pb) {
			vb = pb[i]
		}
		if va != vb {
			if va < vb {
				return -1
			}
			return 1
		}
	}
	return 0
}

func versionParse(v string) ([]int, bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if v == "" {
		return nil, false
	}
	parts := strings.Split(v, ".")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
}
