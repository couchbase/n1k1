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
//   - Drop-in JS scalar UDFs (Tier 2, goja) are OPT-IN -- an embedder calls
//     RegisterJSDir/RegisterJSFunc explicitly. They are NOT auto-loaded, since
//     executing user JS in-process is a real attack surface (see the Caveats in
//     DESIGN-extensions.md); the embedder decides when/whether to enable them.

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

// RegisterJSFunc registers a single goja JS scalar UDF: source must define a
// function whose name equals name, which then resolves as name(args) in SQL++.
// Safe to call at startup before parsing; not safe to call concurrently with
// query parsing.
func RegisterJSFunc(name, source string) error {
	return registerJSFunc(name, source)
}

// RegisterJSDir scans dir for "*.js" files and registers each as a scalar UDF
// whose SQL++ name is the file's base name (minus ".js"); the file must define
// a JS function of that same name. Returns the registered names (sorted).
//
// This is the "a bunch of JS in a directory / git repo" registry from
// DESIGN-extensions.md Tier 2: the directory IS the catalog; `git pull` to
// update. Intentionally opt-in (an embedder calls this) for the security
// reasons noted above.
func RegisterJSDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("RegisterJSDir %q: %w", dir, err)
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".js") {
			continue
		}
		name := strings.ToLower(strings.TrimSuffix(e.Name(), ".js"))
		src, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return names, fmt.Errorf("RegisterJSDir reading %q: %w", e.Name(), err)
		}
		if err := registerJSFunc(name, string(src)); err != nil {
			return names, err
		}
		names = append(names, name)
	}

	sort.Strings(names)
	return names, nil
}
