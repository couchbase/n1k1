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

package builtins

import (
	"strings"
	"testing"
)

// TestRegistryLint is the shipping gate for EVERY embedded builtin: it parses, it
// declares a version, every declared param renders, and the template contains no
// undeclared placeholder. Adding a broken *.sql++ to this directory fails here, not
// in a user's session.
func TestRegistryLint(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	if len(all) == 0 {
		t.Fatal("no embedded builtins found")
	}
	for _, q := range all {
		t.Run(q.Name, func(t *testing.T) {
			if q.Version == "" {
				t.Errorf("%s: missing `-- version:` front-matter (the artifact's version)", q.Name)
			}
			if q.Description == "" {
				t.Errorf("%s: missing `-- description:`", q.Name)
			}
			// Fill every param (defaults + a placeholder for required) and render:
			// this catches type errors AND undeclared $(...) placeholders.
			params := map[string]string{}
			for _, p := range q.Params {
				switch {
				case p.Type == "int" && p.Default == "":
					params[p.Name] = "1"
				case p.Required:
					params[p.Name] = "x"
				}
			}
			resolved, rerr := q.Resolve(params)
			if rerr != nil {
				t.Fatalf("%s: Resolve: %v", q.Name, rerr)
			}
			sql, rerr := q.Render(resolved)
			if rerr != nil {
				t.Fatalf("%s: Render: %v", q.Name, rerr)
			}
			if strings.Contains(sql, "$") { // no unresolved $name may survive (adjust if a builtin ever needs a literal $)
				t.Errorf("%s: rendered SQL still contains a $ reference:\n%s", q.Name, sql)
			}
			// Every DECLARED param must appear in the template (a declared-but-unused
			// param is a doc lie).
			for _, p := range q.Params {
				if !strings.Contains(q.Template, "$"+p.Name) {
					t.Errorf("%s: declared param %q never used in the template", q.Name, p.Name)
				}
			}
		})
	}
}

// TestParamResolution pins the param contract on census.sql++: defaults apply,
// required is enforced by name, unknown params fail loud, and each type renders
// safely (ident backticked, int validated, list as a JSON string array).
func TestParamResolution(t *testing.T) {
	q, ok := Lookup("census.sql++")
	if !ok {
		t.Fatal("census.sql++ not embedded")
	}
	if q.Version == "" || !strings.HasPrefix(q.Version, "v") {
		t.Fatalf("census.sql++ version: got %q, want a v-prefixed artifact version", q.Version)
	}

	// Missing required keyspace names itself.
	if _, err := q.Resolve(map[string]string{}); err == nil || !strings.Contains(err.Error(), "keyspace") {
		t.Fatalf("missing keyspace: got %v", err)
	}
	// Unknown param fails loud (a typo silently ignored runs the wrong census).
	if _, err := q.Resolve(map[string]string{"keyspace": "k", "depht": "1"}); err == nil ||
		!strings.Contains(err.Error(), "depht") {
		t.Fatalf("unknown param: got %v", err)
	}
	// Defaults apply; explicit values win; rendering is type-safe.
	resolved, err := q.Resolve(map[string]string{"keyspace": "s", "exclude": "a, b"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved["type-field"] != "type" || resolved["depth"] != "2" {
		t.Fatalf("defaults not applied: %v", resolved)
	}
	sql, err := q.Render(resolved)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"FROM `s` r", `["a", "b"]`, "2 >= 2", "r.`type`"} {
		if !strings.Contains(sql, want) {
			t.Errorf("rendered SQL missing %q:\n%s", want, sql)
		}
	}
	// A non-integer depth refuses.
	bad, _ := q.Resolve(map[string]string{"keyspace": "s", "depth": "two"})
	if _, err := q.Render(bad); err == nil || !strings.Contains(err.Error(), "depth") {
		t.Fatalf("bad int: got %v", err)
	}
	// A backtick-smuggling identifier refuses.
	bad2, _ := q.Resolve(map[string]string{"keyspace": "s` UNION SELECT `x"})
	if _, err := q.Render(bad2); err == nil {
		t.Fatal("backticked identifier must refuse")
	}
}
