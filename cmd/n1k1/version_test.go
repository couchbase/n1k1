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

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestPrintVersion(t *testing.T) {
	var buf bytes.Buffer
	printVersion(&buf)
	out := buf.String()

	// First line is "<prog> <version>"; the version var defaults to "dev" when
	// no -ldflags injection happened (as in a plain `go test`).
	if !strings.Contains(out, version) {
		t.Errorf("version %q not in output:\n%s", version, out)
	}
	// The Go toolchain line is always emitted.
	if !strings.Contains(out, "go:") {
		t.Errorf("missing 'go:' line:\n%s", out)
	}
	// The test binary carries embedded build info, so deps (with the couchbase/query
	// => n1k1-query replace) should be listed with their SHAs.
	if !strings.Contains(out, "dependencies (") {
		t.Errorf("missing dependency listing:\n%s", out)
	}
	if strings.Contains(out, "github.com/couchbase/query") && !strings.Contains(out, "=>") {
		t.Errorf("expected the couchbase/query replace to show '=>':\n%s", out)
	}
}
