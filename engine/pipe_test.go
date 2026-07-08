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

package engine

import (
	"reflect"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// TestMemPipeScan runs a datastore-scan-records leaf through engine.ExecOp with a
// MemPipe as the datastore source -- proving a query reads inline data with only
// engine + base (this test package imports no cbq/glue). This is the
// zero-datastore-deps standalone path DESIGN-prepare.md phase 2 targets.
func TestMemPipeScan(t *testing.T) {
	mem := &MemPipe{Data: map[string][]MemRecord{
		"b": {
			{ID: "k1", Doc: []byte(`{"i":1}`)},
			{ID: "k2", Doc: []byte(`{"i":2}`)},
		},
	}}
	scan := &base.Op{Kind: "datastore-scan-records", Labels: base.Labels{`.["b"]`, "^id"}}
	vars := &base.Vars{Ctx: &base.Ctx{Pipe: mem}}

	// Route ExecOp's default (datastore-leaf) branch to the request's Pipe. (glue
	// wires this for the interpreter; the compiled path will emit a Pipe call.)
	orig := ExecOpEx
	defer func() { ExecOpEx = orig }()
	ExecOpEx = func(o *base.Op, v *base.Vars, yv base.YieldVals, ye base.YieldErr, p, pn string) {
		v.Ctx.Pipe.Op(o, v, yv, ye, p, pn)
	}

	var got [][]string
	yieldVals := func(vals base.Vals) {
		row := make([]string, len(vals))
		for i, v := range vals {
			row[i] = string(v) // copy out of the reused Vals buffer
		}
		got = append(got, row)
	}
	var gotErr error
	yieldErr := func(e error) {
		if e != nil {
			gotErr = e
		}
	}

	ExecOp(scan, vars, yieldVals, yieldErr, "", "")
	if gotErr != nil {
		t.Fatalf("scan error: %v", gotErr)
	}

	want := [][]string{
		{`{"i":1}`, `"k1"`}, // {doc, ^id}, ^id canonical-quoted like the file scan
		{`{"i":2}`, `"k2"`},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("MemPipe scan rows = %v, want %v", got, want)
	}
}

// TestMemPipeScanAlias checks the alias extraction from a scan op's `.["alias"]`
// output label, and the unsupported-op error for a kind a MemPipe can't serve.
func TestMemPipeScanAlias(t *testing.T) {
	cases := map[string]string{
		`.["b"]`:      "b",
		`.["orders"]`: "orders",
		"^id":         "", // not an alias label
		".":           "", // whole-row
		"":            "",
	}
	for label, want := range cases {
		op := &base.Op{Labels: base.Labels{label}}
		if got := MemPipeScanAlias(op); got != want {
			t.Errorf("MemPipeScanAlias(%q) = %q, want %q", label, got, want)
		}
	}

	// A kind MemPipe doesn't serve (it holds whole records) errors cleanly.
	mem := &MemPipe{}
	var err error
	mem.Op(&base.Op{Kind: "datastore-fetch"}, &base.Vars{},
		func(base.Vals) {}, func(e error) { err = e }, "", "")
	if err == nil {
		t.Error("MemPipe should reject datastore-fetch with an error")
	}
}
