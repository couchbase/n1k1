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
	"encoding/json"
	"testing"
)

// TestJSUDFNestedObjectMarshal covers ISSUE-01: an object/array argument must reach
// a JS UDF as a fully-plain value at EVERY depth — nested field access, nested
// arrays (Array.isArray/.length/[i]), and a depth-1 number's typeof must all behave.
// Before the fix a shallow value.Actual() left nested values boxed, so JS saw the Go
// method set and depth>=2 access was undefined.
func TestJSUDFNestedObjectMarshal(t *testing.T) {
	src := `function cc_shapeof(x){
		return {
			deep:      x.u.cc.n,                       // depth-2 field
			isArr:     Array.isArray(x.blocks),        // nested array is a real array
			len:       x.blocks.length,                // ...with a length
			elem0type: x.blocks[0].type,               // ...and indexable objects
			numType:   (typeof x.u.cc.n)               // a nested number is typeof "number"
		};
	}`
	if err := RegisterJSFunc("cc_shapeof", src); err != nil {
		t.Fatalf("RegisterJSFunc: %v", err)
	}

	sess, err := OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	res, err := sess.Run(`SELECT cc_shapeof({"blocks":[{"type":"text"}],"u":{"cc":{"n":7}}}) AS p`)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(res.Rows))
	}
	var got struct {
		P struct {
			Deep      float64 `json:"deep"`
			IsArr     bool    `json:"isArr"`
			Len       float64 `json:"len"`
			Elem0Type string  `json:"elem0type"`
			NumType   string  `json:"numType"`
		} `json:"p"`
	}
	if err := json.Unmarshal(res.Rows[0], &got); err != nil {
		t.Fatalf("unmarshal %s: %v", res.Rows[0], err)
	}
	if got.P.Deep != 7 {
		t.Errorf("depth-2 field x.u.cc.n: got %v, want 7 (nested value stayed boxed?)", got.P.Deep)
	}
	if !got.P.IsArr {
		t.Errorf("Array.isArray(x.blocks): got false (nested array boxed?)")
	}
	if got.P.Len != 1 {
		t.Errorf("x.blocks.length: got %v, want 1", got.P.Len)
	}
	if got.P.Elem0Type != "text" {
		t.Errorf("x.blocks[0].type: got %q, want \"text\"", got.P.Elem0Type)
	}
	if got.P.NumType != "number" {
		t.Errorf("typeof x.u.cc.n: got %q, want \"number\"", got.P.NumType)
	}

	// The idiomatic guard the reporter hit: typeof v === "number" must accept a
	// numeric object field (was "object" => rejected => silent 0).
	if err := RegisterJSFunc("cc_tok", `function cc_tok(u){return (typeof u.input_tokens==="number")?u.input_tokens:0;}`); err != nil {
		t.Fatalf("RegisterJSFunc: %v", err)
	}
	r2, err := sess.Run(`SELECT cc_tok({"input_tokens":10}) AS n`)
	if err != nil {
		t.Fatalf("Run cc_tok: %v", err)
	}
	var got2 struct {
		N float64 `json:"n"`
	}
	json.Unmarshal(r2.Rows[0], &got2)
	if got2.N != 10 {
		t.Errorf(`cc_tok({"input_tokens":10}): got %v, want 10`, got2.N)
	}
}
