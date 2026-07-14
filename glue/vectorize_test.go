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
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

// vecRow1 runs a single-row SELECT and returns row 0 decoded as a map.
func vecRow1(t *testing.T, sess *Session, q string) map[string]interface{} {
	t.Helper()
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%q: %d rows, want 1", q, len(res.Rows))
	}
	var m map[string]interface{}
	if err := json.Unmarshal(res.Rows[0], &m); err != nil {
		t.Fatalf("decode %q: %v", q, err)
	}
	return m
}

// TestVectorizeBatchFake: the offline deterministic path -- id preserved, dim honored,
// unit vector, and identical text -> identical vector (distinct text -> distinct).
func TestVectorizeBatchFake(t *testing.T) {
	sess, err := OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatal(err)
	}

	e := vecRow1(t, sess, `SELECT VECTORIZE_BATCH([{"id":"a","text":"disk full"}], {"dim":8})[0] AS e`)["e"].(map[string]interface{})
	if e["id"] != "a" {
		t.Errorf("id not preserved: %v", e["id"])
	}
	vec, ok := e["vec"].([]interface{})
	if !ok || len(vec) != 8 {
		t.Fatalf("vec dim = %v, want 8", e["vec"])
	}
	var ss float64
	for _, x := range vec {
		f := x.(float64)
		ss += f * f
	}
	if math.Abs(ss-1.0) > 1e-9 {
		t.Errorf("fake vector is not unit-length: |v|^2 = %v", ss)
	}

	d := vecRow1(t, sess, `SELECT `+
		`VECTORIZE_BATCH([{"t":"x"}],{"text":"t","dim":8})[0].vec = VECTORIZE_BATCH([{"t":"x"}],{"text":"t","dim":8})[0].vec AS same, `+
		`VECTORIZE_BATCH([{"t":"x"}],{"text":"t","dim":8})[0].vec = VECTORIZE_BATCH([{"t":"y"}],{"text":"t","dim":8})[0].vec AS diff`)
	if d["same"] != true {
		t.Errorf("identical text should give equal vectors, got same=%v", d["same"])
	}
	if d["diff"] != false {
		t.Errorf("different text should give different vectors, got diff=%v", d["diff"])
	}
}

// TestVectorizeBatchHTTP: the real endpoint path (cgo-free net/http), driven by a stub
// ollama-shaped embeddings server -- proves the whole batch round-trip with no model.
func TestVectorizeBatchHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		// Return a deterministic vector per input whose last element = len(text), so the
		// test can tell inputs apart and confirm order/pairing.
		embs := make([][]float64, len(req.Input))
		for i, s := range req.Input {
			embs[i] = []float64{1, 0, float64(len(s))}
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"embeddings": embs})
	}))
	defer srv.Close()

	sess, err := OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatal(err)
	}
	q := `SELECT VECTORIZE_BATCH([{"text":"ab"},{"text":"abcd"}], {"endpoint":"` + srv.URL + `","model":"m"}) AS out`
	out := vecRow1(t, sess, q)["out"].([]interface{})
	if len(out) != 2 {
		t.Fatalf("got %d vectors, want 2", len(out))
	}
	v0 := out[0].(map[string]interface{})["vec"].([]interface{})
	v1 := out[1].(map[string]interface{})["vec"].([]interface{})
	if len(v0) != 3 || v0[2].(float64) != 2 { // len("ab") == 2
		t.Errorf("http vec[0] = %v, want [...,2]", v0)
	}
	if v1[2].(float64) != 4 { // len("abcd") == 4
		t.Errorf("http vec[1] = %v, want [...,4]", v1)
	}
}
