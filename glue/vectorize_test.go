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
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// TestVectorizeBatchBase64: the endpoint returns each embedding as a base64 string of
// little-endian float32 bytes (OpenAI encoding_format:"base64"). VECTORIZE_BATCH must
// decode it back to the float vector -- proving the transport-decode path.
func TestVectorizeBatchBase64(t *testing.T) {
	want := []float32{0.5, -0.25, 1.5}
	b64 := func(fs []float32) string {
		buf := make([]byte, 4*len(fs))
		for i, f := range fs {
			binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
		}
		return base64.StdEncoding.EncodeToString(buf)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input          []string `json:"input"`
			EncodingFormat string   `json:"encoding_format"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.EncodingFormat != "base64" {
			t.Errorf("server expected encoding_format=base64, got %q", req.EncodingFormat)
		}
		embs := make([]string, len(req.Input))
		for i := range req.Input {
			embs[i] = b64(want)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"embeddings": embs})
	}))
	defer srv.Close()

	sess, err := OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatal(err)
	}
	q := `SELECT VECTORIZE_BATCH([{"text":"x"}], {"endpoint":"` + srv.URL + `","model":"m","encoding":"base64"})[0].vec AS v`
	v := vecRow1(t, sess, q)["v"].([]interface{})
	if len(v) != len(want) {
		t.Fatalf("base64 vec len %d, want %d", len(v), len(want))
	}
	for i, w := range want {
		if math.Abs(v[i].(float64)-float64(w)) > 1e-6 {
			t.Errorf("base64 vec[%d] = %v, want %v", i, v[i], w)
		}
	}
}

// TestVectorDistanceNativeMatchesBoxed: the native byte-lane VECTOR_DISTANCE
// (EnableNativeVectorDistance, default on) must produce BYTE-IDENTICAL rows to cbq's
// boxed vectorDistance -- it's a drop-in replacement, so a differential over the same
// query with the flag toggled is the correctness guarantee.
func TestVectorDistanceNativeMatchesBoxed(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "default", "vd")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := []string{
		`{"id":1,"vec":[1,0,0]}`,
		`{"id":2,"vec":[0,1,0]}`,
		`{"id":3,"vec":[0.9,0.1,0]}`,
		`{"id":4,"vec":[0,0,0]}`,   // zero vector -> cosine NULL
		`{"id":5,"vec":[1,2]}`,     // wrong dim -> NULL
		`{"id":6,"vec":"nope"}`,    // non-array -> NULL
		`{"id":7}`,                 // missing vec -> MISSING
	}
	for i, d := range docs {
		if err := os.WriteFile(filepath.Join(dir, "d"+string(rune('0'+i))+".json"), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	run := func(native bool, q string) []string {
		prev := EnableNativeVectorDistance
		EnableNativeVectorDistance = native
		defer func() { EnableNativeVectorDistance = prev }()
		sess, err := OpenSession(root, "default")
		if err != nil {
			t.Fatalf("OpenSession: %v", err)
		}
		res, err := sess.Run(q)
		if err != nil {
			t.Fatalf("Run (native=%v) %q: %v", native, q, err)
		}
		out := make([]string, len(res.Rows))
		for i, r := range res.Rows {
			out[i] = string(r)
		}
		return out
	}

	for _, metric := range []string{"cosine", "l2", "l2_squared", "dot"} {
		q := `SELECT t.id, VECTOR_DISTANCE(t.vec, [1,0,0], "` + metric + `") AS d FROM vd t ORDER BY t.id`
		native, boxed := run(true, q), run(false, q)
		if len(native) != len(boxed) {
			t.Fatalf("%s: native %d rows, boxed %d rows", metric, len(native), len(boxed))
		}
		for i := range native {
			if native[i] != boxed[i] {
				t.Errorf("%s row %d: native %s != boxed %s", metric, i, native[i], boxed[i])
			}
		}
	}
}
