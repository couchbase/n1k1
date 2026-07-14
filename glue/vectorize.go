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

// vectorize.go implements MULTI-QUERY-adjacent VECTOR support (DESIGN-vectors.md):
// the VECTORIZE_BATCH(batch, opts) scalar function -- Phase 0. It embeds a BATCH of
// texts in ONE call (array-in, array-out) so a whole GROUP-BY page turns into one
// model round-trip, never one per row. Search itself needs no new code: cbq's
// VECTOR_DISTANCE(field, query, metric) already evaluates pure-Go (no cgo/FAISS), so
// `ORDER BY VECTOR_DISTANCE(v, $q, "cosine") ASC LIMIT k` is brute-force top-K today.
//
// Input: an ARRAY of objects, each with a text field (opts.text, default "text").
// Output: the SAME objects with a vector field added (opts.into, default "vec"), so
// id/other fields stay glued to the vector (no positional pairing). Then a wrapping
// query UNNESTs the result back to per-row {id, vec} (see the @vectorize_field macro).
//
// opts (an object, arg 2): {text, into, fake, dim, endpoint, model}. With no endpoint
// (or fake:true) it produces DETERMINISTIC pseudo-vectors (hash -> unit vector) so the
// whole pipeline is testable with no model and no network -- the Phase-0 de-risk. With
// an endpoint it POSTs the batch to an embeddings API (ollama /api/embed shape:
// {"model","input":[...]} -> {"embeddings":[[...]]}) over pure-Go net/http (cgo-free).

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// VectorizeBatchFuncName is the user-facing name: VECTORIZE_BATCH (cbq lower-cases).
const VectorizeBatchFuncName = "vectorize_batch"

// vectorizeHTTPTimeout bounds one embeddings round-trip.
var vectorizeHTTPTimeout = 60 * time.Second

// registerVectorizeBatchFunc wires VECTORIZE_BATCH into the cbq parser as a scalar
// (array-returning) function. Always-on, like MULTI_MATCHES; no grammar change. It
// only reaches the network when given an `endpoint` opt -- the default (no endpoint /
// fake:true) is offline deterministic vectors, so registering it is side-effect-free.
func registerVectorizeBatchFunc() {
	if _, ok := expression.GetFunction(VectorizeBatchFuncName); ok {
		return // never shadow a stock builtin
	}
	expression.RegisterFunction(VectorizeBatchFuncName, newVectorizeBatchFunc(VectorizeBatchFuncName))
	extOurs[VectorizeBatchFuncName] = true
}

type vectorizeBatchFunc struct {
	expression.FunctionBase
}

func newVectorizeBatchFunc(name string, operands ...expression.Expression) expression.Function {
	rv := &vectorizeBatchFunc{}
	rv.Init(name, operands...)
	rv.SetExpr(rv)
	return rv
}

func (this *vectorizeBatchFunc) Accept(visitor expression.Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}

func (this *vectorizeBatchFunc) Type() value.Type { return value.ARRAY }

func (this *vectorizeBatchFunc) MinArgs() int { return 1 } // the batch array
func (this *vectorizeBatchFunc) MaxArgs() int { return 2 } // + optional opts object

func (this *vectorizeBatchFunc) Constructor() expression.FunctionConstructor {
	name := this.Name()
	return func(operands ...expression.Expression) expression.Function {
		return newVectorizeBatchFunc(name, operands...)
	}
}

// vectorizeOpts is the arg-2 options object, read leniently.
type vectorizeOpts struct {
	text     string // field on each batch object holding the text to embed
	into     string // field to add with the produced vector
	fake     bool   // force offline deterministic vectors (also the default with no endpoint)
	dim      int    // fake-vector dimension
	endpoint string // embeddings API URL (ollama /api/embed shape)
	model    string // model name sent to the endpoint
	encoding string // request-side encoding_format hint (e.g. "base64"); "" = endpoint default
}

func parseVectorizeOpts(v value.Value) vectorizeOpts {
	o := vectorizeOpts{text: "text", into: "vec", dim: 8}
	if v == nil || v.Type() != value.OBJECT {
		return o
	}
	if x, ok := v.Field("text"); ok && x.Type() == value.STRING {
		o.text, _ = x.Actual().(string)
	}
	if x, ok := v.Field("into"); ok && x.Type() == value.STRING {
		o.into, _ = x.Actual().(string)
	}
	if x, ok := v.Field("fake"); ok && x.Type() == value.BOOLEAN {
		o.fake, _ = x.Actual().(bool)
	}
	if x, ok := v.Field("dim"); ok && x.Type() == value.NUMBER {
		o.dim = int(value.AsNumberValue(x).Float64())
	}
	if x, ok := v.Field("endpoint"); ok && x.Type() == value.STRING {
		o.endpoint, _ = x.Actual().(string)
	}
	if x, ok := v.Field("model"); ok && x.Type() == value.STRING {
		o.model, _ = x.Actual().(string)
	}
	if x, ok := v.Field("encoding"); ok && x.Type() == value.STRING {
		o.encoding, _ = x.Actual().(string)
	}
	if o.dim < 1 {
		o.dim = 8
	}
	return o
}

func (this *vectorizeBatchFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	ops := this.Operands()

	batchV, err := ops[0].Evaluate(item, context)
	if err != nil {
		return nil, err
	}
	if batchV.Type() == value.MISSING {
		return value.MISSING_VALUE, nil
	}
	if batchV.Type() != value.ARRAY {
		return value.NULL_VALUE, nil
	}

	opts := vectorizeOpts{text: "text", into: "vec", dim: 8}
	if len(ops) >= 2 {
		optV, err := ops[1].Evaluate(item, context)
		if err != nil {
			return nil, err
		}
		opts = parseVectorizeOpts(optV)
	}

	// Collect the batch elements and their texts (navigate via Index -- an evaluated
	// array's elements are themselves value.Value).
	var elems []value.Value
	var texts []string
	for i := 0; ; i++ {
		e, ok := batchV.Index(i)
		if !ok {
			break
		}
		t := ""
		if tv, ok := e.Field(opts.text); ok && tv.Type() == value.STRING {
			t, _ = tv.Actual().(string)
		}
		elems = append(elems, e)
		texts = append(texts, t)
	}

	// Embed the whole batch in one shot.
	var vecs [][]interface{}
	if opts.fake || opts.endpoint == "" {
		vecs = make([][]interface{}, len(texts))
		for i, t := range texts {
			vecs[i] = fakeVector(t, opts.dim)
		}
	} else {
		vecs, err = httpEmbedBatch(opts.endpoint, opts.model, opts.encoding, texts)
		if err != nil {
			return nil, fmt.Errorf("VECTORIZE_BATCH: %w", err)
		}
	}

	// Add the vector to a copy of each object, keeping id/other fields glued.
	out := make([]interface{}, len(elems))
	for i, e := range elems {
		oe := e.Copy()
		if err := oe.SetField(opts.into, vecs[i]); err != nil {
			return nil, fmt.Errorf("VECTORIZE_BATCH: setting %q: %w", opts.into, err)
		}
		out[i] = oe
	}
	return value.NewValue(out), nil
}

// fakeVector returns a DETERMINISTIC unit vector for text: identical text -> identical
// vector (distance 0), different text -> a distinct pseudo-random direction. No semantic
// meaning -- it exercises the ingest/store/search plumbing with no model or network.
func fakeVector(text string, dim int) []interface{} {
	raw := make([]float64, dim)
	var ss float64
	for i := 0; i < dim; i++ {
		h := fnv.New32a()
		_, _ = h.Write([]byte(text))
		_, _ = h.Write([]byte{byte(i), byte(i >> 8)})
		f := float64(h.Sum32()%2000)/1000.0 - 1.0 // [-1, 1)
		raw[i] = f
		ss += f * f
	}
	n := math.Sqrt(ss)
	if n == 0 {
		n = 1
	}
	out := make([]interface{}, dim)
	for i := 0; i < dim; i++ {
		out[i] = raw[i] / n
	}
	return out
}

// httpEmbedBatch POSTs texts to an embeddings endpoint (ollama /api/embed shape) over
// pure-Go net/http (cgo-free) and returns one float vector per text. It cracks the
// TRANSPORT encoding of each embedding (DESIGN-vectors.md "Who cracks the API response
// encoding"): the `embeddings` field's elements may be a plain JSON float array OR a
// base64 string of little-endian float32 bytes (the OpenAI `encoding_format:"base64"`
// bandwidth trick) -- auto-detected per element. Bit-packed integer arrays ride through
// the float-array branch as numbers. `encoding` (if set) is sent as the request-side
// encoding_format hint.
func httpEmbedBatch(endpoint, model, encoding string, texts []string) ([][]interface{}, error) {
	req := map[string]interface{}{"model": model, "input": texts}
	if encoding != "" {
		req["encoding_format"] = encoding
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: vectorizeHTTPTimeout}
	resp, err := client.Post(endpoint, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("embeddings endpoint %s: HTTP %d: %s", endpoint, resp.StatusCode, b)
	}
	// Decode elements lazily: each may be a float array or a base64 string.
	var out struct {
		Embeddings []json.RawMessage `json:"embeddings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decoding embeddings response: %w", err)
	}
	if len(out.Embeddings) != len(texts) {
		return nil, fmt.Errorf("endpoint returned %d vectors for %d texts", len(out.Embeddings), len(texts))
	}
	vecs := make([][]interface{}, len(out.Embeddings))
	for i, raw := range out.Embeddings {
		v, err := decodeEmbedding(raw)
		if err != nil {
			return nil, fmt.Errorf("embedding %d: %w", i, err)
		}
		vecs[i] = v
	}
	return vecs, nil
}

// decodeEmbedding turns one response embedding into a float vector. A leading '"' marks a
// base64 string of little-endian float32 bytes; otherwise it is a JSON number array.
func decodeEmbedding(raw json.RawMessage) ([]interface{}, error) {
	r := bytes.TrimSpace(raw)
	if len(r) > 0 && r[0] == '"' {
		var s string
		if err := json.Unmarshal(r, &s); err != nil {
			return nil, err
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("base64: %w", err)
		}
		if len(b)%4 != 0 {
			return nil, fmt.Errorf("base64 float32 payload not a multiple of 4 bytes (%d)", len(b))
		}
		v := make([]interface{}, len(b)/4)
		for j := range v {
			v[j] = float64(math.Float32frombits(binary.LittleEndian.Uint32(b[j*4:])))
		}
		return v, nil
	}
	var fs []float64
	if err := json.Unmarshal(r, &fs); err != nil {
		return nil, err
	}
	v := make([]interface{}, len(fs))
	for j, f := range fs {
		v[j] = f
	}
	return v, nil
}
