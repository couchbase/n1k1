//go:build js && n1ql

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

// Command n1k1-wasm is the browser build of n1k1: it compiles the SQL++/N1QL
// engine to WebAssembly and exposes a single query entry point to JavaScript.
//
// The page's JS populates an in-memory filesystem (see web/index.html's fs
// shim) at dataRoot with a <namespace>/<keyspace>/<key>.json tree, then this
// program opens one Session over it and registers globalThis.n1k1RunQuery(sql),
// which returns a JSON string {ok, rows, warnings, elapsedMs, count} (or
// {ok:false, error}). Everything runs client-side; there is no server.
package main

import (
	"encoding/json"
	"fmt"
	"syscall/js"
	"time"

	"github.com/couchbase/n1k1/glue"
)

// dataRoot is the virtual directory the JS fs shim mounts the sample datasets
// under. It must match the mount path in web/index.html.
const dataRoot = "/n1k1data"

// namespace is the only namespace the file datastore uses (see cmd/n1k1).
const namespace = "default"

// session is the process-wide engine session over the mounted datasets. Opened
// once at startup; every n1k1RunQuery reuses it.
var session *glue.Session

func main() {
	sess, err := glue.OpenSession(dataRoot, namespace)
	if err != nil {
		// Surface the failure to the page and stop; the UI shows #status.
		js.Global().Set("n1k1InitError", js.ValueOf(err.Error()))
		fmt.Println("n1k1 OpenSession error:", err)
		select {}
	}
	session = sess

	js.Global().Set("n1k1RunQuery", js.FuncOf(runQuery))
	js.Global().Set("n1k1Ready", js.ValueOf(true))
	fmt.Println("n1k1 wasm ready; datasets mounted at", dataRoot)

	// Keep the Go runtime alive so the exported callback stays callable.
	select {}
}

// runQuery is the JS-callable entry point: args[0] is the SQL++ statement, and
// it returns a JSON string the page renders. Marshaling the whole response to a
// string (rather than a live JS object) keeps the boundary simple and the
// []json.RawMessage rows verbatim.
func runQuery(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 || args[0].Type() != js.TypeString {
		return respondError("n1k1RunQuery(sql): expected a SQL string")
	}
	stmt := args[0].String()

	start := time.Now()
	res, err := session.Run(stmt)
	if err != nil {
		return respondError(err.Error())
	}

	// Result.Rows are already canonical JSON values; splice them into one array
	// without re-parsing.
	rows := make([]json.RawMessage, len(res.Rows))
	copy(rows, res.Rows)

	warnings := make([]string, 0, len(res.Warnings))
	for _, w := range res.Warnings {
		warnings = append(warnings, w.Error())
	}

	elapsedMs := float64(res.Elapsed) / float64(time.Millisecond)
	if res.Elapsed == 0 {
		elapsedMs = float64(time.Since(start)) / float64(time.Millisecond)
	}

	out, merr := json.Marshal(struct {
		OK        bool              `json:"ok"`
		Rows      []json.RawMessage `json:"rows"`
		Warnings  []string          `json:"warnings"`
		ElapsedMs float64           `json:"elapsedMs"`
		Count     int               `json:"count"`
	}{true, rows, warnings, elapsedMs, len(rows)})
	if merr != nil {
		return respondError("result marshal: " + merr.Error())
	}
	return string(out)
}

// respondError returns the JSON error envelope the page expects.
func respondError(msg string) string {
	b, _ := json.Marshal(struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}{false, msg})
	return string(b)
}
