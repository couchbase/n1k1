//go:build n1ql

//  Copyright (c) 2019 Couchbase, Inc.
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
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/rt"

	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"
)

// ServiceRequestEx runs a planned statement through n1k1's own operators.
// (Formerly took a query/server.Request as its first arg -- dropped as part of
// decoupling n1k1 from query/server, which pulled in cgo deps. The arg was
// unused.)
func ServiceRequestEx(p plan.Operator,
	ctx *GlueContext, timeout time.Duration, asyncReadyCB func()) bool {
	texter, ok := p.(interface{ Text() string })
	if !ok || !strings.HasSuffix(texter.Text(), " n1k1 */") {
		return false
	}

	// Attempt to convert the plan.Operator to base.Op.
	op, temps, err := ExecConv(p)
	if err != nil || op == nil {
		fmt.Printf("ServiceRequestEx: op: %v,\n  err: %v\n", op, err)

		return false // We saw an unsupported operator.
	}

	// A -> B wiring (DESIGN-merging.md §3): now that ctx + temps are in hand, fire the
	// UNION-ALL -> merge-scan rewrite for any order(union-all) whose sort key Track A's
	// SortedSourceMeta proves is a normalized int64 sorted source, with real per-branch
	// sortedness/disorder/zone-map Params. A no-op (leaves order(union-all)) otherwise.
	// Wrap the bare temps in a throwaway Conv so the per-file merge expansion can
	// register single-file keyspacers via AddTemp; read temps back afterwards.
	mergeConv := &Conv{Temps: temps}
	op = WireTemporalMergeMeta(op, mergeConv, ctx)
	temps = mergeConv.Temps

	cv, err := NewConvertVals(op.Labels)
	if err != nil {
		fmt.Printf("ServiceRequestEx: NewConvertVals, op: %v, err: %v\n", op, err)

		return false // We couldn't create a convert-vals.
	}

	go asyncReadyCB()

	tmpDir, vars := MakeVars("", "n1k1TmpDir") // TODO: Config.

	defer os.RemoveAll(tmpDir)

	vars.Temps = vars.Temps[:0]

	vars.Temps = append(vars.Temps, ctx)

	vars.Temps = append(vars.Temps, temps[1:]...)

	debug := strings.HasSuffix(texter.Text(), " debug n1k1 */")
	if debug {
		fmt.Printf("ServiceRequestEx, p: %#v\n", p)

		jop, _ := json.MarshalIndent(op, " ", " ")
		fmt.Printf("  jop: %s\n", jop)

		fmt.Printf("  tmpDir: %s\n", tmpDir)
		fmt.Printf("  vars.Temps: %#v\n", vars.Temps)
	}

	for i := 0; i < 16; i++ { // TODO: Config.
		vars.Temps = append(vars.Temps, nil)
	}

	err = nil

	yieldErr := func(errIn error) {
		if errIn != nil && err == nil {
			err = errIn // Keep first err.
		}
	}

	yieldVals := func(vals base.Vals) {
		v, err := cv.Convert(vals)
		if err == nil {
			item, ok := v.(value.AnnotatedValue)
			if !ok {
				item = value.NewAnnotatedValue(v)
			}

			ok = ctx.Result(item)

			_ = ok // TODO: Do something with the ok?

			// TODO: Handle non-nil err?
		}
	}

	// TODO: YieldStats.

	// TODO: Better allocators / recyclers.

	// TODO: The SetUp() method disappeared after CB 6.5, but
	// perhaps was replaced by some other method or call path.
	// ctx.SetUp()

	vars.Ctx.ExecOp(op, vars, yieldVals, yieldErr, "", "")

	if debug {
		fmt.Printf("  glue/ServiceRequestEx err: %v\n", err)
	}

	ctx.CloseResults()

	return true
}

func MakeVars(dir, prefix string) (string, *base.Vars) {
	// TODO: Need err propagation & cleanup of temp dir?
	tmpDir, _ := ioutil.TempDir(dir, prefix)

	// The Ctx -- ValComparer + the spill-backed allocator pools GROUP/ORDER/join
	// hang off -- is built by rt.NewSpillCtx, the SAME cbq-free constructor the
	// compiled standalone EXECUTE child uses (see glue/compiled_exec.go). Sharing it
	// is deliberate: a hand-rolled child Ctx once omitted these pools, nil-panicking
	// every aggregate in the compiled lane while the interpreter was fine.
	return tmpDir, &base.Vars{
		Temps: make([]interface{}, 16),
		Ctx:   rt.NewSpillCtx(tmpDir),
	}
}
