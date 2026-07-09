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

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// A STREAMING table-valued source is a FROM-clause function that produces rows ONE
// AT A TIME instead of returning one big materialized array -- so `FROM src(...) AS
// x` never builds the whole result set in memory: each row flows straight into the
// pipeline (with early-exit backpressure), composing with WHERE/GROUP BY/LIMIT at
// bounded memory.
//
// StreamSource is the small contract such a source implements. conv.go's
// VisitExpressionScan recognizes ANY FROM expression that implements it and routes
// it to the single generic "stream-fn" op (StreamFnOp) -- so a new streaming source
// needs only to implement this interface; it does NOT need its own op or its own
// dispatch case. Today the JS streaming source (*.stream.js, ext_jsvm_stream.go)
// and RULE_MATCHES (rule_matches.go) both ride this one op.
type StreamSource interface {
	// StreamRows drives the source. It evaluates its own SQL operands against
	// (item, ctx), then calls emit(row) for each produced row -- one row per call,
	// where row is the JSON bytes bound to the FROM alias. emit returns true to keep
	// producing and false once the consumer wants no more (an early-exit was
	// signalled, e.g. a satisfied LIMIT), so a long/endless source can stop. gc is
	// the GlueContext (nil if the FROM-expr runs without one). Return a terminal
	// error, or nil for a clean end-of-stream.
	StreamRows(vars *base.Vars, gc *GlueContext, ctx expression.Context,
		item value.Value, emit func(base.Val) bool) error
}

// streamFnPollEvery is how often (in rows emitted) the generic op polls YieldStats
// for an early-exit request (e.g. a satisfied LIMIT), mirroring op_scan.
const streamFnPollEvery = 256

// StreamFnOp is the generic FROM-clause streaming-source operator. It owns the
// parts every streaming source shares -- eval-context / correlation resolution, the
// emit callback, the YieldStats stop-poll, and terminal-error propagation -- and
// delegates only the row PRODUCTION to the StreamSource at Temps[Params[0]].
//
// Caveat (matches every n1k1 source today): a source runs to completion -- there is
// no per-producer early-exit into the source's own scan yet, so `LIMIT k` yields the
// right rows but a source that ignores emit's false return only stops feeding the
// pipeline, it doesn't stop its own work. Sources that CAN stop (checking emit's
// return) do so; either way memory stays bounded because we stop yielding.
func StreamFnOp(o *base.Op, vars *base.Vars, yieldVals base.YieldVals, yieldErr base.YieldErr) {
	idx, ok := o.Params[0].(int)
	if !ok {
		yieldErr(fmt.Errorf("stream-fn: expected int Temps index, got %T", o.Params[0]))
		return
	}
	src, ok := vars.Temps[idx].(StreamSource)
	if !ok {
		yieldErr(fmt.Errorf("stream-fn: Temps[%d] is not a StreamSource (got %T)", idx, vars.Temps[idx]))
		return
	}

	// Eval context + correlation scope, as ExprScanOp resolves them.
	var ctx expression.Context
	var item value.Value
	var gc *GlueContext
	if c, ok := vars.Temps[0].(*GlueContext); ok {
		gc, ctx, item = c, c, c.corrParent
	} else if c, ok := vars.Temps[0].(expression.Context); ok {
		ctx = c
	}

	// stopErr carries an early-exit sentinel (from YieldStats, e.g. LIMIT) or a
	// terminal error; nil for a clean end-of-stream.
	var stopErr error
	rowsOut := 0

	// pollStop asks whether a downstream consumer wants us to stop (LIMIT), the same
	// YieldStats gate scans use. Only active when stats are on.
	pollStop := func() bool {
		if vars.Ctx != nil && vars.Ctx.YieldStats != nil && vars.Ctx.Stats != nil {
			if ctl := vars.Ctx.YieldStats(vars.Ctx.Stats); ctl.Stop != nil {
				stopErr = ctl.Stop
				return true
			}
		}
		return false
	}

	// emit(row) yields one row into the pipeline and returns whether the source
	// should keep producing (false once an early-exit is signalled).
	emit := func(row base.Val) bool {
		if stopErr != nil {
			return false
		}
		yieldVals(base.Vals{row})
		rowsOut++
		if rowsOut%streamFnPollEvery == 0 && pollStop() {
			return false
		}
		return true
	}

	if err := src.StreamRows(vars, gc, ctx, item, emit); err != nil && stopErr == nil {
		stopErr = err
	}

	// Propagate the early-exit sentinel / error / clean nil, as scans do.
	yieldErr(stopErr)
}
