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
	"time"

	"github.com/couchbase/query/encryption"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/tenant"
	"github.com/couchbase/query/value"
)

// GlueContext is n1k1's own evaluation + datastore context. It replaces
// query/execution.Context, which n1k1 previously reused but which transitively
// pulls cgo dependencies (cbft/jemalloc, sigar). Avoiding it lets n1k1 build
// with CGO_ENABLED=0 into a single, cross-compilable, self-contained binary.
//
// GlueContext satisfies the two query interfaces n1k1's glue actually needs:
//   - expression.Context: for expression.Expression.Evaluate(). Provided by the
//     embedded query/expression.IndexContext (no-op/default impls), with Now()
//     overridden. Inheriting the interface keeps n1k1 compiling as query evolves.
//   - datastore.Context: for datastore.NewIndexConnection() + index scans against
//     the file datastore. Implemented below as no-ops, except Error/Warning/Fatal
//     which accumulate into errs.
type GlueContext struct {
	*expression.IndexContext

	now  time.Time
	errs []errors.Error

	// Results collects rows when GlueContext is used as a result sink
	// (e.g. by ServiceRequestEx). The test path instead drives n1k1.ExecOp
	// directly with its own yield callbacks and ignores this.
	Results []value.AnnotatedValue

	// subq drives expression-subquery evaluation (see subquery.go). nil until
	// InitSubqueries is called; when nil, EvaluateSubquery errors.
	subq *subqEvaluator
}

// NewGlueContext returns a GlueContext stamped with the given "now".
func NewGlueContext(now time.Time) *GlueContext {
	return &GlueContext{IndexContext: &expression.IndexContext{}, now: now}
}

func (c *GlueContext) Now() time.Time { return c.now }

// Result collects a result row (stand-in for execution.Context.Result).
func (c *GlueContext) Result(item value.AnnotatedValue) bool {
	c.Results = append(c.Results, item)
	return true
}

// CloseResults is a no-op stand-in for execution.Context.CloseResults.
func (c *GlueContext) CloseResults() {}

// glueRequestId is the fixed request id used for n1k1's file-datastore index
// scans (n1k1 has no per-request id concept like the query server does).
const glueRequestId = "n1k1"

// --- datastore.Context ---

func (c *GlueContext) GetActiveEncryptionKey(dt encryption.KeyDataType) (*encryption.EaRKey, errors.Error) {
	return nil, nil
}

func (c *GlueContext) GetScanCap() int64   { return 0 }
func (c *GlueContext) MaxParallelism() int { return 1 }

func (c *GlueContext) Fatal(e errors.Error)   { c.errs = append(c.errs, e) }
func (c *GlueContext) Error(e errors.Error)   { c.errs = append(c.errs, e) }
func (c *GlueContext) Warning(e errors.Error) { c.errs = append(c.errs, e) }

func (c *GlueContext) GetErrors() []errors.Error { return c.errs }

func (c *GlueContext) GetReqDeadline() time.Time { return time.Time{} }

func (c *GlueContext) TenantCtx() tenant.Context { return nil }

func (c *GlueContext) SetFirstCreds(string)       {}
func (c *GlueContext) FirstCreds() (string, bool) { return "", false }

func (c *GlueContext) RecordFtsRU(ru tenant.Unit) {}
func (c *GlueContext) RecordGsiRU(ru tenant.Unit) {}
func (c *GlueContext) RecordKvRU(ru tenant.Unit)  {}
func (c *GlueContext) RecordKvWU(wu tenant.Unit)  {}

func (c *GlueContext) ScanReportWait() time.Duration { return 0 }
func (c *GlueContext) SkipKey(key string) bool       { return false }
