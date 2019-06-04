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

package exec

import (
	"time"

	"github.com/couchbase/query/execution"
	"github.com/couchbase/query/plan"
)

// ExecRequest represents the subset of query/server's Request
// interface that's needed for execution.
type ExecRequest interface {
	Output() execution.Output
}

// ExecParams represents additional params that are not already
// available from the context or the request.
type ExecParams struct {
	Timeout time.Duration
}

func ExecMaybe(context *execution.Context, request ExecRequest,
	prepared plan.Operator, params ExecParams) bool {
	return false
}
