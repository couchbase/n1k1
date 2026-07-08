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

package base

// DatastorePipe serves a query's datastore leaf ops (scans, fetches) -- the one
// boundary the plan can't execute itself. It generalizes the process-global
// engine.ExecOpEx hook into an interface, so a query can be pointed at different
// providers WITHOUT changing its code: the in-process file datastore
// (glue.DatastoreOp), an in-memory inline-data provider (engine.MemPipe), or a
// child process over a pipe. The interface is expressed in base types only, so a
// query that reaches its data through a Pipe links no cbq/records -- the key to a
// compiled standalone program. See DESIGN-prepare.md "abstract the datastore
// leaves behind one interface".
//
// A Pipe is installed per request on Ctx.Pipe; when set, ExecOp routes datastore
// leaf ops to it (glue.DatastoreOp defers to it too). nil = the process default.
type DatastorePipe interface {
	// Op serves one datastore leaf op -- its Kind selects the operation
	// (datastore-scan-records, datastore-fetch, ...) -- yielding each result row's
	// Vals to yieldVals and any error to yieldErr. This is exactly the ExecOpEx
	// contract, so an existing ExecOpEx implementation is trivially a Pipe.
	Op(o *Op, vars *Vars, yieldVals YieldVals, yieldErr YieldErr, path, pathNext string)
}
