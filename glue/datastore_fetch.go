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

	"github.com/couchbase/query/execution"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"
)

func DatastoreFetch(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	context := vars.Temps[0].(*execution.Context)

	plan := vars.Temps[o.Params[0].(int)].(*plan.Fetch)

	keyspace := plan.Keyspace()
	subPaths := plan.SubPaths()

	batchSize := 200 // TODO: Configurability.
	batchChSize := 0 // TODO: Configurability.

	stage := base.NewStage(1, batchChSize, vars, yieldVals, yieldErr)

	stage.StartActor(func(vars *base.Vars, yieldVals base.YieldVals,
		yieldErr base.YieldErr, actorData interface{}) {
		n1k1.ExecOp(o.Children[0], vars, yieldVals, yieldErr, pathNext, "DF")
	}, nil, batchSize)

	var vals base.Vals

	var keys []string // Same len() as batch.

	fetchMap := map[string]value.AnnotatedValue{}

	stage.ProcessBatchesFromActors(func(batch []base.Vals) {
		keys = keys[:0]

		// TODO: The datastore's Fetch API inherently allocates memory
		// or creates garbage, so that needs a redesign.
		for _, vals := range batch {
			var key string

			err := json.Unmarshal(vals[0], &key)
			if err != nil {
				key = string(vals[0]) // BINARY key.
			}

			keys = append(keys, key)
		}

		for k := range fetchMap {
			// TODO: Will golang's fetchMap resize downwards, or keep
			// the same buckets?
			// TODO: Need a Fetch API that allows us to use rhmap.
			delete(fetchMap, k)
		}

		errs := keyspace.Fetch(keys, fetchMap, context, subPaths)
		for _, err := range errs {
			yieldErr(err)
		}

		// Keep the same ordering as the batch.
		for i, key := range keys {
			if key != "" {
				v, ok := fetchMap[key]
				if ok && v != nil {
					jv, err := json.Marshal(v)
					if err != nil {
						jv = v.Actual().([]byte) // TODO: BINARY?
					}

					vals = append(vals[:0], batch[i]...)
					vals = append(vals, jv)

					yieldVals(vals)
				}
			}
		}
	})

	stage.M.Lock()
	stage.YieldErr(stage.Err)
	stage.M.Unlock()
}
