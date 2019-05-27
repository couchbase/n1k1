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
	"github.com/couchbase/n1k1/base"
)

func GlueOp(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathItem string) {
	switch o.Kind {
	case "glue-primary-scan":
		GluePrimaryScan(o, vars, yieldVals, yieldErr)
	case "glue-index-scan":
		GlueIndexScan(o, vars, yieldVals, yieldErr)
	case "glue-fetch":
		GlueFetch(o, vars, yieldVals, yieldErr)
	}
}

func GluePrimaryScan(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
}

func GlueIndexScan(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
}

func GlueFetch(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr) {
}
