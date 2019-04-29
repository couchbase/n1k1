package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func ExecOp(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr, path, pathItem string) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	if o == nil {
		return
	}

	switch o.Kind {
	case "scan":
		OpScan(o.Params, o.Fields, lzVars, lzYieldVals, lzYieldStats, lzYieldErr) // !lz

	case "filter":
		OpFilter(o, lzVars, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "project":
		OpProject(o, lzVars, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "join-nl-inner":
		OpJoinNestedLoop(o, lzVars, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "join-nl-outerLeft":
		OpJoinNestedLoop(o, lzVars, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "order-by-offset-limit":
		OpOrderByOffsetLimit(o, lzVars, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "union-all":
		OpUnionAll(o, lzVars, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz
	}
}
