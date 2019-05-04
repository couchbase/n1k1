package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func ExecOp(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathItem string) {
	if o == nil {
		return
	}

	pathNext := EmitPush(path, pathItem)

	switch o.Kind {
	case "scan":
		OpScan(o, lzVars, lzYieldVals, lzYieldErr) // !lz

	case "filter":
		OpFilter(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "project":
		OpProject(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "join-nl-inner":
		OpJoinNestedLoop(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "join-nl-outerLeft":
		OpJoinNestedLoop(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "order-by-offset-limit":
		OpOrderByOffsetLimit(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "union-all":
		OpUnionAll(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz
	}

	EmitPop(path, pathItem)
}
