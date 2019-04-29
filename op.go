package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func ExecOp(o *base.Op, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr,
	path, pathItem string) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	if o == nil {
		return
	}

	switch o.Kind {
	case "scan":
		OpScan(o.Params, o.Fields, lzYieldVals, lzYieldStats, lzYieldErr) // !lz

	case "filter":
		OpFilter(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "project":
		OpProject(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "join-nl-inner":
		OpJoinNestedLoop(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "join-nl-outerLeft":
		OpJoinNestedLoop(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "order-by-offset-limit":
		OpOrderByOffsetLimit(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "union-all":
		OpUnionAll(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz
	}
}
