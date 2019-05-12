package base

import (
	"encoding/binary"
)

// YieldChainedVals invokes the yield callback on all the vals found
// by chasing the offset/size references in the chain starting at the
// given offset/size. The optional valsSufix is appended to each
// emitted vals. The optional valsOut is allows for the caller to
// provide a pre-allocated slice.
func YieldChainedVals(yieldVals YieldVals, valsSuffix Vals, chainBytes []byte,
	offset, size uint64, valsOut Vals) (valsOutRV Vals) {
	for offset > 0 {
		chainItem := chainBytes[offset : offset+size]

		valsOut = ValsSplit(chainItem[16:], valsOut[:0])

		valsOut = append(valsOut, valsSuffix...)

		yieldVals(valsOut)

		offset = binary.LittleEndian.Uint64(chainItem[:8])
		size = binary.LittleEndian.Uint64(chainItem[8:16])
	}

	return valsOut
}
