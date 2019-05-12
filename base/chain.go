package base

import (
	"encoding/binary"
)

// YieldChainedVals invokes the yield callback on all the vals found
// by chasing the references in the chain, given with the starting
// chain item ref. The optional valsSuffix is appended to each
// emitted vals. The optional valsOut allows for the caller to provide
// a pre-allocated slice.
func YieldChainedVals(yieldVals YieldVals, valsSuffix Vals, chain []byte,
	ref []byte, valsOut Vals) (valsOutRV Vals) {
	for {
		offset := binary.LittleEndian.Uint64(ref[:8])
		if offset <= 0 {
			break
		}

		ref = chain[offset : offset+binary.LittleEndian.Uint64(ref[8:16])]

		valsOut = ValsSplit(ref[16:], valsOut[:0])

		valsOut = append(valsOut, valsSuffix...)

		yieldVals(valsOut)
	}

	return valsOut // Return extended valsOut for caller reusability.
}
