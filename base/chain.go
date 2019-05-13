package base

import (
	"encoding/binary"
)

// YieldChainedVals invokes the yieldVals callback on all the vals
// found by chasing the references in the chain, given a starting
// chain item ref. The optional valsSuffix is appended to each emitted
// vals. The optional valsOut allows for the caller to provide
// resuable, pre-allocated slice memory.
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
