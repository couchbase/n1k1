package base

import (
	"encoding/binary"
	"fmt"

	"github.com/couchbase/rhmap/store"
)

// YieldChainedVals invokes the yieldVals callback on all the vals
// found by chasing the references in the chain, given a starting
// chain item ref. The optional valsSuffix is appended to each emitted
// vals. The optional valsOut allows for the caller to provide
// resuable, pre-allocated slice memory.
func YieldChainedVals(yieldVals YieldVals, valsSuffix Vals, chunks *store.Chunks,
	ref []byte, valsOut Vals) (valsOutRV Vals, err error) {
	for {
		offset := binary.LittleEndian.Uint64(ref[:8])
		if offset <= 0 {
			break
		}

		ref, err = chunks.BytesRead(offset, binary.LittleEndian.Uint64(ref[8:16]))
		if err != nil {
			return valsOut, fmt.Errorf("YieldChainedVals: err: %v", err)
		}

		valsOut = ValsDecode(ref[16:], valsOut[:0])

		valsOut = append(valsOut, valsSuffix...)

		yieldVals(valsOut)
	}

	return valsOut, nil // Return extended valsOut for caller reusability.
}
