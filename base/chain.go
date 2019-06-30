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

package base

import (
	"encoding/binary"
	"fmt"

	"github.com/couchbase/rhmap/store"
)

// YieldChainedVals invokes the yieldVals callback on all the vals
// found by chasing the references in an encoded chain of items, given
// a starting chain item ref. The optional valsSuffix is appended to
// each emitted vals. The optional valsOut allows for the caller to
// provide resuable, pre-allocated slice memory.
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
