package n1k1

import (
	"encoding/binary" // <== genCompiler:hide

	"strings"

	"github.com/couchbase/rhmap/store" // <== genCompiler:hide

	"github.com/couchbase/n1k1/base"
)

// OpJoinHash implements...
//  feature:            info tracked in probe map values:     yieldsUnprobed:
//   joinHash-inner      [                         leftVals ]  f
//   joinHash-outerLeft  [ tracksProbing           leftVals ]  t
//   intersect-all       [               leftCount          ]  f
//   intersect-distinct  [ tracksProbing                    ]  f
//   except-all          [ tracksProbing leftCount          ]  t
//   except-distinct     [ tracksProbing                    ]  t
func OpJoinHash(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	kindParts := strings.Split(o.Kind, "-")

	opIntersect := kindParts[0] == "intersect"

	var exprLeft, exprRight []interface{}

	var canonical, tracksProbing, leftCount, leftVals, yieldsUnprobed bool

	if kindParts[0] == "joinHash" {
		exprLeft = o.Params[0].([]interface{})
		exprRight = o.Params[1].([]interface{})

		canonical = false

		if kindParts[1] == "outerLeft" {
			tracksProbing, yieldsUnprobed = true, true
		}

		leftVals = true
	} else {
		// INTERSECT & EXCEPT canonicalize their incoming vals so
		// they're usable as map lookup keys.
		exprLeft = []interface{}{"valsCanonical"}
		exprRight = []interface{}{"valsCanonical"}

		canonical = true

		if o.Kind != "intersect-all" {
			tracksProbing = true
		}

		if kindParts[1] == "all" {
			leftCount = true
		}

		yieldsUnprobed = kindParts[0] == "except"
	}

	// ---------------------------------------------------------------

	if LzScope {
		var lzBytes8, lzOne8 [8]byte
		var lzZero16 [16]byte

		binary.LittleEndian.PutUint64(lzOne8[:], uint64(1))

		// TODO: Configurable initial size for chunks, and reusable chunks.
		// TODO: Reuse backing bytes for chunks.
		lzChunks, lzErr := lzVars.Ctx.AllocChunks()
		if lzErr != nil {
			lzYieldErr(lzErr)
		}

		var lzMap *store.RHStore

		if lzErr == nil {
			// Chain ends at offset 0, size 0.
			lzChunks.BytesAppend(lzZero16[:])

			// TODO: Configurable initial size for RHStore, and reusable RHStore.
			// TODO: Reuse backing bytes for lzMap.
			lzMap, lzErr = lzVars.Ctx.AllocMap()
			if lzErr != nil {
				lzYieldErr(lzErr)
			}
		}

		var lzVal, lzValOut base.Val

		var lzValsOut base.Vals

		var lzProbeValNew, lzLeftBytes []byte

		_, _, _ = lzBytes8, lzValOut, lzLeftBytes

		exprLeftFunc :=
			MakeExprFunc(lzVars, o.Children[0].Labels, exprLeft, pathNext, "JHL") // !lz

		exprRightFunc :=
			MakeExprFunc(lzVars, o.Children[1].Labels, exprRight, pathNext, "JHR") // !lz

		EmitPush(pathNext, "JHF") // !lz

		lzYieldValsOrig := lzYieldVals

		// Callback for left side, which fills the probe map.
		lzYieldVals = func(lzVals base.Vals) {
			var lzErr error

			lzVal = exprLeftFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext "JHL"

			lzProbeKey := lzVal
			if !canonical { // !lz
				lzProbeKey, lzErr = lzVars.Ctx.ValComparer.CanonicalJSON(lzProbeKey, lzValOut[:0])

				lzValOut = lzProbeKey[:0]
			} // !lz

			if lzErr == nil && base.ValHasValue(lzProbeKey) {
				// Check if we have an entry for the probe key.
				lzProbeVal, lzProbeKeyFound := lzMap.Get([]byte(lzProbeKey))
				if !lzProbeKeyFound {
					// Initialze a brand new probe val to insert into
					// the probe map.
					lzProbeValNew = lzProbeValNew[:0]

					if tracksProbing { // !lz
						lzProbeValNew = append(lzProbeValNew, byte(0))
					} // !lz

					if leftCount { // !lz
						lzProbeValNew = append(lzProbeValNew, lzOne8[:]...)
					} // !lz

					if leftVals { // !lz
						// End chain has offset/size of 0/0.
						lzLeftBytes = append(lzLeftBytes[:0], lzZero16[:16]...)
						lzLeftBytes = base.ValsEncode(lzVals, lzLeftBytes)

						lzOffset, lzSize, lzErr := lzChunks.BytesAppend(lzLeftBytes)
						if lzErr != nil {
							lzYieldErr(lzErr)
						}

						binary.LittleEndian.PutUint64(lzBytes8[:], lzOffset)
						lzProbeValNew = append(lzProbeValNew, lzBytes8[:]...)
						binary.LittleEndian.PutUint64(lzBytes8[:], lzSize)
						lzProbeValNew = append(lzProbeValNew, lzBytes8[:]...)
					} // !lz

					lzMap.Set(store.Key(lzProbeKey), lzProbeValNew)
				} else {
					// Not the first time that we're seeing this probe
					// key, so increment its leftCount, append to its
					// leftVals chain, etc.
					lzProbeValNew = lzProbeValNew[:0]

					lzProbeValOld := lzProbeVal

					if tracksProbing { // !lz
						lzProbeValNew = append(lzProbeValNew, byte(0))
						lzProbeValOld = lzProbeValOld[1:]
					} // !lz

					if leftCount { // !lz
						lzLeftCount := binary.LittleEndian.Uint64(lzProbeValOld[:8])
						binary.LittleEndian.PutUint64(lzBytes8[:], lzLeftCount+1)
						lzProbeValNew = append(lzProbeValNew, lzBytes8[:]...)
						lzProbeValOld = lzProbeValOld[8:]
					} // !lz

					if leftVals { // !lz
						// Copy previous offset/size to extend the chain.
						lzLeftBytes = append(lzLeftBytes[:0], lzProbeValOld[:16]...)
						lzLeftBytes = base.ValsEncode(lzVals, lzLeftBytes)

						lzOffset, lzSize, lzErr := lzChunks.BytesAppend(lzLeftBytes)
						if lzErr != nil {
							lzYieldErr(lzErr)
						}

						binary.LittleEndian.PutUint64(lzBytes8[:], lzOffset)
						lzProbeValNew = append(lzProbeValNew, lzBytes8[:]...)
						binary.LittleEndian.PutUint64(lzBytes8[:], lzSize)
						lzProbeValNew = append(lzProbeValNew, lzBytes8[:]...)
					} // !lz

					// The updated probe val has the same size as the
					// existing probe val, so we can optimize by
					// in-place overwriting the existing probe val.
					copy(lzProbeVal, lzProbeValNew)
				}
			}
		}

		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn != nil {
				lzErr = lzErrIn

				lzYieldErrOrig(lzErrIn)
			}
		}

		EmitPop(pathNext, "JHF") // !lz

		if lzErr == nil {
			// Run the left side to fill the probe map.
			ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "JHL") // !lz
		}

		// -----------------------------------------------------------

		if lzErr == nil {
			// No error, so at this point the probe map has been
			// filled with left-hand-side probe entries, and next we
			// will visit the right-hand-side.

			EmitPush(pathNext, "JHP") // !lz

			// Callback for right side, which probes the probe map.
			lzYieldVals = func(lzVals base.Vals) {
				var lzErr error

				lzVal = exprRightFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext "JHR"

				lzProbeKey := lzVal
				if !canonical { // !lz
					lzProbeKey, lzErr = lzVars.Ctx.ValComparer.CanonicalJSON(lzProbeKey, lzValOut[:0])

					lzValOut = lzProbeKey[:0]
				} // !lz

				if lzErr == nil && base.ValHasValue(lzProbeKey) {
					lzProbeVal, lzProbeKeyFound := lzMap.Get([]byte(lzProbeKey))
					if lzProbeKeyFound {
						if tracksProbing { // !lz
							if lzProbeVal[0] == byte(0) {
								lzProbeVal[0] = byte(1) // Mark as probed.

								if opIntersect { // !lz
									// Ex: intersect-distinct.
									lzValsOut = base.ValsDecode(lzProbeKey, lzValsOut[:0])

									lzYieldValsOrig(lzValsOut)
								} // !lz
							}

							lzProbeVal = lzProbeVal[1:]
						} // !lz

						if leftCount { // !lz
							if opIntersect { // !lz
								// Ex: intersect-all.
								lzValsOut = base.ValsDecode(lzProbeKey, lzValsOut[:0])

								lzLeftCount := binary.LittleEndian.Uint64(lzProbeVal[:8])

								for lzI := uint64(0); lzI < lzLeftCount; lzI++ {
									lzYieldValsOrig(lzValsOut)
								}
							} // !lz

							lzProbeVal = lzProbeVal[8:]
						} // !lz

						if leftVals { // !lz
							// Ex: joinHash-inner, joinHash-outerLeft.
							lzValsOut, lzErr = base.YieldChainedVals(lzYieldValsOrig, lzVals, lzChunks, lzProbeVal, lzValsOut)
							if lzErr != nil {
								lzYieldErr(lzErr)
							}
						} // !lz
					}
				}
			}

			lzYieldErr = func(lzErrIn error) {
				if lzErrIn == nil {
					// No error, so yield unprobed items if needed.
					// Ex: joinHash-outerLeft, except.
					if tracksProbing && yieldsUnprobed { // !lz
						rightLabelsLen := len(o.Children[1].Labels) // !lz
						_ = rightLabelsLen                          // !lz

						lzRightSuffix := make(base.Vals, rightLabelsLen)
						_ = lzRightSuffix

						lzMapVisitor := func(lzProbeKey store.Key, lzProbeVal store.Val) bool {
							if lzProbeVal[0] == byte(0) { // Unprobed.
								lzProbeVal = lzProbeVal[1:]

								if leftCount { // !lz
									// Ex: except-all.
									lzLeftCount := binary.LittleEndian.Uint64(lzProbeVal[:8])

									lzValsOut = base.ValsDecode(lzProbeKey, lzValsOut[:0])

									for lzI := uint64(0); lzI < lzLeftCount; lzI++ {
										lzYieldValsOrig(lzValsOut)
									}

									lzProbeVal = lzProbeVal[8:]
								} // !lz

								if leftVals { // !lz
									// Ex: joinHash-outerLeft.
									lzValsOut, lzErr = base.YieldChainedVals(lzYieldValsOrig, lzRightSuffix, lzChunks, lzProbeVal, lzValsOut)
									if lzErr != nil {
										lzYieldErrOrig(lzErr)
									}
								} // !lz

								if !leftCount && !leftVals { // !lz
									// Ex: except-distinct.
									lzValsOut = base.ValsDecode(lzProbeKey, lzValsOut[:0])

									lzYieldValsOrig(lzValsOut)
								} // !lz
							}

							return true
						}

						lzMap.Visit(lzMapVisitor)
					} // !lz
				}

				lzYieldErrOrig(lzErrIn)
			}

			EmitPop(pathNext, "JHP") // !lz

			if lzErr == nil {
				// Run the right side to probe the probe map.
				ExecOp(o.Children[1], lzVars, lzYieldVals, lzYieldErr, pathNext, "JHR") // !lz
			}
		}

		lzVars.Ctx.RecycleMap(lzMap)
		lzVars.Ctx.RecycleChunks(lzChunks)
	}
}
