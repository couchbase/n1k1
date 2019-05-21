package n1k1

import (
	"encoding/binary" // <== genCompiler:hide

	"strings"

	"github.com/couchbase/rhmap/store" // <== genCompiler:hide

	"github.com/couchbase/n1k1/base"
)

// OpJoinHash implements...
//  o.Kind:             info tracked in probe map values:  yieldsUnprobed:
//   joinHash-inner      [                      leftVals ]  f
//   joinHash-leftOuter  [ probeCount           leftVals ]  t
//   intersect-all       [ probeCount leftCount          ]  f
//   intersect-distinct  [ probeCount                    ]  f
//   except-all          [ probeCount leftCount          ]  t
//   except-distinct     [ probeCount                    ]  t
func OpJoinHash(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	kindParts := strings.Split(o.Kind, "-")

	opIntersect := kindParts[0] == "intersect"

	var exprLeft, exprRight []interface{}

	// Analyze the Op's config according to the above table of flags.
	var canonical, probeCount, leftCount, leftVals, yieldsUnprobed bool

	if kindParts[0] == "joinHash" {
		exprLeft = o.Params[0].([]interface{})
		exprRight = o.Params[1].([]interface{})

		canonical = false

		if kindParts[1] == "leftOuter" {
			probeCount, yieldsUnprobed = true, true
		}

		leftVals = true
	} else {
		// INTERSECT & EXCEPT canonicalize their incoming vals so
		// they're usable as map lookup keys.
		exprLeft = []interface{}{"valsCanonical"}
		exprRight = []interface{}{"valsCanonical"}

		canonical = true

		probeCount = true

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

		// As the left side is visited to fill or build the probe map,
		// any left vals, if they are needed for a join, are stored as
		// a chain of entries in the lzChunks.
		//
		// TODO: Configurable initial size for chunks, and reusable chunks.
		// TODO: Reuse backing bytes for chunks.
		lzChunks, lzErr := lzVars.Ctx.AllocChunks()
		if lzErr != nil {
			lzYieldErr(lzErr)
		}

		var lzMap *store.RHStore // The probe map.

		if lzErr == nil {
			// Every chain of left vals ends at offset 0, size 0.
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

		var lzProbeCount uint64

		_, _, _, _ = lzBytes8, lzValOut, lzLeftBytes, lzProbeCount

		exprLeftFunc :=
			MakeExprFunc(lzVars, o.Children[0].Labels, exprLeft, pathNext, "JHL") // !lz

		exprRightFunc :=
			MakeExprFunc(lzVars, o.Children[1].Labels, exprRight, pathNext, "JHR") // !lz

		EmitPush(pathNext, "JHF") // !lz

		lzYieldValsOrig := lzYieldVals

		// Callback for left side, which fills the probe map.
		lzYieldVals = func(lzVals base.Vals) {
			var lzErr error

			// Prepare the probe key.
			lzVal = exprLeftFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext "JHL"

			lzProbeKey := lzVal
			if !canonical { // !lz
				lzProbeKey, lzErr = lzVars.Ctx.ValComparer.CanonicalJSON(lzProbeKey, lzValOut[:0])

				lzValOut = lzProbeKey[:0]
			} // !lz

			// The probe key must be valued.
			if lzErr == nil && base.ValHasValue(lzProbeKey) {
				// Check if we have an entry for the probe key.
				lzProbeVal, lzProbeKeyFound := lzMap.Get([]byte(lzProbeKey))
				if !lzProbeKeyFound {
					// Initialze a brand new probe val to insert into
					// the probe map.
					lzProbeValNew = lzProbeValNew[:0]

					if probeCount { // !lz
						lzProbeValNew = append(lzProbeValNew, lzZero16[:8]...)
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

					if probeCount { // !lz
						lzProbeValNew = append(lzProbeValNew, lzZero16[:8]...)
						lzProbeValOld = lzProbeValOld[8:]
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
			// filled with left-side probe entries, and next we will
			// visit the right-side.

			EmitPush(pathNext, "JHP") // !lz

			// Callback for right side, which probes the probe map.
			lzYieldVals = func(lzVals base.Vals) {
				var lzErr error

				// Prepare the probe key.
				lzVal = exprRightFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext "JHR"

				lzProbeKey := lzVal
				if !canonical { // !lz
					lzProbeKey, lzErr = lzVars.Ctx.ValComparer.CanonicalJSON(lzProbeKey, lzValOut[:0])

					lzValOut = lzProbeKey[:0]
				} // !lz

				// The probe key must be valued.
				if lzErr == nil && base.ValHasValue(lzProbeKey) {
					lzProbeVal, lzProbeKeyFound := lzMap.Get([]byte(lzProbeKey))
					if lzProbeKeyFound {
						if probeCount { // !lz
							// Increment probe count.
							lzProbeCount = binary.LittleEndian.Uint64(lzProbeVal[:8]) + 1
							binary.LittleEndian.PutUint64(lzProbeVal[:8], lzProbeCount)

							if lzProbeCount == 1 {
								if opIntersect && !leftCount { // !lz
									// Ex: intersect-distinct.
									lzValsOut = base.ValsDecode(lzProbeKey, lzValsOut[:0])

									lzYieldValsOrig(lzValsOut)
								} // !lz
							}

							lzProbeVal = lzProbeVal[8:]
						} // !lz

						if leftCount { // !lz
							if opIntersect { // !lz
								// Ex: intersect-all.
								lzLeftCount := binary.LittleEndian.Uint64(lzProbeVal[:8])
								if lzLeftCount >= lzProbeCount {
									lzValsOut = base.ValsDecode(lzProbeKey, lzValsOut[:0])

									lzYieldValsOrig(lzValsOut)
								}
							} // !lz

							lzProbeVal = lzProbeVal[8:]
						} // !lz

						if leftVals { // !lz
							// Ex: joinHash-inner, joinHash-leftOuter.
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
					// Ex: joinHash-leftOuter, except.
					if probeCount && yieldsUnprobed { // !lz
						rightLabelsLen := len(o.Children[1].Labels) // !lz
						_ = rightLabelsLen                          // !lz

						lzRightSuffix := make(base.Vals, rightLabelsLen)
						_ = lzRightSuffix

						// Callback for entries in the probe map.
						lzMapVisitor := func(lzProbeKey store.Key, lzProbeVal store.Val) bool {
							lzProbeCount := binary.LittleEndian.Uint64(lzProbeVal[:8])
							if lzProbeCount == 0 { // Entry was unprobed.
								lzProbeVal = lzProbeVal[8:]

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
									// Ex: joinHash-leftOuter.
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
