package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

// StageStartActor is used for data-staging and "pipeline breaking"
// and spawns a concurrent actor (goroutine) related to the given
// stage.  A batchSize > 0 means there will be batching of results.  A
// batchSize of 1, for example, means send each incoming result as its
// own batch-of-1.  A batchSize of <= 0 means an actor will send a
// single, giant batch at the end.
func StageStartActor(lzStage *base.Stage,
	lzActorFunc base.ActorFunc, lzActorData interface{}, batchSize int) {
	lzStage.M.Lock()
	lzStage.NumActors++
	lzStopCh := lzStage.StopCh // Own copy for reading.
	lzStage.M.Unlock()

	if lzStopCh != nil {
		var lzErr error

		var lzBatch []base.Vals

		lzBatchSend := func() {
			if len(lzBatch) > 0 {
				select {
				case <-lzStopCh: // Sibling actor had an error.
					lzStage.M.Lock()
					if lzErr == nil {
						lzErr = lzStage.Err
					}
					lzStage.M.Unlock()

				case lzStage.BatchCh <- lzBatch:
					// NO-OP.
				}

				lzBatch = nil
			}
		}

		lzYieldVals := func(lzVals base.Vals) {
			if lzErr == nil {
				lzValsCopy, _, _ := base.ValsDeepCopy(lzVals, nil, nil)

				lzBatch = append(lzBatch, lzValsCopy)

				if batchSize > 0 { // !lz
					if len(lzBatch) >= batchSize {
						lzBatchSend()
					}
				} // !lz
			}
		}

		lzYieldErr := func(lzErrIn error) {
			if lzErrIn != nil {
				lzErr = lzErrIn

				lzStage.M.Lock()

				if lzStage.Err == nil {
					lzStage.Err = lzErrIn // First error by any actor.

					// Closed & nil'ed under lock to have single close().
					if lzStage.StopCh != nil {
						close(lzStage.StopCh)
						lzStage.StopCh = nil
					}
				}

				lzStage.M.Unlock()
			}

			if lzErr == nil {
				lzBatchSend() // Send the last, in-flight batch.
			}
		}

		lzActorFuncWrap := func() {
			lzActorFunc(lzStage.Vars, lzYieldVals, lzStage.YieldStats, lzYieldErr, lzActorData)

			lzStage.BatchCh <- nil // A nil means actor is done.
		}

		go lzActorFuncWrap()
	}
}

func StageWaitForActors(lzStage *base.Stage) {
	lzStage.M.Lock()
	lzNumActors := lzStage.NumActors
	lzStage.M.Unlock()

	var lzNumActorsDone int

	for lzNumActorsDone < lzNumActors {
		lzBatch := <-lzStage.BatchCh
		if lzBatch == nil {
			lzNumActorsDone++
		} else {
			for _, lzVals := range lzBatch {
				lzStage.YieldVals(lzVals)
			}
		}
	}

	lzStage.M.Lock()

	lzStage.YieldErr(lzStage.Err)

	lzStage.M.Unlock()
}
