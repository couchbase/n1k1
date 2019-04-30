package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func StageStartActor(lzStage *base.Stage,
	lzActorFunc base.ActorFunc, lzActorData interface{}) {
	lzStage.WaitGroup.Add(1)

	if LzScope {
		var lzErr error

		var lzValsMine []base.Vals

		lzYieldVals := func(lzVals base.Vals) {
			if lzErr == nil {
				lzValsCopy, _, _ := base.ValsDeepCopy(lzVals, nil, nil)

				lzValsMine = append(lzValsMine, lzValsCopy)
			}
		}

		lzYieldErr := func(lzErrIn error) {
			lzErr = lzErrIn

			lzStage.M.Lock()

			if lzErrIn == nil {
				lzStage.Vals = append(lzStage.Vals, lzValsMine)

				lzValsMine = nil
			}

			if lzStage.Err == nil {
				lzStage.Err = lzErrIn
			}

			lzStage.M.Unlock()
		}

		lzActorFuncWrap := func() {
			lzActorFunc(lzStage.Vars, lzYieldVals, lzStage.YieldStats, lzYieldErr, lzActorData)

			lzStage.WaitGroup.Done()
		}

		go lzActorFuncWrap()
	}
}

func StageWaitForActors(lzStage *base.Stage) {
	lzStage.WaitGroup.Wait()

	lzStage.M.Lock()

	if lzStage.Err == nil {
		for _, lzValsBatched := range lzStage.Vals {
			for _, lzVals := range lzValsBatched {
				lzStage.YieldVals(lzVals)
			}
		}
	}

	lzStage.YieldErr(lzStage.Err)

	lzStage.M.Unlock()
}
