package base

import (
	"sync"
)

// Stage represents a data-staging "pipeline breaker", that's
// processed by one or more concurrent actors.
type Stage struct {
	Vars *Vars

	YieldVals  YieldVals
	YieldStats YieldStats
	YieldErr   YieldErr

	BatchCh chan []Vals

	M sync.Mutex // Protects the fields that follow.

	NumActors int

	StopCh chan struct{}

	Err error
}

func NewStage(batchChSize int, vars *Vars,
	yieldVals YieldVals, yieldStats YieldStats, yieldErr YieldErr) *Stage {
	return &Stage{
		Vars:       vars,
		YieldVals:  yieldVals,
		YieldStats: yieldStats,
		YieldErr:   yieldErr,

		BatchCh: make(chan []Vals, batchChSize),

		StopCh: make(chan struct{}),
	}
}

type ActorFunc func(*Vars, YieldVals, YieldStats, YieldErr, interface{})

// StageStartActor is used for data-staging and "pipeline breaking"
// and spawns a concurrent actor (goroutine) related to the given
// stage.  A batchSize > 0 means there will be batching of results.  A
// batchSize of 1, for example, means send each incoming result as its
// own batch-of-1.  A batchSize of <= 0 means an actor will send a
// single, giant batch at the end.
func StageStartActor(stage *Stage,
	actorFunc ActorFunc, actorData interface{}, batchSize int) {
	stage.M.Lock()

	stage.NumActors++

	stopCh := stage.StopCh // Own copy for reading.

	stage.M.Unlock()

	if stopCh == nil {
		return
	}

	var err error

	var batch []Vals

	batchSend := func() {
		if len(batch) > 0 {
			select {
			case <-stopCh: // Sibling actor had an error.
				stage.M.Lock()
				if err == nil {
					err = stage.Err
				}
				stage.M.Unlock()

			case stage.BatchCh <- batch:
				// NO-OP.
			}

			batch = nil
		}
	}

	yieldVals := func(vals Vals) {
		if err == nil {
			valsCopy, _, _ := ValsDeepCopy(vals, nil, nil)

			batch = append(batch, valsCopy)

			if batchSize > 0 {
				if len(batch) >= batchSize {
					batchSend()
				}
			}
		}
	}

	yieldErr := func(errIn error) {
		if errIn != nil {
			err = errIn

			stage.M.Lock()

			if stage.Err == nil {
				stage.Err = errIn // First error by any actor.

				// Closed & nil'ed under lock to have single close().
				if stage.StopCh != nil {
					close(stage.StopCh)
					stage.StopCh = nil
				}
			}

			stage.M.Unlock()
		}

		if err == nil {
			batchSend() // Send the last, in-flight batch.
		}
	}

	go func() {
		actorFunc(stage.Vars, yieldVals, stage.YieldStats, yieldErr, actorData)

		stage.BatchCh <- nil // A nil means actor is done.
	}()
}

func StageWaitForActors(stage *Stage) {
	stage.M.Lock()
	numActors := stage.NumActors
	stage.M.Unlock()

	var numActorsDone int

	for numActorsDone < numActors {
		batch := <-stage.BatchCh
		if batch == nil {
			numActorsDone++
		} else {
			for _, vals := range batch {
				stage.YieldVals(vals)
			}
		}
	}

	stage.M.Lock()

	stage.YieldErr(stage.Err)

	stage.M.Unlock()
}
