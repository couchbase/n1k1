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
	"sync"
)

// Stage represents a data-staging "pipeline breaker" that's processed
// by one or more concurrent actor goroutines.
type Stage struct {
	NumActors int

	Vars *Vars

	YieldVals YieldVals
	YieldErr  YieldErr

	ActorReadyCh chan struct{}

	BatchCh chan []Vals

	M sync.Mutex // Protects the fields that follow.

	StopCh chan struct{} // When error, close()'ed and nil'ed.

	Err error

	Recycled [][]Vals
}

// NewStage returns a ready-to-use Stage instance.
func NewStage(numActors int, batchChSize int,
	vars *Vars, yieldVals YieldVals, yieldErr YieldErr) *Stage {
	return &Stage{
		NumActors: numActors,

		Vars:      vars,
		YieldVals: yieldVals,
		YieldErr:  yieldErr,

		ActorReadyCh: make(chan struct{}),
		BatchCh:      make(chan []Vals, batchChSize),
		StopCh:       make(chan struct{}),
	}
}

// ActorFunc is the signature for the actor callback.
type ActorFunc func(*Vars, YieldVals, YieldErr, interface{})

// StartActor is used for data-staging and "pipeline breaking" and
// spawns a concurrent actor (goroutine). A batchSize > 0 means there
// will be batching of results. A batchSize of 1, for example, means
// send each incoming result as its own batch-of-1. A batchSize of <=
// 0 means an actor will send a single, giant batch at the end.
func (stage *Stage) StartActor(aFunc ActorFunc, aData interface{}, batchSize int) {
	stage.M.Lock()
	stopCh := stage.StopCh // Own copy for reading.
	stage.M.Unlock()

	var err error

	var batch []Vals

	batchSend := func() {
		if len(batch) > 0 {
			select {
			case <-stopCh: // Sibling actor had an error.
				if err == nil {
					stage.M.Lock()
					err = stage.Err
					stage.M.Unlock()
				}

			case stage.BatchCh <- batch:
				// NO-OP.
			}

			batch = nil
		}
	}

	yieldVals := func(vals Vals) {
		if err == nil {
			// Need to materialize or deep-copy the incoming vals into
			// the batch, so reuse slices from previously recycled
			// batch, if any.
			if batch == nil {
				batch = stage.AcquireBatch()[:0]
			}

			// Grab preallocVals/preallocVal from next slot in the
			// batch, as we're going to overwrite / append() over that
			// slot anyways, so it's good as a recycled source.
			var preallocVals Vals
			var preallocVal Val

			if cap(batch) > len(batch) {
				preallocVals := batch[0 : len(batch)+1][len(batch)]
				preallocVals = preallocVals[0:cap(preallocVals)]

				if len(preallocVals) > 0 {
					preallocVal = preallocVals[0]
					preallocVal = preallocVal[0:cap(preallocVal)]
				}
			}

			valsCopy, _, _ := ValsDeepCopy(vals, preallocVals, preallocVal)

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
		stage.ActorReadyCh <- struct{}{}

		aFunc(stage.Vars, yieldVals, yieldErr, aData)

		stage.BatchCh <- nil // Must send last nil, meaning this actor is done.
	}()
}

// --------------------------------------------------------

// YieldResultsFromActors receives batches from the actors and yields
// them onwards, until all the actors are done.
func (stage *Stage) YieldResultsFromActors() {
	stage.ProcessBatchesFromActors(func(batch []Vals) {
		for _, vals := range batch {
			stage.YieldVals(vals)
		}
	})

	stage.M.Lock()
	stage.YieldErr(stage.Err)
	stage.M.Unlock()
}

// --------------------------------------------------------

// ProcessBatchesFromActors receives batches from the actors and
// invokes the given callback, until all the actors are done.
func (stage *Stage) ProcessBatchesFromActors(cb func([]Vals)) {
	var numActorsReady int
	for numActorsReady < stage.NumActors {
		<-stage.ActorReadyCh
		numActorsReady++
	}

	var numActorsDone int
	for numActorsDone < stage.NumActors {
		batch := <-stage.BatchCh
		if batch == nil {
			numActorsDone++
		} else {
			cb(batch)

			stage.RecycleBatch(batch)
		}
	}
}

// --------------------------------------------------------

// RecycleBatch holds onto a batch for a future AcquireBatch().
func (stage *Stage) RecycleBatch(batch []Vals) {
	stage.M.Lock()
	stage.Recycled = append(stage.Recycled, batch)
	stage.M.Unlock()
}

// AcquireBatch returns either a previously recycled batch or nil if
// there aren't any.
func (stage *Stage) AcquireBatch() (rv []Vals) {
	stage.M.Lock()
	n := len(stage.Recycled)
	if n > 0 {
		rv = stage.Recycled[n-1]
		stage.Recycled[n-1] = nil
		stage.Recycled = stage.Recycled[0 : n-1]
	}
	stage.M.Unlock()

	return rv
}
