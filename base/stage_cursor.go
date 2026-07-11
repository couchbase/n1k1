//  Copyright (c) 2026 Couchbase, Inc.
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

// StageCursor is a pipeline-stage breaker in the shape Stage doesn't offer: a SINGLE actor
// goroutine producing one ORDERED stream that a consumer PULLS at its own pace (NextBatch),
// as opposed to Stage's many-actors-into-one-mixed-channel push-consumed model. It exists
// for the sorted-merge family (op_merge_scan / op_merge_join), where the coordinator must
// co-advance K per-child ordered streams and may RETAIN rows across batches (the ASOF
// held-row / per-partition queues), which rules out Stage's batch/Vals recycling.
//
// Shared design with Stage.StartActor: the actor runs on a per-actor Vars clone
// (ChainExtend, so it never races the coordinator over the shared Ctx), and its yielded
// rows are DEEP-COPIED (they cross the goroutine and outlive the child scan's reused
// buffers) and BATCHED -- a per-row channel handoff otherwise spends most of its CPU in
// goroutine park/unpark (pthread_cond_signal). Batches are NOT recycled (the consumer may
// retain rows), so a fresh batch is allocated per flush (one small slice per batchSize
// rows); ValsDeepCopy packs each row's Vals into a single contiguous allocation.
type StageCursor struct {
	ch       chan []Vals
	done     chan struct{}
	finished chan struct{}

	mu      sync.Mutex
	err     error
	stopped bool
}

// NewStageCursor starts actor on its own goroutine and returns a cursor its rows can be
// pulled from in batches. batchSize is rows per batch (amortizes the wakeup); chanCap is
// the channel depth in batches (backpressure). The actor is handed a per-actor Vars clone
// and the usual yield callbacks; a yielded error stops production and surfaces via Wait.
func NewStageCursor(vars *Vars, batchSize, chanCap int,
	actor func(*Vars, YieldVals, YieldErr)) *StageCursor {
	if batchSize < 1 {
		batchSize = 1
	}
	bc := &StageCursor{
		ch:       make(chan []Vals, chanCap),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}

	go func() {
		defer close(bc.finished)
		defer close(bc.ch)

		cVars := vars.ChainExtend() // per-actor Ctx clone; race-safe vs the consumer.

		var batch []Vals
		stop := false

		flush := func() {
			if len(batch) == 0 {
				return
			}
			select {
			case bc.ch <- batch:
				batch = nil // fresh buffer next -- the consumer keeps this one.
			case <-bc.done: // consumer gave up; stop feeding.
				stop = true
			}
		}

		yieldVals := func(vals Vals) {
			if stop {
				return
			}
			if batch == nil {
				batch = make([]Vals, 0, batchSize)
			}
			cp, _, _ := ValsDeepCopy(vals, nil, nil)
			batch = append(batch, cp)
			if len(batch) >= batchSize {
				flush()
			}
		}

		yieldErr := func(err error) {
			if err != nil {
				bc.mu.Lock()
				if bc.err == nil {
					bc.err = err
				}
				bc.mu.Unlock()
				stop = true
			}
		}

		actor(cVars, yieldVals, yieldErr)
		flush() // final partial batch.
	}()

	return bc
}

// NextBatch pulls the next batch, blocking until one is available or the stream ends
// (ok=false at end). The returned rows are stable (never recycled), so the consumer may
// retain them across further NextBatch calls.
func (bc *StageCursor) NextBatch() ([]Vals, bool) {
	b, ok := <-bc.ch
	return b, ok
}

// Stop signals the actor to stop producing (it drops any unsent tail). Idempotent; safe
// to call before Wait even if the stream was fully drained.
func (bc *StageCursor) Stop() {
	bc.mu.Lock()
	if !bc.stopped {
		close(bc.done)
		bc.stopped = true
	}
	bc.mu.Unlock()
}

// Wait blocks until the actor goroutine has exited and returns its first error. Call after
// draining (or after Stop) so the producer is joined -- no goroutine leak -- and any
// build-side error is surfaced.
func (bc *StageCursor) Wait() error {
	<-bc.finished
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.err
}
