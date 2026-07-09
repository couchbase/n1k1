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

// AhoCorasick is a pure-Go multi-substring matcher: build it ONCE over a set of
// literal patterns, then scan any number of byte slices to learn WHICH patterns
// are present -- in a single O(len(input)) pass regardless of how many patterns
// there are.
//
// It exists to power the PREPARE++ "predicate index" (DESIGN-prepare.md, the
// "Predicate index (the scale trick)" bullet): thousands of detectors are
// indexed by a cheap required literal, and one Aho-Corasick pass over a row's
// bytes wakes only the few detectors whose literal is present. So the classic
// automaton (trie goto + failure links + output links) is the natural fit.
//
// Zero allocation per scan after warmup: a reused MatchSet accumulates the
// matched pattern ids, and Advance keeps the automaton state in a plain int the
// caller threads across slices (so a row spread over several byte slices is one
// logical scan). All allocation happens in Build.
type AhoCorasick struct {
	// next[node] maps a byte to the child node in the goto trie (a match-time
	// fallback via fail[] fills in the missing transitions, so this stays sparse).
	next []map[byte]int

	// fail[node] is the failure link: the node for the longest proper suffix of
	// the path-to-node that is also a trie prefix.
	fail []int

	// output[node] lists the ids of every pattern that ENDS at node -- directly,
	// or at any node reachable by following failure links (flattened at build
	// time so match-time needs no fail-chain walk for outputs).
	output [][]int

	// numPatterns is how many patterns were added (ids 0..numPatterns-1). A
	// pattern may share a terminal node with an equal pattern; both ids are still
	// reported.
	numPatterns int

	// emptyIDs are pattern ids whose pattern was empty (""). An empty pattern is a
	// substring of everything, so it always matches; it is reported once at the
	// start of every scan rather than threaded through the automaton.
	emptyIDs []int
}

// MatchSet is a reusable, zero-garbage accumulator of matched pattern ids. Build
// one with AhoCorasick.NewMatchSet and Reset it before each logical scan.
type MatchSet struct {
	seen []bool // seen[id] == true once id has been added this scan
	ids  []int  // the ids added this scan, in first-seen order
}

// NewMatchSet returns a MatchSet sized for this automaton's patterns.
func (ac *AhoCorasick) NewMatchSet() *MatchSet {
	return &MatchSet{seen: make([]bool, ac.numPatterns)}
}

// Reset clears the set for a fresh scan without freeing its backing arrays.
func (ms *MatchSet) Reset() {
	for _, id := range ms.ids {
		ms.seen[id] = false
	}
	ms.ids = ms.ids[:0]
}

// IDs returns the matched pattern ids accumulated since the last Reset. The
// slice aliases the set's reused backing array -- do not retain it across scans.
func (ms *MatchSet) IDs() []int { return ms.ids }

// add records id as present (idempotent within a scan).
func (ms *MatchSet) add(id int) {
	if id >= 0 && id < len(ms.seen) && !ms.seen[id] {
		ms.seen[id] = true
		ms.ids = append(ms.ids, id)
	}
}

// BuildAhoCorasick constructs the automaton over the given patterns; a pattern's
// id is its index in the slice. Duplicate patterns are allowed (each id is still
// reported). Empty patterns match every input.
func BuildAhoCorasick(patterns []string) *AhoCorasick {
	ac := &AhoCorasick{numPatterns: len(patterns)}

	// Root is node 0.
	ac.next = []map[byte]int{{}}
	ac.fail = []int{0}
	ac.output = [][]int{nil}

	// (1) Insert each pattern into the goto trie, recording its id at the
	// terminal node.
	for id, p := range patterns {
		if len(p) == 0 {
			ac.emptyIDs = append(ac.emptyIDs, id)
			continue
		}
		node := 0
		for i := 0; i < len(p); i++ {
			b := p[i]
			nxt, ok := ac.next[node][b]
			if !ok {
				nxt = len(ac.next)
				ac.next = append(ac.next, map[byte]int{})
				ac.fail = append(ac.fail, 0)
				ac.output = append(ac.output, nil)
				ac.next[node][b] = nxt
			}
			node = nxt
		}
		ac.output[node] = append(ac.output[node], id)
	}

	// (2) BFS from the root to set failure + flattened output links. A depth-1
	// node fails to the root; deeper nodes fail to goto(fail(parent), edge).
	queue := make([]int, 0, len(ac.next))
	for _, child := range ac.next[0] {
		ac.fail[child] = 0
		queue = append(queue, child)
	}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		for b, child := range ac.next[node] {
			// Failure link of child: walk the parent's failure chain looking for a
			// node that has an edge on b.
			f := ac.fail[node]
			for {
				if nxt, ok := ac.next[f][b]; ok && f != node {
					ac.fail[child] = nxt
					break
				}
				if f == 0 {
					ac.fail[child] = 0
					break
				}
				f = ac.fail[f]
			}

			// Flatten outputs: a match at fail(child) is also a match at child (it
			// is a suffix). fail links point strictly closer to the root, which BFS
			// has already finalized, so this closure is complete.
			if outs := ac.output[ac.fail[child]]; len(outs) > 0 {
				ac.output[child] = append(ac.output[child], outs...)
			}

			queue = append(queue, child)
		}
	}

	return ac
}

// Advance feeds one byte slice into the automaton starting from state (0 to
// begin a fresh scan), recording every pattern found into ms, and returns the
// state to pass to the next Advance. Thread several slices through one Reset to
// scan a logically-contiguous input (a match that straddles a slice boundary is
// reported -- harmless over-matching for the predicate-index use, where any
// extra wake is re-checked by the full predicate).
//
// Call ms.Reset() once before the first Advance of a scan; on that first call
// pass state 0 so empty-pattern ids are emitted exactly once.
func (ac *AhoCorasick) Advance(state int, b []byte, ms *MatchSet) int {
	if state == 0 {
		for _, id := range ac.emptyIDs {
			ms.add(id)
		}
	}

	for i := 0; i < len(b); i++ {
		c := b[i]
		// goto(state, c) with failure fallback.
		for {
			if nxt, ok := ac.next[state][c]; ok {
				state = nxt
				break
			}
			if state == 0 {
				break // stay at root
			}
			state = ac.fail[state]
		}
		if outs := ac.output[state]; len(outs) > 0 {
			for _, id := range outs {
				ms.add(id)
			}
		}
	}

	return state
}

// Match is the one-shot convenience form: Reset ms, scan a single byte slice,
// and return ms's matched ids. For a multi-slice input use Reset + Advance.
func (ac *AhoCorasick) Match(b []byte, ms *MatchSet) []int {
	ms.Reset()
	ac.Advance(0, b, ms)
	return ms.IDs()
}
