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
	"reflect"
	"sort"
	"testing"
)

// matchedSorted returns the sorted set of pattern ids present in b -- the order-
// independent ground truth for these tests.
func matchedSorted(ac *AhoCorasick, ms *MatchSet, b string) []int {
	ids := append([]int(nil), ac.Match([]byte(b), ms)...)
	sort.Ints(ids)
	return ids
}

func TestAhoCorasickBasic(t *testing.T) {
	pats := []string{"he", "she", "his", "hers"}
	ac := BuildAhoCorasick(pats)
	ms := ac.NewMatchSet()

	cases := []struct {
		in   string
		want []int
	}{
		// "ushers" contains "she"(1), "he"(0), "hers"(3).
		{"ushers", []int{0, 1, 3}},
		{"his", []int{2}},
		{"hello there", []int{0}}, // "he" only
		{"nothing here", []int{0}},
		{"xyz", nil},
		{"", nil},
	}
	for _, c := range cases {
		got := matchedSorted(ac, ms, c.in)
		if len(got) == 0 && len(c.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Fatalf("Match(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestAhoCorasickSubstringPatterns covers patterns that are substrings of one
// another: a match of the longer pattern must ALSO report the shorter one (via
// output links), and vice-versa the shorter one alone must not report the longer.
func TestAhoCorasickSubstringPatterns(t *testing.T) {
	pats := []string{"ab", "abc", "b", "bcd"}
	ac := BuildAhoCorasick(pats)
	ms := ac.NewMatchSet()

	// "abcd": "ab"(0), "abc"(1), "b"(2), "bcd"(3) all present.
	if got, want := matchedSorted(ac, ms, "abcd"), []int{0, 1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Match(abcd) = %v, want %v", got, want)
	}
	// "ab": only "ab"(0) and "b"(2); NOT "abc"(1) or "bcd"(3).
	if got, want := matchedSorted(ac, ms, "ab"), []int{0, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Match(ab) = %v, want %v", got, want)
	}
}

// TestAhoCorasickOverlapping covers overlapping occurrences of the same and
// different patterns within one input.
func TestAhoCorasickOverlapping(t *testing.T) {
	pats := []string{"aa", "aaa"}
	ac := BuildAhoCorasick(pats)
	ms := ac.NewMatchSet()

	if got, want := matchedSorted(ac, ms, "aaaa"), []int{0, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Match(aaaa) = %v, want %v", got, want)
	}
	if got := matchedSorted(ac, ms, "a"); len(got) != 0 {
		t.Fatalf("Match(a) = %v, want none", got)
	}
}

// TestAhoCorasickEmptyPattern: an empty pattern is a substring of everything, so
// it matches every input (including the empty string).
func TestAhoCorasickEmptyPattern(t *testing.T) {
	ac := BuildAhoCorasick([]string{"x", ""})
	ms := ac.NewMatchSet()

	if got, want := matchedSorted(ac, ms, "abc"), []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Match(abc) = %v, want %v (empty pattern id 1 always matches)", got, want)
	}
	if got, want := matchedSorted(ac, ms, ""), []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Match(\"\") = %v, want %v", got, want)
	}
	if got, want := matchedSorted(ac, ms, "xyz"), []int{0, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Match(xyz) = %v, want %v", got, want)
	}
}

// TestAhoCorasickStreaming feeds an input as several slices through one Reset and
// checks it matches the same as one contiguous scan (boundary-straddling
// over-matches are acceptable, but WITHIN-slice matches must all be found).
func TestAhoCorasickStreaming(t *testing.T) {
	pats := []string{"error", "panic", "warn"}
	ac := BuildAhoCorasick(pats)
	ms := ac.NewMatchSet()

	ms.Reset()
	state := 0
	state = ac.Advance(state, []byte("some error and a "), ms)
	state = ac.Advance(state, []byte("panic occurred"), ms)
	_ = state

	got := append([]int(nil), ms.IDs()...)
	sort.Ints(got)
	if want := []int{0, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("streaming match = %v, want %v (error, panic)", got, want)
	}
}

// TestAhoCorasickDuplicatePatterns: two equal patterns share a terminal node but
// both ids must be reported.
func TestAhoCorasickDuplicatePatterns(t *testing.T) {
	ac := BuildAhoCorasick([]string{"dup", "dup"})
	ms := ac.NewMatchSet()
	if got, want := matchedSorted(ac, ms, "a dup here"), []int{0, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Match = %v, want %v", got, want)
	}
}

// TestAhoCorasickResetReuse proves a MatchSet is reusable with no leakage across
// scans (the zero-garbage contract).
func TestAhoCorasickResetReuse(t *testing.T) {
	ac := BuildAhoCorasick([]string{"foo", "bar"})
	ms := ac.NewMatchSet()

	if got, want := matchedSorted(ac, ms, "foo"), []int{0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first = %v, want %v", got, want)
	}
	// Second scan must not carry "foo" over.
	if got, want := matchedSorted(ac, ms, "bar"), []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second = %v, want %v", got, want)
	}
}

// TestAhoCorasickZeroAlloc asserts Match allocates nothing after warmup -- the
// per-row promise the predicate index relies on.
func TestAhoCorasickZeroAlloc(t *testing.T) {
	ac := BuildAhoCorasick([]string{"alpha", "beta", "gamma", "delta"})
	ms := ac.NewMatchSet()
	line := []byte("a line mentioning beta and gamma somewhere within")

	avg := testing.AllocsPerRun(200, func() {
		ms.Reset()
		ac.Advance(0, line, ms)
	})
	if avg != 0 {
		t.Fatalf("AhoCorasick Match allocated %v times/run, want 0", avg)
	}
}
