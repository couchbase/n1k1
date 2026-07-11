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
	"strconv"
	"testing"
)

// foldRef re-folds the named aggregate over vals[beg:end) from scratch -- the
// brute-force reference the sliding fast paths must match bit-for-bit.
func foldRef(name string, vals []Val, beg, end int) string {
	vc := &ValComparer{}
	agg := Aggs[AggCatalog[name]]
	st := agg.Init(nil, nil)
	for i := beg; i < end; i++ {
		st, _, _ = agg.Update(nil, vals[i], nil, st, vc)
	}
	v, _, _ := agg.Result(nil, st, nil)
	return string(v)
}

// slideSpan is a fixed [beg,end) frame per position, both edges monotone -- the shape
// the sliding fast paths handle (ROWS BETWEEN b PRECEDING AND a FOLLOWING).
func slideSpan(pos, n, before, after int) (int, int) {
	beg := pos - before
	if beg < 0 {
		beg = 0
	}
	end := pos + after + 1
	if end > n {
		end = n
	}
	return beg, end
}

// TestSlideSumAvgMatchesFold drives SlideSum* over sliding windows and checks the
// incremental SUM/AVG result equals the fresh fold for integer operands (exact), and
// that SlideSumExact() correctly flags float/large operands so the caller re-folds.
func TestSlideSumAvgMatchesFold(t *testing.T) {
	tests := []struct {
		name         string
		vals         []string
		wantAllExact bool
	}{
		{"ints", []string{"3", "1", "4", "1", "5", "9", "2", "6"}, true},
		{"ints-with-null", []string{"3", "null", "4", "1", "null", "9"}, true},
		{"ints-with-missing", []string{"3", "", "4", "1", "", "9"}, true},
		{"ints-with-string", []string{"3", `"x"`, "4", "1", `"y"`, "9"}, true},
		{"negatives", []string{"-3", "5", "-1", "-4", "2", "-6"}, true},
		{"zeros", []string{"0", "0", "0", "0"}, true},
		{"floats-inexact", []string{"0.1", "0.2", "0.3", "0.1"}, false},
		{"big-nonint", []string{"1.5", "2", "3", "4"}, false},
	}

	for _, before := range []int{0, 1, 2, 3} {
		for _, after := range []int{0, 1, 2} {
			for _, tc := range tests {
				vals := make([]Val, len(tc.vals))
				for i, s := range tc.vals {
					vals[i] = Val(s)
				}
				n := len(vals)

				var wf WindowFrame
				vc := &ValComparer{}
				wf.SlideSumReset()

				prevBeg, prevEnd := 0, 0
				allExact := true
				for pos := 0; pos < n; pos++ {
					beg, end := slideSpan(pos, n, before, after)
					for i := prevEnd; i < end; i++ {
						wf.SlideSumEnter(vals[i])
					}
					for i := prevBeg; i < beg; i++ {
						wf.SlideSumLeave(vals[i])
					}
					prevBeg, prevEnd = beg, end

					exact := wf.SlideSumExact()
					allExact = allExact && exact

					if !exact {
						continue // caller would re-fold; incremental sum not trusted here
					}

					gotSum, _ := wf.SlideSumResult(nil)
					if wantSum := foldRef("sum", vals, beg, end); string(gotSum) != wantSum {
						t.Errorf("%s b=%d a=%d pos=%d: slide SUM=%q want %q",
							tc.name, before, after, pos, gotSum, wantSum)
					}
					gotAvg, _ := wf.SlideAvgResult(nil)
					if wantAvg := foldRef("avg", vals, beg, end); string(gotAvg) != wantAvg {
						t.Errorf("%s b=%d a=%d pos=%d: slide AVG=%q want %q",
							tc.name, before, after, pos, gotAvg, wantAvg)
					}
					_ = vc
				}

				if allExact != tc.wantAllExact {
					t.Errorf("%s b=%d a=%d: allExact=%v want %v",
						tc.name, before, after, allExact, tc.wantAllExact)
				}
			}
		}
	}
}

// TestSlideMinMaxMatchesFold drives the monotonic deque over sliding windows and checks
// MIN/MAX equal the fresh AggMin/AggMax fold (which does NOT skip NULL/MISSING).
func TestSlideMinMaxMatchesFold(t *testing.T) {
	valSets := []struct {
		vals       []string
		hasMissing bool // AggMin/AggMax's length-as-count quirk on MISSING -> deque re-folds
	}{
		{[]string{"3", "1", "4", "1", "5", "9", "2", "6"}, false},
		{[]string{"5", "5", "5", "5"}, false},
		{[]string{"1", "2", "3", "4", "5"}, false},    // ascending
		{[]string{"5", "4", "3", "2", "1"}, false},    // descending
		{[]string{"3", "null", "4", "1", "9"}, false}, // null: a normal comparable
		{[]string{"3", "", "4", "1", "9"}, true},      // missing: order-dependent -> re-fold
		{[]string{`"b"`, `"a"`, `"c"`, `"a"`}, false}, // strings compare too
		{[]string{"10", `"x"`, "2", "30"}, false},     // mixed number/string
	}

	for _, isMax := range []bool{false, true} {
		for _, before := range []int{0, 1, 2, 3} {
			for _, after := range []int{0, 1, 2} {
				for si, set := range valSets {
					vals := make([]Val, len(set.vals))
					for i, s := range set.vals {
						vals[i] = Val(s)
					}
					n := len(vals)

					var wf WindowFrame
					vc := &ValComparer{}
					wf.SlideMinMaxReset()

					refName := "min"
					if isMax {
						refName = "max"
					}

					sawInexact := false
					prevEnd := 0
					for pos := 0; pos < n; pos++ {
						beg, end := slideSpan(pos, n, before, after)
						for i := prevEnd; i < end; i++ {
							wf.SlideMinMaxEnter(int64(i), vals[i], isMax, vc)
						}
						prevEnd = end
						wf.SlideMinMaxExpire(int64(beg))

						if !wf.SlideMinMaxExact() {
							sawInexact = true
							continue // caller re-folds; deque result not trusted here
						}

						got, _ := wf.SlideMinMaxResult(nil)
						want := foldRef(refName, vals, beg, end)
						if string(got) != want {
							t.Errorf("%s set=%d b=%d a=%d pos=%d [%d,%d): got=%q want=%q",
								refName, si, before, after, pos, beg, end, got, want)
						}
					}

					if sawInexact != set.hasMissing {
						t.Errorf("%s set=%d b=%d a=%d: sawInexact=%v want hasMissing=%v",
							refName, si, before, after, sawInexact, set.hasMissing)
					}
				}
			}
		}
	}
}

// --- benchmarks: sliding fast path vs brute-force per-row re-fold ---
//
// N rows, a fixed window of half-width k (2k+1 wide). The deque / invertible sum are
// O(N) regardless of k; the brute re-fold is O(N*k). Run e.g.
//   go test ./base -run x -bench 'SlideMinMax|SlideSum' -benchmem

const benchN = 4000
const benchK = 200 // window half-width -> 2k+1 = 401 wide

func benchVals(n int) []Val {
	vals := make([]Val, n)
	for i := range vals {
		vals[i] = Val(strconv.Itoa((i*2654435761)%100000 - 50000)) // integer, pseudo-shuffled
	}
	return vals
}

func benchBruteFold(b *testing.B, name string) {
	vals := benchVals(benchN)
	vc := &ValComparer{}
	agg := Aggs[AggCatalog[name]]
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		for pos := 0; pos < benchN; pos++ {
			beg, end := slideSpan(pos, benchN, benchK, benchK)
			st := agg.Init(nil, nil)
			for i := beg; i < end; i++ {
				st, _, _ = agg.Update(nil, vals[i], nil, st, vc)
			}
			agg.Result(nil, st, nil)
		}
	}
}

func BenchmarkSlideMinMaxBrute(b *testing.B) { benchBruteFold(b, "min") }

func BenchmarkSlideMinMaxDeque(b *testing.B) {
	vals := benchVals(benchN)
	vc := &ValComparer{}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		var wf WindowFrame
		wf.SlideMinMaxReset()
		prevEnd := 0
		for pos := 0; pos < benchN; pos++ {
			beg, end := slideSpan(pos, benchN, benchK, benchK)
			for i := prevEnd; i < end; i++ {
				wf.SlideMinMaxEnter(int64(i), vals[i], false, vc)
			}
			prevEnd = end
			wf.SlideMinMaxExpire(int64(beg))
			wf.SlideMinMaxResult(nil)
		}
	}
}

func BenchmarkSlideSumBrute(b *testing.B) { benchBruteFold(b, "sum") }

func BenchmarkSlideSumInvertible(b *testing.B) {
	vals := benchVals(benchN)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		var wf WindowFrame
		wf.SlideSumReset()
		prevBeg, prevEnd := 0, 0
		for pos := 0; pos < benchN; pos++ {
			beg, end := slideSpan(pos, benchN, benchK, benchK)
			for i := prevEnd; i < end; i++ {
				wf.SlideSumEnter(vals[i])
			}
			for i := prevBeg; i < beg; i++ {
				wf.SlideSumLeave(vals[i])
			}
			prevBeg, prevEnd = beg, end
			wf.SlideSumResult(nil)
		}
	}
}
