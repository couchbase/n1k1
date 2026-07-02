//go:build n1ql

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

package main

import (
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// TestStatsLines checks the per-operator footer formatting: one line per op,
// indented by tree depth (the '/'-count of its id), with "Name=value" per counter
// read from the flat Counters array at each op's Base.
func TestStatsLines(t *testing.T) {
	s := &base.Stats{
		Counters: []int64{1, 1, 6, 5, 6}, // group RowsIn/GroupsOut, filter RowsIn/RowsOut, scan RowsOut
		Ops: []base.StatsOpInfo{
			{Id: "0", Kind: "group", Base: 0, Names: []string{"RowsIn", "GroupsOut"}},
			{Id: "0/0", Kind: "filter", Base: 2, Names: []string{"RowsIn", "RowsOut"}},
			{Id: "0/0/0", Kind: "datastore-scan-records", Base: 4, Names: []string{"RowsOut"}},
		},
	}

	got := strings.Join(statsLines(s), "\n")
	want := "group  RowsIn=1  GroupsOut=1\n" +
		"  filter  RowsIn=6  RowsOut=5\n" +
		"    datastore-scan-records  RowsOut=6"

	if got != want {
		t.Errorf("statsLines mismatch:\n got:\n%s\nwant:\n%s", got, want)
	}
}

// TestStatsLinesEmpty: no counter-contributing ops -> no lines (the caller then
// prints nothing).
func TestStatsLinesEmpty(t *testing.T) {
	if got := statsLines(&base.Stats{}); len(got) != 0 {
		t.Errorf("statsLines(empty) = %v, want none", got)
	}
}
