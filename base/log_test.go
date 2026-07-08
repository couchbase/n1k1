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
	"fmt"
	"testing"
)

// TestLogf: Logf emits only when LogLevel >= level, formats via the message, and
// routes through the overridable LogSink (the library-embedder seam). LogEnabled
// mirrors the gate.
func TestLogf(t *testing.T) {
	origLevel, origSink := LogLevel, LogSink
	defer func() { LogLevel, LogSink = origLevel, origSink }()

	var got []string
	LogSink = func(level int, tag, msg string) {
		got = append(got, fmt.Sprintf("%d|%s|%s", level, tag, msg))
	}

	LogLevel = 0
	Logf(1, "t", "a %d", 1) // suppressed: 1 > LogLevel 0
	if len(got) != 0 {
		t.Fatalf("level 1 at LogLevel 0 should be suppressed; got %v", got)
	}
	if LogEnabled(1) {
		t.Error("LogEnabled(1) should be false at LogLevel 0")
	}

	LogLevel = 1
	Logf(1, "recipe", "loaded, name: %s", "x") // emits (1 <= 1)
	Logf(2, "recipe", "detail")                // suppressed (2 > 1)
	if len(got) != 1 || got[0] != "1|recipe|loaded, name: x" {
		t.Fatalf("LogLevel 1 gating/format wrong; got %v", got)
	}
	if !LogEnabled(1) || LogEnabled(2) {
		t.Error("LogEnabled gating wrong at LogLevel 1")
	}
}
