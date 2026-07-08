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

package glue

import (
	"os/exec"
	"strings"
	"sync"
)

// GoToolchain describes the external `go` build toolchain, which the compiled
// PREPARE levels (data/full) need to build the emitted program. When it is
// Available == false, compiled EXECUTE must degrade gracefully to the interpreter
// (see PrepareLevelAchievable) -- n1k1 stays a single self-contained binary and
// never *requires* a toolchain; it only *uses* one, opt-in, to compile.
type GoToolchain struct {
	Path      string // absolute path to the `go` binary, "" when not found
	Version   string // `go version` output (one line), "" when unavailable
	Available bool
}

var (
	goToolchainOnce   sync.Once
	goToolchainCached GoToolchain
)

// GoToolchainDetect probes for the `go` build toolchain on PATH (result cached for
// the process). Never errors: an absent toolchain returns a zero GoToolchain with
// Available == false.
func GoToolchainDetect() GoToolchain {
	goToolchainOnce.Do(func() { goToolchainCached = goToolchainProbe() })
	return goToolchainCached
}

func goToolchainProbe() GoToolchain {
	path, err := exec.LookPath("go")
	if err != nil {
		return GoToolchain{}
	}
	tc := GoToolchain{Path: path, Available: true}
	if out, err := exec.Command(path, "version").Output(); err == nil {
		tc.Version = strings.TrimSpace(string(out))
	}
	return tc
}

// PrepareLevelAchievable caps a desired PREPARE ceiling to what this environment
// can actually run. The compiled levels (data/full) need the `go` toolchain to
// build the emitted program; without it, EXECUTE falls back to the interpreter, so
// the achievable ceiling is PrepareInterpreted. want is returned unchanged when the
// toolchain is present, or when want is already the interpreter floor.
func PrepareLevelAchievable(want PrepareLevel) PrepareLevel {
	return prepareLevelAchievable(want, GoToolchainDetect().Available)
}

func prepareLevelAchievable(want PrepareLevel, goAvailable bool) PrepareLevel {
	if want == PrepareInterpreted || goAvailable {
		return want
	}
	return PrepareInterpreted
}
