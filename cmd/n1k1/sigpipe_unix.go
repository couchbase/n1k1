//go:build n1ql && !windows

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
	"os/signal"
	"syscall"
)

// ignoreSIGPIPE stops the Go runtime from terminating the process when a write to a
// broken stdout/stderr pipe (fd 1/2) raises SIGPIPE -- e.g. `n1k1 -c ... | head`. With
// SIGPIPE ignored, that write returns EPIPE instead, which the row-writer turns into a
// cooperative HALT (Session.Interrupt) so the query stops cleanly rather than crashing.
func ignoreSIGPIPE() { signal.Ignore(syscall.SIGPIPE) }
