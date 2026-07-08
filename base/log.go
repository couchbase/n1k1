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
	"os"
)

// Diagnostic logging for n1k1. One process-global level + one overridable sink, so
// the whole engine logs the same way and a library embedder can redirect it.
//
// Convention (keep messages greppable):
//   - `level` matches the -v/-verbose value: a message emits when LogLevel >= level.
//     0 = quiet (default); >0 = info; >1 = more detail. (No separate log flag --
//     logging rides the existing verbosity knob.)
//   - `tag` is a STABLE component label naming where the line comes from, e.g.
//     "records/describe", "glue/recipe", "merge-scan" -- grep the tag to the source
//     (cheaper + more stable than runtime.Caller).
//   - format emitted values as "key: val" pairs, e.g.
//       Logf(1, "records/describe", "matched recipe, file: %s, format: %s, records: %d",
//            name, format, n)
//
// NEVER call Logf on a per-row hot loop -- it is for planning/describe/setup (cold).

// LogLevel is the global verbosity threshold. Logf(level, ...) emits only when
// level <= LogLevel. The CLI sets it from -v/-verbose; a library embedder sets it
// directly (or leaves it 0 for silence).
var LogLevel int

// LogSink is the global closure every Logf routes through -- an embedder replaces it
// to redirect, reformat, structure, or suppress n1k1's logs (it is only ever called
// for messages that already passed the LogLevel gate). The default writes
// "<tag>: <msg>\n" to stderr (results go to stdout, so logs stay out of the way).
// `level` is passed through so a custom sink can map it to its own severity.
var LogSink = func(level int, tag, msg string) {
	fmt.Fprintf(os.Stderr, "%s: %s\n", tag, msg)
}

// Logf emits a diagnostic at verbosity `level` (matching -v/-verbose): a no-op
// unless LogLevel >= level, else it formats and hands (level, tag, msg) to LogSink.
// See the file comment for the tag / key:val conventions.
func Logf(level int, tag, format string, args ...interface{}) {
	if level > LogLevel || LogSink == nil {
		return
	}
	LogSink(level, tag, fmt.Sprintf(format, args...))
}

// LogEnabled reports whether a message at `level` would emit -- a guard for callers
// that must do non-trivial work to build a log's arguments, so that work is skipped
// when logging is off. (Plain Logf calls need no guard.)
func LogEnabled(level int) bool { return level <= LogLevel && LogSink != nil }
