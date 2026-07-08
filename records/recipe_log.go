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

package records

// The built-in "ns_server_log" recipe: a generic timestamped multiline log, modeled
// on ns_server-style Couchbase lines (DESIGN-data.md §4, DESIGN-extensions.md
// "Extract functions"). Each record is a lead line
//
//	[module:level,2026-05-17T15:36:11.198+02:00,node:...]message
//
// plus any following continuation lines (indented, or Erlang term dumps that begin
// with '[' but aren't themselves lead lines). It is the first real, regular format
// wired end-to-end through the native describe/extract seam: describe returns the
// declarative ExtractSpec (framing=multiline, named-capture fields, RFC3339 tz-aware
// time, near-sorted order with a measured disorder bound) and the measured
// SortedSourceMeta; SpecApply executes that spec natively (no per-row JS).
//
// This is the pure-Go analogue of a cb_ns_server.extract.js recipe -- the JS loader
// that PRODUCES such a spec from a *.extract.js file is the deferred second step.

// nsLogFields is the record-lead / field-capture pattern. Anchored at '^' so it also
// serves as the multiline record-boundary detector (a continuation line -- indented,
// or an Erlang list like `[{a,b}]` whose '[' is not followed by `module:level,ts` --
// does not match at offset 0, so it folds into the current record). Named groups:
//
//	module  the emitting subsystem   (ns_server, stats, couch_log, ...)
//	level   the log level            (info, warn, error, ...)
//	ts      the RFC3339 timestamp    -> normalized to int64 epoch-nanos
//	node    the emitting node id     (up to the first ':')
//	msg     the message body         ((?s:...) so continuation lines are included)
const nsLogFields = `^\[(?P<module>\w+):(?P<level>\w+),(?P<ts>[^,]+),(?P<node>[^:]+):[^\]]*\](?P<msg>(?s:.*))`

// nsLogSpec is the declarative ExtractSpec for the ns_server_log format. The Order's
// disorder bound here is the format author's DECLARED default (loggers flush thread
// buffers slightly out of order); describe() overrides it with the value MEASURED
// from the file's sample (see NSLogDescribe / describeMeasure).
func nsLogSpec() ExtractSpec {
	return ExtractSpec{
		Format:  "ns_server_log",
		Framing: Framing{Kind: FramingMultiline, Continuation: `^\s|^\[`},
		Fields:  Fields{Pattern: nsLogFields},
		Time:    &TimeSpec{Field: "ts", Layout: TimeLayoutRFC3339, TZDefault: "+02:00"},
		Order:   OrderSpec{By: "ts", Sorted: SortedNear, Disorder: DisorderBound{WindowNanos: 2_000_000_000}},
	}
}

// NSLogDescribe is the ns_server_log recipe's describe pass: it returns the constant
// declarative spec and MEASURES the sorted-source metadata (min/max epoch-nanos key,
// sortedness, disorder bound, record count) by sampling the file's head. The measured
// disorder bound replaces the spec's declared default so the merge sees this file's
// real bound. Cheap: it samples rather than full-scanning (describeMeasure).
func NSLogDescribe(path string) (ExtractSpec, SortedSourceMeta, error) {
	spec := nsLogSpec()
	meta, err := describeMeasure(spec, path)
	if err != nil {
		return spec, SortedSourceMeta{}, err
	}
	// Reflect the measured order back into the spec so a consumer that only keeps the
	// spec still sees this file's real sortedness/bound.
	spec.Order.Sorted = meta.Sortedness
	spec.Order.Disorder = meta.Disorder
	return spec, meta, nil
}

func init() {
	RecipeRegister(&Recipe{
		Name: "ns_server_log",
		// Claim .log files whose (dataset-relative) name is an ns_server-family log
		// -- specific enough to leave generic .log files to the whole-file text
		// extractor. Higher priority than a would-be generic `\.log$` recipe.
		Match: ExtractMatch{
			Exts:     []string{".log"},
			Names:    []string{`ns_server\..*\.log$`, `(^|/)diag\.log$`, `(^|/)info\.log$`},
			Priority: 10,
		},
		Describe: NSLogDescribe,
		// Extract nil: the format is regular, so SpecApply runs the spec natively.
	})
}
