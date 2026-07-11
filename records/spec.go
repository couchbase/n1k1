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

// Phase-0 shared contract (see DESIGN-data.md §4 "The extract provider" and
// "Sorted & near-sorted sources: the merge-join contract"; DESIGN-extensions.md
// "Extract functions"; DESIGN-merging.md).
//
// This file pins the *data contract* three parallel work-tracks agree on, so
// they can proceed without stepping on each other:
//
//   - Track A (extract & JS recipes): PRODUCES an ExtractSpec from describe(file),
//     applies it to yield records, and derives SortedSourceMeta by sampling.
//   - Track B (merge & ASOF): CONSUMES SortedSourceMeta. The engine merge op does
//     NOT import this package -- glue translates the neutral scalar fields below
//     (sort-key label, sortedness, disorder-bound nanos, min/max key) into
//     base.Op Params; the engine reads plain ints/strings. Keeping the merge op's
//     inputs scalar is what lets it stay codegen/compiler-friendly (DESIGN.md
//     Futamura path -- the engine switch stays static, no runtime type here).
//   - glue: reads ExtractSpec/SortedSourceMeta (memoized in .n1k1/, DESIGN-data.md
//     §5) and wires both the extract decode and the merge op Params.
//
// These types are JSON-serializable because a describe() result is memoized into
// the .n1k1 sidecar keyed by file fingerprint (computed once per file, reused by
// every later extract across all queries -- DESIGN-data.md §4 "Describe once,
// reuse forever"). Field names here are the canonical contract; changing one is a
// cross-track change.
//
// NOTE: these are the agreed shapes, not yet wired to a live sidecar or decoder;
// the parallel tracks flesh out the producers/consumers against them.

// ExtractMatch declares which files an extractor recipe claims. Matching is by
// file extension AND/OR regexp over the (dataset-relative) path; the
// highest-Priority match wins on overlap (DESIGN-data.md §4 "Matching a file to
// an extractor"). This same matcher is DESIGN-prepare.md's source-routing /
// late-binding resolver ("convention" and "content-sniffing" rungs).
type ExtractMatch struct {
	Exts     []string `json:"exts,omitempty"`     // e.g. [".log", ".pdf"] (lower-cased, with dot)
	Names    []string `json:"names,omitempty"`    // regexps over the dataset-relative path
	Priority int      `json:"priority,omitempty"` // higher wins on overlap; default 0
}

// Framing kinds -- how a file's bytes split into records (DESIGN-data.md §4).
const (
	FramingLine      = "line"      // one record per line
	FramingMultiline = "multiline" // a lead line + continuation lines (see Continuation)
	FramingJSON      = "json"      // JSONL / newline-delimited JSON
	FramingSection   = "section"   // one record per banner/section block (see Section)
	FramingWhole     = "whole"     // one record for the whole file (office/PDF baseline)
	FramingOpaque    = "opaque"    // intentionally UNframed: one {kind:"opaque",note} record
)

// Framing describes how to cut a file into records.
type Framing struct {
	Kind string `json:"kind"` // one of the Framing* constants

	// Continuation (Kind==multiline): a regexp matching a line that CONTINUES the
	// previous record rather than starting a new one (e.g. `^\s|^\[`).
	Continuation string `json:"continuation,omitempty"`

	// Section (Kind==section): a regexp matching the start of a new section block
	// (e.g. `^={10,}$` for cbcollect banners). The command/title within a section
	// is lifted into per-record provenance by the recipe.
	Section string `json:"section,omitempty"`

	// Banner (line/multiline framing): a regexp matching a NON-data separator line
	// (e.g. cbbrowse_logs' `==== couchbase logs ====` header that precedes a framed
	// log). A framed record whose lead line matches is DROPPED, not emitted -- so it
	// doesn't inflate COUNT(*) or skew .schema with its banner-only {text} shape.
	Banner string `json:"banner,omitempty"`

	// Note (Kind==opaque): a human description of WHY a file is unframable (e.g.
	// "binary Go CPU profile (pprof protobuf)"). It rides the single opaque record so
	// the file is self-documenting rather than a mystery "unframed" entry.
	Note string `json:"note,omitempty"`
}

// Fields describes how to lift typed columns out of each framed record. Native,
// byte-oriented (DESIGN-exprs.md regex work) so it stays off the boxed lane.
type Fields struct {
	// Pattern is a single regexp with named capture groups; each named group
	// (?P<name>...) becomes a field on the emitted record (e.g. ts/level/node/msg).
	Pattern string `json:"pattern,omitempty"`

	// Grok is an optional grok-style pattern as an alternative to a raw regexp.
	Grok string `json:"grok,omitempty"`
}

// Time-layout tags for TimeSpec.Layout. A recipe maps its source's timestamp
// representation to one of these; the extract layer normalizes each record's
// timestamp into a single sortable int64 epoch-NANOS key (DESIGN-data.md
// "The normalized sort key"), timezone-normalized so streams from different
// files/nodes are directly comparable.
const (
	TimeLayoutRFC3339 = "RFC3339"  // e.g. 2026-05-17T15:36:11.198+02:00
	TimeLayoutEpochS  = "epoch_s"  // seconds since epoch (may be fractional)
	TimeLayoutEpochMs = "epoch_ms" // milliseconds since epoch
	TimeLayoutEpochUs = "epoch_us" // microseconds since epoch
	TimeLayoutEpochNs = "epoch_ns" // nanoseconds since epoch
	// Any other value is treated as a Go reference-time layout ("2006-01-02...")
	// or strftime spec, per the recipe.
)

// TimeSpec is the sort-key contract: which field carries the timestamp, how to
// parse it, and the default timezone when the value omits one. The single field
// the merge join requires (DESIGN-merging.md).
type TimeSpec struct {
	Field     string `json:"field"`                // record field holding the timestamp
	Layout    string `json:"layout"`               // a TimeLayout* tag or a Go/strftime layout
	TZDefault string `json:"tz_default,omitempty"` // e.g. "+02:00" or "UTC" when the value has none
}

// Sortedness classification of a source by its sort key (DESIGN-data.md
// "Sortedness, classified").
const (
	SortedStrict = "strict" // every record's key >= its predecessor
	SortedNear   = "near"   // mostly sorted, bounded disorder (see DisorderBound)
	SortedNone   = "none"   // unsorted; must be spill-sorted before merging
)

// DisorderBound bounds how out-of-order a "near"-sorted source can be -- the
// load-bearing number for a watermarked merge (DESIGN-data.md "The disorder_bound";
// DESIGN-merging.md §1c). Exactly one of WindowNanos / Span is expected to be set.
type DisorderBound struct {
	// WindowNanos: a record's key is never more than this many nanoseconds behind
	// an already-seen key (bounded lateness -- the Flink/Dataflow watermark model).
	WindowNanos int64 `json:"window_nanos,omitempty"`

	// Span: a record is never more than this many positions from its sorted place.
	Span int `json:"span,omitempty"`
}

// OrderSpec is the sortedness half of the sort-key contract, as declared by a
// describe() recipe.
type OrderSpec struct {
	By       string        `json:"by,omitempty"`       // the sort field (usually TimeSpec.Field)
	Sorted   string        `json:"sorted"`             // one of Sorted* constants
	Disorder DisorderBound `json:"disorder,omitempty"` // when Sorted==near
}

// ExtractSpec is what describe(file) returns: the declarative recipe n1k1 applies
// natively to produce records, keeping per-row work off the JS/boxed lane
// (DESIGN-data.md §4 "Declarative spec vs imperative extract"). It is memoized in
// the .n1k1 sidecar and handed back to extract(file, meta, emit).
type ExtractSpec struct {
	Format  string    `json:"format,omitempty"` // recipe-chosen format tag, e.g. "ns_server_log"
	Framing Framing   `json:"framing"`
	Fields  Fields    `json:"fields,omitempty"`
	Time    *TimeSpec `json:"time,omitempty"`  // nil for non-temporal sources
	Order   OrderSpec `json:"order,omitempty"` // nil/none for unsorted sources

	// Provenance: constants lifted from the file once (e.g. a banner command, a
	// node id parsed from content) that ride every record's _meta. Generic /
	// domain-agnostic -- keys are recipe-defined, n1k1 core ascribes no meaning.
	Provenance map[string]string `json:"provenance,omitempty"`
}

// SyncPoint maps a normalized sort key to a byte offset within the (logical /
// decompressed) source stream. Periodic sync points (every N records) let a merge
// cursor SEEK to a start time instead of scanning from the top, and double as the
// seekable doc-ID index (DESIGN-data.md §6). Key is int64 epoch-nanos.
type SyncPoint struct {
	Key    int64 `json:"key"`
	Offset int64 `json:"offset"`
}

// SortedSourceMeta is the measured, per-file metadata the K-way merge consumes
// (DESIGN-data.md §5 "Extract/sorted-source fields"; DESIGN-merging.md). Produced
// by describe() (declared or sampled) and memoized in .n1k1/. glue reads it to set
// the merge op's scalar Params; the engine op never imports this struct.
type SortedSourceMeta struct {
	// SortKeyLabel: the record field/label the file is ordered by, normalized to an
	// int64 epoch-nanos key by the extract layer.
	SortKeyLabel string `json:"sort_key_label,omitempty"`

	Sortedness string        `json:"sortedness"`         // one of Sorted* constants
	Disorder   DisorderBound `json:"disorder,omitempty"` // when Sortedness==near

	// Time zone map on the sort key (int64 epoch-nanos): powers merge concatenation
	// (disjoint ranges -> no heap) AND `WHERE ts BETWEEN` pruning. Valid only when
	// RecordCount > 0.
	MinKey int64 `json:"min_key,omitempty"`
	MaxKey int64 `json:"max_key,omitempty"`

	RecordCount int64 `json:"record_count,omitempty"`

	// SyncPoints: periodic key->offset checkpoints (ascending by Key), for
	// seek-by-time and seekable fetch. Optional.
	SyncPoints []SyncPoint `json:"sync_points,omitempty"`
}
