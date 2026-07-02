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
	"fmt"
	"io"

	"github.com/couchbase/n1k1/glue"
)

// indexProgress renders the progress of a concurrent eager index build
// (glue.EagerBuildSecondaryIndexes). Events arrive serialized on one goroutine,
// so no locking is needed. On an interactive TTY it draws a live, in-place
// multi-line display (one bar per index, updating as they build in parallel); on
// a pipe/redirect it prints one plain line per index as it finishes. All output
// goes to stderr so query results on stdout stay clean.
type indexProgress struct {
	w     io.Writer
	fancy bool
	order []string           // index keys, first-seen order (stable row layout)
	st    map[string]*ixProg // per-index state
	drawn int                // rows drawn on the last fancy render
}

type ixProg struct {
	label   string
	docs    int
	total   int
	entries int
	size    int64
	done    bool
	failed  bool
	errMsg  string
}

func newIndexProgress(w io.Writer, fancy bool) *indexProgress {
	return &indexProgress{w: w, fancy: fancy, st: map[string]*ixProg{}}
}

func evKey(ev glue.IndexBuildEvent) string {
	return ev.Namespace + ":" + ev.Keyspace + "." + ev.Name
}

// handle consumes one build event.
func (p *indexProgress) handle(ev glue.IndexBuildEvent) {
	key := evKey(ev)
	s := p.st[key]
	if s == nil {
		s = &ixProg{label: key, total: ev.Total}
		p.st[key] = s
		p.order = append(p.order, key)
	}
	switch ev.Phase {
	case "start":
		s.total = ev.Total
	case "progress":
		s.docs = ev.Docs
	case "done":
		s.done, s.docs, s.entries, s.size = true, ev.Docs, ev.Entries, ev.SizeBytes
	case "error":
		s.failed, s.done = true, true
		if ev.Err != nil {
			s.errMsg = ev.Err.Error()
		}
	}

	if p.fancy {
		p.render()
	} else if ev.Phase == "done" || ev.Phase == "error" {
		fmt.Fprintln(p.w, p.line(s))
	}
}

// finish leaves the cursor below the final render (fancy only).
func (p *indexProgress) finish() {
	if p.fancy && p.drawn > 0 {
		fmt.Fprint(p.w, "\n")
	}
}

// render redraws all rows in place: move the cursor up over the previously drawn
// rows, then rewrite each (clearing to end-of-line).
func (p *indexProgress) render() {
	if p.drawn > 0 {
		fmt.Fprintf(p.w, "\033[%dA", p.drawn) // cursor up p.drawn lines
	}
	for _, key := range p.order {
		fmt.Fprintf(p.w, "\r\033[K%s\n", p.line(p.st[key])) // clear line + content
	}
	p.drawn = len(p.order)
}

// line formats one index's status row.
func (p *indexProgress) line(s *ixProg) string {
	switch {
	case s.failed:
		return fmt.Sprintf("  ✗ %s  %s", s.label, s.errMsg)
	case s.done:
		return fmt.Sprintf("  ✓ %s  %d entries, %s", s.label, s.entries, humanBytes(s.size))
	default:
		return fmt.Sprintf("  %s %s  %s", spinnerBar(s.docs, s.total), s.label, countText(s))
	}
}

func countText(s *ixProg) string {
	if s.total > 0 {
		return fmt.Sprintf("%d/%d docs", s.docs, s.total)
	}
	return fmt.Sprintf("%d docs", s.docs)
}

// spinnerBar renders a fixed-width bar; a known total gives a real percentage,
// an unknown total (0) gives a moving marker so the row still shows life.
func spinnerBar(docs, total int) string {
	const width = 16
	if total > 0 {
		filled := docs * width / total
		if filled > width {
			filled = width
		}
		bar := make([]byte, width)
		for i := range bar {
			if i < filled {
				bar[i] = '#'
			} else {
				bar[i] = '-'
			}
		}
		return "[" + string(bar) + "]"
	}
	// Indeterminate: a single marker that walks across the bar as docs grow.
	pos := (docs / 512) % width
	bar := make([]byte, width)
	for i := range bar {
		if i == pos {
			bar[i] = '#'
		} else {
			bar[i] = '-'
		}
	}
	return "[" + string(bar) + "]"
}
