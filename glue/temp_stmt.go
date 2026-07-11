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

// CREATE / DROP TEMP KEYSPACE statement handling (IDEA-0027). The couchbase/query
// fork grammar has no CTAS / SELECT-INTO form, so these statements are recognized at
// the TEXT level BEFORE the fork parser (which would reject them) -- the same
// "intercept before planning" spirit as PREPARE/EXECUTE/INSERT, one rung earlier.
// The recognizer is deliberately strict (a small hand tokenizer, not a loose regexp)
// so an ordinary statement is never mis-claimed; anything that doesn't match falls
// through to the normal parse path untouched.
//
// Syntax:
//
//	CREATE [OR REPLACE] TEMP[ORARY] KEYSPACE <name> AS <select>
//	DROP TEMP[ORARY] KEYSPACE [IF EXISTS] <name>
//
// <name> is a bare identifier or a `backtick`-quoted one.

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbase/query/algebra"

	"github.com/couchbase/n1k1/records"
)

// tempKeyspaceStmt is a recognized CREATE/DROP TEMP KEYSPACE statement.
type tempKeyspaceStmt struct {
	kind      string // "create" | "drop"
	name      string
	inner     string // create: the SELECT after AS
	ifExists  bool   // drop
	orReplace bool   // create
}

// parseTempKeyspaceStmt recognizes a CREATE/DROP TEMP KEYSPACE statement in stmt,
// returning its parts. ok is false (with the statement left for the normal parser)
// for anything else.
func parseTempKeyspaceStmt(stmt string) (*tempKeyspaceStmt, bool) {
	s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(stmt), ";"))

	if rest, ok := stripWordCI(s, "CREATE"); ok {
		rest = ltrimSpace(rest)
		orReplace := false
		if r2, ok := stripWordCI(rest, "OR"); ok {
			r3, ok := stripWordCI(ltrimSpace(r2), "REPLACE")
			if !ok {
				return nil, false
			}
			rest, orReplace = ltrimSpace(r3), true
		}
		rest, ok := stripTempKeyword(rest)
		if !ok {
			return nil, false
		}
		rest, ok = stripWordCI(ltrimSpace(rest), "KEYSPACE")
		if !ok {
			return nil, false
		}
		name, rest, ok := readName(ltrimSpace(rest))
		if !ok {
			return nil, false
		}
		rest, ok = stripWordCI(ltrimSpace(rest), "AS")
		if !ok {
			return nil, false
		}
		inner := strings.TrimSpace(rest)
		if inner == "" {
			return nil, false
		}
		return &tempKeyspaceStmt{kind: "create", name: name, inner: inner, orReplace: orReplace}, true
	}

	if rest, ok := stripWordCI(s, "DROP"); ok {
		rest, ok := stripTempKeyword(ltrimSpace(rest))
		if !ok {
			return nil, false
		}
		rest, ok = stripWordCI(ltrimSpace(rest), "KEYSPACE")
		if !ok {
			return nil, false
		}
		rest = ltrimSpace(rest)
		ifExists := false
		if r2, ok := stripWordCI(rest, "IF"); ok {
			r3, ok := stripWordCI(ltrimSpace(r2), "EXISTS")
			if !ok {
				return nil, false
			}
			rest, ifExists = ltrimSpace(r3), true
		}
		name, rest, ok := readName(rest)
		if !ok || strings.TrimSpace(rest) != "" {
			return nil, false
		}
		return &tempKeyspaceStmt{kind: "drop", name: name, ifExists: ifExists}, true
	}

	return nil, false
}

// stripWordCI removes a leading keyword (case-insensitive) that is followed by a
// word boundary (whitespace or end-of-string), returning the remainder (leading
// whitespace intact) and whether it matched. It does NOT match a longer identifier
// that merely starts with the word (e.g. "CREATED").
func stripWordCI(s, word string) (string, bool) {
	if len(s) < len(word) || !strings.EqualFold(s[:len(word)], word) {
		return s, false
	}
	rest := s[len(word):]
	if rest == "" || isSpaceByte(rest[0]) {
		return rest, true
	}
	return s, false
}

// stripTempKeyword strips a leading TEMP or TEMPORARY keyword.
func stripTempKeyword(s string) (string, bool) {
	if r, ok := stripWordCI(s, "TEMPORARY"); ok {
		return r, true
	}
	return stripWordCI(s, "TEMP")
}

// readName reads a keyspace name -- a `backtick`-quoted identifier (which may hold
// dots/slashes) or a bare token up to the next whitespace -- returning the name, the
// remainder, and whether one was found.
func readName(s string) (name, rest string, ok bool) {
	if s == "" {
		return "", s, false
	}
	if s[0] == '`' {
		if end := strings.IndexByte(s[1:], '`'); end >= 0 {
			return s[1 : 1+end], s[1+end+1:], true
		}
		return "", s, false
	}
	i := 0
	for i < len(s) && !isSpaceByte(s[i]) {
		i++
	}
	if i == 0 {
		return "", s, false
	}
	return s[:i], s[i:], true
}

func isSpaceByte(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }
func ltrimSpace(s string) string {
	return strings.TrimLeft(s, " \t\n\r")
}

// TempKeyspaceRun dispatches a recognized CREATE/DROP TEMP KEYSPACE statement.
func (s *Session) TempKeyspaceRun(t *tempKeyspaceStmt) (*Result, error) {
	if s.Store == nil || s.Store.Temp == nil {
		return nil, fmt.Errorf("TEMP KEYSPACE: no datastore")
	}
	switch t.kind {
	case "create":
		return s.createTempKeyspace(t)
	case "drop":
		return s.dropTempKeyspace(t)
	}
	return nil, fmt.Errorf("TEMP KEYSPACE: unknown op %q", t.kind)
}

// createTempKeyspace runs the inner SELECT, captures its result rows in memory, and
// registers them as the temp keyspace t.name (session-scoped). It mirrors InsertRun's
// row-capture (reroute s.OnRow to a collector, run the query, restore) -- but keeps
// the rows in memory instead of writing a jsonl file.
func (s *Session) createTempKeyspace(t *tempKeyspaceStmt) (*Result, error) {
	if _, exists := s.Store.Temp.get(t.name); exists && !t.orReplace {
		return nil, fmt.Errorf("CREATE TEMP KEYSPACE %q: already exists "+
			"(use CREATE OR REPLACE TEMP KEYSPACE, or DROP TEMP KEYSPACE first)", t.name)
	}

	parsed, err := ParseStatement(t.inner, s.Namespace, true)
	if err != nil {
		return nil, fmt.Errorf("CREATE TEMP KEYSPACE %q: %w", t.name, err)
	}
	if _, ok := parsed.(*algebra.Select); !ok {
		return nil, fmt.Errorf("CREATE TEMP KEYSPACE %q AS: expected a SELECT, got %s",
			t.name, parsed.Type())
	}

	var recs []records.Record
	var idBuf []byte
	origOnRow := s.OnRow
	s.OnRow = func(row []byte) {
		// Copy: OnRow's buffer is reused for the next row; the temp keyspace holds
		// these bytes for the session's lifetime.
		idBuf = strconv.AppendInt(append(idBuf[:0], t.name...), int64(len(recs)), 10)
		recs = append(recs, records.Record{
			ID:  append([]byte(nil), idBuf...),
			Doc: append([]byte(nil), row...),
		})
	}
	_, runErr := s.StatementRun(parsed, s.NamedArgs, s.PositionalArgs)
	s.OnRow = origOnRow
	if runErr != nil {
		return nil, fmt.Errorf("CREATE TEMP KEYSPACE %q source query failed: %w", t.name, runErr)
	}

	if err := s.Store.Temp.Put(t.name, recs); err != nil {
		return nil, err
	}
	return s.tempSummary(map[string]interface{}{"created": t.name, "rows": len(recs)})
}

func (s *Session) dropTempKeyspace(t *tempKeyspaceStmt) (*Result, error) {
	if !s.Store.Temp.Drop(t.name) {
		if t.ifExists {
			return s.tempSummary(map[string]interface{}{"dropped": t.name, "existed": false})
		}
		return nil, fmt.Errorf("DROP TEMP KEYSPACE %q: no such temp keyspace "+
			"(use DROP TEMP KEYSPACE IF EXISTS to ignore)", t.name)
	}
	return s.tempSummary(map[string]interface{}{"dropped": t.name, "existed": true})
}

// tempSummary returns a one-row mutation summary the CLI renders (or streams), the
// same shape convention as InsertRun's summary.
func (s *Session) tempSummary(m map[string]interface{}) (*Result, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if s.OnRow != nil {
		s.OnRow(b)
		return &Result{Count: 1}, nil
	}
	return &Result{Rows: []json.RawMessage{b}, Count: 1}, nil
}
