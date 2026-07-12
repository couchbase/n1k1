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

import "strings"

// IsReserved reports whether word is a SQL++ reserved keyword that can't be used as
// a bare identifier -- a field, alias, or keyspace name -- and so must be backticked
// (e.g. `level`). It is the proactive companion to the reactive reserved-word hint
// (cmd/n1k1 reservedWordHint): an author or an agent building detectors can check a
// name up front instead of hitting the parse error.
//
// The answer comes from cbq's OWN parser -- it parses `SELECT 1 AS <word>` and looks
// for the "(reserved word)" error the lexer raises when a keyword lands where an
// identifier is expected -- so it always tracks the grammar (never a hardcoded list).
// Probing the identifier (alias) position is what matters for the common authoring
// mistakes (field/alias/temp-keyspace names); a word that IS a keyword token yet is
// still accepted as an identifier there (e.g. TYPE) is correctly reported usable.
//
// word must be a simple identifier ([A-Za-z_][A-Za-z0-9_]*); anything else (empty,
// already-quoted, dotted, or with odd chars) returns false rather than build a bogus
// probe statement. Case-insensitive: cbq upper-cases before the keyword check.
func IsReserved(word string) bool {
	if !isSimpleIdent(word) {
		return false
	}
	_, err := ParseStatement("SELECT 1 AS "+word, "default", true)
	return err != nil && strings.Contains(err.Error(), "(reserved word)")
}

// isSimpleIdent reports whether s is a bare unquoted identifier: [A-Za-z_] then
// [A-Za-z0-9_]*. Guards IsReserved's probe against injection / malformed input.
func isSimpleIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
