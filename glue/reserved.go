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
	"sort"
	"strings"
	"sync"
)

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

// reservedCandidates is a snapshot of cbq's `yyToknames` token names (the array its
// own reserved-word check scans), lower-cased. It is the CANDIDATE set only: it also
// carries lexical-class tokens (ident/int/num/str/…) and punctuation names (lparen/eq/
// dot/…) that are NOT reserved identifiers -- ReservedWords probe-filters those out, so
// the candidate set may safely over-include. Regenerate when tracking a newer query:
//
//	QDIR=$(go list -m -f '{{.Dir}}' github.com/couchbase/query)
//	awk '/var yyToknames = /{f=1} f&&/^}/{f=0} f' "$QDIR/parser/n1ql/y.go" |
//	  grep -oE '"[A-Z][A-Z0-9_]*"' | tr -d '"' | grep -vE '^_' | tr A-Z a-z | sort -u
const reservedCandidates = `
advise all alter analyze and any array as asc at begin between binary boolean break bucket
build by cache call case cast catalog cluster collate collection colon comma commit
committed concat connect consume continue create credentialstore current cycle database
dataset datastore declare decrement default delete dense deq derived desc describe distinct
div do dot drop each element else end eq escape every except exclude execute exists explain
external false fetch filter first flatten flatten_keys flush following for force from fts
function ge golang grant group groups gsi gt hash having ident ident_icase if ignore ilike
in include increment index infer inline inner insert int intersect into is isolation
javascript join key keys keyspace known language last lateral lbrace lbracket le left let
letting level like limit lparen lsm lt map mapping matched materialized maxvalue merge
minus minvalue missing mod multi named_param namespace namespace_id ne nest next nextval
next_param nl no not not_a_token nth_value null nulls num number object offset on
optim_hints option options or order others outer over parse partition password path plus
pool positional_param pow preceding prepare prev prevval primary private privilege probe
procedure public random_element range raw rbrace rbracket rbracket_icase read realm
recursive reduce rename replace respect restart restrict return returning revoke right role
roles rollback row rows rparen satisfies save savepoint schema scope select self semi
sequence set show snapshot some source sparse star start statistics str string system then
ties timestamp to tran transaction trigger true truncate type uminus unbounded under union
unique unknown unnest unset update upsert use user users using validate value valued values
vector via view when where while window with within work xor`

var (
	reservedOnce sync.Once
	reservedList []string
)

// ReservedWords returns the sorted SQL++ reserved keywords -- the words that cannot be
// used as a bare identifier (a field, alias, or keyspace name) without backticks. It
// probe-filters reservedCandidates through IsReserved, so the result reflects cbq's OWN
// parser: lexical-class token names (str/int/ident) and keyword tokens still accepted as
// identifiers (e.g. type) are excluded, and the list re-syncs to the grammar just by
// re-probing (never a hand-maintained list). Computed once and cached (~11ms over ~270
// candidates), so `.help reserved-words` can print the whole list cheaply.
func ReservedWords() []string {
	reservedOnce.Do(func() {
		for _, w := range strings.Fields(reservedCandidates) {
			if IsReserved(w) {
				reservedList = append(reservedList, w)
			}
		}
		sort.Strings(reservedList)
	})
	return reservedList
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
