#!/usr/bin/env python3
#  Copyright (c) 2026 Couchbase, Inc.  Apache License, Version 2.0.
"""
import_gsi_data_cases.py -- import DATA-BACKED SQL++ cases from the couchbase/query
fork's test/gsi/test_cases into an isolated suite root (test/suite/json-gsi), so
they don't collide with the default corpus's `orders` keyspace.

For each selected category it (a) converts insert.json's
`INSERT INTO <ks> (KEY,VALUE) VALUES("<key>", <json-object>)` statements into
file-datastore docs at json-gsi/default/<ks>/<key>.json (docs from all categories
merge into shared keyspaces -- keys are test_id-suffixed so they don't collide,
and each case's WHERE test_id="..." scopes it), and (b) copies the category's
{statements,results|error} cases to json-gsi/default/cases/case_gsi_<cat>.json,
dropping only wall-clock/random/id cases (non-reproducible).

Usage (from repo root):
  python3 test/suite/import_gsi_data_cases.py "$(go list -m -f '{{.Dir}}' github.com/couchbase/query)"
"""
import json, os, re, sys, glob

CATEGORIES = [
    "string_functions", "number_functions", "array_functions", "obj_functions",
    "json_functions", "comp_functions", "conditional_unkn_functions",
    "case_functions", "typeconv_functions", "select_functions", "where_functions",
    "alias_functions", "any_functions", "from_functions", "order_functions",
    "key_functions", "meta_functions",
]
# Keyspaces the fork loads with ~10,000 docs (~100 per INSERT statement). Too
# large for a one-file-per-doc corpus + no-index primary scans, so only a light
# per-statement sample is imported (see main). Moderate keyspaces import fully.
MEGA_KEYSPACES = {"purchase", "review"}
NONDET = re.compile(r"\b(now_\w+|clock_\w+|random|rand|uuid|newid)\s*\(", re.IGNORECASE)
INSERT_PREFIX = re.compile(r'\s*INSERT\s+INTO\s+(\w+)\s*\(\s*KEY\s*,\s*VALUE\s*\)', re.IGNORECASE)

# n1k1 merges every category's docs into one shared keyspace and relies on each
# case's `WHERE test_id="<cat>"` predicate to re-create the per-category bucket
# isolation the fork gets by reloading buckets between categories. A tiny number
# of source cases query a shared keyspace WITHOUT any test_id predicate: against
# the fork's isolated bucket they match a single doc, but against our merged
# corpus they match that same logical doc once per category (identical rows), so
# n1k1 emits duplicates the fork never sees. We append the missing scope so the
# case still exercises the same function against the same expected output. Each
# patch is (category, lowercase-substring-to-match, clause-to-append, note); the
# note lands in the case's `description` (a runner-allowed field) to document the
# deliberate divergence from the source. Only applied when the matched statement
# has no test_id predicate already.
SCOPE_PATCHES = [
    ("obj_functions",
     "object_pairs_nested(customer",
     ' AND test_id="obj_func"',
     'n1k1: appended `AND test_id="obj_func"` to the fork\'s unscoped source '
     'statement. The fork runs this against a customer bucket holding only '
     'obj_functions docs; n1k1\'s merged customer keyspace holds one copy of '
     'this doc per category, so without the scope OBJECT_PAIRS_NESTED returns '
     'that row once per category. The `e$` pattern excludes the differing '
     'test_id field, so the expected result is unchanged.'),
]

def apply_scope_patch(cat, c):
    """Return c, possibly with a shared-keyspace scope predicate appended to its
    statement (see SCOPE_PATCHES). Copies c before mutating so the source stays
    untouched."""
    stmt = c.get("statements", "")
    low = stmt.lower()
    if "test_id" in low:
        return c
    for pcat, needle, clause, note in SCOPE_PATCHES:
        if cat == pcat and needle in low:
            c = dict(c)
            c["statements"] = stmt + clause
            c["description"] = note
            return c
    return c
QUOTED = re.compile(r'"((?:[^"\\]|\\.)*)"')

def brace_match(s, start):
    # s[start] == '{'; return index just past the matching '}', honoring strings/escapes.
    depth, i, in_str, esc = 0, start, False, False
    while i < len(s):
        c = s[i]
        if in_str:
            if esc: esc = False
            elif c == '\\': esc = True
            elif c == '"': in_str = False
        else:
            if c == '"': in_str = True
            elif c == '{': depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0: return i + 1
        i += 1
    raise ValueError("unbalanced braces")


def parse_inserts(stmt):
    """Yield (keyspace, index-in-statement, key, value-object-text) for every
    VALUES tuple. The fork packs many docs per statement
    (VALUES("k1",{..}), VALUES("k2",{..}), ...); the value objects are the only
    top-level {...} spans, and each key is the quoted string just before its
    value object."""
    m = INSERT_PREFIX.match(stmt)
    if not m:
        return
    ks, i, idx = m.group(1), m.end(), 0
    while True:
        b = stmt.find("{", i)
        if b < 0:
            return
        keys = QUOTED.findall(stmt[i:b])
        e = brace_match(stmt, b)
        if keys:
            yield ks, idx, keys[-1], stmt[b:e]
            idx += 1
        i = e


def referenced_keys(cdir):
    """All quoted string literals appearing in a category's case statements. A
    doc whose KEY matches one is referenced directly (e.g. USE KEYS "k"), so it
    must be imported even though the keyspace is otherwise sparsely sampled."""
    refs = set()
    for cf in sorted(glob.glob(os.path.join(cdir, "case*.json"))):
        try:
            cases = json.load(open(cf))
        except Exception:
            continue
        for c in cases if isinstance(cases, list) else []:
            s = c.get("statements") if isinstance(c, dict) else None
            if isinstance(s, str):
                refs.update(QUOTED.findall(s))
    return refs


def main(qf):
    tc = os.path.join(qf, "test/gsi/test_cases")
    root = os.path.join(os.path.dirname(__file__), "json-gsi/default")
    ncase = ndoc = 0
    for cat in CATEGORIES:
        cdir = os.path.join(tc, cat)
        # (a) data. Moderate keyspaces are imported fully; the MEGA keyspaces
        # (purchase/review -- the fork packs ~100 docs/statement, i.e. 10,000-doc
        # keyspaces) are impractical for a file-per-doc corpus (repo bloat + slow
        # no-index primary scans), so we keep only a light sample of them: the
        # first doc of each INSERT statement. Either way, always import a doc whose
        # KEY is referenced directly by a case (e.g. USE KEYS "k"), which needs
        # that exact doc. (Cases that aggregate/ORDER BY LIMIT over a full mega
        # keyspace stay in gsiExpectedNonPass -- they'd need all 10k docs.)
        ins = os.path.join(cdir, "insert.json")
        if os.path.exists(ins):
            refs = referenced_keys(cdir)
            for c in json.load(open(ins)):
                stmt = c.get("statements", "")
                for ks, idx, key, obj in parse_inserts(stmt):
                    if ks in MEGA_KEYSPACES and idx != 0 and key not in refs:
                        continue
                    val = json.loads(obj)  # validate + normalize
                    ksdir = os.path.join(root, ks)
                    os.makedirs(ksdir, exist_ok=True)
                    json.dump(val, open(os.path.join(ksdir, key + ".json"), "w"))
                    ndoc += 1
        # (b) cases
        picked = []
        for cf in sorted(glob.glob(os.path.join(cdir, "case*.json"))):
            try: cases = json.load(open(cf))
            except Exception: continue
            if not isinstance(cases, list): continue
            for c in cases:
                if not isinstance(c, dict): continue
                stmt = c.get("statements")
                if not isinstance(stmt, str) or NONDET.search(stmt): continue
                picked.append(apply_scope_patch(cat, c))
        if picked:
            os.makedirs(os.path.join(root, "cases"), exist_ok=True)
            json.dump(picked, open(os.path.join(root, "cases", f"case_gsi_{cat}.json"), "w"), indent=2)
            ncase += len(picked)
    print(f"imported {ndoc} docs, {ncase} cases across {len(CATEGORIES)} categories")

if __name__ == "__main__":
    main(sys.argv[1])
