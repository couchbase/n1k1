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
import json, os, re, sys, glob, hashlib

CATEGORIES = [
    "string_functions", "number_functions", "array_functions", "obj_functions",
    "json_functions", "comp_functions", "conditional_unkn_functions",
    "case_functions", "typeconv_functions", "select_functions", "where_functions",
    "alias_functions", "any_functions", "from_functions", "order_functions",
    "key_functions", "meta_functions",
    "arith_functions", "bitwise_functions", "nav_functions", "integers",
    "date_functions", "aggregate_functions",
    "subqexp",
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
# the fork's isolated bucket they see only that category's docs, but against our
# merged corpus they see every category's (duplicate/extra rows n1k1 emits that
# the fork never sees). We inject the missing scope so the case still exercises
# the same function against the same expected output. Each patch is (category,
# lowercase-substring-to-match, test_id, note); the note lands in the case's
# `description` (a runner-allowed field) to document the deliberate divergence
# from the source. Only applied when the matched statement has no test_id yet.
SCOPE_PATCHES = [
    ("obj_functions", "object_pairs_nested(customer", "obj_func",
     'n1k1: scoped the fork\'s unscoped source statement with test_id="obj_func". '
     'The fork runs it against a customer bucket holding only obj_functions docs; '
     "n1k1's merged customer keyspace holds one copy of this doc per category, so "
     "unscoped OBJECT_PAIRS_NESTED returns that row once per category. The e$ "
     "pattern excludes the differing test_id field, so expected output is unchanged."),
    ("json_functions", "json_encode(ccinfo)", "json_func",
     'n1k1: scoped with test_id="json_func" (see obj_functions patch). Unscoped, '
     "the ORDER BY over the merged customer keyspace ranks other categories' rows "
     "(incl. docs with no ccInfo -> JSON_ENCODE null) ahead of the json_func ones."),
    ("json_functions", "encode_json(ccinfo)", "json_func",
     'n1k1: scoped with test_id="json_func" (see obj_functions patch). Unscoped, '
     "the ORDER BY over the merged customer keyspace ranks other categories' rows "
     "(incl. docs with no ccInfo -> ENCODE_JSON null) ahead of the json_func ones."),
    ("json_functions", "encoded_size(ccinfo)", "json_func",
     'n1k1: scoped with test_id="json_func" (see obj_functions patch). Unscoped, '
     "ENCODED_SIZE's ORDER BY over the merged customer keyspace ranks other "
     "categories' rows ahead of the json_func ones."),
    ("json_functions", "poly_length(ccinfo)", "json_func",
     'n1k1: scoped with test_id="json_func" (see obj_functions patch). Unscoped, '
     "POLY_LENGTH's ORDER BY over the merged customer keyspace ranks other "
     "categories' rows ahead of the json_func ones."),
]

def inject_test_id_scope(stmt, tid):
    """Add a `test_id="<tid>"` predicate to stmt: fold it into an existing WHERE
    with AND, else insert a WHERE clause. Placed before any trailing ORDER BY /
    GROUP BY / HAVING / LIMIT so it stays a valid predicate."""
    m = re.search(r'\b(order\s+by|group\s+by|having|limit)\b', stmt, re.IGNORECASE)
    cut = m.start() if m else len(stmt)
    head, tail = stmt[:cut].rstrip(), stmt[cut:]
    conj = "AND" if re.search(r'\bwhere\b', head, re.IGNORECASE) else "WHERE"
    scoped = f'{head} {conj} test_id="{tid}"'
    return (scoped + " " + tail).rstrip() if tail else scoped

def apply_scope_patch(cat, c):
    """Return c, possibly with a shared-keyspace scope predicate injected into its
    statement (see SCOPE_PATCHES). Copies c before mutating so the source stays
    untouched."""
    stmt = c.get("statements", "")
    low = stmt.lower()
    if "test_id" in low:
        return c
    for pcat, needle, tid, note in SCOPE_PATCHES:
        if cat == pcat and needle in low:
            c = dict(c)
            c["statements"] = inject_test_id_scope(stmt, tid)
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
    value object. When the KEY is a non-literal expression rather than a quoted
    string -- e.g. VALUES(UUID(), {..}) -- there's no literal to use as the file
    name, so synthesize a deterministic key from a hash of the value object (the
    doc's identity for our corpus is its content/test_id, not its random key)."""
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
        obj = stmt[b:e]
        key = keys[-1] if keys else "gen_" + hashlib.md5(obj.encode()).hexdigest()[:16]
        yield ks, idx, key, obj
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


# An id-like scalar: a keyspace-ish name prefix followed by digits, e.g.
# "purchase10", "customer128", "product85". Used to force-import the specific
# mega-keyspace docs a case's EXPECTED RESULTS reference (see below); the tight
# shape avoids matching ratings/counts/free-text that happen to appear in a row.
IDLIKE = re.compile(r"^[a-z]+[0-9]+$")

def _collect_idlike(v, out):
    """Add id-like scalar strings DIRECTLY present in v (top level only): v itself
    if scalar, or the immediate scalar values of a dict / elements of a list.
    Deliberately non-recursive -- descending into nested arrays (e.g. a purchase's
    lineItems) would collect unrelated product ids and over-match docs (a doc
    sharing any nested product would be pulled in). Result rows and the docs we
    match against both carry their identifying ids at the top level, so top-level
    matching is both sufficient and precise."""
    if isinstance(v, str):
        if IDLIKE.match(v):
            out.add(v)
        return
    items = v.values() if isinstance(v, dict) else v if isinstance(v, list) else []
    for x in items:
        if isinstance(x, str) and IDLIKE.match(x):
            out.add(x)

# Matches a statement whose tail is `FROM <ks> WHERE test_id="<tid>" ORDER BY
# <cols> LIMIT <n>` with NO other predicate (test_id is immediately followed by
# ORDER BY). That shape means every doc of that (keyspace, test_id) qualifies,
# so the result is simply the first n after sorting -- see order_limit_boundary.
ORDER_LIMIT = re.compile(
    r'\bfrom\s+(\w+)\s+where\s+test_id\s*=\s*["\'](\w+)["\']\s+'
    r'order\s+by\s+([\w\s,]+?)\s+limit\s+(\d+)\s*;?\s*$',
    re.IGNORECASE)

def _n1ql_sort_key(v):
    # Coarse N1QL collation bucket so mixed-type ORDER BY sorts deterministically
    # the same way the engine does: missing < null < false < true < number <
    # string < array/object. Within a bucket, sort by the natural value.
    if v is None:
        return (1, 0)
    if isinstance(v, bool):
        return (2, int(v))
    if isinstance(v, (int, float)):
        return (3, v)
    if isinstance(v, str):
        return (4, v)
    return (5, json.dumps(v, sort_keys=True))

def order_limit_boundary(cdir, docs_by_ks):
    """Keys of the docs a `... test_id=X ORDER BY cols LIMIT n` case needs from a
    mega keyspace: the first n docs of that (keyspace, test_id) in ORDER BY order.
    Only the fixed shape ORDER_LIMIT matches is handled (predicate is test_id
    alone, so all docs qualify and the projection is irrelevant to WHICH rows
    win). Sound for the same reason as result_ref_values: these n are the global
    minima, so any sampled doc sorts at or after them. Skips a case if the sort
    ties across the LIMIT boundary (docs[n-1] == docs[n]) -- then cbq's cut is
    tie-broken/non-deterministic and no fixed import can reliably match it."""
    keep = set()
    for cf in sorted(glob.glob(os.path.join(cdir, "case*.json"))):
        try:
            cases = json.load(open(cf))
        except Exception:
            continue
        for c in cases if isinstance(cases, list) else []:
            if not isinstance(c, dict):
                continue
            stmt = c.get("statements")
            if not isinstance(stmt, str):
                continue
            m = ORDER_LIMIT.search(stmt)
            if not m:
                continue
            ks, tid, cols_s, n = m.group(1), m.group(2), m.group(3), int(m.group(4))
            if ks not in MEGA_KEYSPACES or ks not in docs_by_ks or n < 1:
                continue
            cols = []  # (field, descending?)
            for part in cols_s.split(","):
                toks = part.split()
                if not toks:
                    cols = None
                    break
                cols.append((toks[0], len(toks) > 1 and toks[1].lower() == "desc"))
            if not cols:
                continue
            rows = [(k, v) for k, v in docs_by_ks[ks] if v.get("test_id") == tid]
            # Sort by successive keys, last-to-first, honoring per-column desc
            # (stable sort composes correctly).
            for field, desc in reversed(cols):
                rows.sort(key=lambda kv: _n1ql_sort_key(kv[1].get(field)), reverse=desc)
            if n >= len(rows):
                keep.update(k for k, _ in rows)
                continue
            def sk(kv):
                return tuple(_n1ql_sort_key(kv[1].get(f)) for f, _ in cols)
            if sk(rows[n - 1]) == sk(rows[n]):  # boundary tie -> non-deterministic
                continue
            keep.update(k for k, _ in rows[:n])
    return keep

def result_ref_values(cdir):
    """Id-like scalar values a category's cases assert on in their EXPECTED
    RESULTS. The fork computes each result over the full ~10k-doc mega keyspace,
    so a case that projects e.g. purchaseId/customerId names specific docs that
    a sampled corpus won't contain -- its result then differs (usually fewer or
    mis-ordered rows). Force-importing exactly the referenced docs makes those
    cases pass without importing all 10k docs. This is sound, not circular:
    every expected result is cbq's answer over the FULL dataset, so adding any
    real fork doc can only move our result toward that answer, never away (a
    doc that "should" rank in an ORDER BY ... LIMIT is already referenced, hence
    imported; any other real doc either fails the predicate or sorts past the
    limit). A doc is matched if one of its own id-like values is referenced."""
    vals = set()
    for cf in sorted(glob.glob(os.path.join(cdir, "case*.json"))):
        try:
            cases = json.load(open(cf))
        except Exception:
            continue
        for c in cases if isinstance(cases, list) else []:
            if isinstance(c, dict):
                for row in c.get("results", []) or []:
                    _collect_idlike(row, vals)
    return vals


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
        # first doc of each INSERT statement, plus (1) any doc whose KEY is
        # referenced directly by a case (e.g. USE KEYS "k"), (2) any doc a case's
        # EXPECTED RESULTS reference by an id-like value (result_ref_values), and
        # (3) the first-n boundary docs of a `test_id=X ORDER BY cols LIMIT n`
        # case (order_limit_boundary, for cases whose projection hides the sort
        # keys). Together these let ORDER BY ... LIMIT / full-scan cases over a
        # mega keyspace pass without importing all 10k docs.
        ins = os.path.join(cdir, "insert.json")
        if os.path.exists(ins):
            refs = referenced_keys(cdir)
            resvals = result_ref_values(cdir)
            all_docs = []       # [(ks, idx, key, val)] for this category
            docs_by_ks = {}     # ks -> [(key, val)], for boundary computation
            for c in json.load(open(ins)):
                for ks, idx, key, obj in parse_inserts(c.get("statements", "")):
                    val = json.loads(obj)  # validate + normalize
                    all_docs.append((ks, idx, key, val))
                    docs_by_ks.setdefault(ks, []).append((key, val))
            boundary = order_limit_boundary(cdir, docs_by_ks)
            for ks, idx, key, val in all_docs:
                if ks in MEGA_KEYSPACES and idx != 0 and key not in refs \
                        and key not in boundary:
                    docvals = set()
                    _collect_idlike(val, docvals)
                    if not (docvals & resvals):
                        continue
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
