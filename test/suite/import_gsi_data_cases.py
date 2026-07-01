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
NONDET = re.compile(r"\b(now_\w+|clock_\w+|random|rand|uuid|newid)\s*\(", re.IGNORECASE)
INSERT = re.compile(r'INSERT\s+INTO\s+(\w+)\s*\(KEY\s*,\s*VALUE\s*\)\s*VALUES\s*\(\s*"([^"]+)"\s*,\s*',
                    re.IGNORECASE | re.DOTALL)

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

def main(qf):
    tc = os.path.join(qf, "test/gsi/test_cases")
    root = os.path.join(os.path.dirname(__file__), "json-gsi/default")
    ncase = ndoc = 0
    for cat in CATEGORIES:
        cdir = os.path.join(tc, cat)
        # (a) data
        ins = os.path.join(cdir, "insert.json")
        if os.path.exists(ins):
            for c in json.load(open(ins)):
                stmt = c.get("statements", "")
                m = INSERT.search(stmt)
                if not m: continue
                ks, key = m.group(1), m.group(2)
                b = stmt.index("{", m.end() - 1)
                obj = stmt[b:brace_match(stmt, b)]
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
                picked.append(c)
        if picked:
            os.makedirs(os.path.join(root, "cases"), exist_ok=True)
            json.dump(picked, open(os.path.join(root, "cases", f"case_gsi_{cat}.json"), "w"), indent=2)
            ncase += len(picked)
    print(f"imported {ndoc} docs, {ncase} cases across {len(CATEGORIES)} categories")

if __name__ == "__main__":
    main(sys.argv[1])
