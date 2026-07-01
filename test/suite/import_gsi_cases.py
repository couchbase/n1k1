#!/usr/bin/env python3
#  Copyright (c) 2026 Couchbase, Inc.
#  Licensed under the Apache License, Version 2.0 (the "License").

"""
import_gsi_cases.py -- import portable SQL++ test cases from the couchbase/query
(n1k1-query) fork's data-driven test corpus into n1k1's conformance suite.

The fork's test/gsi/test_cases/<category>/ dirs hold case_*.json files of
{statements, results} pairs (integration tests that normally run against a live
Couchbase+GSI cluster, seeding data via insert.json INSERTs). n1k1 can't run
INSERT/mutations, but the *constant-expression* cases -- SELECTs with no FROM
clause -- need no data at all and exercise the scalar/aggregate/function
expression engine directly (n1k1 evaluates what it can natively and delegates the
rest to the embedded cbq evaluator). Those are what this script imports.

Selection rules (kept deterministic + host-independent on purpose):
  - category not in SKIP_CATS (DDL-text / UDF categories aren't expressions)
  - statement starts with SELECT and has no `from` (so it needs no dataset)
  - statement has no wall-clock / random / id function (NOW_*, CLOCK_*, RANDOM,
    UUID, NEWID) -- those can't have a reproducible expected value
  - epoch-millis date functions (MILLIS_TO_*, DATE_*_MILLIS) ARE kept: they format
    through the local zone, but test/main_test.go pins the process to UTC so their
    results are stable across hosts.

Output: one test/suite/json/default/cases/case_gsi_<category>.json per category,
picked up by TestSuiteCases (interpreter) and TestSuiteWithCompiler (compiler).

Usage (from the repo root):
  python3 test/suite/import_gsi_cases.py "$(go list -m -f '{{.Dir}}' github.com/couchbase/query)"
"""

import json, os, re, glob, sys

SKIP_CATS = {"sanitize_statement_function", "extractddl", "udf"}
FROM_RE = re.compile(r"\bfrom\b", re.IGNORECASE)
NONDET_RE = re.compile(r"\b(now_\w+|clock_\w+|random|rand|uuid|newid)\s*\(", re.IGNORECASE)


def main(qf):
    tc = os.path.join(qf, "test/gsi/test_cases")
    dest = os.path.join(os.path.dirname(__file__), "json/default/cases")
    total = 0
    for catdir in sorted(glob.glob(os.path.join(tc, "*"))):
        cat = os.path.basename(catdir)
        if cat in SKIP_CATS:
            continue
        picked = []
        for cf in sorted(glob.glob(os.path.join(catdir, "case*.json"))):
            try:
                cases = json.load(open(cf))
            except Exception:
                continue
            if not isinstance(cases, list):
                continue
            for c in cases:
                if not isinstance(c, dict):
                    continue
                stmt, res = c.get("statements"), c.get("results")
                if not isinstance(stmt, str) or res is None:
                    continue
                if not stmt.strip().lower().startswith("select"):
                    continue
                if FROM_RE.search(stmt) or NONDET_RE.search(stmt):
                    continue
                keep = {"statements": stmt, "results": res}
                if "description" in c:
                    keep["description"] = c["description"]
                picked.append(keep)
        if picked:
            with open(os.path.join(dest, f"case_gsi_{cat}.json"), "w") as f:
                json.dump(picked, f, indent=2)
            total += len(picked)
            print(f"  {len(picked):4d} {cat}")
    print(f"imported {total} cases")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: import_gsi_cases.py <couchbase/query module dir>")
    main(sys.argv[1])
