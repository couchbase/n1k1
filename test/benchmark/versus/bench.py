#!/usr/bin/env python3
"""Head-to-head benchmark: n1k1 vs cbq, apples-to-apples, over the SAME directory
of *.json files (the classic cbq file datastore). Both engines use cbq's
parser+planner (identical plan); what differs is the execution engine -- n1k1's
[]byte byte-engine vs cbq's boxed value.AnnotatedValue executor.

Both columns are measured the SAME way: a tiny in-process runner does the FULL
parse->plan->execute per query, warm (median of REPS reps, first few dropped), and
reports median ms + median allocated MB (runtime.MemStats TotalAlloc delta):

  n1k1 -- test/benchmark/versus/n1k1bench (glue.Session.Run); built here.
  cbq  -- the fork's cmd/localbench (test/filestore over the same dir:); build it
          from the n1k1-query local-benchmark branch and pass CBQ_LOCALBENCH=<bin>.

Two scenarios:
  files -- orders/cust one-doc-per-file: realistic but I/O-bound (a scan opens
           every file, a cost both engines pay), so wall time is close.
  bulk  -- a few docs holding large `items` arrays, driven by UNNEST: the volume
           lives INSIDE documents, so file I/O is trivial and per-row execution
           dominates -- this is where the engine/value-model gap shows.

Env: N1K1BENCH=<bin>  DATA=<dir>  NDOCS=<n>  BULK_ITEMS=<n>  REPS=<n>
     CBQ_LOCALBENCH=<bin>
"""
import os
import re
import sys
import statistics
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
DATA = os.environ.get("DATA", os.path.join(HERE, "data"))
NDOCS = int(os.environ.get("NDOCS", "20000"))
BULK_ITEMS = int(os.environ.get("BULK_ITEMS", "20000"))
BULK_DOCS = int(os.environ.get("BULK_DOCS", "4"))
REPS = int(os.environ.get("REPS", "11"))
CBQ_LOCALBENCH = os.environ.get("CBQ_LOCALBENCH", "")

FILE_QUERIES = [
    ("count+filter", "SELECT COUNT(*) c FROM orders WHERE amount >= 0"),
    ("filter+project", "SELECT o.custId, o.amount FROM orders o WHERE o.amount > 500"),
    ("group+agg", "SELECT o.category, COUNT(*) c, SUM(o.amount) s, AVG(o.amount) a "
                  "FROM orders o GROUP BY o.category"),
    ("sort+limit", "SELECT o.custId, o.amount FROM orders o WHERE o.amount >= 0 "
                   "ORDER BY o.amount DESC LIMIT 10"),
    ("expr-heavy", "SELECT o.id, (o.amount * o.qty) + 1 AS t FROM orders o "
                   "WHERE o.amount * o.qty > 2000"),
    ("join-count", "SELECT COUNT(*) c FROM orders o JOIN cust k ON KEYS o.custId"),
    ("join+group", "SELECT k.tier, COUNT(*) c, SUM(o.amount) s FROM orders o "
                   "JOIN cust k ON KEYS o.custId GROUP BY k.tier"),
    ("unnest-count", "SELECT COUNT(*) c FROM orders o UNNEST o.items i WHERE i.qty > 2"),
]

# bulk scenario: UNNEST a few big in-document arrays -> I/O-trivial, compute-bound.
BULK_QUERIES = [
    ("unnest+group", "SELECT i.category, COUNT(*) c, SUM(i.amount) s, AVG(i.amount) a "
                     "FROM bulk b UNNEST b.items i GROUP BY i.category"),
    ("unnest+filter", "SELECT COUNT(*) c FROM bulk b UNNEST b.items i WHERE i.amount > 500"),
    ("unnest+expr", "SELECT COUNT(*) c FROM bulk b UNNEST b.items i "
                    "WHERE i.amount * i.qty > 2000"),
    ("unnest+sort", "SELECT i.id, i.amount FROM bulk b UNNEST b.items i "
                    "ORDER BY i.amount DESC LIMIT 10"),
    ("unnest+join", "SELECT k.tier, COUNT(*) c, SUM(i.amount) s FROM bulk b "
                    "UNNEST b.items i JOIN cust k ON KEYS i.custId GROUP BY k.tier"),
]


def die(m):
    print("ERROR: " + m, file=sys.stderr)
    sys.exit(1)


def build_n1k1():
    b = os.environ.get("N1K1")
    if b and os.path.exists(b):
        return b
    out = os.path.join(HERE, ".n1k1.bin")
    print("building n1k1 CLI ...", file=sys.stderr)
    subprocess.run(["make", "build-intermed"], cwd=REPO, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    r = subprocess.run(["go", "build", "-tags", "n1ql", "-o", out, "./cmd/n1k1"],
                       cwd=REPO, capture_output=True, text=True)
    if r.returncode != 0:
        die("n1k1 build failed:\n" + r.stderr)
    return out


_DUR = re.compile(r"in ([0-9.]+)(ns|µs|ms|s)\b")
_ALLOC = re.compile(r"runtime: ([0-9.]+)([KMG]?B) allocated")
_UMS = {"ns": 1e-6, "µs": 1e-3, "ms": 1.0, "s": 1000.0}
_UMB = {"B": 1e-6, "KB": 1e-3, "MB": 1.0, "GB": 1000.0}


def median(xs):
    return statistics.median(xs) if xs else 0.0


def run_n1k1(binary, queries):
    """Drive the n1k1 CLI (one warm REPL session over DATA); {query: (median_ms, median_MB)}.
    Since RunElapsed lands in the footer, the CLI reports the SAME full parse+plan+
    execute time + allocated-MB as the cbq runner -- no separate n1k1 runner needed."""
    lines = [".mode jsonlines", ".stats final", ".timer on"]
    for q in queries:
        lines += ["%s;" % q] * REPS  # REPS warm reps per query, grouped
    r = subprocess.run([binary, DATA], input="\n".join(lines) + "\n",
                       capture_output=True, text=True)
    ms, mb = [], []
    for line in r.stderr.splitlines():  # timer + -stats go to stderr
        d = _DUR.search(line)
        if d and "row(s)" in line:
            ms.append(float(d.group(1)) * _UMS[d.group(2)])
        a = _ALLOC.search(line)
        if a:
            mb.append(float(a.group(1)) * _UMB[a.group(2)])
    need = len(queries) * REPS
    if len(ms) < need or len(mb) < need:
        die("n1k1 CLI gave %d ms / %d MB lines, need %d each\n%s"
            % (len(ms), len(mb), need, r.stderr[-1000:]))
    warm = min(5, REPS // 3)
    out = {}
    for i, q in enumerate(queries):
        seg = slice(i * REPS + warm, (i + 1) * REPS)
        out[q] = (median(ms[seg]), median(mb[seg]))
    return out


def run_cbq(binary, queries):
    """The fork's cmd/localbench: one process over DATA, RESULT<TAB>ms<TAB>MB<TAB>rows."""
    env = dict(os.environ, REPS=str(REPS))
    r = subprocess.run([binary, DATA], input="\n".join(queries) + "\n",
                       env=env, capture_output=True, text=True)
    rows = [ln.split("\t") for ln in r.stdout.splitlines() if ln.startswith("RESULT\t")]
    if len(rows) != len(queries):
        die("localbench gave %d/%d RESULT lines\n%s"
            % (len(rows), len(queries), r.stderr[-1000:]))
    return {q: (float(c[1]), float(c[2])) for q, c in zip(queries, rows)}


def table(title, queries, n1, cbq):
    print("\n%s" % title)
    print("-" * 78)
    if cbq:
        print("%-16s%9s%9s%8s%10s%10s%8s"
              % ("query", "n1k1 ms", "cbq ms", "x(t)", "n1k1 MB", "cbq MB", "x(m)"))
    else:
        print("%-16s%12s%14s" % ("query", "n1k1 ms", "n1k1 MB/q"))
    print("-" * 78)
    for name, q in queries:
        nms, nmb = n1[q]
        if cbq:
            cms, cmb = cbq[q]
            print("%-16s%9.2f%9.2f%7.2fx%10.2f%10.2f%7.1fx"
                  % (name, nms, cms, cms / nms if nms else 0,
                     nmb, cmb, cmb / nmb if nmb else 0))
        else:
            print("%-16s%12.3f%14.3f" % (name, nms, nmb))
    print("-" * 78)


def main():
    subprocess.run([sys.executable, os.path.join(HERE, "gen.py"), DATA, str(NDOCS),
                    str(BULK_DOCS), str(BULK_ITEMS)], check=True)
    n1bin = build_n1k1()

    all_q = [q for _, q in FILE_QUERIES + BULK_QUERIES]
    n1 = run_n1k1(n1bin, all_q)
    cbq = run_cbq(CBQ_LOCALBENCH, all_q) if CBQ_LOCALBENCH else {}

    print("\ncbq-vs-n1k1  |  files: %d docs   bulk: %d docs x %d-elem arrays"
          "  |  warm median of %d reps" % (NDOCS, BULK_DOCS, BULK_ITEMS, REPS))
    print("both columns = full parse+plan+execute; MB = allocated/query")
    table("SCENARIO: files (one doc per file -- I/O-bound)", FILE_QUERIES, n1, cbq)
    table("SCENARIO: bulk (few docs, big in-doc arrays via UNNEST -- compute-bound)",
          BULK_QUERIES, n1, cbq)
    if not cbq:
        print("\ncbq column: build cmd/localbench from the n1k1-query local-benchmark")
        print("branch and set CBQ_LOCALBENCH=<binary>. See README.md.")


if __name__ == "__main__":
    main()
