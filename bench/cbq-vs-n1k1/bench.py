#!/usr/bin/env python3
"""Head-to-head benchmark: n1k1 vs cbq, apples-to-apples, over the SAME directory
of *.json files (the classic cbq file datastore: <root>/<ns>/<keyspace>/<key>.json).

Both engines use cbq's parser+planner, so the plan is identical; what differs is
the execution engine.

Columns:
  n1k1     -- always measured. n1k1's byte engine over DATA, warm median (ms) and
              allocated MB per query (from `-stats`).
  cbq      -- measured only if CBQ_URL points at a real cbq /query/service
              endpoint loaded with the same data (median server-side
              metrics.executionTime, ms). See README.md for how to bring one up
              (a standalone cbq-engine over a bare dir: datastore does NOT start
              its query service without cbauth on 7.6.x).

Metric: warm median over REPS runs (first WARMUP dropped). n1k1 is timed via the
REPL's per-query footer inside one warm process; cbq via metrics.executionTime.

Env: N1K1=<binary>  DATA=<dir>  NDOCS=<n>  REPS=<n>  CBQ_URL=<url>  CBQ_CREDS=user:pass
"""
import os
import re
import sys
import json
import statistics
import subprocess
import urllib.request
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
DATA = os.environ.get("DATA", os.path.join(HERE, "data"))
NDOCS = int(os.environ.get("NDOCS", "5000"))
REPS = int(os.environ.get("REPS", "15"))
WARMUP = min(5, REPS // 3)
CBQ_URL = os.environ.get("CBQ_URL", "")
CBQ_CREDS = os.environ.get("CBQ_CREDS", "")
# Preferred cbq column: a `localbench` binary built from the n1k1-query fork's
# local-benchmark branch, which runs cbq's real executor over the SAME dir:
# file datastore (see README.md). It reads queries on stdin and prints
# "RESULT\t<median-ms>\t<rows>" per query.
CBQ_LOCALBENCH = os.environ.get("CBQ_LOCALBENCH", "")

QUERIES = [
    ("count+filter", "SELECT COUNT(*) c FROM orders WHERE amount >= 0"),
    ("filter+project", "SELECT o.custId, o.amount FROM orders o WHERE o.amount > 500"),
    ("group+agg", "SELECT o.category, COUNT(*) c, SUM(o.amount) s, AVG(o.amount) a "
                  "FROM orders o GROUP BY o.category"),
    ("sort+limit", "SELECT o.custId, o.amount FROM orders o WHERE o.amount >= 0 "
                   "ORDER BY o.amount DESC LIMIT 10"),
    ("expr-heavy", "SELECT o.id, (o.amount * o.qty) + 1 AS t FROM orders o "
                   "WHERE o.amount * o.qty > 2000"),
]

_DUR = re.compile(r"in ([0-9.]+)(ns|µs|ms|s)\b")
_ALLOC = re.compile(r"runtime: ([0-9.]+)([KMG]?B) allocated")
_UNIT_MS = {"ns": 1e-6, "µs": 1e-3, "ms": 1.0, "s": 1000.0}
_UNIT_MB = {"B": 1e-6, "KB": 1e-3, "MB": 1.0, "GB": 1000.0}


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


def run_n1k1(binary, query):
    """REPS warm reps of query in one REPL; return (median ms, allocated MB per query)."""
    stdin = ".mode jsonlines\n.stats final\n.timer on\n" + ("%s;\n" % query) * REPS
    r = subprocess.run([binary, DATA], input=stdin, capture_output=True, text=True)
    ms, mb = [], []
    # n1k1 writes the timer ("row(s) in") and -stats ("runtime:") lines to stderr;
    # query result rows go to stdout.
    for line in r.stderr.splitlines():
        d = _DUR.search(line)
        if d and "row(s)" in line:
            ms.append(float(d.group(1)) * _UNIT_MS[d.group(2)])
        a = _ALLOC.search(line)
        if a:
            mb.append(float(a.group(1)) * _UNIT_MB[a.group(2)])
    if len(ms) < REPS:
        die("n1k1 gave %d/%d timings for %r\n%s" % (len(ms), REPS, query, r.stderr[-800:]))
    med_mb = statistics.median(mb[WARMUP:]) if len(mb) >= REPS else float("nan")
    return statistics.median(ms[WARMUP:]), med_mb


def run_cbq_localbench(queries):
    """One localbench process over DATA; feed all queries on stdin, in order.
    Returns {query_text: median_ms}. localbench does its own warm/median with REPS."""
    env = dict(os.environ, REPS=str(REPS))
    r = subprocess.run([CBQ_LOCALBENCH, DATA], input="\n".join(queries) + "\n",
                       env=env, capture_output=True, text=True)
    meds = [float(ln.split("\t")[1]) for ln in r.stdout.splitlines()
            if ln.startswith("RESULT\t")]
    if len(meds) != len(queries):
        die("localbench gave %d/%d RESULT lines\n%s"
            % (len(meds), len(queries), r.stderr[-800:]))
    return dict(zip(queries, meds))


def run_cbq_url(query):
    body = {"statement": query}
    if CBQ_CREDS:
        body["creds"] = json.dumps([{"user": CBQ_CREDS.split(":")[0],
                                     "pass": CBQ_CREDS.split(":", 1)[1]}])
    ms = []
    for _ in range(REPS):
        req = urllib.request.Request(CBQ_URL, data=urllib.parse.urlencode(body).encode())
        with urllib.request.urlopen(req, timeout=120) as resp:
            d = json.load(resp)
        et = d.get("metrics", {}).get("executionTime", "")
        m = _DUR.search("in " + et) if et else None
        if not m:
            die("cbq: no metrics.executionTime: %s" % json.dumps(d)[:400])
        ms.append(float(m.group(1)) * _UNIT_MS[m.group(2)])
    return statistics.median(ms[WARMUP:])


def main():
    subprocess.run([sys.executable, os.path.join(HERE, "gen.py"), DATA, str(NDOCS)],
                   check=True)
    binary = build_n1k1()

    # cbq column source: localbench (real executor over the same dir:) preferred,
    # else a live cbq /query/service via CBQ_URL, else n1k1-only.
    cbq_kind = "localbench" if CBQ_LOCALBENCH else ("url" if CBQ_URL else "")
    cbq_meds = run_cbq_localbench([q for _, q in QUERIES]) if cbq_kind == "localbench" else {}

    def cbq_ms(q):
        return cbq_meds[q] if cbq_kind == "localbench" else run_cbq_url(q)

    print("\ncbq-vs-n1k1  |  %d docs  |  warm median of %d reps (%d warmup dropped)"
          % (NDOCS, REPS, WARMUP))
    if cbq_kind:
        print("cbq column: %s"
              % ("cbq real executor over the same dir: file datastore (filestore harness)"
                 if cbq_kind == "localbench" else "cbq /query/service " + CBQ_URL))
    print("-" * 74)
    if cbq_kind:
        print("%-16s%11s%11s%11s%12s" % ("query", "n1k1 ms", "n1k1 MB", "cbq ms", "cbq/n1k1"))
    else:
        print("%-16s%12s%14s" % ("query", "n1k1 ms", "n1k1 MB/q"))
    print("-" * 74)
    for name, q in QUERIES:
        nms, nmb = run_n1k1(binary, q)
        if cbq_kind:
            cms = cbq_ms(q)
            print("%-16s%11.2f%11.2f%11.2f%11.2fx" % (name, nms, nmb, cms, cms / nms))
        else:
            print("%-16s%12.3f%14.3f" % (name, nms, nmb))
    print("-" * 74)
    if not cbq_kind:
        print("cbq column: set CBQ_LOCALBENCH=<localbench binary> (real cbq over the same")
        print("            dir: datastore) or CBQ_URL=<.../query/service>. See README.md.")


if __name__ == "__main__":
    main()
