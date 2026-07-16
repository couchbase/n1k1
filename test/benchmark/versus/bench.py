#!/usr/bin/env python3
"""Head-to-head benchmark: n1k1 vs cbq, apples-to-apples, over the SAME directory
of *.json files (the classic cbq file datastore). Both engines use cbq's
parser+planner (identical plan); what differs is the execution engine -- n1k1's
[]byte byte-engine vs cbq's boxed value.AnnotatedValue executor.

Both columns are measured the SAME way: a tiny in-process runner does the FULL
parse->plan->execute per query, warm (median of REPS reps, first few dropped), and
reports median ms + median allocated MB (runtime.MemStats TotalAlloc delta):

  n1k1 -- the n1k1 CLI itself, driven over one warm REPL. Its footer reports
          Result.RunElapsed (whole Session.Run: parse+plan+convert+execute) and
          .stats reports allocated bytes/query -- both fair, no separate runner.
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
COMPILED = os.environ.get("COMPILED", "") not in ("", "0", "false")

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


def run_n1k1_compiled(binary, queries):
    """Drive the n1k1 CLI at the -prepare=full ceiling: each query is PREPAREd once
    (which go-builds a standalone cbq-free child binary) then EXECUTEd REPS times.
    Returns {query: median_warm_ms or None}. None means the query did NOT compile to
    a standalone child (it errored or fell back) -- e.g. a JOIN ... ON KEYS, whose
    per-row datastore-fetch the thin child's MemPipe can't serve.

    Only TIME is reported: the compiled compute runs in a CHILD process, so the
    parent's .stats heap-alloc counter (the MB column) can't see it -- an
    apples-to-apples memory number would need child RSS, out of scope here. The first
    EXECUTE includes the one-time `go build`; warm-dropping the first reps excludes it.

    Requires the `go` toolchain and N1K1_SRC (the n1k1 checkout) to build the child."""
    lines = [".prepare full", ".timer on", ".mode jsonlines"]
    for i, q in enumerate(queries):
        lines.append("PREPARE cp%d AS %s;" % (i, q))
        lines += ["EXECUTE cp%d;" % i] * REPS
    env = dict(os.environ, N1K1_SRC=REPO)
    r = subprocess.run([binary, DATA], input="\n".join(lines) + "\n",
                       env=env, capture_output=True, text=True)
    # Each EXECUTE that ran (compiled or interpreted-fallback) prints one
    # "... row(s) in <dur>" footer; a query whose child failed prints an Error and no
    # footer. Group footers back to their query by the fixed REPS-per-query cadence,
    # but since failures drop footers we instead tag each query's block by scanning
    # sequentially. Simpler + robust: split stderr on the PREPARE confirmations.
    blocks = re.split(r'prepared "cp\d+"', r.stderr)[1:]  # block i = EXECUTEs of cp{i}
    out = {}
    warm = min(5, REPS // 3)
    for q, block in zip(queries, blocks):
        durs = [float(m.group(1)) * _UMS[m.group(2)]
                for line in block.splitlines() if "row(s)" in line
                for m in [_DUR.search(line)] if m]
        # A standalone-compiled query yields REPS footers; anything fewer means it
        # errored/fell back -> report n/a rather than a misleading interpreted time.
        out[q] = median(durs[warm:]) if len(durs) >= REPS else None
    for q in queries:
        out.setdefault(q, None)
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


def table(title, queries, n1, comp, cbq):
    """Columns: interpreted n1k1 ms | compiled-standalone n1k1 ms (+ speedup vs
    interpreted) | cbq ms | interpreted n1k1 MB | cbq MB. Compiled has no MB (child
    process). 'n/a' in the comp column = the query did not compile standalone."""
    print("\n%s" % title)
    print("-" * 92)
    print("%-16s%9s%9s%8s%9s%10s%10s"
          % ("query", "interp", "comp", "c:i", "cbq ms", "interp MB", "cbq MB"))
    print("%-16s%9s%9s%8s%9s%10s%10s"
          % ("", "ms", "ms", "", "", "", ""))
    print("-" * 92)
    for name, q in queries:
        nms, nmb = n1[q]
        cms = comp.get(q) if comp else None
        comp_s = "%.2f" % cms if cms else "n/a"
        ratio_s = "%.2fx" % (cms / nms) if (cms and nms) else "-"
        cbq_ms = "%.2f" % cbq[q][0] if cbq else "-"
        cbq_mb = "%.2f" % cbq[q][1] if cbq else "-"
        print("%-16s%9.2f%9s%8s%9s%10.2f%10s"
              % (name, nms, comp_s, ratio_s, cbq_ms, nmb, cbq_mb))
    print("-" * 92)


def main():
    subprocess.run([sys.executable, os.path.join(HERE, "gen.py"), DATA, str(NDOCS),
                    str(BULK_DOCS), str(BULK_ITEMS)], check=True)
    n1bin = build_n1k1()

    all_q = [q for _, q in FILE_QUERIES + BULK_QUERIES]
    n1 = run_n1k1(n1bin, all_q)
    comp = run_n1k1_compiled(n1bin, all_q) if COMPILED else {}
    cbq = run_cbq(CBQ_LOCALBENCH, all_q) if CBQ_LOCALBENCH else {}

    print("\ncbq-vs-n1k1  |  files: %d docs   bulk: %d docs x %d-elem arrays"
          "  |  warm median of %d reps" % (NDOCS, BULK_DOCS, BULK_ITEMS, REPS))
    print("interp/comp/cbq ms = full parse+plan+execute; c:i = compiled/interpreted;"
          " MB = allocated/query")
    table("SCENARIO: files (one doc per file -- I/O-bound)", FILE_QUERIES, n1, comp, cbq)
    table("SCENARIO: bulk (few docs, big in-doc arrays via UNNEST -- compute-bound)",
          BULK_QUERIES, n1, comp, cbq)
    if COMPILED:
        print("\ncomp = n1k1 -prepare=full standalone-compiled EXECUTE (go-built child,"
              " build cost excluded via warm-drop).")
        print("  'n/a' = did not compile standalone (e.g. JOIN ... ON KEYS: the thin"
              " child can't do a per-row datastore fetch).")
        print("  no compiled MB: the compute runs in a child process, invisible to the"
              " parent's heap-alloc counter.")
    else:
        print("\ncompiled column OFF; set COMPILED=1 (needs the `go` toolchain) to add"
              " the n1k1 standalone-compiled column.")
    if not cbq:
        print("cbq column: build cmd/localbench from the n1k1-query local-benchmark")
        print("branch and set CBQ_LOCALBENCH=<binary>. See README.md.")


if __name__ == "__main__":
    main()
