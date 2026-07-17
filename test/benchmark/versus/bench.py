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
# packed (.jsonl container) scenario doc count; defaults to NDOCS so the packed
# table is directly comparable to the files table at the same doc count -- bump it
# far higher (e.g. 500000) to get well-resolved times (a container file is fast).
NDOCS_JSONL = int(os.environ.get("NDOCS_JSONL", str(NDOCS)))
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

# packed scenario: the SAME logical file-scenario queries, but the outer keyspace is
# the one-container orders_jsonl instead of a directory of NDOCS one-doc files. Same
# docs, different on-disk layout -> isolates the per-file-open syscall overhead (see
# DESIGN-benchmark.md "file-layout lesson"). n1k1 reads .jsonl natively; cbq's file
# datastore is one-doc-per-.json only, so this scenario is n1k1-only for now.
PACKED_QUERIES = [(name, q.replace("orders", "orders_jsonl"))
                  for name, q in FILE_QUERIES]


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


_CORE = re.compile(r"compiled core compute \(non-I/O\): ([0-9.]+)(ns|µs|ms|s)\b")


def run_n1k1_compiled(binary, queries):
    """Drive the n1k1 CLI at the -prepare=full ceiling: each query is PREPAREd once
    (which go-builds a standalone cbq-free child binary) then EXECUTEd REPS times.
    Returns {query: (full_ms, core_ms)}, each None when unavailable.

      full_ms -- the whole compiled round-trip: the parent scans the files, JSON-pipes
                 every input record to the child, the child computes, and pipes rows
                 back. None means the query did NOT compile standalone (errored / fell
                 back) -- e.g. JOIN ... ON KEYS, whose per-row datastore-fetch the thin
                 child's MemPipe can't serve.
      core_ms -- the child's OWN report of its core compute (non-I/O), the "N1K1_CORE_NS"
                 it prints once it has parsed the piped payload: the specialized,
                 Futamura-projected query code running over the in-memory records,
                 EXCLUDING the parent<->child IPC. This is the number to compare
                 against the interpreter to see if compilation helps -- cleanest on the
                 bulk (near-zero-I/O) scenario, where interp is ~all compute too.

    No compiled MB: the compute runs in a child process, invisible to the parent's
    heap-alloc counter. The first EXECUTE includes the one-time `go build`; warm-
    dropping the first reps excludes it. Needs the `go` toolchain + N1K1_SRC."""
    lines = [".prepare full", ".timer on", ".mode jsonlines"]
    for i, q in enumerate(queries):
        lines.append("PREPARE cp%d AS %s;" % (i, q))
        lines += ["EXECUTE cp%d;" % i] * REPS
    env = dict(os.environ, N1K1_SRC=REPO)
    r = subprocess.run([binary, DATA], input="\n".join(lines) + "\n",
                       env=env, capture_output=True, text=True)
    # Split stderr on the PREPARE confirmations so block i holds cp{i}'s EXECUTEs
    # (robust to queries that drop footers by erroring).
    blocks = re.split(r'prepared "cp\d+"', r.stderr)[1:]
    out = {}
    warm = min(5, REPS // 3)
    for q, block in zip(queries, blocks):
        full, core = [], []
        for line in block.splitlines():
            if "row(s)" in line:
                m = _DUR.search(line)
                if m:
                    full.append(float(m.group(1)) * _UMS[m.group(2)])
            m = _CORE.search(line)
            if m:
                core.append(float(m.group(1)) * _UMS[m.group(2)])
        # A standalone-compiled query yields REPS footers; fewer => errored/fell back.
        f = median(full[warm:]) if len(full) >= REPS else None
        c = median(core[warm:]) if len(core) >= REPS else None
        out[q] = (f, c)
    for q in queries:
        out.setdefault(q, (None, None))
    return out


def run_cbq(binary, queries, site="dir:"):
    """The fork's cmd/localbench over DATA: RESULT<TAB>ms<TAB>MB<TAB>rows. `site` picks
    the datastore -- "dir:" (one-doc-per-.json) or "jsonl:" (in-memory .jsonl container
    keyspaces, for the packed scenario)."""
    env = dict(os.environ, REPS=str(REPS), SITE=site)
    r = subprocess.run([binary, DATA], input="\n".join(queries) + "\n",
                       env=env, capture_output=True, text=True)
    rows = [ln.split("\t") for ln in r.stdout.splitlines() if ln.startswith("RESULT\t")]
    if len(rows) != len(queries):
        die("localbench (%s) gave %d/%d RESULT lines\n%s"
            % (site, len(rows), len(queries), r.stderr[-1000:]))
    return {q: (float(c[1]), float(c[2])) for q, c in zip(queries, rows)}


def table(title, queries, n1, comp, cbq):
    """Columns: interp ms | comp ms (whole compiled round-trip) | core ms (the child's
    core compute, non-I/O, IPC excluded) | core:i (core/interp -- <1.0x = the
    Futamura-projected code is faster at the actual compute) | cbq ms | interp MB |
    cbq MB. Compiled has no MB (child process). 'n/a' = did not compile standalone."""
    print("\n%s" % title)
    print("-" * 100)
    print("%-16s%9s%9s%9s%8s%9s%10s%10s"
          % ("query", "interp", "comp", "core", "core:i", "cbq ms", "interp MB", "cbq MB"))
    print("%-16s%9s%9s%9s%8s%9s%10s%10s"
          % ("", "ms", "ms", "ms", "", "", "", ""))
    print("-" * 100)
    for name, q in queries:
        nms, nmb = n1[q]
        cms, core = comp.get(q, (None, None)) if comp else (None, None)
        comp_s = "%.2f" % cms if cms else "n/a"
        core_s = "%.2f" % core if core else "n/a"
        ci_s = "%.2fx" % (core / nms) if (core and nms) else "-"
        cq = cbq.get(q) if cbq else None
        cbq_ms = "%.2f" % cq[0] if cq else "-"
        cbq_mb = "%.2f" % cq[1] if cq else "-"
        print("%-16s%9.2f%9s%9s%8s%9s%10.2f%10s"
              % (name, nms, comp_s, core_s, ci_s, cbq_ms, nmb, cbq_mb))
    print("-" * 100)


def main():
    subprocess.run([sys.executable, os.path.join(HERE, "gen.py"), DATA, str(NDOCS),
                    str(BULK_DOCS), str(BULK_ITEMS), str(NDOCS_JSONL)], check=True)
    n1bin = build_n1k1()

    all_q = [q for _, q in FILE_QUERIES + BULK_QUERIES]
    n1 = run_n1k1(n1bin, all_q)
    comp = run_n1k1_compiled(n1bin, all_q) if COMPILED else {}
    cbq = run_cbq(CBQ_LOCALBENCH, all_q) if CBQ_LOCALBENCH else {}

    packed_q = [q for _, q in PACKED_QUERIES]
    n1p = run_n1k1(n1bin, packed_q)
    compp = run_n1k1_compiled(n1bin, packed_q) if COMPILED else {}
    # cbq packed: the jsonl: datastore exposes only orders_jsonl (cust has no .jsonl),
    # so the ON KEYS join queries can't run there -- skip them for the cbq column.
    cbqp = run_cbq(CBQ_LOCALBENCH,
                   [q for name, q in PACKED_QUERIES if "join" not in name],
                   site="jsonl:") if CBQ_LOCALBENCH else {}

    print("\ncbq-vs-n1k1  |  files: %d docs   bulk: %d docs x %d-elem arrays"
          "   packed: %d-doc .jsonl  |  warm median of %d reps"
          % (NDOCS, BULK_DOCS, BULK_ITEMS, NDOCS_JSONL, REPS))
    print("interp/comp/cbq ms = full parse+plan+execute; core = compiled child's core"
          " compute (non-I/O, IPC excluded); core:i = core/interp; MB = allocated/query")
    table("SCENARIO: files (one doc per file -- I/O-bound)", FILE_QUERIES, n1, comp, cbq)
    table("SCENARIO: bulk (few docs, big in-doc arrays via UNNEST -- compute-bound)",
          BULK_QUERIES, n1, comp, cbq)
    table("SCENARIO: packed (same order docs in ONE .jsonl container; cbq via jsonl: "
          "in-mem datastore -- joins n/a, cust has no .jsonl)",
          PACKED_QUERIES, n1p, compp, cbqp)
    if COMPILED:
        print("\ncomp  = n1k1 -prepare=full standalone-compiled EXECUTE, whole round-trip"
              " (parent scan + JSON-pipe inputs + child compute + pipe rows back);")
        print("        go-build cost excluded via warm-drop.")
        print("core  = the child's core compute (non-I/O): the Futamura-projected query"
              " code over in-memory records, IPC excluded. core:i = core/interp.")
        print("        On the bulk (near-zero-I/O) rows interp is ~all compute too, so"
              " core:i < 1.0x means the compiled code is genuinely faster -- the payoff the")
        print("        IPC in `comp` hides. 'n/a' = did not compile standalone (JOIN ON"
              " KEYS: the thin child can't do a per-row datastore fetch).")
    else:
        print("\ncompiled column OFF; set COMPILED=1 (needs the `go` toolchain) to add"
              " the n1k1 standalone-compiled column.")
    if not cbq:
        print("cbq column: build cmd/localbench from the n1k1-query local-benchmark")
        print("branch and set CBQ_LOCALBENCH=<binary>. See README.md.")


if __name__ == "__main__":
    main()
