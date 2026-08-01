#!/usr/bin/env python3
"""k-means / IVF partitioning / cluster census over an n1k1 dataset -- NO engine changes:
every step is plain SQL++ issued through the n1k1 CLI (this script is just the loop
driver + JSON plumbing). See examples/kmeans/README.md for the walkthrough and
DESIGN-vectors.md "Phase 1.5" for the design rationale.

Subcommands (typical order):

  fit        Lloyd's k-means over a vector field: deterministic farthest-first init,
             then one SQL++ statement per iteration (assignment argmin + per-cluster
             element-wise mean via UNNEST-by-position + GROUP BY). Writes centroids.json.
  partition  IVF-style layout: writes each cluster's rows into its own Parquet keyspace
             (part_<k>/data.parquet, the columnar VECTOR_DISTANCE-ready vec column) and
             the centroids into a queryable centroids/ keyspace.
  census     "What's in this dataset": per-cluster sizes, exemplar docs nearest each
             centroid, and (optionally) top text tokens per cluster.
  probe      ANN-style query: distance to centroids picks the nprobe nearest partitions,
             scans only those, and reports recall + timing vs the brute-force scan.

The SQL++ argmin needs no extensions (ARRAY_SORT over [dist, i] pairs). --use-udf swaps
in the vector_nearest() goja UDF (extensions/functions/) -- same results, terser SQL.
Note VECTOR_DISTANCE itself can't do the argmin: it requires a STATIC query vector
(constant / $param / WITH alias), so ranging it over the centroid list is rejected; it
IS used wherever the centroid is a per-query constant (census exemplars, probe scans).

Examples:
  python3 examples/kmeans/gen_vectors.py --out examples/kmeans/data
  python3 examples/kmeans/kmeans.py fit       --data examples/kmeans/data --k 5
  python3 examples/kmeans/kmeans.py partition --data examples/kmeans/data
  python3 examples/kmeans/kmeans.py census    --data examples/kmeans/data --text-field txt
  python3 examples/kmeans/kmeans.py probe     --data examples/kmeans/data \\
      --qvec "$(python3 -c 'import json;print(json.dumps([0.1]*16))')" --nprobe 2
"""

import argparse
import json
import math
import os
import subprocess
import sys
import time

# ---------------------------------------------------------------- n1k1 plumbing


def find_n1k1(explicit):
    if explicit:
        return explicit
    env = os.environ.get("N1K1")
    if env:
        return env
    for cand in ("./n1k1", "n1k1"):
        try:
            subprocess.run([cand, "-h"], capture_output=True)
            return cand
        except FileNotFoundError:
            continue
    sys.exit("n1k1 binary not found: pass --n1k1, set $N1K1, or `make cli` first")


def run_sql(args, stmt):
    """Run one SQL++ statement via the CLI, return (rows, wall_seconds)."""
    cmd = [args.n1k1_bin, "-mode", "json"]
    if args.use_udf:
        cmd += ["-ext", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "..", "..", "extensions", "functions", "js")]
    cmd += ["-c", stmt, args.data]
    t0 = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.perf_counter() - t0
    if p.returncode != 0:
        sys.exit("n1k1 failed (%d):\n%s\n%s\nstatement was:\n%s"
                 % (p.returncode, p.stdout.strip(), p.stderr.strip(), stmt))
    try:
        return json.loads(p.stdout), dt
    except json.JSONDecodeError:
        sys.exit("could not parse n1k1 output as JSON:\n%s\nstatement was:\n%s" % (p.stdout, stmt))


def bt(name):  # backtick-quote an identifier
    return "`" + name.replace("`", "") + "`"


# ---------------------------------------------------------------- SQL++ fragments


def argmin_expr(args, vec_ref, cents_ref="cents"):
    """SQL++ for: index of the centroid (in cents_ref -- a WITH alias, or an inline
    JSON literal where WITH isn't available, e.g. under INSERT) nearest to vec_ref.

    Pure-SQL++ spelling: build [squared_dist, i] per centroid, ARRAY_SORT (arrays compare
    elementwise, so distance sorts first), take [0][1]. With --use-udf, one goja call.
    """
    if args.use_udf:
        return "vector_nearest(%s, %s)" % (vec_ref, cents_ref)
    return ("ARRAY_SORT(ARRAY [ARRAY_SUM(ARRAY POWER(%s[q]-c[q],2) "
            "FOR q IN ARRAY_RANGE(0,ARRAY_LENGTH(%s)) END), i] "
            "FOR i:c IN %s END)[0][1]" % (vec_ref, vec_ref, cents_ref))


def valued_src(args):
    """Inner select normalizing the vec field to `v` and dropping vec-less rows."""
    lim = " LIMIT %d" % args.limit if getattr(args, "limit", 0) else ""
    return ("(SELECT d0.%s AS v FROM %s d0 WHERE d0.%s IS VALUED%s) AS d"
            % (bt(args.vec_field), bt(args.keyspace), bt(args.vec_field), lim))


def lloyd_stmt(args, cents):
    """One full Lloyd iteration as a single statement: per-DOC argmin assignment
    (innermost select, so it computes once per doc -- an UNNEST-level LET would
    recompute it per (doc, position), dim x slower), then per-(cluster, position)
    AVG via UNNEST ARRAY_RANGE, then reassemble each cluster's centroid array
    ordered by position. Returns {cl, centroid, n} rows."""
    return """WITH cents AS (%s)
SELECT g.cl AS cl, ARRAY x[1] FOR x IN ARRAY_SORT(ARRAY_AGG([g.p, g.m])) END AS centroid, MAX(g.n) AS n
FROM (
  SELECT dd.cl, p, AVG(dd.v[p]) AS m, COUNT(1) AS n
  FROM (SELECT d.v AS v, %s AS cl FROM %s) AS dd
  UNNEST ARRAY_RANGE(0, ARRAY_LENGTH(dd.v)) AS p
  GROUP BY dd.cl, p
) AS g
GROUP BY g.cl ORDER BY g.cl""" % (json.dumps(cents), argmin_expr(args, "d.v"), valued_src(args))


def euclid(a, b):
    return math.sqrt(sum((x - y) * (x - y) for x, y in zip(a, b)))


def lloyd_driver_mean(args, cents):
    """One Lloyd iteration, the PRACTICAL split: the O(N*K*dim) assignment scan runs
    in SQL++ (argmin per row), the O(N*dim) element-wise mean runs here in the driver.

    Why not all-SQL++ (lloyd_pure_sql)? Two measured engine behaviors (2026-08, 20K x
    64-d x k=8): a derived table is FLATTENED by the optimizer, so a subquery-computed
    assignment is re-evaluated per (doc, position) under the UNNEST -- dim x the work;
    and even over a stored cl column the UNNEST-by-position GROUP BY runs the boxed
    row lane at ~3.5us per (doc, position). Until a native VECTOR_AVG aggregate exists
    (see DESIGN-vectors.md Phase 1.5 proposals), the mean is cheaper at the driver."""
    stmt = ("WITH cents AS (%s) SELECT %s AS cl, d.v AS v FROM %s"
            % (json.dumps(cents), argmin_expr(args, "d.v"), valued_src(args)))
    rows, dt = run_sql(args, stmt)
    dim = len(cents[0])
    sums = {}
    sizes = {}
    for r in rows:
        cl, v = int(r["cl"]), r["v"]
        acc = sums.get(cl)
        if acc is None:
            sums[cl] = list(v)
            sizes[cl] = 1
        else:
            for i in range(dim):
                acc[i] += v[i]
            sizes[cl] += 1
    new = list(cents)  # an emptied cluster keeps its old centroid
    for cl, acc in sums.items():
        n = sizes[cl]
        new[cl] = [x / n for x in acc]
    return new, sizes, dt


def lloyd_pure_sql(args, cents):
    """One Lloyd iteration entirely in SQL++ (the expressibility demo; see
    lloyd_driver_mean for why it is not the default at scale)."""
    rows, dt = run_sql(args, lloyd_stmt(args, cents))
    new = list(cents)
    sizes = {}
    for r in rows:
        new[int(r["cl"])] = r["centroid"]
        sizes[int(r["cl"])] = r["n"]
    return new, sizes, dt


# ---------------------------------------------------------------- subcommands


def min_dist_expr(args, vec_ref):
    """SQL++ for: squared distance from vec_ref to its NEAREST centroid in `cents`."""
    if args.use_udf:
        return "POWER(vector_nearest_dist(%s, cents)[1], 2)" % vec_ref
    return ("ARRAY_SORT(ARRAY ARRAY_SUM(ARRAY POWER(%s[q]-c[q],2) "
            "FOR q IN ARRAY_RANGE(0,ARRAY_LENGTH(%s)) END) FOR c IN cents END)[0]"
            % (vec_ref, vec_ref))


def check_norms(args):
    """Sample vector norms and WARN when they aren't ~unit. Euclidean k-means over
    un-normalized vectors clusters by MAGNITUDE, not direction -- and nothing else in
    the pipeline would error. Real-world cause (n1k1-for-ai feedback): ollama's older
    /api/embeddings returns raw magnitudes (norm ~23) while /api/embed (the batch API)
    returns unit vectors (norm 1.0)."""
    rows, _ = run_sql(args, "SELECT SQRT(ARRAY_SUM(ARRAY POWER(x, 2) FOR x IN d.v END)) AS nrm "
                            "FROM %s LIMIT 16" % valued_src(args))
    norms = [r["nrm"] for r in rows if isinstance(r.get("nrm"), (int, float))]
    if not norms:
        return
    lo, hi = min(norms), max(norms)
    if lo < 0.9 or hi > 1.1:
        print("WARNING: sampled vector norms are not ~1 (min %.3g, max %.3g).\n"
              "  Euclidean k-means over un-normalized vectors clusters by MAGNITUDE, not\n"
              "  meaning. For cosine semantics, embed with unit-normalized output --\n"
              "  ollama: use /api/embed (batch API, unit vectors), NOT /api/embeddings\n"
              "  (raw magnitudes) -- or normalize the vectors before fitting."
              % (lo, hi), file=sys.stderr)


def cmd_fit(args):
    check_norms(args)
    # Deterministic farthest-first init (the deterministic cousin of k-means++):
    # seed with the first valued vector, then K-1 times take the doc that maximizes
    # the distance to its nearest existing seed. Each step is one SQL++ scan, capped
    # at --init-limit rows: farthest-first over a few thousand rows places seeds
    # essentially as well as over everything, at a fraction of the K-1-scan cost.
    saved_limit = args.limit
    if args.init_limit and (not args.limit or args.limit > args.init_limit):
        args.limit = args.init_limit
    rows, _ = run_sql(args, "SELECT d.v FROM %s LIMIT 1" % valued_src(args))
    if not rows:
        sys.exit("keyspace %s has no rows with a valued %r field" % (args.keyspace, args.vec_field))
    cents = [rows[0]["v"]]
    while len(cents) < args.k:
        stmt = ("WITH cents AS (%s) SELECT d.v FROM %s "
                "ORDER BY %s DESC LIMIT 1"
                % (json.dumps(cents), valued_src(args), min_dist_expr(args, "d.v")))
        rows, _ = run_sql(args, stmt)
        cents.append(rows[0]["v"])
    args.limit = saved_limit
    print("init: %d farthest-first seeds" % len(cents), file=sys.stderr)

    sizes = {}
    for it in range(1, args.iters + 1):
        new, sizes, dt = (lloyd_pure_sql if args.pure_sql else lloyd_driver_mean)(args, cents)
        shift = max(euclid(a, b) for a, b in zip(cents, new))
        cents = new
        print("iter %2d: shift %.6f, sizes %s (%.2fs)" %
              (it, shift, [sizes.get(i, 0) for i in range(args.k)], dt), file=sys.stderr)
        if shift < args.tol:
            break

    out = {
        "k": args.k, "dim": len(cents[0]), "metric": "euclidean",
        "keyspace": args.keyspace, "vec_field": args.vec_field, "id_field": args.id_field,
        "iterations": it, "final_shift": shift,
        "sizes": [sizes.get(i, 0) for i in range(args.k)],
        "centroids": cents,
    }
    with open(args.centroids, "w") as f:
        json.dump(out, f)
    print("wrote %s (k=%d, dim=%d, %d iterations)" % (args.centroids, args.k, out["dim"], it))


def load_centroids(args):
    try:
        with open(args.centroids) as f:
            return json.load(f)
    except FileNotFoundError:
        sys.exit("no %s -- run `fit` first (or pass --centroids)" % args.centroids)


def cmd_partition(args):
    model = load_centroids(args)
    cents = model["centroids"]
    vec, kid = bt(args.vec_field), bt(args.id_field)
    # One assignment pass materialized to a temp Parquet keyspace, then K cheap
    # filtered copies from it -- K x cheaper than re-running the argmin per part.
    # (No WITH on INSERT: the grammar accepts WITH only on a SELECT, so the
    # centroid list is inlined into the argmin expression.)
    tmp = os.path.join(args.data, "default", "tmp_assign")
    if os.path.isdir(tmp):
        for f in os.listdir(tmp):
            os.remove(os.path.join(tmp, f))
        os.rmdir(tmp)
    stmt = ("INSERT INTO `tmp_assign/data.parquet` (KEY UUID(), VALUE self) "
            "SELECT d.%s AS id, d.%s AS vec, %s AS cl FROM %s d WHERE d.%s IS VALUED"
            % (kid, vec, argmin_expr(args, "d.%s" % vec, json.dumps(cents)),
               bt(args.keyspace), vec))
    rows, dt = run_sql(args, stmt)
    print("assigned: %s (%.2fs)" % (rows[0] if rows else "?", dt))
    total = 0
    for k in range(len(cents)):
        stmt = ("INSERT INTO `part_%d/data.parquet` (KEY UUID(), VALUE self) "
                "SELECT d.id AS id, d.vec AS vec FROM tmp_assign d WHERE d.cl = %d"
                % (k, k))
        rows, dt = run_sql(args, stmt)
        n = rows[0].get("inserted", "?") if rows else "?"
        print("part_%d: %s rows (%.2fs)" % (k, n, dt))
        total += 1
    for f in os.listdir(tmp):
        os.remove(os.path.join(tmp, f))
    os.rmdir(tmp)
    # The centroids as data: a queryable keyspace, so SQL++ can join/scan them
    # (e.g. distance-to-every-centroid reports) without this script.
    cdir = os.path.join(args.data, "default", "centroids")
    os.makedirs(cdir, exist_ok=True)
    with open(os.path.join(cdir, "centroids.jsonl"), "w") as f:
        for k, c in enumerate(cents):
            f.write(json.dumps({"id": "c%d" % k, "part": "part_%d" % k, "vec": c,
                                "size": model["sizes"][k] if k < len(model.get("sizes", [])) else None}) + "\n")
    print("wrote %d partition keyspaces + the centroids/ keyspace under %s" % (total, args.data))


def cmd_census(args):
    model = load_centroids(args)
    cents = model["centroids"]
    vec, kid = bt(args.vec_field), bt(args.id_field)

    if args.text_field:
        # Fail LOUD when the text field has no valued rows: a projection of a MISSING
        # field is silently dropped by SQL++ (and by the parquet writer), so a wrong
        # field name would otherwise just yield empty top_terms for every cluster with
        # no error anywhere (n1k1-for-ai feedback). Classic cause: @vectorize_field
        # outputs {id, text, vec} -- the text is named `text`, not the source field.
        rows, _ = run_sql(args, "SELECT COUNT(1) AS n FROM %s d WHERE d.%s IS VALUED"
                          % (bt(args.keyspace), bt(args.text_field)))
        if not rows or not rows[0].get("n"):
            sys.exit("census: --text-field %r has NO valued rows in keyspace %r.\n"
                     "  A missing field is silently dropped from SQL++ projections (and from\n"
                     "  materialized parquet), so this would otherwise report empty top_terms\n"
                     "  everywhere. Check the real column names: SELECT * FROM %s LIMIT 1\n"
                     "  (note: @vectorize_field emits {id, text, vec} -- the embedded text is\n"
                     "  always named `text`, not the source field's name)."
                     % (args.text_field, args.keyspace, args.keyspace))

    sizes_stmt = ("WITH cents AS (%s) SELECT cl, COUNT(1) AS n FROM %s d "
                  "LET cl = %s WHERE d.%s IS VALUED GROUP BY cl ORDER BY cl"
                  % (json.dumps(cents), bt(args.keyspace), argmin_expr(args, "d.%s" % vec), vec))
    sizes, _ = run_sql(args, sizes_stmt)
    by_cl = {int(r["cl"]): r["n"] for r in sizes}
    total = sum(by_cl.values())

    show = "".join(", d.%s AS %s" % (bt(f), bt(f)) for f in
                   (args.show.split(",") if args.show else []) if f)
    report = []
    for k, c in enumerate(cents):
        # Exemplars: the docs nearest this centroid. The centroid is a constant
        # here, so the engine's VECTOR_DISTANCE applies (and on a Parquet vec
        # column this is the fused columnar top-K path).
        ex_stmt = ("WITH qv AS (%s) SELECT d.%s AS id%s, "
                   "ROUND(VECTOR_DISTANCE(d.%s, qv, \"euclidean\"), 4) AS dist "
                   "FROM %s d WHERE d.%s IS VALUED ORDER BY dist LIMIT %d"
                   % (json.dumps(c), kid, show, vec, bt(args.keyspace), vec, args.exemplars))
        exemplars, _ = run_sql(args, ex_stmt)
        entry = {"cluster": k, "size": by_cl.get(k, 0),
                 "pct": round(100.0 * by_cl.get(k, 0) / total, 1) if total else 0,
                 "exemplars": exemplars}
        if args.text_field:
            tf = bt(args.text_field)
            terms_stmt = ("WITH cents AS (%s) SELECT t, COUNT(1) AS n FROM %s d "
                          "UNNEST TOKENS(LOWER(d.%s)) AS t LET cl = %s "
                          "WHERE d.%s IS VALUED AND cl = %d "
                          "GROUP BY t ORDER BY n DESC, t LIMIT %d"
                          % (json.dumps(cents), bt(args.keyspace), tf,
                             argmin_expr(args, "d.%s" % vec), vec, k, args.terms))
            terms, _ = run_sql(args, terms_stmt)
            entry["top_terms"] = terms
        report.append(entry)

    print(json.dumps({"keyspace": args.keyspace, "docs": total, "clusters": report}, indent=2))


def cmd_probe(args):
    model = load_centroids(args)
    cents = model["centroids"]
    qv = json.loads(args.qvec)

    # Brute force over the original keyspace: the ground truth (and the baseline time).
    vec, kid = bt(args.vec_field), bt(args.id_field)
    brute_stmt = ("WITH qv AS (%s) SELECT d.%s AS id, "
                  "VECTOR_DISTANCE(d.%s, qv, \"euclidean\") AS dist "
                  "FROM %s d WHERE d.%s IS VALUED ORDER BY dist LIMIT %d"
                  % (json.dumps(qv), kid, vec, bt(args.keyspace), vec, args.topk))
    brute, brute_t = run_sql(args, brute_stmt)
    brute_ids = [r["id"] for r in brute]

    # IVF probe: nearest nprobe centroids (computed here -- K tiny), scan only those parts.
    order = sorted(range(len(cents)), key=lambda k: euclid(qv, cents[k]))
    probed = order[:args.nprobe]
    hits, probe_t = [], 0.0
    for k in probed:
        stmt = ("WITH qv AS (%s) SELECT d.id AS id, "
                "VECTOR_DISTANCE(d.vec, qv, \"euclidean\") AS dist "
                "FROM `part_%d` d ORDER BY dist LIMIT %d"
                % (json.dumps(qv), k, args.topk))
        rows, dt = run_sql(args, stmt)
        hits += rows
        probe_t += dt
    hits.sort(key=lambda r: r["dist"])
    probe_ids = [r["id"] for r in hits[:args.topk]]

    recall = len(set(brute_ids) & set(probe_ids)) / float(len(brute_ids)) if brute_ids else 0.0
    print(json.dumps({
        "topk": args.topk, "nprobe": args.nprobe, "partitions_probed": ["part_%d" % k for k in probed],
        "recall_vs_brute": round(recall, 3),
        "brute_seconds": round(brute_t, 3), "probe_seconds": round(probe_t, 3),
        "note": "wall times include CLI startup per statement; relative shape holds, "
                "absolute engine-side gap is larger",
        "brute_top": brute, "probe_top": hits[:args.topk],
    }, indent=2))


# ---------------------------------------------------------------- CLI


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--data", required=True, help="dataRoot dir (the n1k1 datastore)")
        p.add_argument("--keyspace", default="vecs", help="keyspace holding the vectors (default: vecs)")
        p.add_argument("--vec-field", default="vec", help="vector field name (default: vec)")
        p.add_argument("--id-field", default="id", help="doc id field name (default: id)")
        p.add_argument("--centroids", default=None, help="centroids.json path (default: <data>/centroids.json)")
        p.add_argument("--n1k1", dest="n1k1_bin", default=None, help="n1k1 binary (default: $N1K1, ./n1k1, or PATH)")
        p.add_argument("--use-udf", action="store_true",
                       help="use the vector_nearest() goja UDF (loads -ext extensions/functions) "
                            "instead of the pure-SQL++ argmin")

    p = sub.add_parser("fit", help="k-means fit; writes centroids.json")
    common(p)
    p.add_argument("--k", type=int, default=8, help="number of clusters (default: 8)")
    p.add_argument("--iters", type=int, default=20, help="max Lloyd iterations (default: 20)")
    p.add_argument("--tol", type=float, default=1e-4, help="stop when max centroid shift < tol")
    p.add_argument("--limit", type=int, default=0,
                   help="fit on the first N rows only (sampled fit; 0 = all rows)")
    p.add_argument("--init-limit", type=int, default=5000,
                   help="rows scanned per farthest-first init step (0 = all; default 5000)")
    p.add_argument("--pure-sql", action="store_true",
                   help="run each Lloyd iteration as ONE SQL++ statement (assignment + "
                        "element-wise mean in-engine; the expressibility demo -- slower at "
                        "scale, see lloyd_driver_mean's docstring)")
    p.set_defaults(fn=cmd_fit)

    p = sub.add_parser("partition", help="write per-cluster part_<k>/data.parquet + centroids/ keyspace")
    common(p)
    p.set_defaults(fn=cmd_partition)

    p = sub.add_parser("census", help="per-cluster sizes + exemplar docs (+ top terms)")
    common(p)
    p.add_argument("--exemplars", type=int, default=5, help="exemplar docs per cluster (default: 5)")
    p.add_argument("--text-field", default=None, help="text field for per-cluster top terms (optional)")
    p.add_argument("--terms", type=int, default=8, help="top terms per cluster (default: 8)")
    p.add_argument("--show", default=None, help="extra comma-separated doc fields to include in exemplars")
    p.set_defaults(fn=cmd_census)

    p = sub.add_parser("probe", help="IVF probe query vs brute force: recall + timing")
    common(p)
    p.add_argument("--qvec", required=True, help="query vector as a JSON array")
    p.add_argument("--topk", type=int, default=10)
    p.add_argument("--nprobe", type=int, default=2, help="partitions to probe (default: 2)")
    p.set_defaults(fn=cmd_probe)

    args = ap.parse_args()
    args.n1k1_bin = find_n1k1(args.n1k1_bin)
    if not args.centroids:
        args.centroids = os.path.join(args.data, "centroids.json")
    args.fn(args)


if __name__ == "__main__":
    main()
