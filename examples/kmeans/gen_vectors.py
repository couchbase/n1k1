#!/usr/bin/env python3
"""Generate a synthetic clustered-vector dataset for the k-means / IVF demo.

Writes <out>/default/vecs/vecs.jsonl: N docs, each
  { "id": "d00042", "vec": [<dim> floats], "txt": "<a few topic words>" }

The vectors are drawn around K well-separated random unit "topic" centers
(gaussian noise), and each doc's txt carries that topic's vocabulary -- so a
k-means fit should rediscover the topics, and the cluster census's top-terms
and exemplars should visibly cohere. Deterministic via --seed.

This stands in for a real embedded dataset. On real data you'd produce vectors
with VECTORIZE_BATCH / @vectorize_field instead (see DESIGN-vectors.md) and
skip this script entirely.

Usage:
  python3 examples/kmeans/gen_vectors.py --out examples/kmeans/data \
      --n 2000 --dim 16 --clusters 5 --seed 42
"""

import argparse
import json
import math
import os
import random

VOCAB = [
    ["disk", "storage", "volume", "iops", "raid"],
    ["network", "latency", "packet", "tcp", "gateway"],
    ["auth", "login", "token", "session", "oauth"],
    ["billing", "invoice", "payment", "refund", "ledger"],
    ["search", "index", "query", "ranking", "recall"],
    ["deploy", "rollout", "canary", "build", "release"],
    ["memory", "heap", "alloc", "leak", "gc"],
    ["video", "codec", "frame", "bitrate", "stream"],
]


def unit_gaussian_vec(rng, dim):
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True, help="dataRoot to create (writes default/vecs/vecs.jsonl under it)")
    ap.add_argument("--n", type=int, default=2000, help="number of docs")
    ap.add_argument("--dim", type=int, default=16, help="vector dimensions")
    ap.add_argument("--clusters", type=int, default=5, help="number of true topic clusters (<= %d)" % len(VOCAB))
    ap.add_argument("--noise", type=float, default=0.15, help="gaussian noise stddev around each center")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    if args.clusters > len(VOCAB):
        ap.error("--clusters must be <= %d (the built-in vocabulary size)" % len(VOCAB))

    rng = random.Random(args.seed)
    centers = [unit_gaussian_vec(rng, args.dim) for _ in range(args.clusters)]

    ks_dir = os.path.join(args.out, "default", "vecs")
    os.makedirs(ks_dir, exist_ok=True)
    path = os.path.join(ks_dir, "vecs.jsonl")
    with open(path, "w") as f:
        for i in range(args.n):
            t = rng.randrange(args.clusters)
            vec = [round(c + rng.gauss(0.0, args.noise), 6) for c in centers[t]]
            words = rng.sample(VOCAB[t], 3)
            doc = {"id": "d%05d" % i, "vec": vec, "txt": " ".join(words)}
            f.write(json.dumps(doc) + "\n")
    print("wrote %d docs (%d-d vectors, %d true clusters) -> %s" % (args.n, args.dim, args.clusters, path))


if __name__ == "__main__":
    main()
