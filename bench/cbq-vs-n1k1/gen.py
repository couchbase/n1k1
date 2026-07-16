#!/usr/bin/env python3
"""Generate a deterministic file-datastore dataset for the cbq-vs-n1k1 benchmark.

Layout is the classic cbq file-datastore shape that BOTH engines read
(<root>/<namespace>/<keyspace>/<key>.json, one JSON doc per file):

    <root>/default/orders/order-00000.json ...

Idempotent: if the keyspace already holds exactly the requested count, it is
left untouched, so re-running the benchmark doesn't rewrite the data.

Usage: gen.py <root-dir> <n-orders>
"""
import os
import sys
import json
import random

CATS = ["books", "toys", "food", "tech", "home"]


def main():
    root, n = sys.argv[1], int(sys.argv[2])
    ks = os.path.join(root, "default", "orders")
    # Idempotence: skip regen when the dir already has n files.
    if os.path.isdir(ks) and sum(1 for _ in os.scandir(ks)) == n:
        print(f"gen: {ks} already has {n} docs (skipping)")
        return
    os.makedirs(ks, exist_ok=True)
    for f in os.scandir(ks):  # clear stale
        os.remove(f.path)
    random.seed(42)  # deterministic
    for i in range(n):
        d = {
            "id": i,
            "custId": "c%d" % (i % 37),
            "amount": round(random.uniform(1, 1000), 2),
            "category": CATS[i % len(CATS)],
            "qty": 1 + i % 9,
        }
        with open(os.path.join(ks, "order-%06d.json" % i), "w") as fp:
            json.dump(d, fp)
    print(f"gen: wrote {n} docs to {ks}")


if __name__ == "__main__":
    main()
