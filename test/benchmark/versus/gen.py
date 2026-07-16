#!/usr/bin/env python3
"""Generate a deterministic file-datastore dataset for the cbq-vs-n1k1 benchmark.

Classic cbq file-datastore layout that BOTH engines read
(<root>/<namespace>/<keyspace>/<key>.json, one JSON doc per file):

    <root>/default/orders/order-000000.json ...   (n docs)
    <root>/default/cust/c0.json ...                (NCUST docs, key = custId)

Each order has an `items` array (for UNNEST) and a `custId` that references a
`cust` doc by key (for joins: `... JOIN cust c ON KEYS o.custId`).

Idempotent: skips regen when orders already holds exactly n files.

Usage: gen.py <root-dir> <n-orders>
"""
import os
import sys
import json
import random

CATS = ["books", "toys", "food", "tech", "home"]
SKUS = ["sku-a", "sku-b", "sku-c", "sku-d", "sku-e", "sku-f"]
TIERS = ["bronze", "silver", "gold"]
NCUST = 37


def write(path, obj):
    with open(path, "w") as fp:
        json.dump(obj, fp)


def main():
    root, n = sys.argv[1], int(sys.argv[2])
    orders = os.path.join(root, "default", "orders")
    cust = os.path.join(root, "default", "cust")

    if os.path.isdir(orders) and sum(1 for _ in os.scandir(orders)) == n \
            and os.path.isdir(cust):
        print(f"gen: {orders} already has {n} docs (skipping)")
        return

    for d in (orders, cust):
        os.makedirs(d, exist_ok=True)
        for f in os.scandir(d):
            os.remove(f.path)

    random.seed(42)  # deterministic

    for c in range(NCUST):
        write(os.path.join(cust, "c%d.json" % c),
              {"id": "c%d" % c, "name": "Customer %d" % c, "tier": TIERS[c % 3]})

    for i in range(n):
        nitems = 2 + (i % 3)  # 2..4 line items
        items = [{"sku": SKUS[(i + j) % len(SKUS)], "qty": 1 + (i + j) % 5}
                 for j in range(nitems)]
        write(os.path.join(orders, "order-%06d.json" % i), {
            "id": i,
            "custId": "c%d" % (i % NCUST),
            "amount": round(random.uniform(1, 1000), 2),
            "category": CATS[i % len(CATS)],
            "qty": 1 + i % 9,
            "items": items,
        })
    print(f"gen: wrote {n} orders + {NCUST} cust to {root}/default")


if __name__ == "__main__":
    main()
