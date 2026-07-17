#!/usr/bin/env python3
"""Generate a deterministic file-datastore dataset for the cbq-vs-n1k1 benchmark.

Classic cbq file-datastore layout that BOTH engines read
(<root>/<namespace>/<keyspace>/<key>.json, one JSON doc per file):

    <root>/default/orders/order-000000.json ...   (n docs)
    <root>/default/cust/c0.json ...                (NCUST docs, key = custId)

Each order has an `items` array (for UNNEST) and a `custId` that references a
`cust` doc by key (for joins: `... JOIN cust c ON KEYS o.custId`).

Also writes a PACKED "container" keyspace -- the SAME order docs, but all in ONE
newline-delimited file (n1k1 reads .jsonl natively):

    <root>/default/orders_jsonl/data.jsonl        (n_jsonl docs, one JSON per line)

so the benchmark can contrast the syscall-heavy one-doc-per-file layout against a
single container file (see DESIGN-benchmark.md "file-layout lesson"). n_jsonl is a
separate knob (default = n) so it can be scaled far larger -- n1k1 is fast enough on
a container file that 20K docs is too quick to time well.

Idempotent: skips regen when orders holds exactly n files AND orders_jsonl holds
exactly n_jsonl lines.

Usage: gen.py <root-dir> <n-orders> [bulk_docs] [bulk_items] [n_jsonl]
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


def gen_item(i):
    return {"id": i, "custId": "c%d" % (i % NCUST),
            "amount": round(random.uniform(1, 1000), 2),
            "category": CATS[i % len(CATS)], "qty": 1 + i % 9}


def order_doc(i):
    """One order doc (same shape as an orders/*.json file), deterministic in i."""
    rnd = random.Random(1000 + i)  # per-doc seed -> layout-independent, reproducible
    nitems = 2 + (i % 3)  # 2..4 line items
    d = {"id": i, "custId": "c%d" % (i % NCUST),
         "amount": round(rnd.uniform(1, 1000), 2),
         "category": CATS[i % len(CATS)], "qty": 1 + i % 9,
         "items": [{"sku": SKUS[(i + j) % len(SKUS)], "qty": 1 + (i + j) % 5}
                   for j in range(nitems)]}
    return d


def main():
    root, n = sys.argv[1], int(sys.argv[2])
    # `bulk` scenario: a FEW docs each holding a large `items` array, so the volume
    # lives inside documents -> minimal file I/O, compute-bound, exercised by UNNEST.
    bulk_docs = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    bulk_items = int(sys.argv[4]) if len(sys.argv) > 4 else 20000
    # `packed` scenario: the same order docs in ONE container .jsonl (own doc count).
    n_jsonl = int(sys.argv[5]) if len(sys.argv) > 5 else n

    orders = os.path.join(root, "default", "orders")
    cust = os.path.join(root, "default", "cust")
    bulk = os.path.join(root, "default", "bulk")
    ojsonl = os.path.join(root, "default", "orders_jsonl")
    ojsonl_file = os.path.join(ojsonl, "data.jsonl")

    def jsonl_lines():
        try:
            with open(ojsonl_file) as fp:
                return sum(1 for _ in fp)
        except OSError:
            return -1

    if os.path.isdir(orders) and sum(1 for _ in os.scandir(orders)) == n \
            and os.path.isdir(cust) and os.path.isdir(bulk) \
            and sum(1 for _ in os.scandir(bulk)) == bulk_docs \
            and jsonl_lines() == n_jsonl:
        print(f"gen: {root}/default already current (skipping)")
        return

    for d in (orders, cust, bulk, ojsonl):
        os.makedirs(d, exist_ok=True)
        for f in os.scandir(d):
            os.remove(f.path)

    random.seed(42)  # deterministic (cust + bulk)

    for c in range(NCUST):
        write(os.path.join(cust, "c%d.json" % c),
              {"id": "c%d" % c, "name": "Customer %d" % c, "tier": TIERS[c % 3]})

    for i in range(n):
        write(os.path.join(orders, "order-%06d.json" % i), order_doc(i))

    # Packed container: the same order-doc shape, one compact JSON per line.
    with open(ojsonl_file, "w") as fp:
        for i in range(n_jsonl):
            fp.write(json.dumps(order_doc(i)) + "\n")

    big = [gen_item(i) for i in range(bulk_items)]
    for d in range(bulk_docs):
        write(os.path.join(bulk, "bulk-%d.json" % d), {"id": d, "items": big})

    print(f"gen: wrote {n} orders + {NCUST} cust + {bulk_docs} bulk"
          f" (x{bulk_items}-elem items) + {n_jsonl}-doc orders_jsonl to {root}/default")


if __name__ == "__main__":
    main()
