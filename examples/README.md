# n1k1 data-source examples

Sample input trees for the scenarios in `DESIGN-data.md` ("Worked examples"),
so you can see the on-disk layouts and try them from the CLI. Each top-level
directory here is a **dataRoot** you point the CLI at:

```
n1k1 -c "<statement>" examples/<root>
```

The datastore maps `<dataRoot>/<namespace>/<keyspace>/…`, and `-ns` defaults to
`default`, so `FROM default:orders` reads `<dataRoot>/default/orders/`.

Status legend (matches the design doc):
✅ works today · 🟢 MVP (dir-union + JSONL/multi-doc JSON + gzip) ·
🟡 later decoder (CSV/Parquet/zip/office).

| Dir | Doc | Scenario | Try | Status |
|---|---|---|---|---|
| `shop/` | A | one JSON doc per file (today's convention) | `SELECT * FROM default:orders WHERE total > 100` | ✅ |
| `sales/` | B | flat root — a bare dir of files = one keyspace | `SELECT * FROM sales` | 🟢 |
| `logs/` | C | multi-file keyspace, many records/file (JSONL) | `SELECT action, COUNT(*) FROM default:events GROUP BY action` | 🟢 |
| `metrics/` | E | deep/recursive tree, unkeyed union (JSONL) | `SELECT host, AVG(value) FROM default:cpu GROUP BY host` | 🟢 |
| `archive/` | H | transparent gzip (`.jsonl.gz`) | `SELECT * FROM default:orders` | 🟢 |
| `kb/` | L | office/unstructured docs → extractor rows | `SELECT filename, text FROM default:docs WHERE text LIKE '%vacation%'` | 🟡 |

Only **A works today**; B/C/E/H are the MVP target (this branch); L (office
extraction) is a later phase. Indexing (`DESIGN-indexing.md`) is also later.

## Layout details

```
shop/            (A) ✅  <root>/default/<keyspace>/<key>.json — one object per file
  default/orders/order-1001.json …          (3 orders)
  default/customers/alice.json …            (2 customers)

sales/           (B) 🟢  flat root: data files directly under the root, no ns/keyspace subdirs
  2026-01.json 2026-02.json 2026-03.json    (one monthly-summary object per file)

logs/            (C) 🟢  a keyspace = the union of many JSONL files (many records each)
  default/events/2026-01-0{1,2,3}.jsonl     (8 event records total)

metrics/         (E) 🟢  recurse subdirs; a keyspace = union of all files beneath it
  default/cpu/hostA/2026/01/data-0001.jsonl
  default/cpu/hostA/2026/02/data-0002.jsonl
  default/cpu/hostB/2026/01/data-0003.jsonl (5 samples total; host/date dirs are invisible)

archive/         (H) 🟢  transparent decompression by inner extension
  default/orders/2025.jsonl.gz 2026.jsonl.gz (gzip'd JSONL; 5 orders total)

kb/              (L) 🟡  one extractor row (or many) per document
  default/docs/handbook.pdf                 (PDF text — mentions "vacation")
  default/docs/q1-report.docx               (Word — mentions "vacation")
  default/docs/budget.xlsx                  (Excel — a small budget table)
```

## Regenerating the binaries

The JSON/JSONL files are plain text, checked in directly. The gzip and Office/PDF
files are generated (minimal-but-valid, no third-party deps):

```
python3 examples/generate_binaries.py
```
