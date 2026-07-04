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
| `finance/` | J | CSV rows → JSON objects (header keys, inferred types) | `SELECT currency, SUM(amount) FROM txns GROUP BY currency` | 🟢 |
| `kb/` | L | docs & media → extract-provider rows (`{filename,kind,text,…}`) | `SELECT filename FROM default:docs WHERE text LIKE '%vacation%'` | 🟢 |
| `infra/` | Y | YAML → JSON records: `---` multi-doc streams + top-level sequences | `SELECT team, SUM(replicas) FROM services GROUP BY team` | 🟢 |

**A/B/C/E/H/J/L/Y work today.** Parquet (K) is a later phase; indexing
(`DESIGN-indexing.md`) is also later. CSV/TSV cells get light type inference
(numbers/bools/null, string fallback); leading-zero values like `007` stay
strings. The `extract` provider is pure-Go (no cgo): **text** from `.pdf`,
`.docx`/`.pptx`/`.xlsx` (zip+XML), `.txt`/`.log`/`.md`/`.markdown`, and `.rtf`;
**metadata only** for media — `.png`/`.jpg`/`.jpeg` (width/height) and
`.mp4`/`.mov` (duration/width/height/created). It's great for text documents, but
image text (OCR), scanned/exotic-font PDFs, and video transcription (ASR) need
the optional Tika/extractous+Tesseract backend (a later cgo build tag). Restrict
discovery with `-scan` groups: `doc`, `text`, `image`, `video`, or `extract`.

## File metadata (`_meta`)

Records can carry a `_meta` sub-object with the source file's `` `path` ``
(dir-relative, incl. subdirs), `name`, `ext`, `size` (bytes), and `mtime`
(RFC3339). Records that live inside a container file (JSONL, CSV, gzip,
JSON-array, YAML multi-doc/sequence) also get `pos` — their 0-based ordinal within that file.
Controlled by `-meta`:

- `-meta=auto` (default) — extracted documents/media get `_meta`; structured
  JSON/CSV data does not (so plain data stays clean).
- `-meta=on` — every record gets `_meta`.
- `-meta=off` — no record does.

```
n1k1 -c "SELECT d.filename, d._meta.size, d._meta.mtime FROM docs d ORDER BY d._meta.size" examples/kb
n1k1 -meta=on -c "SELECT c._meta.\`path\` FROM cpu c" examples/metrics
```

(`path` is a SQL++ reserved word, so query it back-quoted: `` _meta.`path` ``.
Metadata lives in the doc because the fork's `META()` only exposes a fixed field
set — id/cas/keyspace/…)

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

infra/           (Y) 🟢  YAML files decoded to JSON records
  default/services/api.yaml workers.yaml     (`---` multi-doc streams; 4 services unioned)
  default/regions/regions.yaml               (one top-level sequence; 3 regions, one per element)

kb/              (L) 🟢  one extract row per document/media file
  default/docs/handbook.pdf                 (PDF text — mentions "vacation")
  default/docs/q1-report.docx               (Word — mentions "vacation")
  default/docs/budget.xlsx                  (Excel — a small budget table)
  default/docs/deck.pptx                    (PowerPoint — 3 slides of text)
  default/docs/notes.txt readme.md memo.rtf (plain text / Markdown / RTF)
  default/media/logo.png                    (image — width/height metadata, no text)
  default/media/clip.mp4                     (video — duration/width/height/created)
```

## Regenerating the binaries

The JSON/JSONL files are plain text, checked in directly. The gzip and
PDF/DOCX/XLSX files are generated (minimal-but-valid, no third-party deps):

```
python3 examples/generate_binaries.py
```

## Inline charts: sparkline() & histogram() aggregates

n1k1 ships two extension aggregates that render a group's numbers as a little
unicode chart (▁▂▃▄▅▆▇█) — great for eyeballing shape at a glance. They are
**always available** (no loading needed) and, like every n1k1 aggregate, work
with `GROUP BY` or over the whole result:

- `sparkline(x)` — a mini line chart of the values **in scan order** (long
  series are downsampled to ~100 bars); use it for time series.
- `histogram(x)` — a bar chart of the value **distribution** across 20 buckets.

```sh
# CPU per host: a trend line (over time) + a value-distribution histogram.
# `value` is a reserved word, so back-quote it.
n1k1 -c "SELECT host, COUNT(*) AS n, sparkline(\`value\`) AS trend, histogram(\`value\`) AS dist
         FROM cpu GROUP BY host ORDER BY host" examples/metrics
```

```
host    n   trend                                              dist
hostA  48   ▂▂▁▁▁▁▁▂▂▃▃▄▄▅▅▅▆▅▅▅▄▄▃▃▄▃▃▃▂▃▃▃▄▅▅▆▇▇█████▇▇▆▅▅   ▆▁▃▃▆▃▅▃▃▃▅▁█▁▃▁▃▃▃▄
hostB  24   ▁▂▂█▁▁▁▂▂█▁▁▁▂▂█▁▁▁▁▂█▁▁                           █▆▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▃
hostC  24   ▁▂▂▂▃▃▄▄▅▅▆▆▇▇██████████                           ▂▂▁▂▂▂▂▁▂▂▁▂▂▂▂▁▂▁▂█
```

hostA's two-month daily wave, hostB's periodic batch-job spikes, and hostC's
cold-start ramp all read at a glance. A distribution histogram over order spend:

```sh
n1k1 -c "SELECT COUNT(*) AS orders, ROUND(AVG(total),2) AS avg, histogram(total) AS spend
         FROM orders" examples/shop
#  orders  avg     spend
#  20      97.47   █▇▅▂▂▄▁▂▁▁▂▁▂▁▁▁▁▁▁▂     (right-skewed: many small, a long tail)
```

## Loading JavaScript UDFs (`-ext` / `.ext`)

Scalar user-defined functions are plain JavaScript files whose base name is the
SQL++ function name. Point `-ext` at a directory (or a single file); the kind is
auto-detected from the file extension. `-ext` is repeatable and comma-friendly,
so you can load several dirs/files. See `extensions/functions/js/`.

```sh
n1k1 -ext extensions/functions/js \
     -c "SELECT o.customer, celsius_to_fahrenheit(20) AS f, slugify(o.customer) AS slug
         FROM orders o LIMIT 3" examples/shop
```

In the REPL, manage them live with `.extensions` (alias `.ext`):
`.extensions list` shows what's loaded, `.extensions load <dir-or-file>` adds
more, `.extensions unload <name>` disables one. Loading is opt-in — user code
runs in-process. The `sparkline`/`histogram` aggregates above need no loading.
See `DESIGN-extensions.md`.
