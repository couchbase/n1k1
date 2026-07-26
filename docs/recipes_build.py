#!/usr/bin/env python3
"""Build the SQL++ recipes doc from its structured source.

Source of truth: docs/recipes.yaml — a plain YAML *sequence of recipe records*, so
n1k1 itself can query it:

    n1k1 -c 'SELECT title, sqlpp FROM recipes WHERE sqlpp LIKE "%UNNEST%"' docs/recipes.yaml

Renders to:
  - docs/recipes.md   — GitHub-friendly Markdown (SQL++ runnable; other dialects folded)
  - docs/recipes.html — self-contained interactive table (fixed switcher + table headers,
    a colspan description row per recipe, frozen SQL++ column)

Modes:
  python3 docs/recipes_build.py --md | --html | --all(default) | --check

`--check` runs every SQL++ example against ./n1k1 (over examples/shop where needed).
Only SQL++ is executed; the other dialects are hand-written reference translations.

The YAML is a deliberately small subset (block-literal `|-` values, single-quoted
inline scalars) parsed by a stdlib-only loader below — no PyYAML needed. n1k1 reads
the same file with its full YAML decoder. (Adding TOML support to n1k1 would let the
source be TOML too; for now YAML is the format n1k1 can slice and dice.)
"""

import sys, os, re, html, subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(HERE, "recipes.yaml")

# ---- presentation config (the YAML owns the queryable recipe DATA; this owns chrome)

DIALECTS = [
    {"id": "sqlpp", "label": "SQL++", "primary": True},
    {"id": "sql", "label": "SQL (Postgres)"},
    {"id": "duckdb", "label": "SQL (DuckDB)"},
    {"id": "js", "label": "JavaScript"},
    {"id": "python", "label": "Python"},
    {"id": "mongo", "label": "MongoDB"},
    {"id": "jq", "label": "jq"},
]

# per-dialect identity hue (light, dark) — color encodes *which dialect*
DIALECT_HUE = {
    "sqlpp": ("#4f46e5", "#8b93ff"),   # indigo — the hero
    "sql": ("#2f6fb3", "#6fa8dc"),     # postgres blue
    "duckdb": ("#c2892a", "#e3b341"),  # duck amber
    "js": ("#c2410c", "#e0965a"),      # burnt orange
    "python": ("#356a9a", "#6fa8d6"),  # steel blue
    "mongo": ("#128a4a", "#3ddc84"),   # leaf green
    "jq": ("#6b7280", "#9aa1ad"),      # slate
}

SECTION_INTROS = {
    "2. Iterating & projecting":
        "The pivot between the models: SQL++/SQL/DuckDB scan a collection, JS/Python "
        "loop, MongoDB pipelines, jq streams.",
    "5. Aggregating & grouping":
        "SQL's home turf — GROUP BY and aggregates — meets jq's group_by / unique / "
        "add / max_by. MongoDB's aggregation pipeline is the natural fit here.",
    "8. Recursion & deep search":
        "SQL++'s answer to jq's .. is the WITHIN operator (descend into any nested "
        "value) plus ANY … WITHIN … SATISFIES. Postgres uses jsonpath ($..). "
        "DuckDB/JS/Python/Mongo have no clean one-liner — you recurse.",
}

HTML_TITLE = "SQL++ recipes"
HTML_SUB = ("The same JSON maneuver in SQL++, SQL (Postgres & DuckDB), JavaScript, Python, MongoDB, and jq.")
HTML_FOOTER = ("Generated from docs/recipes.yaml by docs/recipes_build.py")

PREAMBLE = """# SQL++ recipes — a SQL++ / SQL / jq Rosetta stone

_Slicing and dicing JSON: the same maneuver across seven tools._

> **This file is generated** from `docs/recipes.yaml` by `docs/recipes_build.py`.
> Edit the `.yaml`, not this `.md`. An interactive HTML version with toggleable
> dialect columns lives at `docs/recipes.html`. And because the source is a plain
> YAML sequence of records, **n1k1 can query it directly**:
>
> ```sh
> n1k1 -c 'SELECT r.section, COUNT(*) AS n FROM recipes r GROUP BY r.section' docs/recipes.yaml
> ```

This guide is about doing JSON surgery in **SQL++** (the N1QL dialect n1k1 runs),
cross-translated so you can lean on what you already know. Each recipe shows SQL++
first (the runnable column), then SQL (PostgreSQL `jsonb`), then jq — and the HTML
view adds **DuckDB, JavaScript, Python, and MongoDB** columns you can toggle on.

## Running the examples

**Every SQL++ block is a complete statement you can copy and run** — each was
executed against n1k1 and its output shown as a `→` comment. Paste one into `-c`:

```sh
n1k1 -c 'SELECT ARRAY_SUM([1,2,3]) AS total'      # → {"total":6}
```

Blocks that read literal arrays (`FROM [ … ] AS x`) are self-contained. Blocks that
read `FROM orders` / `FROM customers` use the bundled shop dataset — pass it as the
data root:

```sh
n1k1 -c 'SELECT * FROM orders LIMIT 1' examples/shop
# {"type":"order","id":"1005","customer":"dave","total":22.0,"items":1,"status":"shipped","ts":"2026-01-06"}
```

## The three mental models

| | SQL++ | SQL | jq |
|---|---|---|---|
| **Unit of work** | a collection of documents | rows in a table | one JSON value, streamed |
| **JSON is** | the native value type | a `json`/`jsonb` column | the whole world |
| **"for each element"** | `FROM arr AS x` / `UNNEST` | `jsonb_array_elements(col)` | `.[]` |
| **transform each** | `SELECT f …` / `ARRAY f FOR x IN … END` | `SELECT f FROM …` | `map(f)` |
| **build an object** | `{"a": x}` | `jsonb_build_object('a', x)` | `{a: .x}` |
| **object ↔ pairs** | `OBJECT_PAIRS` / `OBJECT … FOR … END` | `jsonb_each` / `jsonb_object_agg` | `to_entries`/`from_entries` |

**SQL++ and SQL treat every task as a query over a collection; jq treats it as a
stream rewrite.** DuckDB is SQL with list comprehensions; JS/Python are imperative
map/filter; MongoDB is a document pipeline (its twin of jq's pipe *and* SQL's
`GROUP BY`). What makes SQL++ special is that it has jq's JSON-surgery verbs
(`ARRAY … FOR`, `OBJECT … FOR`, `WITHIN`, `UNNEST`, `OBJECT_PAIRS`) *inside* a set
query — so you rarely have to choose.
"""

OUTRO = """
## Gotchas worth pinning

- **0-based `SUBSTR`/`POSITION`** — `SUBSTR("hello",1,3)` → `"ell"`. (SQL's are 1-based; DuckDB lists/strings are 1-based too.)
- **`OBJECT_PAIRS` yields `{name, val}`**, not jq's `{key, value}`.
- **Object keys must be strings** — wrap non-string keys in `TO_STRING`.
- **Constructed-object keys render sorted** (canonical JSON); top-level `SELECT` columns keep their written order.
- **Negative array index wraps** to the end (`a[-1]` = last); an out-of-range positive index — and a slice whose end runs past the array — is `MISSING`. Clamp with `LEAST(end, ARRAY_LENGTH(a))`; guard `i >= 0` on computed indices.
- **`OBJECT_PAIRS`/`OBJECT_NAMES` work on objects only** (an array → `NULL`); index arrays positionally.
- **`ARRAY_FLATTEN` needs an explicit depth** (jq flattens fully).
- **Reserved words need backticks** — `` `value` ``, `` `last` ``, `` `type` `` as identifiers.
- **`UNION`/`UNION ALL` align by field name**, not position — alias every column.

## MISSING vs NULL — the SQL++ superpower

The one concept with no jq or standard-SQL equivalent. **`MISSING`** = the field
isn't there (SQL models this as `NULL`; jq as an error needing `?`). **`NULL`** =
the field is present with value `null`. n1k1 keeps them distinct: a `MISSING` field
is *omitted* from output, a `NULL` field is rendered. So `x.foo IS MISSING` ≠
`x.foo IS NULL` (use `IS VALUED` for "present and not null"), and aggregates/array
builders skip `MISSING` — which is why jq's `.foo?` needs no translation.

## See also

- The jq manual (`jqlang.org/manual`) and Remy Sharp's jq recipes (`remysharp.com/drafts/jq-recipes`).
- `examples/queries/*.sql++` — runnable SQL++ showpieces (Conway's Life, Mandelbrot, unicode charts).
- Couchbase N1QL function reference — n1k1 speaks the same dialect.
"""


# ------------------------------------------------------- stdlib YAML subset ----

def load_yaml_recipes(path):
    """Parse our recipes.yaml subset: a sequence of records whose values are either
    single-quoted inline scalars or `|-` block literals (indent 2 for keys, 4 for
    block bodies)."""
    lines = open(path).read().split("\n")
    recs, cur, i, N = [], None, 0, len(lines)

    def unq(v):
        v = v.strip()
        if len(v) >= 2 and v[0] == "'" and v[-1] == "'":
            return v[1:-1].replace("''", "'")
        return v

    while i < N:
        raw = lines[i]
        s = raw.strip()
        if not s or s.startswith("#"):
            i += 1
            continue
        if raw.startswith("- "):
            cur = {}
            recs.append(cur)
            keytext = raw[2:]
        elif raw.startswith("  "):
            keytext = raw[2:]
        else:
            i += 1
            continue
        key, sep, val = keytext.partition(":")
        if not sep:
            i += 1
            continue
        key, val = key.strip(), val.strip()
        if val in ("|-", "|"):
            i += 1
            body = []
            while i < N:
                bl = lines[i]
                if bl.strip() == "":
                    body.append("")
                    i += 1
                    continue
                if bl.startswith("    "):
                    body.append(bl[4:])
                    i += 1
                    continue
                break
            while body and body[-1] == "":
                body.pop()
            cur[key] = "\n".join(body)
        else:
            cur[key] = unq(val)
            i += 1
    return recs


def load():
    """Return an ordered list of (section_title, [recipe, …])."""
    recs = load_yaml_recipes(SRC)
    order, by = [], {}
    for r in recs:
        s = r.get("section", "")
        if s not in by:
            by[s] = []
            order.append(s)
        by[s].append(r)
    return [(s, by[s]) for s in order]


def secondary():
    return [d for d in DIALECTS if not d.get("primary")]


def full_sqlpp(r):
    """The runnable SQL++: the concise query the table shows, prefixed with a
    `WITH <bind>` that binds the recipe's source data to a name (so the query — like
    the other dialects — just references that name instead of inlining a literal)."""
    s = (r.get("sqlpp") or "").strip()
    b = (r.get("bind") or "").strip()
    return f"WITH {b}\n{s}" if b else s


def short_title(t):
    """A compact label for the table of contents (drop the ' — …' / ' (…)' tail)."""
    return t.split(" — ")[0].split(" (")[0].strip()


def slugify(t):
    """A short, URL-safe anchor slug for a recipe — the head of the title, kebab-cased
    (e.g. 'map — transform every element' → 'map'). A recipe may override it with an
    explicit `slug:` field. Callers dedupe collisions."""
    base = short_title(t).split(",")[0]
    base = re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-")
    return base or "recipe"


# dialects with a one-liner CLI: (prefix, suffix) wrapped around the collapsed snippet.
CLI = {
    "sqlpp": ("n1k1 -c '", "'"),
    "sql": ('psql -c "', '"'),
    "duckdb": ('duckdb -c "', '"'),
    "mongo": ("mongosh --eval '", "'"),
    "jq": ("jq '", "'"),
}


def cli_text(did, code, r):
    """The 'full command line' form of a cell, or None if this dialect has no CLI."""
    if did not in CLI:
        return None
    snippet = full_sqlpp(r) if did == "sqlpp" else code
    if not snippet or snippet.strip() == "—":
        return None
    pre, suf = CLI[did]
    cmd = pre + " ".join(snippet.split()) + suf
    if did == "sqlpp" and r.get("needs") == "shop":
        cmd += " examples/shop"
    return cmd


# ---------------------------------------------------------------- Markdown ----

def render_md():
    prim = next(d for d in DIALECTS if d.get("primary"))
    out = [PREAMBLE.rstrip(), ""]
    for title, recipes in load():
        out.append(f"## {title}\n")
        if SECTION_INTROS.get(title):
            out.append(SECTION_INTROS[title].strip() + "\n")
        for r in recipes:
            out.append(f"### {r['title']}")
            if r.get("note"):
                out.append(r["note"].strip())
            if r.get("data"):
                out.append("_Source data:_\n```\n" + r["data"].strip() + "\n```")
            elif r.get("needs") == "shop":
                out.append("_Over the shop `orders` / `customers` keyspaces._")
            sqlpp = full_sqlpp(r)
            if sqlpp:
                block = sqlpp + (f"\n-- → {r['out'].strip()}" if r.get("out") else "")
                out.append("```sql\n" + block + "\n```")
            rows = []
            for d in secondary():
                code = (r.get(d["id"]) or "").strip()
                if d["id"] in ("sql", "duckdb"):
                    code = " ".join(code.split())   # one line for the fold's table cell
                if code and code != "—":
                    rows.append(f"| **{d['label']}** | `{code.replace(chr(124), chr(92)+chr(124))}` |")
            if rows:
                out.append("<details><summary>other dialects</summary>\n")
                out += ["| | |", "|---|---|"] + rows + ["\n</details>"]
            out.append("")
    out.append(OUTRO.rstrip() + "\n")
    return "\n".join(out)


# -------------------------------------------------------------------- HTML ----

CSS = """
:root{
  --bg:#fcfcfd;--fg:#1c2024;--muted:#626772;--faint:#8b909a;
  --line:#e7e9ee;--line2:#eef0f3;--sticky:#fcfcfd;--band:#f3f4f8;--desc:#f7f8fb;
  --accent:#4f46e5;--sqlpp:#eef0ff;--sqlpp-edge:#c9ccff;--focus:#4f46e5;--tb:52px;
  --gutter:16px;   /* shared left edge: TOC toggle, TOC text, and the SQL++ column */
}
@media(prefers-color-scheme:dark){:root{
  --bg:#0f1216;--fg:#dfe3ea;--muted:#8a919e;--faint:#5c636e;
  --line:#232a33;--line2:#1a1f27;--sticky:#0f1216;--band:#161b22;--desc:#141922;
  --accent:#8b93ff;--sqlpp:#161a2e;--sqlpp-edge:#2b3168;--focus:#8b93ff;
}}
:root[data-theme=light]{--bg:#fcfcfd;--fg:#1c2024;--muted:#626772;--faint:#8b909a;
  --line:#e7e9ee;--line2:#eef0f3;--sticky:#fcfcfd;--band:#f3f4f8;--desc:#f7f8fb;
  --accent:#4f46e5;--sqlpp:#eef0ff;--sqlpp-edge:#c9ccff;--focus:#4f46e5;}
:root[data-theme=dark]{--bg:#0f1216;--fg:#dfe3ea;--muted:#8a919e;--faint:#5c636e;
  --line:#232a33;--line2:#1a1f27;--sticky:#0f1216;--band:#161b22;--desc:#141922;
  --accent:#8b93ff;--sqlpp:#161a2e;--sqlpp-edge:#2b3168;--focus:#8b93ff;}
*{box-sizing:border-box}
html,body{margin:0;height:100%;overflow:hidden}
body{color:var(--fg);background:var(--bg);
  font:15px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  text-rendering:optimizeLegibility}
a{color:var(--accent);text-underline-offset:2px}
/* app = a full-viewport column: a fixed top bar over a single scroller. This makes
   the table's own thead reliably sticky (a real bounded scroll ancestor) even when
   embedded in an auto-height iframe. */
.app{height:100dvh;display:flex;flex-direction:column;overflow:hidden}
/* .inner spans the full scroll width (= the table's), so a sticky-left child stays
   pinned across the ENTIRE horizontal scroll — a sticky element only sticks within
   its containing block, and a plain block child of .wrap is only viewport-wide */
.inner{width:max-content;min-width:100%}
/* header — inside the scroller: scrolls away vertically, but its text stays pinned
   left (sticky left:0) so it doesn't drift when the table scrolls horizontally */
header{width:100%;border-bottom:1px solid var(--line);background:var(--bg)}
.hin{position:sticky;left:0;max-width:min(96vw,900px);padding:22px var(--gutter) 18px}
h1{margin:0 0 8px;font-size:24px;font-weight:680;letter-spacing:-.015em;text-wrap:balance;max-width:30ch}
.sub{color:var(--muted);font-size:13.5px;line-height:1.5;max-width:82ch}
/* toolbar — a fixed top bar (dialect switcher + filter); never scrolls */
.toolbar{flex:none;position:relative;z-index:5;background:var(--sticky);
  border-bottom:1px solid var(--line);padding:10px var(--gutter);min-height:var(--tb);
  display:flex;flex-wrap:wrap;gap:8px 9px;align-items:center}
.toolbar .lbl{font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:var(--faint);margin-right:2px}
.pill{font-size:12.5px;display:inline-flex;gap:6px;align-items:center;cursor:pointer;
  padding:4px 11px 4px 9px;border:1px solid var(--line);border-radius:999px;
  user-select:none;color:var(--muted);transition:border-color .12s,color .12s}
.pill:hover{border-color:var(--faint)}
.pill:has(input:checked){color:var(--fg);border-color:color-mix(in srgb,var(--d) 55%,var(--line))}
.pill input{position:absolute;opacity:0;width:0;height:0}
.pill:focus-within{outline:2px solid var(--focus);outline-offset:2px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--d);flex:none;
  box-shadow:0 0 0 3px color-mix(in srgb,var(--d) 16%,transparent)}
.pill:not(:has(input:checked)) .dot{background:var(--faint);box-shadow:none}
.toolbar .lead{font-size:13px;font-weight:600;color:var(--fg);margin-right:2px}
.toc-toggle{order:-1;flex:none;margin-left:-8px;margin-right:8px;   /* the primary top-left anchor */
  border:1px solid var(--line);background:transparent;color:var(--muted);
  border-radius:7px;padding:4px 9px;cursor:pointer;font-size:14px;line-height:1}
.toc-toggle:hover{border-color:var(--faint);color:var(--fg)}
.toc-toggle:focus-visible{outline:2px solid var(--focus);outline-offset:2px}
/* body = the TOC sidebar + the table scroller */
.body{flex:1;display:flex;min-height:0}
.toc{flex:none;width:238px;overflow:auto;border-right:1px solid var(--line);
  background:var(--bg);padding:6px 0 24px}
.app.toc-off .toc{display:none}
.toc-head{font-size:10px;font-weight:680;text-transform:uppercase;letter-spacing:.09em;
  color:var(--faint);padding:10px var(--gutter) 4px}
.toc ul{list-style:none;margin:0;padding:0}
.toc-sec{font-size:10px;font-weight:680;letter-spacing:.04em;text-transform:uppercase;
  color:var(--muted);padding:7px var(--gutter);margin-top:6px;line-height:1.35;
  border-left:3px solid var(--sc, transparent);padding-left:calc(var(--gutter) - 3px);
  background:color-mix(in srgb, var(--sc, transparent) 13%, var(--bg))}
.toc a{display:block;padding:4px 14px 4px var(--gutter);font-size:12.5px;color:var(--muted);
  text-decoration:none;border-left:2px solid transparent;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.toc a:hover{color:var(--fg);background:var(--line2)}
.toc a.active{color:var(--fg);font-weight:550;border-left-color:var(--accent);
  background:color-mix(in srgb, var(--accent) 10%, transparent)}
.toc-sec{cursor:pointer}
.toc-sec.active{color:var(--fg);border-left-color:var(--sc, var(--accent));
  background:color-mix(in srgb, var(--sc, var(--accent)) 24%, var(--bg))}
/* the table scroller: fills the space beside the TOC, scrolls both axes */
.wrap{flex:1;min-height:0;overflow:auto}
tr.desc{scroll-margin-top:44px}
/* SQL++ column sits a step RIGHT of the recipe title (which is at --gutter), so the
   query reads as a child of the recipe in the outline (child X >= parent X). The
   higher-specificity `.code td.c-sqlpp` is needed to beat `.code td`'s padding shorthand. */
th.c-sqlpp,.code td.c-sqlpp{padding-left:calc(var(--gutter) + 10px)}
/* "full command line" toggle: swap each runnable cell for its CLI invocation */
.cli-pill{--d:var(--accent)}
pre.cell-cli{display:none}
.app.cli-on td.has-cli pre.cell-plain{display:none}
.app.cli-on td.has-cli pre.cell-cli{display:block;color:var(--fg)}
table{border-collapse:separate;border-spacing:0;width:max-content;min-width:100%;font-size:13px}
td,th{border-bottom:1px solid var(--line);vertical-align:top;text-align:left}
/* column headers — pinned to the top of the scroller */
thead th{position:sticky;top:0;z-index:30;background:var(--band);white-space:nowrap;
  padding:10px 14px;border-right:1px solid var(--line2);
  font-size:10.5px;font-weight:650;text-transform:uppercase;letter-spacing:.07em;color:var(--muted)}
thead th .dot{display:inline-block;margin-right:6px;vertical-align:middle}
/* frozen SQL++ hero column (leftmost) */
.c-sqlpp{position:sticky;left:0;z-index:20;background:var(--sqlpp);
  min-width:300px;border-right:1px solid var(--sqlpp-edge)}
thead .c-sqlpp{z-index:31;background:var(--sqlpp);border-right-color:var(--sqlpp-edge)}
.code td{padding:11px 14px;border-right:1px solid var(--line2)}
.code td.empty{color:var(--line);text-align:center;font-family:ui-monospace,monospace}
/* section banner + recipe description rows: full width, text stays pinned left */
.sec td,.desc td{padding:0;border-right:none}
/* per-section tint (--sc set per row from the YAML) grounds where you are while scrolling */
.sec td{background:color-mix(in srgb, var(--sc, transparent) 16%, var(--band))}
.sec .stick{border-left:3px solid var(--sc, var(--line));padding-left:calc(var(--gutter) - 3px)}
.desc td{background:var(--desc)}
.stick{position:sticky;left:0;display:inline-flex;flex-direction:column;align-items:flex-start;
  gap:4px;padding:10px var(--gutter);max-width:min(94vw,760px)}
.sec .stick{font-size:11px;font-weight:680;letter-spacing:.05em;text-transform:uppercase;color:var(--muted)}
.rhead{display:inline-flex;align-items:baseline;gap:9px;flex-wrap:wrap}
.rtitle{font-weight:640;font-size:13.5px;color:var(--fg);letter-spacing:-.01em}
/* per-recipe permalink — a subtle "#" that appears on hover; click copies the recipe URL */
.permalink{margin-left:-3px;color:var(--faint);text-decoration:none;font-weight:700;font-size:12.5px;
  line-height:1;opacity:0;transition:opacity .12s,color .12s}
.rhead:hover .permalink,.permalink:focus-visible{opacity:.85}
.permalink:hover{color:var(--accent)}
@media(hover:none){.permalink{opacity:.5}}   /* touch: no hover, so keep it visible */
.toast{position:fixed;left:50%;bottom:26px;transform:translateX(-50%) translateY(8px);
  background:var(--fg);color:var(--bg);font-size:12.5px;font-weight:500;padding:7px 14px;
  border-radius:8px;opacity:0;pointer-events:none;transition:opacity .18s,transform .18s;z-index:50}
.toast.show{opacity:.95;transform:translateX(-50%) translateY(0)}
.rnote{color:var(--muted);font-size:12px;line-height:1.45;font-weight:400}
.rline{display:inline-flex;flex-wrap:wrap;align-items:baseline;gap:8px}  /* note + example-data chip */
.rneeds{font-size:10.5px;color:var(--muted);border:1px solid var(--line);border-radius:10px;
  padding:1px 7px;font-family:ui-monospace,monospace;white-space:nowrap}
/* "example data" expando — chip inline on the title/note line, body drops below when open */
.src{display:inline-block;vertical-align:baseline}
.src summary{cursor:pointer;list-style:none;display:inline-flex;align-items:center;gap:4px;
  font-size:11px;color:var(--accent);font-weight:500}
.src summary::-webkit-details-marker{display:none}
.src summary::before{content:"▸";font-size:9px;transition:transform .12s}
.src[open] summary::before{transform:rotate(90deg)}
.src pre{margin:6px 0 2px;font-size:11.5px;color:var(--muted);background:var(--band);
  border:1px solid var(--line);border-radius:6px;padding:8px 11px;max-width:min(90vw,640px)}
tbody tr.code:hover td:not(.c-sqlpp){background:var(--line2)}
/* code cells wrap: cap line length so columns stay narrow (more fits horizontally) */
pre{margin:0;font-family:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
  font-size:12.5px;line-height:1.5;white-space:pre-wrap;overflow-wrap:anywhere;
  font-variant-numeric:tabular-nums;max-width:46ch}
.out{color:var(--faint)}
/* output row — a full-width line below the dialects; text pinned left, may wrap/multi-line */
.outrow td{padding:0;border-right:none;background:var(--bg)}
.outbox{position:sticky;left:0;display:flex;gap:8px;align-items:flex-start;
  padding:5px var(--gutter) 12px;max-width:min(94vw,760px)}
.out-arrow{color:var(--faint);font-family:ui-monospace,monospace;font-size:12px;line-height:1.5;flex:none}
.outpre{margin:0;font-family:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
  font-size:12px;line-height:1.5;color:var(--muted);white-space:pre-wrap;overflow-wrap:anywhere}
col.hidden,td.hidden,th.hidden{display:none}
footer{position:sticky;left:0;max-width:min(96vw,900px);padding:18px 26px;
  color:var(--muted);font-size:12.5px;line-height:1.6;border-top:1px solid var(--line)}
@media(prefers-reduced-motion:reduce){*{transition:none!important}}
"""

JS = """
// dialect column show/hide
const boxes=[...document.querySelectorAll('.toolbar input[data-col]')];
function apply(){for(const b of boxes){
  document.querySelectorAll('.col-'+b.dataset.col).forEach(el=>el.classList.toggle('hidden',!b.checked));}}
boxes.forEach(b=>b.addEventListener('change',apply));
apply();

// collapse / expand the table of contents
const app=document.querySelector('.app');
const tt=document.querySelector('.toc-toggle');
if(tt)tt.addEventListener('click',()=>app.classList.toggle('toc-off'));

// "full command line" — swap runnable cells for their CLI invocation
const cliBox=document.querySelector('#cli-toggle');
if(cliBox)cliBox.addEventListener('change',()=>app.classList.toggle('cli-on',cliBox.checked));

// TOC ↔ table: click to scroll (snappy), scroll to highlight (scrollspy)
const wrap=document.querySelector('.wrap');
const thead=document.querySelector('thead');
// items = section titles AND recipe links, flat & in document order
const items=[...document.querySelectorAll('.toc [data-spy]')];
const rows=items.map(el=>document.getElementById(el.dataset.spy));
const reduce=matchMedia('(prefers-reduced-motion: reduce)').matches;
const head=()=>thead?thead.offsetHeight:0;
const ease=t=>1-Math.pow(1-t,3);
function scrollWrapTo(to,dur){
  to=Math.max(0,to);
  if(reduce||!dur){wrap.scrollTop=to;return;}
  const s=wrap.scrollTop,d=to-s,t0=performance.now();
  (function step(now){const p=Math.min(1,(now-t0)/dur);
    wrap.scrollTop=s+d*ease(p);if(p<1)requestAnimationFrame(step);})(performance.now());
}
function offsetOf(el){return wrap.scrollTop+(el.getBoundingClientRect().top-wrap.getBoundingClientRect().top)-head()-6;}
function scrollToId(id,dur){const el=document.getElementById(id);if(el)scrollWrapTo(offsetOf(el),dur);return !!el;}
function setHash(h){try{history.replaceState(null,'','#'+h);}catch(_){try{location.hash=h;}catch(e2){}}}
let toastEl;
function toast(msg){
  if(!toastEl){toastEl=document.createElement('div');toastEl.className='toast';document.body.appendChild(toastEl);}
  toastEl.textContent=msg;toastEl.classList.add('show');
  clearTimeout(toast._t);toast._t=setTimeout(()=>toastEl.classList.remove('show'),1300);
}
items.forEach((el,i)=>el.addEventListener('click',e=>{
  e.preventDefault();const row=rows[i];if(!row)return;
  scrollWrapTo(offsetOf(row),220);   // snappy
  const s=el.dataset.spy;if(s&&!s.startsWith('sec-'))setHash(s);   // reflect the recipe in the URL
}));
let ticking=false;
function spy(){
  ticking=false;
  const cut=wrap.getBoundingClientRect().top+head()+8;
  let act=0;
  for(let i=0;i<rows.length;i++){if(rows[i]&&rows[i].getBoundingClientRect().top<=cut)act=i;else break;}
  items.forEach((el,i)=>el.classList.toggle('active',i===act));
  if(items[act])items[act].scrollIntoView({block:'nearest'});
}
wrap.addEventListener('scroll',()=>{if(!ticking){ticking=true;requestAnimationFrame(spy);}});
spy();

// per-recipe permalinks: click copies the recipe's URL and deep-links via the hash
document.querySelectorAll('a.permalink').forEach(a=>a.addEventListener('click',e=>{
  e.preventDefault();
  const id=a.getAttribute('href').slice(1);
  setHash(id);scrollToId(id,220);
  const url=location.href;
  if(navigator.clipboard&&navigator.clipboard.writeText)
    navigator.clipboard.writeText(url).then(()=>toast('Link copied'),()=>toast('Link is in the address bar'));
  else toast('Link is in the address bar');
}));
// deep-link: jump to the recipe named in the URL hash, on load and on later hash changes
function goToHash(dur){const h=decodeURIComponent(location.hash.slice(1));if(h)scrollToId(h,dur);}
addEventListener('hashchange',()=>goToHash(220));
requestAnimationFrame(()=>requestAnimationFrame(()=>goToHash(0)));   // after layout settles
"""


def dot_css():
    css = []
    for d in DIALECTS:
        if d["id"] in DIALECT_HUE:
            css.append(f".d-{d['id']}{{--d:{DIALECT_HUE[d['id']][0]}}}")
    css.append("@media(prefers-color-scheme:dark){"
               + "".join(f".d-{d['id']}{{--d:{DIALECT_HUE[d['id']][1]}}}"
                         for d in DIALECTS if d["id"] in DIALECT_HUE) + "}")
    for d in DIALECTS:
        if d["id"] in DIALECT_HUE:
            css.append(f":root[data-theme=light] .d-{d['id']}{{--d:{DIALECT_HUE[d['id']][0]}}}")
            css.append(f":root[data-theme=dark] .d-{d['id']}{{--d:{DIALECT_HUE[d['id']][1]}}}")
    return "".join(css)


def cell(kind, plain_html, clitext):
    """A code <td>. When the dialect has a CLI form, carry both — the 'full command
    line' toggle swaps them via a body class (no re-render)."""
    if clitext:
        return (f'<td class="{kind} has-cli"><pre class="cell-plain">{plain_html}</pre>'
                f'<pre class="cell-cli">{html.escape(clitext)}</pre></td>')
    return f'<td class="{kind}"><pre>{plain_html}</pre></td>'


def code_td(did, code, r, kind):
    if not code or code.strip() == "—":
        return f'<td class="empty {kind}">—</td>'
    # every dialect (incl. the SQL family) is authored fully-formatted in the YAML
    return cell(kind, html.escape(code.strip()), cli_text(did, code, r))


def render_html_body():
    prim = next(d for d in DIALECTS if d.get("primary"))
    sec = secondary()
    ncols = 1 + len(sec)

    # number every recipe once, and give each a unique slug — the shareable anchor id
    # the TOC, the permalink, and hash deep-linking all agree on.
    numbered, rid, seen = [], 0, set()
    for title, recipes in load():
        lst = []
        for r in recipes:
            rid += 1
            base = r.get("slug") or slugify(r["title"])
            slug, k = base, 2
            while slug in seen:
                slug, k = f"{base}-{k}", k + 1
            seen.add(slug)
            lst.append((rid, slug, r))
        numbered.append((title, lst))

    T = [f"<style>{CSS}{dot_css()}</style>", '<div class="app">']

    # top bar: contents toggle · "SQL++ compared to" · dialect switcher · CLI toggle
    T.append('<div class="toolbar">')
    T.append('<button class="toc-toggle" title="Toggle contents" aria-label="Toggle contents">☰</button>')
    T.append('<span class="lead">SQL++ compared to</span>')
    for d in sec:
        chk = "" if d.get("hidden_default") else "checked"
        T.append(f'<label class="pill d-{d["id"]}"><input type="checkbox" data-col="{d["id"]}" {chk}>'
                 f'<span class="dot"></span>{html.escape(d["label"])}</label>')
    T.append('<label class="pill cli-pill"><input type="checkbox" id="cli-toggle">'
             '<span class="dot"></span>full command line</label>')
    T.append('</div>')

    T.append('<div class="body">')

    # left: table of contents — scrollspy-tracked, click scrolls the table
    T.append('<nav class="toc"><div class="toc-head">Contents</div><ul>')
    for si, (title, lst) in enumerate(numbered):
        sc = lst[0][2].get("section_color", "")
        scstyle = f' style="--sc:{sc}"' if sc else ""
        T.append(f'<li class="toc-sec" data-spy="sec-{si}"{scstyle}>{html.escape(title)}</li>')
        for rid, slug, r in lst:
            T.append(f'<li><a data-spy="{slug}" href="#{slug}" '
                     f'title="{html.escape(r["title"])}">{html.escape(short_title(r["title"]))}</a></li>')
    T.append('</ul></nav>')

    # right: the scroller. header scrolls away; thead + SQL++ column stay pinned within it.
    # .inner is table-wide so the header's sticky-left content pins across the whole scroll.
    T.append('<div class="wrap"><div class="inner">')
    T.append(f'<header><div class="hin"><h1>{html.escape(HTML_TITLE)}</h1>'
             f'<div class="sub">{html.escape(HTML_SUB)}</div></div></header>')
    T.append("<table>")
    T.append("<colgroup><col class='c-sqlpp'>" + "".join(f"<col class='col-{d['id']}'>" for d in sec) + "</colgroup>")
    T.append("<thead><tr>")
    T.append(f'<th class="c-sqlpp"><span class="dot d-{prim["id"]}"></span>{html.escape(prim["label"])}</th>')
    for d in sec:
        T.append(f'<th class="col-{d["id"]}"><span class="dot d-{d["id"]}"></span>{html.escape(d["label"])}</th>')
    T.append("</tr></thead><tbody>")

    for si, (title, lst) in enumerate(numbered):
        sc = lst[0][2].get("section_color", "")
        scstyle = f' style="--sc:{sc}"' if sc else ""
        T.append(f'<tr class="sec" id="sec-{si}"{scstyle}><td colspan="{ncols}"><span class="stick">'
                 f'{html.escape(title)}</span></td></tr>')
        for rid, slug, r in lst:
            # description row (colspan) — text stays pinned left; id is the scroll target.
            # the "example data" chip rides inline on the note line (or the title line) to
            # save a row when collapsed, expanding below when opened.
            title = html.escape(r["title"])
            note = html.escape(r["note"].strip()) if r.get("note") else ""
            needs = '<span class="rneeds">shop dataset</span>' if r.get("needs") else ""
            src = r["data"].strip() if r.get("data") else ""
            chip = (f'<details class="src"><summary>example data</summary>'
                    f'<pre>{html.escape(src)}</pre></details>') if src else ""
            plink = (f'<a class="permalink" href="#{slug}" title="Copy link to this recipe" '
                     f'aria-label="Copy link to this recipe">#</a>')
            head = (f'<span class="rhead"><span class="rtitle">{title}</span>{plink}{needs}'
                    f'{"" if note else chip}</span>')
            line2 = f'<span class="rline"><span class="rnote">{note}</span>{chip}</span>' if note else ""
            T.append(f'<tr class="desc" id="{slug}" data-r="{rid}"><td colspan="{ncols}">'
                     f'<span class="stick">{head}{line2}</span></td></tr>')
            # dialect row — SQL++ (frozen) then each secondary dialect. The cell shows the
            # concise query (the WITH binding stays hidden); its FROM references the bound
            # name, and the "example data" chip shows the actual data.
            T.append(f'<tr class="code" data-r="{rid}">')
            sqlpp = (r.get("sqlpp") or "").strip()
            T.append(cell("c-sqlpp", html.escape(sqlpp), cli_text("sqlpp", sqlpp, r)))
            for d in sec:
                T.append(code_td(d["id"], r.get(d["id"], ""), r, "col-" + d["id"]))
            T.append("</tr>")
            # output row — its own full-width line (can be multi-line), text pinned left
            if r.get("out"):
                T.append(f'<tr class="outrow" data-r="{rid}"><td colspan="{ncols}">'
                         f'<div class="outbox"><span class="out-arrow">→</span>'
                         f'<pre class="outpre">{html.escape(r["out"].strip())}</pre></div></td></tr>')
    T.append("</tbody></table>")
    T.append(f"<footer>{html.escape(HTML_FOOTER)}</footer>")
    T.append("</div></div>")   # close .inner, .wrap
    T.append("</div></div>")   # close .body, .app
    T.append(f"<script>{JS}</script>")
    return "".join(T)


def render_html():
    return ("<!doctype html><html lang=en><head><meta charset=utf-8>"
            "<meta name=viewport content='width=device-width,initial-scale=1'>"
            f"<title>{html.escape(HTML_TITLE)}</title></head>"
            f"<body>{render_html_body()}</body></html>")


# ------------------------------------------------------------------- check ----

def check():
    binpath = os.path.join(ROOT, "n1k1")
    if not os.path.exists(binpath):
        print("SKIP: ./n1k1 not found; build it to run --check", file=sys.stderr)
        return 0
    shop = os.path.join(ROOT, "examples", "shop")
    n = fails = 0
    for title, recipes in load():
        for r in recipes:
            sqlpp = full_sqlpp(r)
            if not sqlpp:
                continue
            n += 1
            args = [binpath, "-c", sqlpp] + ([shop] if r.get("needs") == "shop" else [])
            res = subprocess.run(args, capture_output=True, text=True)
            if res.returncode != 0 or "Error" in res.stderr + res.stdout[:200]:
                fails += 1
                print(f"FAIL [{title} :: {r['title']}]")
                print("  " + sqlpp.replace("\n", "\n  "))
                print("  " + (res.stderr or res.stdout).strip().splitlines()[0])
    print(f"\n{n} SQL++ recipes checked, {fails} failed")
    return 1 if fails else 0


def main():
    args = set(sys.argv[1:]) or {"--all"}
    if "--check" in args:
        sys.exit(check())
    if "--md" in args or "--all" in args:
        open(os.path.join(HERE, "recipes.md"), "w").write(render_md() + "\n")
        print("wrote docs/recipes.md")
    if "--html" in args or "--all" in args:
        open(os.path.join(HERE, "recipes.html"), "w").write(render_html() + "\n")
        print("wrote docs/recipes.html")


if __name__ == "__main__":
    main()
