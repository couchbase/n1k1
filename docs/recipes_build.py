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

import sys, os, html, subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(HERE, "recipes.yaml")

# ---- presentation config (the YAML owns the queryable recipe DATA; this owns chrome)

DIALECTS = [
    {"id": "sqlpp", "label": "SQL++", "primary": True},
    {"id": "sql", "label": "SQL (Postgres)"},
    {"id": "duckdb", "label": "DuckDB", "hidden_default": True},
    {"id": "js", "label": "JavaScript", "hidden_default": True},
    {"id": "python", "label": "Python", "hidden_default": True},
    {"id": "mongo", "label": "MongoDB", "hidden_default": True},
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

HTML_TITLE = "SQL++ recipes — a JSON slice-and-dice Rosetta stone"
HTML_SUB = ("The same JSON maneuver in SQL++, SQL, DuckDB, JavaScript, Python, MongoDB, "
            "and jq. SQL++ (frozen, left) is the runnable column — each example ran against "
            "n1k1. Toggle the other dialects on; scroll the table; filter with the search box.")
HTML_FOOTER = ("Generated from docs/recipes.yaml by docs/recipes_build.py — which n1k1 can "
               "itself query. Sources: the jq manual (jqlang.org/manual) and Remy Sharp's jq recipes.")

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
            sqlpp = (r.get("sqlpp") or "").strip()
            if sqlpp:
                block = sqlpp + (f"\n-- → {r['out'].strip()}" if r.get("out") else "")
                out.append("```sql\n" + block + "\n```")
            rows = []
            for d in secondary():
                code = (r.get(d["id"]) or "").strip()
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
html,body{margin:0}
body{color:var(--fg);background:var(--bg);
  font:15px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  text-rendering:optimizeLegibility}
a{color:var(--accent);text-underline-offset:2px}
/* header — scrolls away */
header{padding:26px 26px 20px;border-bottom:1px solid var(--line)}
h1{margin:0 0 8px;font-size:25px;font-weight:680;letter-spacing:-.015em;text-wrap:balance;max-width:30ch}
.sub{color:var(--muted);font-size:13.5px;line-height:1.5;max-width:82ch}
/* toolbar — the only chrome that stays pinned, alongside the table headers */
.toolbar{position:sticky;top:0;z-index:50;background:var(--sticky);
  border-bottom:1px solid var(--line);padding:10px 26px;min-height:var(--tb);
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
#q{margin-left:auto;padding:6px 11px;border:1px solid var(--line);border-radius:8px;
  background:var(--bg);color:var(--fg);font-size:13px;min-width:190px}
#q:focus-visible{outline:2px solid var(--focus);outline-offset:1px;border-color:transparent}
/* the table scroller: fills the viewport under the pinned toolbar, scrolls both axes */
.wrap{overflow:auto;height:calc(100dvh - var(--tb))}
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
.sec td{background:var(--band)}
.desc td{background:var(--desc)}
.stick{position:sticky;left:0;display:inline-flex;flex-direction:column;align-items:flex-start;
  gap:4px;padding:10px 16px;max-width:min(94vw,760px)}
.sec .stick{font-size:11px;font-weight:680;letter-spacing:.05em;text-transform:uppercase;color:var(--muted)}
.rhead{display:inline-flex;align-items:baseline;gap:9px;flex-wrap:wrap}
.rtitle{font-weight:640;font-size:13.5px;color:var(--fg);letter-spacing:-.01em}
.rnote{color:var(--muted);font-size:12px;line-height:1.45;font-weight:400}  /* on its own line below the title */
.rneeds{font-size:10.5px;color:var(--muted);border:1px solid var(--line);border-radius:10px;
  padding:1px 7px;font-family:ui-monospace,monospace;white-space:nowrap}
tbody tr.code:hover td:not(.c-sqlpp){background:var(--line2)}
/* code cells wrap: cap line length so columns stay narrow (more fits horizontally) */
pre{margin:0;font-family:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
  font-size:12.5px;line-height:1.5;white-space:pre-wrap;overflow-wrap:anywhere;
  font-variant-numeric:tabular-nums;max-width:46ch}
.out{color:var(--faint)}
col.hidden,td.hidden,th.hidden{display:none}
footer{padding:18px 26px;color:var(--muted);font-size:12.5px;line-height:1.6;border-top:1px solid var(--line)}
@media(prefers-reduced-motion:reduce){*{transition:none!important}}
"""

JS = """
const boxes=[...document.querySelectorAll('.toolbar input[data-col]')];
function apply(){for(const b of boxes){
  document.querySelectorAll('.col-'+b.dataset.col).forEach(el=>el.classList.toggle('hidden',!b.checked));}}
boxes.forEach(b=>b.addEventListener('change',apply));
const groups={};
document.querySelectorAll('tr[data-r]').forEach(tr=>{(groups[tr.dataset.r]??=[]).push(tr);});
const q=document.getElementById('q');
q.addEventListener('input',()=>{const t=q.value.toLowerCase();
  for(const k in groups){const rows=groups[k];
    const hit=!t||rows.some(r=>r.textContent.toLowerCase().includes(t));
    rows.forEach(r=>r.style.display=hit?'':'none');}});
apply();
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


def code_td(code, kind):
    if not code or code.strip() == "—":
        return f'<td class="empty {kind}">—</td>'
    return f'<td class="{kind}"><pre>{html.escape(code.strip())}</pre></td>'


def render_html_body():
    prim = next(d for d in DIALECTS if d.get("primary"))
    sec = secondary()
    ncols = 1 + len(sec)

    T = [f"<style>{CSS}{dot_css()}</style>"]
    T.append(f'<header><h1>{html.escape(HTML_TITLE)}</h1>'
             f'<div class="sub">{html.escape(HTML_SUB)}</div></header>')

    # toolbar = dialect switcher + filter (stays pinned)
    T.append('<div class="toolbar"><span class="lbl">dialects</span>')
    for d in sec:
        chk = "" if d.get("hidden_default") else "checked"
        T.append(f'<label class="pill d-{d["id"]}"><input type="checkbox" data-col="{d["id"]}" {chk}>'
                 f'<span class="dot"></span>{html.escape(d["label"])}</label>')
    T.append('<input id="q" type="search" placeholder="filter recipes…" aria-label="filter recipes"></div>')

    T.append('<div class="wrap"><table>')
    T.append("<colgroup><col class='c-sqlpp'>" + "".join(f"<col class='col-{d['id']}'>" for d in sec) + "</colgroup>")
    T.append("<thead><tr>")
    T.append(f'<th class="c-sqlpp"><span class="dot d-{prim["id"]}"></span>{html.escape(prim["label"])}</th>')
    for d in sec:
        T.append(f'<th class="col-{d["id"]}"><span class="dot d-{d["id"]}"></span>{html.escape(d["label"])}</th>')
    T.append("</tr></thead><tbody>")

    rid = 0
    for title, recipes in load():
        T.append(f'<tr class="sec"><td colspan="{ncols}"><span class="stick">{html.escape(title)}</span></td></tr>')
        for r in recipes:
            rid += 1
            # description row (colspan) — its text stays pinned left
            note = f'<span class="rnote">{html.escape(r["note"].strip())}</span>' if r.get("note") else ""
            needs = '<span class="rneeds">needs examples/shop</span>' if r.get("needs") else ""
            T.append(f'<tr class="desc" data-r="{rid}"><td colspan="{ncols}"><span class="stick">'
                     f'<span class="rhead"><span class="rtitle">{html.escape(r["title"])}</span>{needs}</span>'
                     f'{note}</span></td></tr>')
            # dialect row
            T.append(f'<tr class="code" data-r="{rid}">')
            sqlpp = (r.get("sqlpp") or "").strip()
            outc = f'\n<span class="out"># → {html.escape(r["out"].strip())}</span>' if r.get("out") else ""
            T.append(f'<td class="c-sqlpp"><pre>{html.escape(sqlpp)}{outc}</pre></td>')
            for d in sec:
                T.append(code_td(r.get(d["id"], ""), "col-" + d["id"]))
            T.append("</tr>")
    T.append("</tbody></table></div>")
    T.append(f"<footer>{html.escape(HTML_FOOTER)}</footer>")
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
            sqlpp = (r.get("sqlpp") or "").strip()
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
