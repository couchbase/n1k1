#!/usr/bin/env python3
"""Build the SQL++ recipes doc from its structured source.

Single source of truth: docs/recipes.toml (stdlib `tomllib`, zero deps).
Renders to:
  - docs/recipes.md   — GitHub-friendly Markdown (SQL++ runnable; other dialects folded)
  - docs/recipes.html — self-contained interactive table (sticky headers, toggle columns)

Modes:
  python3 docs/build_recipes.py --md      # write recipes.md
  python3 docs/build_recipes.py --html    # write recipes.html
  python3 docs/build_recipes.py --all     # both (default)
  python3 docs/build_recipes.py --check   # run every SQL++ example against ./n1k1

`--check` is the automated test: it drives the built n1k1 binary over each recipe's
SQL++ (against examples/shop when the recipe needs it) and fails on any error.
"""

import sys, os, html, subprocess, tomllib

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(HERE, "recipes.toml")


def load():
    with open(SRC, "rb") as f:
        return tomllib.load(f)


def dialects(doc):
    return doc["meta"]["dialect"]


def recipes(doc):
    for sec in doc["section"]:
        for r in sec.get("recipe", []):
            yield sec, r


# ---------------------------------------------------------------- Markdown ----

def md_escape_inline(s):
    return s.replace("|", "\\|")


def render_md(doc):
    m = doc["meta"]
    dl = dialects(doc)
    by_id = {d["id"]: d for d in dl}
    secondary = [d for d in dl if not d.get("primary")]

    out = [m["preamble"].rstrip(), ""]
    for sec in doc["section"]:
        out.append(f"## {sec['title']}\n")
        if sec.get("intro"):
            out.append(sec["intro"].strip() + "\n")
        for r in sec.get("recipe", []):
            out.append(f"### {r['title']}")
            if r.get("note"):
                out.append(r["note"].strip())
            # SQL++ runnable block
            sqlpp = r.get("sqlpp", "").strip()
            if sqlpp:
                block = sqlpp
                if r.get("out"):
                    block += f"\n-- → {r['out'].strip()}"
                out.append("```sql\n" + block + "\n```")
            # other dialects, folded
            rows = []
            for d in secondary:
                code = r.get(d["id"], "").strip()
                if code and code != "—":
                    rows.append(f"| **{d['label']}** | `{md_escape_inline(code)}` |")
            if rows:
                out.append("<details><summary>other dialects</summary>\n")
                out.append("| | |")
                out.append("|---|---|")
                out.extend(rows)
                out.append("\n</details>")
            out.append("")
    if m.get("outro"):
        out.append(m["outro"].rstrip() + "\n")
    return "\n".join(out)


# -------------------------------------------------------------------- HTML ----

# Per-dialect identity hue (light, dark) — color encodes *which dialect*.
DIALECT_HUE = {
    "sqlpp": ("#4f46e5", "#8b93ff"),   # indigo — the hero
    "sql": ("#2f6fb3", "#6fa8dc"),     # postgres blue
    "duckdb": ("#c2892a", "#e3b341"),  # duck amber
    "js": ("#c2410c", "#e0965a"),      # burnt orange
    "python": ("#356a9a", "#6fa8d6"),  # steel blue
    "mongo": ("#128a4a", "#3ddc84"),   # leaf green
    "jq": ("#6b7280", "#9aa1ad"),      # slate
}

CSS = """
:root{
  --bg:#fcfcfd;--fg:#1c2024;--muted:#626772;--faint:#8b909a;
  --line:#e7e9ee;--line2:#eef0f3;--sticky:#fcfcfd;--band:#f3f4f8;
  --accent:#4f46e5;--sqlpp:#eef0ff;--sqlpp-edge:#c9ccff;--focus:#4f46e5;
}
@media(prefers-color-scheme:dark){:root{
  --bg:#0f1216;--fg:#dfe3ea;--muted:#8a919e;--faint:#5c636e;
  --line:#232a33;--line2:#1a1f27;--sticky:#0f1216;--band:#151a20;
  --accent:#8b93ff;--sqlpp:#161a2e;--sqlpp-edge:#2b3168;--focus:#8b93ff;
}}
:root[data-theme=light]{
  --bg:#fcfcfd;--fg:#1c2024;--muted:#626772;--faint:#8b909a;
  --line:#e7e9ee;--line2:#eef0f3;--sticky:#fcfcfd;--band:#f3f4f8;
  --accent:#4f46e5;--sqlpp:#eef0ff;--sqlpp-edge:#c9ccff;--focus:#4f46e5;
}
:root[data-theme=dark]{
  --bg:#0f1216;--fg:#dfe3ea;--muted:#8a919e;--faint:#5c636e;
  --line:#232a33;--line2:#1a1f27;--sticky:#0f1216;--band:#151a20;
  --accent:#8b93ff;--sqlpp:#161a2e;--sqlpp-edge:#2b3168;--focus:#8b93ff;
}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;color:var(--fg);background:var(--bg);
  font:15px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  font-feature-settings:"kern";text-rendering:optimizeLegibility}
.mono{font-family:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace}
a{color:var(--accent);text-underline-offset:2px}
header{padding:26px 26px 20px;border-bottom:1px solid var(--line)}
h1{margin:0 0 8px;font-size:25px;font-weight:680;letter-spacing:-.015em;text-wrap:balance;max-width:30ch}
.sub{color:var(--muted);font-size:13.5px;line-height:1.5;max-width:82ch}
.sub code{font-family:ui-monospace,"SF Mono",Menlo,monospace;font-size:.92em;color:var(--fg)}
/* toolbar = dialect switcher */
.toolbar{position:sticky;top:0;z-index:40;background:var(--sticky);
  border-bottom:1px solid var(--line);padding:11px 26px;
  display:flex;flex-wrap:wrap;gap:8px 9px;align-items:center}
.toolbar .lbl{font-size:11px;letter-spacing:.09em;text-transform:uppercase;
  color:var(--faint);margin-right:2px}
.pill{font-size:12.5px;display:inline-flex;gap:6px;align-items:center;cursor:pointer;
  padding:4px 11px 4px 9px;border:1px solid var(--line);border-radius:999px;
  user-select:none;color:var(--muted);background:transparent;transition:border-color .12s,color .12s}
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
.wrap{overflow:auto;max-height:calc(100vh - 128px)}
table{border-collapse:separate;border-spacing:0;width:max-content;min-width:100%;font-size:13px}
th,td{border-bottom:1px solid var(--line);border-right:1px solid var(--line2);
  vertical-align:top;padding:11px 14px;text-align:left}
thead th{position:sticky;top:0;z-index:20;background:var(--band);white-space:nowrap;
  font-size:10.5px;font-weight:650;text-transform:uppercase;letter-spacing:.07em;color:var(--muted)}
thead th .dot{display:inline-block;margin-right:6px;vertical-align:middle}
/* frozen left columns: recipe + SQL++ */
.c-recipe{position:sticky;left:0;z-index:10;background:var(--sticky);min-width:236px;max-width:280px}
.c-sqlpp{position:sticky;left:236px;z-index:10;background:var(--sqlpp);min-width:340px;
  border-right:1px solid var(--sqlpp-edge)}
thead .c-recipe{z-index:26}thead .c-sqlpp{z-index:26;background:var(--sqlpp);border-right-color:var(--sqlpp-edge)}
tbody tr:hover td:not(.c-sqlpp){background:var(--line2)}
tbody tr:hover .c-recipe{background:var(--band)}
.sec td{position:sticky;left:0;background:var(--band);z-index:16;border-right:none;
  font-size:11px;font-weight:680;letter-spacing:.05em;text-transform:uppercase;color:var(--muted)}
.rtitle{font-weight:640;font-size:13px;margin-bottom:3px;color:var(--fg);letter-spacing:-.01em}
.rnote{color:var(--muted);font-size:11.5px;line-height:1.45}
.rneeds{display:inline-block;margin-top:6px;font-size:10.5px;color:var(--muted);
  border:1px solid var(--line);border-radius:10px;padding:1px 7px;font-family:ui-monospace,monospace}
pre{margin:0;font-family:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
  font-size:12.5px;line-height:1.5;white-space:pre-wrap;word-break:break-word;
  font-variant-numeric:tabular-nums}
.out{color:var(--faint)}
td.empty{color:var(--line);text-align:center;font-family:ui-monospace,monospace}
col.hidden,th.hidden,td.hidden{display:none}
footer{padding:18px 26px;color:var(--muted);font-size:12.5px;line-height:1.6;border-top:1px solid var(--line)}
@media(prefers-reduced-motion:reduce){*{transition:none!important}}
"""

JS = """
const boxes=[...document.querySelectorAll('.toolbar input[data-col]')];
function apply(){
  for(const b of boxes){
    const on=b.checked;
    document.querySelectorAll('.col-'+b.dataset.col).forEach(el=>el.classList.toggle('hidden',!on));
  }
}
boxes.forEach(b=>b.addEventListener('change',apply));
const q=document.getElementById('q');
q.addEventListener('input',()=>{
  const t=q.value.toLowerCase();
  document.querySelectorAll('tr[data-recipe]').forEach(tr=>{
    tr.style.display = !t || tr.textContent.toLowerCase().includes(t) ? '' : 'none';
  });
});
apply();
"""


def code_cell(code, kind=""):
    if not code or code.strip() == "—":
        return '<td class="empty {k}">—</td>'.replace("{k}", kind)
    return f'<td class="{kind}"><pre>{html.escape(code.strip())}</pre></td>'


def dot_css(dl):
    """Per-dialect identity-hue tokens, theme-aware."""
    light = "".join(f".d-{d['id']}{{--d:{DIALECT_HUE[d['id']][0]}}}" for d in dl if d["id"] in DIALECT_HUE)
    dark = "".join(f".d-{d['id']}{{--d:{DIALECT_HUE[d['id']][1]}}}" for d in dl if d["id"] in DIALECT_HUE)
    return (light
            + "@media(prefers-color-scheme:dark){" + dark + "}"
            + ":root[data-theme=light]{}" + "".join(
                f":root[data-theme=light] .d-{d['id']}{{--d:{DIALECT_HUE[d['id']][0]}}}"
                for d in dl if d["id"] in DIALECT_HUE)
            + "".join(
                f":root[data-theme=dark] .d-{d['id']}{{--d:{DIALECT_HUE[d['id']][1]}}}"
                for d in dl if d["id"] in DIALECT_HUE))


def render_html_body(doc):
    """Body content only (style + markup + script) — no doctype/head/body wrapper."""
    m = doc["meta"]
    dl = dialects(doc)
    prim = next(d for d in dl if d.get("primary"))
    secondary = [d for d in dl if not d.get("primary")]
    ncols = 2 + len(secondary)

    T = [f"<style>{CSS}{dot_css(dl)}</style>"]
    T.append("<header>")
    T.append(f"<h1>{html.escape(m['html_title'])}</h1>")
    T.append(f'<div class="sub">{html.escape(m["html_sub"])}</div>')
    T.append("</header>")

    # toolbar = dialect switcher (toggle secondary columns) + filter
    T.append('<div class="toolbar"><span class="lbl">dialects</span>')
    for d in secondary:
        checked = "" if d.get("hidden_default") else "checked"
        T.append(f'<label class="pill d-{d["id"]}"><input type="checkbox" data-col="{d["id"]}" {checked}>'
                 f'<span class="dot"></span>{html.escape(d["label"])}</label>')
    T.append('<input id="q" type="search" placeholder="filter recipes…" aria-label="filter recipes"></div>')

    T.append('<div class="wrap"><table>')
    T.append("<colgroup>")
    T.append('<col class="c-recipe"><col class="c-sqlpp">')
    for d in secondary:
        T.append(f'<col class="col-{d["id"]}">')
    T.append("</colgroup>")
    T.append("<thead><tr>")
    T.append('<th class="c-recipe">Recipe</th>')
    T.append(f'<th class="c-sqlpp"><span class="dot d-{prim["id"]}"></span>{html.escape(prim["label"])}</th>')
    for d in secondary:
        T.append(f'<th class="col-{d["id"]}"><span class="dot d-{d["id"]}"></span>{html.escape(d["label"])}</th>')
    T.append("</tr></thead><tbody>")

    for sec in doc["section"]:
        T.append(f'<tr class="sec"><td colspan="{ncols}">{html.escape(sec["title"])}</td></tr>')
        for r in sec.get("recipe", []):
            T.append("<tr data-recipe>")
            note = f'<div class="rnote">{html.escape(r["note"].strip())}</div>' if r.get("note") else ""
            needs = '<span class="rneeds">needs examples/shop</span>' if r.get("needs") else ""
            T.append(f'<td class="c-recipe"><div class="rtitle">{html.escape(r["title"])}</div>{note}{needs}</td>')
            sqlpp = (r.get("sqlpp") or "").strip()
            outc = f'\n<span class="out"># → {html.escape(r["out"].strip())}</span>' if r.get("out") else ""
            T.append(f'<td class="c-sqlpp"><pre>{html.escape(sqlpp)}{outc}</pre></td>')
            for d in secondary:
                T.append(code_cell(r.get(d["id"], ""), "col-" + d["id"]))
            T.append("</tr>")
    T.append("</tbody></table></div>")
    T.append(f"<footer>{html.escape(m['html_footer'])}</footer>")
    T.append(f"<script>{JS}</script>")
    return "".join(T)


def render_html(doc):
    m = doc["meta"]
    return ("<!doctype html><html lang=en><head><meta charset=utf-8>"
            "<meta name=viewport content='width=device-width,initial-scale=1'>"
            f"<title>{html.escape(m['html_title'])}</title></head>"
            f"<body>{render_html_body(doc)}</body></html>")


# ------------------------------------------------------------------- check ----

def check(doc):
    binpath = os.path.join(ROOT, "n1k1")
    if not os.path.exists(binpath):
        print("SKIP: ./n1k1 binary not found; build it to run --check", file=sys.stderr)
        return 0
    shop = os.path.join(ROOT, "examples", "shop")
    fails = 0
    n = 0
    for sec, r in recipes(doc):
        sqlpp = (r.get("sqlpp") or "").strip()
        if not sqlpp:
            continue
        n += 1
        args = [binpath, "-c", sqlpp]
        if r.get("needs") == "shop":
            args.append(shop)
        res = subprocess.run(args, capture_output=True, text=True)
        blob = res.stderr + res.stdout[:200]
        if res.returncode != 0 or "Error" in blob:
            fails += 1
            print(f"FAIL [{sec['title']} :: {r['title']}]")
            print("  " + sqlpp.replace("\n", "\n  "))
            print("  " + (res.stderr or res.stdout).strip().splitlines()[0])
    print(f"\n{n} SQL++ recipes checked, {fails} failed")
    return 1 if fails else 0


# -------------------------------------------------------------------- main ----

def main():
    args = set(sys.argv[1:]) or {"--all"}
    doc = load()
    if "--check" in args:
        sys.exit(check(doc))
    do_md = "--md" in args or "--all" in args
    do_html = "--html" in args or "--all" in args
    if do_md:
        p = os.path.join(HERE, "recipes.md")
        open(p, "w").write(render_md(doc) + "\n")
        print("wrote", os.path.relpath(p, ROOT))
    if do_html:
        p = os.path.join(HERE, "recipes.html")
        open(p, "w").write(render_html(doc) + "\n")
        print("wrote", os.path.relpath(p, ROOT))


if __name__ == "__main__":
    main()
