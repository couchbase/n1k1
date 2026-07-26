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

CSS = """
:root{--bg:#fff;--fg:#1a1a1a;--muted:#6a737d;--line:#e1e4e8;--accent:#0b5fff;
--sqlpp:#eef4ff;--head:#f6f8fa;--code:#f6f8fa;--sticky:#fff;}
@media(prefers-color-scheme:dark){:root{--bg:#0d1117;--fg:#e6edf3;--muted:#8b949e;
--line:#30363d;--accent:#589bff;--sqlpp:#12233f;--head:#161b22;--code:#161b22;--sticky:#0d1117;}}
*{box-sizing:border-box}
body{margin:0;font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
color:var(--fg);background:var(--bg)}
header{padding:20px 24px;border-bottom:1px solid var(--line)}
h1{margin:0 0 6px;font-size:22px}
.sub{color:var(--muted);font-size:14px;max-width:70ch}
.toolbar{position:sticky;top:0;z-index:30;background:var(--sticky);border-bottom:1px solid var(--line);
padding:10px 24px;display:flex;flex-wrap:wrap;gap:8px 14px;align-items:center}
.toolbar b{font-size:13px;color:var(--muted);margin-right:4px}
.toolbar label{font-size:13px;display:inline-flex;gap:5px;align-items:center;cursor:pointer;
padding:3px 9px;border:1px solid var(--line);border-radius:14px;user-select:none}
.toolbar input{accent-color:var(--accent)}
#q{margin-left:auto;padding:5px 10px;border:1px solid var(--line);border-radius:6px;
background:var(--bg);color:var(--fg);font-size:13px;min-width:180px}
.wrap{overflow:auto;max-height:calc(100vh - 120px)}
table{border-collapse:separate;border-spacing:0;width:max-content;min-width:100%}
th,td{border-bottom:1px solid var(--line);border-right:1px solid var(--line);
vertical-align:top;padding:10px 12px;text-align:left}
thead th{position:sticky;top:0;z-index:20;background:var(--head);font-size:12px;
text-transform:uppercase;letter-spacing:.04em;color:var(--muted);white-space:nowrap}
/* frozen left columns: recipe + SQL++ */
.c-recipe{position:sticky;left:0;z-index:10;background:var(--sticky);min-width:230px;max-width:280px}
.c-sqlpp{position:sticky;left:230px;z-index:10;background:var(--sqlpp);min-width:340px}
thead .c-recipe{z-index:25}thead .c-sqlpp{z-index:25;background:var(--sqlpp)}
.sec td{position:sticky;left:0;background:var(--head);font-weight:600;font-size:13px;
z-index:15;border-right:none}
.rtitle{font-weight:600;font-size:13px;margin-bottom:3px}
.rnote{color:var(--muted);font-size:12px}
.rneeds{display:inline-block;margin-top:5px;font-size:11px;color:var(--muted);
border:1px solid var(--line);border-radius:10px;padding:0 6px}
pre{margin:0;font:12.5px/1.45 "SF Mono",ui-monospace,Menlo,Consolas,monospace;
white-space:pre-wrap;word-break:break-word}
.out{color:var(--muted)}
td.empty{color:var(--line);text-align:center}
col.hidden,th.hidden,td.hidden{display:none}
footer{padding:16px 24px;color:var(--muted);font-size:13px;border-top:1px solid var(--line)}
a{color:var(--accent)}
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


def render_html(doc):
    m = doc["meta"]
    dl = dialects(doc)
    prim = next(d for d in dl if d.get("primary"))
    secondary = [d for d in dl if not d.get("primary")]
    ncols = 2 + len(secondary)

    T = []
    T.append("<header>")
    T.append(f"<h1>{html.escape(m['html_title'])}</h1>")
    T.append(f'<div class="sub">{html.escape(m["html_sub"])}</div>')
    T.append("</header>")

    # toolbar: toggle secondary dialect columns + search
    T.append('<div class="toolbar"><b>show:</b>')
    for d in secondary:
        checked = "" if d.get("hidden_default") else "checked"
        T.append(f'<label><input type="checkbox" data-col="{d["id"]}" {checked}>'
                 f'{html.escape(d["label"])}</label>')
    T.append('<input id="q" type="search" placeholder="filter recipes…"></div>')

    T.append('<div class="wrap"><table>')
    # colgroup for hide/show
    T.append("<colgroup>")
    T.append('<col class="c-recipe"><col class="c-sqlpp">')
    for d in secondary:
        T.append(f'<col class="col-{d["id"]}">')
    T.append("</colgroup>")
    # head
    T.append("<thead><tr>")
    T.append('<th class="c-recipe">Recipe</th>')
    T.append(f'<th class="c-sqlpp">{html.escape(prim["label"])}</th>')
    for d in secondary:
        T.append(f'<th class="col-{d["id"]}">{html.escape(d["label"])}</th>')
    T.append("</tr></thead><tbody>")

    for sec in doc["section"]:
        T.append(f'<tr class="sec"><td colspan="{ncols}">{html.escape(sec["title"])}</td></tr>')
        for r in sec.get("recipe", []):
            T.append('<tr data-recipe>')
            # recipe cell
            note = f'<div class="rnote">{html.escape(r["note"].strip())}</div>' if r.get("note") else ""
            needs = '<span class="rneeds">needs examples/shop</span>' if r.get("needs") else ""
            T.append(f'<td class="c-recipe"><div class="rtitle">{html.escape(r["title"])}</div>{note}{needs}</td>')
            # sql++ cell (+ expected output)
            sqlpp = (r.get("sqlpp") or "").strip()
            outc = f'\n<span class="out"># → {html.escape(r["out"].strip())}</span>' if r.get("out") else ""
            T.append(f'<td class="c-sqlpp"><pre>{html.escape(sqlpp)}{outc}</pre></td>')
            for d in secondary:
                T.append(code_cell(r.get(d["id"], ""), "col-" + d["id"]))
            T.append("</tr>")
    T.append("</tbody></table></div>")
    T.append(f"<footer>{m['html_footer']}</footer>")

    page = ("<!doctype html><html lang=en><head><meta charset=utf-8>"
            "<meta name=viewport content='width=device-width,initial-scale=1'>"
            f"<title>{html.escape(m['html_title'])}</title><style>{CSS}</style></head>"
            f"<body>{''.join(T)}<script>{JS}</script></body></html>")
    return page


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
