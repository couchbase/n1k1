// version: v1.0
//
// builtin_markdown.js — crack a markdown document open for querying: the heading
// OUTLINE, the link graph, and fenced code blocks.
//
// The scan layer already handles the first level: an .md file yields `text` (verbatim),
// and — when it opens with a `---` YAML frontmatter block — a parsed `front` object plus
// the markdown `body` (records/extract.go mdExtract; Jekyll/Hugo/Obsidian/Google-OKF all
// use that convention). These UDFs are the SECOND level: structure inside the markdown
// itself. They are ordinary scalar functions returning arrays, so UNNEST makes rows:
//
//   SELECT d.filename, s.depth, s.title, s.trail
//     FROM docs AS d UNNEST md_sections(d.body) AS s
//    WHERE s.depth <= 2
//
// Pass `body` when the file has frontmatter (cheapest, and line numbers are relative to
// the body) or `text` either way — a leading frontmatter block is skipped, and then the
// reported `line` counts from the top of the FILE.
//
// Field names avoid SQL++ reserved words on purpose: `depth` not "level", `trail` not
// "path" (both of those would force backticks). See `.help reserved-words`.

// mdStripFront drops a leading `---` … `---` block so the scanners never see it, and
// reports how many lines were skipped (so `line` stays true to the file).
function mdStripFront(text) {
  var lines = String(text == null ? "" : text).split("\n");
  if (lines.length && lines[0].replace(/^\ufeff/, "").trim() === "---") {
    for (var i = 1; i < lines.length; i++) {
      var t = lines[i].trim();
      if (t === "---" || t === "...") {
        return { lines: lines.slice(i + 1), offset: i + 1 };
      }
    }
  }
  return { lines: lines, offset: 0 };
}

// mdIsFence matches a code-fence line, returning its fence char (` or ~) else null.
function mdIsFence(t) {
  var m = t.match(/^(```+|~~~+)/);
  return m ? m[1].charAt(0) : null;
}

// md_sections(markdown) -> one object per ATX heading, in document order:
//   {depth, title, trail, anchor, line, body}
// `trail` is the breadcrumb of enclosing headings ("Schema > Notes"), which is what makes
// a NESTED outline answerable with a FLAT predicate. `body` is the prose under the
// heading up to the next heading of any depth. Fenced code is skipped, so a `# comment`
// inside a ``` block is never mistaken for a heading.
function md_sections(markdown) {
  var f = mdStripFront(markdown);
  var lines = f.lines, out = [], stack = [], fence = null, cur = null;

  function close(atLine) {
    if (cur) {
      cur.body = lines.slice(cur.from, atLine).join("\n").trim();
      delete cur.from;
    }
  }
  for (var i = 0; i < lines.length; i++) {
    var raw = lines[i], t = raw.trim(), fc = mdIsFence(t);
    if (fc) { fence = fence ? (fence === fc ? null : fence) : fc; continue; }
    if (fence) { continue; }
    var h = raw.match(/^(#{1,6})\s+(.*?)\s*#*\s*$/);
    if (!h) { continue; }
    close(i);
    var depth = h[1].length, title = h[2];
    while (stack.length && stack[stack.length - 1].depth >= depth) { stack.pop(); }
    var crumbs = [];
    for (var s = 0; s < stack.length; s++) { crumbs.push(stack[s].title); }
    crumbs.push(title);
    cur = {
      depth: depth, title: title, trail: crumbs.join(" > "),
      anchor: title.toLowerCase().replace(/[^a-z0-9 -]/g, "").replace(/\s+/g, "-"),
      line: f.offset + i + 1, from: i + 1
    };
    out.push(cur);
    stack.push({ depth: depth, title: title });
  }
  close(lines.length);
  return out;
}

// md_links(markdown) -> one object per inline link: {text, target, kind}. `kind` is
// "external" (has a scheme), "absolute" (a bundle-root "/…" ref — OKF's recommended
// form), or "relative" — enough to build a link graph, or find dead links, with a JOIN.
function md_links(markdown) {
  var f = mdStripFront(markdown), body = f.lines.join("\n"), out = [];
  var re = /\[([^\]]*)\]\(\s*([^)\s]+)[^)]*\)/g, m;
  while ((m = re.exec(body)) !== null) {
    var target = m[2], kind = "relative";
    if (/^[a-z][a-z0-9+.-]*:/i.test(target)) { kind = "external"; }
    else if (target.charAt(0) === "/") { kind = "absolute"; }
    out.push({ text: m[1], target: target, kind: kind });
  }
  return out;
}

// md_code(markdown) -> one object per FENCED code block: {lang, code, line}. Handy for
// pulling the SQL out of a docs corpus and checking it still runs.
function md_code(markdown) {
  var f = mdStripFront(markdown), lines = f.lines, out = [], open = null, buf = [];
  for (var i = 0; i < lines.length; i++) {
    var t = lines[i].trim(), m = t.match(/^(```+|~~~+)\s*([A-Za-z0-9_+#-]*)/);
    if (m && !open) { open = { lang: m[2] || null, line: f.offset + i + 1 }; buf = []; continue; }
    if (m && open) { open.code = buf.join("\n"); out.push(open); open = null; continue; }
    if (open) { buf.push(lines[i]); }
  }
  if (open) { open.code = buf.join("\n"); out.push(open); } // unterminated: keep what we have
  return out;
}

exports.functions = [
  {
    name: "md_sections", fn: md_sections,
    examples: [
      {
        desc: "nested headings -> depth + a breadcrumb trail",
        in: ["# Schema\n\nprose\n\n## Notes\n\nbeware\n"],
        out: [
          { depth: 1, title: "Schema", trail: "Schema", anchor: "schema", line: 1, body: "prose" },
          { depth: 2, title: "Notes", trail: "Schema > Notes", anchor: "notes", line: 5, body: "beware" }
        ]
      },
      {
        desc: "a frontmatter block is skipped, and line still counts from the file top",
        in: ["---\ntype: t\n---\n\n# Title\n"],
        out: [{ depth: 1, title: "Title", trail: "Title", anchor: "title", line: 5, body: "" }]
      },
      {
        desc: "a # inside a fenced block is not a heading",
        in: ["# Real\n\n```sh\n# not a heading\n```\n"],
        out: [{ depth: 1, title: "Real", trail: "Real", anchor: "real", line: 1,
                body: "```sh\n# not a heading\n```" }]
      }
    ]
  },
  {
    name: "md_links", fn: md_links,
    examples: [
      {
        desc: "absolute / relative / external links, classified",
        in: ["see [a](/concepts/a), [b](../b.md) and [c](https://example.com)\n"],
        out: [
          { text: "a", target: "/concepts/a", kind: "absolute" },
          { text: "b", target: "../b.md", kind: "relative" },
          { text: "c", target: "https://example.com", kind: "external" }
        ]
      }
    ]
  },
  {
    name: "md_code", fn: md_code,
    examples: [
      {
        desc: "fenced blocks, with the info string as lang",
        in: ["```sql\nSELECT 1\n```\n\n```\nplain\n```\n"],
        out: [
          { lang: "sql", code: "SELECT 1", line: 1 },
          { lang: null, code: "plain", line: 5 }
        ]
      }
    ]
  }
];
