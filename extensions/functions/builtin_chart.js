// version: v1.0
//
// builtin_chart.js — "grammar of graphics" charting for SQL++, as a shipped JS module.
//
// The grammar of graphics (Wilkinson's GoG, popularized by ggplot2) maps data columns
// onto visual channels, picks a geometry, and lets scales/labels follow. Posit's ggsql
// (github.com/posit-dev/ggsql) adds that to SQL as new clauses:
//
//     SELECT date, revenue, region FROM sales WHERE year = 2024
//     VISUALISE date AS x, revenue AS y, region AS color
//     DRAW line
//     LABEL title => 'Sales by Region'
//
// n1k1 needs no new grammar for it: ggsql compiles to a Vega-Lite JSON spec, and a spec
// is just one JSON object folded from N rows — which is exactly an AGGREGATE. So this
// ships as chart_vegalite(), with no parser, engine, or cbq change:
//
//     SELECT chart_vegalite({"x": ts, "y": total, "color": status,
//                            "$mark": "line", "$title": "Orders over time"})
//       FROM orders
//
// One object per row: ordinary keys are visual channels (GoG aesthetics), `$`-prefixed
// keys are spec-level options (constant per query, last one wins).
//
//   GoG concept   here
//   ----------    ----
//   aesthetics    the channel keys: x, y, color, size, shape, …  (any key works)
//   geom          "$mark"    — point (default), line, bar, area, …
//   scales        inferred per channel (see chanType); override on the emitted spec
//   labels        "$title", "$width", "$height"
//
// Because it composes with SQL++ rather than extending it, faceting is free — GROUP BY
// for one spec per group (small multiples):
//
//     SELECT status, chart_vegalite({"x": ts, "y": total, "$mark": "bar"}) AS spec
//       FROM orders GROUP BY status
//
// `@chart(...)` (extensions/macros/chart.macro.js) is a friendlier surface over this.

// chanType picks Vega-Lite's measurement type for a channel — GoG's scale choice —
// from the first non-null value, the way ggplot2/ggsql infer a scale from a column type.
function chanType(rows, field) {
  for (var i = 0; i < rows.length; i++) {
    var val = rows[i][field];
    if (val === null || val === undefined) { continue; }
    if (typeof val === "number") { return "quantitative"; }
    if (typeof val === "string" && /^\d{4}-\d{2}(-\d{2})?([T ]|$)/.test(val)) { return "temporal"; }
    return "nominal";
  }
  return "nominal";
}

// htmlPage wraps a spec in a standalone page. The spec is inlined; vega-embed loads
// from a CDN, so VIEWING needs network (a self-contained renderer would be a different
// function — chart_svg — since it would not be Vega-Lite at all).
function htmlPage(spec) {
  return '<!doctype html>\n<meta charset="utf-8">\n<title>' +
    String(spec.title || "n1k1 chart").replace(/[<&]/g, "") + '</title>\n' +
    '<script src="https://cdn.jsdelivr.net/npm/vega@5"></script>\n' +
    '<script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>\n' +
    '<script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>\n' +
    '<body style="font:14px system-ui;margin:2rem">\n<div id="v"></div>\n' +
    '<script>vegaEmbed("#v", ' + JSON.stringify(spec) + ');</script>\n';
}

function chartInit() { return { rows: [], opt: {}, chans: [] }; }

function chartUpdate(s, v) {
  if (!v || typeof v !== "object") { return s; }
  var row = {};
  for (var k in v) {
    if (k.charAt(0) === "$") {
      s.opt[k.substring(1)] = v[k];   // spec options
    } else {
      row[k] = v[k];                  // visual channels
      if (s.chans.indexOf(k) < 0) { s.chans.push(k); }
    }
  }
  s.rows.push(row);                   // in-place mutation is fine (see DESIGN-extensions.md)
  return s;
}

function chartFinal(s) {
  var enc = {};
  for (var i = 0; i < s.chans.length; i++) {
    var c = s.chans[i];
    enc[c] = { field: c, type: chanType(s.rows, c) };
  }
  var spec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    mark: s.opt.mark || "point",
    encoding: enc,
    data: { values: s.rows }
  };
  if (s.opt.title) { spec.title = s.opt.title; }
  if (s.opt.width) { spec.width = s.opt.width; }
  if (s.opt.height) { spec.height = s.opt.height; }
  // "$format":"html" -> a ready-to-open page, so a chart is one shell pipeline
  // (-mode list emits the raw string):
  //   n1k1 -mode list -c 'SELECT RAW chart_vegalite({… "$format":"html"}) FROM t' > c.html
  if (s.opt.format === "html") { return htmlPage(spec); }
  return spec;
}

// The state is two concatenable lists plus a constant option bag, so this is a monoid:
// declaring merge makes it foldable across windows/shards (glue.CombineAggregate).
function chartMerge(a, b) {
  var opt = {};
  for (var k in b.opt) { opt[k] = b.opt[k]; }
  for (var k2 in a.opt) { opt[k2] = a.opt[k2]; }
  var chans = a.chans.slice(0);
  for (var i = 0; i < b.chans.length; i++) {
    if (chans.indexOf(b.chans[i]) < 0) { chans.push(b.chans[i]); }
  }
  return { rows: a.rows.concat(b.rows), opt: opt, chans: chans };
}

exports.functions = [
  {
    name: "chart_vegalite", kind: "aggregate",
    init: chartInit, update: chartUpdate, final: chartFinal, merge: chartMerge,
    examples: [
      // Two rows -> a two-point spec. `in` is the value sequence; `out` the final spec.
      {
        desc: "x/y channels + $mark fold into a Vega-Lite spec",
        in: [{ x: 1, y: 10, $mark: "line" }, { x: 2, y: 20 }],
        out: {
          "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
          mark: "line",
          encoding: { x: { field: "x", type: "quantitative" },
                      y: { field: "y", type: "quantitative" } },
          data: { values: [{ x: 1, y: 10 }, { x: 2, y: 20 }] }
        }
      },
      {
        desc: "an ISO date channel infers a temporal scale; a string one nominal",
        in: [{ x: "2026-01-15", color: "shipped" }],
        out: {
          "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
          mark: "point",
          encoding: { x: { field: "x", type: "temporal" },
                      color: { field: "color", type: "nominal" } },
          data: { values: [{ x: "2026-01-15", color: "shipped" }] }
        }
      }
    ]
  }
];
