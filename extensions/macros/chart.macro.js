// version: v1.0
//
// chart.macro.js -- a "grammar of graphics" surface for SQL++, as a macro rather than
// new grammar. Posit's ggsql (github.com/posit-dev/ggsql) adds clauses to SQL:
//
//     SELECT date, revenue, region FROM sales WHERE year = 2024
//     VISUALISE date AS x, revenue AS y, region AS color
//     DRAW line
//     LABEL title => 'Sales by Region'
//
// the same thing here, with no parser, engine, or cbq change:
//
//     SELECT RAW @chart(sales, x => date, y => revenue, color => region,
//                       draw => 'line', title => 'Sales by Region')[0]
//
// Expands to a SELECT that folds the rows into a Vega-Lite spec via the
// chart_vegalite() aggregate, so the result is one value holding a renderable spec
// with its data inlined. Add `format => 'html'` for a standalone page (see
// chart_vegalite.agg.js).
//
// Because this composes with SQL++ instead of extending it, faceting comes for free --
// GROUP BY the aggregate directly for one spec per group (small multiples):
//
//     SELECT status, chart_vegalite({"x": ts, "y": total, "$mark": "bar"}) AS spec
//       FROM orders GROUP BY status
//
// ggsql clause  ->  here
//   VISUALISE     x/y/color/size/shape params (GoG aesthetics)
//   DRAW          draw  (GoG geom)
//   LABEL         title
//   SCALE         inferred per channel; override on the emitted spec
//
// See `.macro help`. Sibling of top_per_group.macro.js / sessionize.macro.js.
var macro = {
  name: "chart",
  params: [
    { name: "src", required: true },      // keyspace / subquery -- the data
    // channels (ggsql's VISUALISE): any expression over the source
    { name: "x", required: true },
    { name: "y" },
    { name: "color" },
    { name: "size" },
    { name: "shape" },
    { name: "draw", default: "'point'" }, // ggsql's DRAW <geom>
    { name: "title" },                    // ggsql's LABEL
    { name: "width" },
    { name: "height" },
    { name: "format" }                    // omit for a spec; 'html' for a page
  ]
};

function expand(args, ctx) {
  var chans = ["x", "y", "color", "size", "shape"];
  var pairs = [];
  for (var i = 0; i < chans.length; i++) {
    var c = chans[i];
    if (args[c]) { pairs.push('"' + c + '": ' + args[c]); }
  }
  // spec-level options travel as $-prefixed keys the aggregate peels off
  pairs.push('"$mark": ' + args.draw);
  var opts = ["title", "width", "height", "format"];
  for (var j = 0; j < opts.length; j++) {
    if (args[opts[j]]) { pairs.push('"$' + opts[j] + '": ' + args[opts[j]]); }
  }

  // Alias the source so a SUBQUERY works as the data (SQL++ requires an alias on a
  // FROM-clause subquery); unqualified channel refs still resolve against the one term.
  return "SELECT RAW chart_vegalite({" + pairs.join(", ") + "}) FROM " + args.src +
         " AS " + ctx.gensym("src");
}

macro.examples = [
  {
    desc: 'a line chart of totals over time, colored by status',
    in:  "@chart(orders, x => ts, y => total, color => status, draw => 'line')",
    out: '(SELECT RAW chart_vegalite({"x": ts, "y": total, "color": status, "$mark": \'line\'}) FROM orders AS src__m1)'
  }
];
