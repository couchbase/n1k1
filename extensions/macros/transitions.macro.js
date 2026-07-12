// transitions.macro.js — keep only rows where a field CHANGES value vs the previous
// row (per partition), carrying the previous value alongside.
//
// The state-machine / edge-detector shape: "when did node status flip healthy<->unhealthy",
// "when did the config epoch change", "show only the lines where the log level changed".
// It adds a `prev` column (the prior value); the current value is the row's own field.
//
//   SELECT x.ts, x.node, x.prev_val AS was, x.state AS now
//     FROM @transitions(status_log, of => state, order => ts, part => node) AS x;
//
// A change is detected with IS DISTINCT FROM (null-safe). By default each partition's
// FIRST row is kept (the initial state, prev IS NULL); pass include_first => false to
// keep only genuine changes. Wrap the call in FROM with an alias (AS x), like a subquery.
//
// See `.macro help`. Sibling of grep_context.macro.js / sessionize.macro.js.

var macro = {
  name: "transitions",
  params: [
    { name: "src",   required: true },       // keyspace / subquery
    { name: "of",    required: true },        // the field/expression watched for changes
    { name: "order", default: "ts" },         // ordering column (time)
    { name: "part" },                         // optional PARTITION BY (per node/entity)
    { name: "prev_as", default: "prev_val" }, // emitted previous-value column (avoid the reserved `prev`)
    { name: "include_first", default: true }  // keep each partition's first row?
  ]
};

function expand(args, ctx) {
  var t    = ctx.gensym("src");  // source alias (so t.* FLATTENS the row fields)
  var sub  = ctx.gensym("tr");   // inner-subquery alias
  var cur  = ctx.gensym("cur");  // the watched value, materialized for comparison
  var part = args.part ? ("PARTITION BY " + args.part + " ") : "";
  // include_first arrives as raw text ("true"/"false"); default true.
  var firstClause = (String(args.include_first) === "false")
    ? sub + "." + args.prev_as + " IS NOT NULL AND "
    : sub + "." + args.prev_as + " IS NULL OR ";
  return "SELECT " + sub + ".* FROM (" +
    "SELECT " + t + ".*, (" + args.of + ") AS " + cur + ", " +
      "LAG(" + args.of + ") OVER (" + part + "ORDER BY " + args.order + ") AS " + args.prev_as + " " +
    "FROM " + args.src + " AS " + t + ") AS " + sub + " " +
    "WHERE " + firstClause + sub + "." + cur + " IS DISTINCT FROM " + sub + "." + args.prev_as;
}

macro.examples = [
  {
    desc: 'rows where state changes, per node',
    in:  '@transitions(events, of => state, order => ts, part => node)',
    out: '(SELECT tr__m2.* FROM (SELECT src__m1.*, (state) AS cur__m3, LAG(state) OVER (PARTITION BY node ORDER BY ts) AS prev_val FROM events AS src__m1) AS tr__m2 WHERE tr__m2.prev_val IS NULL OR tr__m2.cur__m3 IS DISTINCT FROM tr__m2.prev_val)'
  }
];
