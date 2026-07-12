// top_per_group.macro.js — keep the top-N rows per partition, ranked by an ORDER BY.
//
// The "N slowest ops per node", "top 3 error types per keyspace", "the highest-latency
// request per collection" shape — a ROW_NUMBER() subquery that's tedious to write by hand.
//
//   SELECT t.node, t.op, t.duration
//     FROM @top_per_group(ops, part => node, order => duration DESC, n => 5) AS t;
//
// `order` is a full ORDER BY expression (may include DESC and multiple keys). With no
// `part`, it's the global top-N. Ties break arbitrarily (ROW_NUMBER gives exactly N).
// Wrap the call in FROM with an alias (AS t), like any subquery.
//
// See `.macro help`. Sibling of grep_context.macro.js / sessionize.macro.js.

var macro = {
  name: "top_per_group",
  params: [
    { name: "src",   required: true },  // keyspace / subquery
    { name: "order", required: true },  // ORDER BY expr for the ranking (e.g. "duration DESC")
    { name: "n",     default: 1 },      // rows to keep per partition
    { name: "part" }                    // optional PARTITION BY (omit for global top-N)
  ]
};

function expand(args, ctx) {
  if (typeof args.$lit.n !== "number") ctx.error("n must be a numeric literal");
  var t    = ctx.gensym("src");  // source alias (so t.* FLATTENS the row fields)
  var sub  = ctx.gensym("top");  // inner-subquery alias
  var rn   = ctx.gensym("rn");   // per-partition rank
  var part = args.part ? ("PARTITION BY " + args.part + " ") : "";
  return "SELECT " + sub + ".* FROM (" +
    "SELECT " + t + ".*, " +
      "ROW_NUMBER() OVER (" + part + "ORDER BY " + args.order + ") AS " + rn + " " +
    "FROM " + args.src + " AS " + t + ") AS " + sub + " " +
    "WHERE " + sub + "." + rn + " <= " + args.n;
}
