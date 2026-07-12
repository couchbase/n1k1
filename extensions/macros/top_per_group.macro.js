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
  // Rank via a running COUNT(1) over a ROWS-UNBOUNDED-PRECEDING frame -- position-based,
  // so it is exactly ROW_NUMBER (distinct 1,2,3,... even across ties). NOTE: we do NOT
  // use ROW_NUMBER()/RANK() here on purpose: a no-operand window function combined with
  // a whole-row `t.*` projection currently trips an engine bug (star + no-arg window ->
  // a value.binaryValue vs map attachment panic in glue Convert). COUNT(1) has an operand
  // and dodges it while giving the same ranks.
  return "SELECT " + sub + ".* FROM (" +
    "SELECT " + t + ".*, " +
      "COUNT(1) OVER (" + part + "ORDER BY " + args.order + " ROWS UNBOUNDED PRECEDING) AS " + rn + " " +
    "FROM " + args.src + " AS " + t + ") AS " + sub + " " +
    "WHERE " + sub + "." + rn + " <= " + args.n;
}
