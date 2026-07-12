// sessionize.macro.js — group consecutive events into sessions/episodes, starting a
// new session whenever the gap to the previous event exceeds a threshold.
//
// The cbcollect log-analysis primitive: cluster a burst of errors, split a node's log
// into activity episodes, isolate one rebalance/warmup run. It adds a `session_id`
// column (1, 2, 3, ...); GROUP BY it (or filter) downstream.
//
//   SELECT s.session_id, MIN(s.ts) AS start, COUNT(*) AS n
//     FROM @sessionize(logs, gap => 30000, order => ts, part => node) AS s
//    GROUP BY s.session_id, s.node;
//
// `gap` and `order` must be in the same units (e.g. epoch millis): a new session begins
// when (order - previous order) > gap, or at each partition's first row. Wrap the call
// in FROM with an alias (AS s), like any subquery.
//
// See `.macro help`. Sibling of grep_context.macro.js / the *.extract.js recipes.

var macro = {
  name: "sessionize",
  params: [
    { name: "src",   required: true },        // keyspace / subquery of events
    { name: "gap",   required: true },        // new session when (order - prev) > gap
    { name: "order", default: "ts" },         // ordering column (time), numeric
    { name: "part" },                         // optional PARTITION BY (per node/file/...)
    { name: "as",    default: "session_id" }  // name of the emitted session-id column
  ]
};

function expand(args, ctx) {
  var s    = ctx.gensym("src");      // source alias (so s.* FLATTENS the row fields)
  var sub  = ctx.gensym("sess");     // inner-subquery alias
  var flag = ctx.gensym("newsess");  // 1 at each session boundary, else 0
  var part = args.part ? ("PARTITION BY " + args.part + " ") : "";
  var win  = "(" + part + "ORDER BY " + args.order + ")";
  // Inner: flag session boundaries via LAG. Outer: session_id = running count of flags.
  return "SELECT " + sub + ".*, " +
    "SUM(" + sub + "." + flag + ") " +
      "OVER (" + part + "ORDER BY " + args.order + " ROWS UNBOUNDED PRECEDING) AS " + args.as + " " +
    "FROM (SELECT " + s + ".*, " +
      "CASE WHEN LAG(" + args.order + ") OVER " + win + " IS NULL " +
           "OR (" + args.order + " - LAG(" + args.order + ") OVER " + win + ") > (" + args.gap + ") " +
           "THEN 1 ELSE 0 END AS " + flag + " " +
      "FROM " + args.src + " AS " + s + ") AS " + sub;
}

// Inline goldens: a call (`in`) and the SQL++ it expands to (`out`).
macro.examples = [
  {
    desc: 'gap-based sessions per node, 1800s idle timeout',
    in:  '@sessionize(events, gap => 1800, order => ts, part => node)',
    out: '(SELECT sess__m2.*, SUM(sess__m2.newsess__m3) OVER (PARTITION BY node ORDER BY ts ROWS UNBOUNDED PRECEDING) AS session_id FROM (SELECT src__m1.*, CASE WHEN LAG(ts) OVER (PARTITION BY node ORDER BY ts) IS NULL OR (ts - LAG(ts) OVER (PARTITION BY node ORDER BY ts)) > (1800) THEN 1 ELSE 0 END AS newsess__m3 FROM events AS src__m1) AS sess__m2)'
  }
];
