// grep_context.macro.js — grep-style -A/-B/-C context, as a one-line SQL++ macro.
//
// Selecting the rows within N lines of a match is the most natural log question,
// and hand-writing it means a windowed subquery most people (and AIs, first try)
// can't recall. This macro turns it into:
//
//   SELECT g.pos, g.line
//     FROM @grep_context(logs, when => sev = "ERROR", before => 2, after => 2) AS g
//    ORDER BY g.pos;
//
// which expands (before cbq's parser) into a subquery that flags any row inside a
// ±(before,after) window of a match and keeps those rows. Because the expansion is
// paren-wrapped, use it in FROM with an alias (AS g), exactly like a subquery.
//
// See `.macro help` / DESIGN-extensions.md "Macros". Sibling of *.extract.js recipes.

var macro = {
  name: "grep_context",
  params: [
    { name: "src",    required: true },            // the keyspace / subquery to scan
    { name: "when",   required: true },             // the match predicate (raw SQL++)
    { name: "before", default: 2 },                 // lines of leading context (-B)
    { name: "after",  default: 2 },                 // lines of trailing context (-A)
    { name: "order",  default: "pos" },             // the row-order column (grep needs a stable order)
    { name: "part" }                                // optional PARTITION BY column (per-file, per-node, ...)
  ]
};

function expand(args, ctx) {
  if (typeof args.$lit.before !== "number" || typeof args.$lit.after !== "number") {
    ctx.error("before/after must be numeric literals");
  }
  // Hygiene: every macro-introduced name is gensym'd, so two uses (or nesting)
  // never collide with each other or with the caller's identifiers.
  var s   = ctx.gensym("src");   // alias for the source (so `s.*` FLATTENS its fields;
  var sub = ctx.gensym("gc");    //   bare `SELECT *` would nest them under the keyspace)
  var hit = ctx.gensym("hit");   // the window "in a context window?" marker column
  var part = args.part ? ("PARTITION BY " + args.part + " ") : "";
  // Frame bounds are SWAPPED vs before/after -- a perspective duality, NOT an engine
  // quirk. A window answers, per row r, "is a match in MY frame [r-PRECEDING,
  // r+FOLLOWING]?". grep asks the mirror: "print the [before,after] frame of each
  // MATCH". Row r is in match m's print-range [m-before, m+after] IFF match m is in
  // r's frame [r-after, r+before]. So to keep `before` lines before a match, each row
  // must look `before` lines FORWARD to see it: FOLLOWING=before, PRECEDING=after.
  return (
    "SELECT " + sub + ".* FROM (" +
      "SELECT " + s + ".*, MAX(CASE WHEN (" + args.when + ") THEN 1 ELSE 0 END) " +
        "OVER (" + part + "ORDER BY " + args.order + " " +
                  "ROWS BETWEEN " + args.after  + " PRECEDING " +
                            "AND " + args.before + " FOLLOWING) AS " + hit + " " +
      "FROM " + args.src + " AS " + s + ") " + sub + " " +
    "WHERE " + sub + "." + hit + " = 1"
  );
}
