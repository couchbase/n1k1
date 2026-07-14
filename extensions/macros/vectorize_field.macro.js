// vectorize_field.macro.js — batch-embed a text field of a keyspace into per-row
// {id, text, vec}, ready for `INSERT INTO vecs SELECT ...` and VECTOR_DISTANCE search
// (DESIGN-vectors.md Phase 0). Sugars the explicit GROUP-BY-page batching wall:
//
//   INSERT INTO `vecs/data.jsonl` (KEY UUID(), VALUE self)
//   SELECT r.* FROM @vectorize_field(logs, field => line, id => META().id,
//                                    batch => 256, opts => {"dim":8}) AS r;
//   -- then: WITH q AS (VECTORIZE_BATCH([{"t":"disk full"}],{"text":"t","dim":8})[0].vec)
//   --       SELECT v.id FROM vecs v ORDER BY VECTOR_DISTANCE(v.vec, q, "cosine") ASC LIMIT 5;
//
// It pages rows via ROW_NUMBER (FLOOR((rn-1)/batch) -> 0-based pages), ARRAY_AGGs each
// page's {id,text}, calls VECTORIZE_BATCH once per page (one model round-trip, never per
// row), and UNNESTs back to per-row {id,text,vec}. `opts` is passed through to
// VECTORIZE_BATCH (dim/endpoint/model/fake); text/into are forced via OBJECT_PUT. No
// model/network with the default (empty endpoint) -> deterministic fake vectors.
//
// Wrap the call in FROM with an alias (AS r), like any subquery. See `.macro help`.

var macro = {
  name: "vectorize_field",
  params: [
    { name: "src",   required: true },        // keyspace / subquery of rows
    { name: "field", required: true },        // the text field/expr to embed (keyspace scope)
    { name: "id",    default: "META().id" },  // per-row id kept beside the vector
    { name: "batch", default: 256 },          // rows per model round-trip (one GROUP-BY page)
    { name: "into",  default: "vec" },        // output vector field name
    { name: "order", default: "" },           // ROW_NUMBER order (default: the id)
    { name: "opts",  default: "{}" }          // VECTORIZE_BATCH opts object: dim/endpoint/model/fake
  ]
};

function expand(args, ctx) {
  var k    = ctx.gensym("k");     // source alias (keyspace scope for field/id/META())
  var rows = ctx.gensym("rows");  // per-row {id,text,rn}
  var pg   = ctx.gensym("pg");    // per-page {batch:[{id,text}]}
  var emb  = ctx.gensym("emb");   // per-page {batch:[{id,text,vec}]}
  var row  = ctx.gensym("row");   // the UNNEST-ed per-row {id,text,vec}
  var rn   = ctx.gensym("rn");    // 1-based row ordinal
  var idc  = ctx.gensym("id");    // materialized id column
  var txt  = ctx.gensym("txt");   // materialized text column
  var order = (args.order && args.order.replace(/\s/g, "") !== "") ? args.order : args.id;
  // Force text/into onto the caller's opts (they can't be overridden): the batch objects
  // are {id,text}, so VECTORIZE_BATCH must read "text" and write the requested field.
  var vopts = 'OBJECT_PUT(OBJECT_PUT(' + args.opts + ', "text", "text"), "into", "' + args.into + '")';

  return "(SELECT " + row + ".* FROM (" +
    "SELECT VECTORIZE_BATCH(" + pg + ".b, " + vopts + ") AS b FROM (" +
      "SELECT ARRAY_AGG({\"id\": " + rows + "." + idc + ", \"text\": " + rows + "." + txt + "}) AS b FROM (" +
        "SELECT (" + args.id + ") AS " + idc + ", (" + args.field + ") AS " + txt + ", " +
               "ROW_NUMBER() OVER (ORDER BY " + order + ") AS " + rn +
        " FROM " + args.src + " AS " + k +
      ") AS " + rows +
      " GROUP BY FLOOR((" + rows + "." + rn + " - 1) / " + args.batch + ")" +
    ") AS " + pg +
  ") AS " + emb + " UNNEST " + emb + ".b AS " + row + ")";
}
