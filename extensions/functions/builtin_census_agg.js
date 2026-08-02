// version: v1.0
// labels: census,schema
//
// builtin_census_agg.js — the WHOLE schema census as one mergeable JS aggregate,
// shipped as an always-available builtin module. Contributed by the n1k1-for-ai team
// (ISSUE-22), bundled with the canonical-ordering fix; this is one of the TWO shipped
// forkable censuses (the other is builtin:census.sql++), and the retired native Go
// census survives only as CI's differential oracle over both (glue/census_oracle_test.go).
//
//     SELECT census_agg(r) AS cells FROM sessions r      -- one row out, ~265 cells inside
//
// Fork it: copy this file anywhere, edit the CENSUS_* knobs (or the walk itself), and
// load it with -ext — a loaded census_agg re-registers over this bundled one. That
// flexibility is the point: example values below cost ~40 lines; histogram buckets and
// deeper nesting are the same shape. VERIFY YOUR FORK: this census is customizable, and
// "customizable" is a different contract from "correct" — cross-check a fork against
// builtin:census.sql++ on a frozen corpus (two independent implementations disagreeing
// is how the two real bugs in this file's own history were caught; its goldens missed
// both).
//
// The fan-out and the grouping happen INSIDE the aggregate: one call per record, a map
// in the accumulator, zero intermediate rows (the shape the UNNEST+GROUP BY census pays
// materialization for). MERGEABLE, and that is the point rather than a bonus: state is
// key -> [docs, first_seen, last_seen, examples, first_enc] where the combine is
// (SUM, MIN, MAX, capped-set-union, MIN) — a commutative monoid, so census_agg over a
// cursor window folds into an accumulated census in any order. Defining merge is what
// lets n1k1 fold it across windows/shards at all.
//
// WHY A FLAT STRING KEY and not a nested {type: {path: ...}}: one map lookup per cell
// instead of two. NUL cannot occur in a JSON object key we would see here, so it is an
// unambiguous separator -- a "." or "|" would collide with the path syntax and with the
// first_enc composite.
//
// COVERAGE IS NOT COMPUTED. Same reason census.sql++ dropped it: a ratio does not
// merge. The denominator rides along as docs_in_type, so a consumer divides at read
// time and the core stays a clean monoid.

var CENSUS_TYPE_FIELD = "type";       // the record-type discriminator
var CENSUS_TIME_FIELD = "timestamp";  // the time axis for first_seen/last_seen
var CENSUS_ID_FIELD = "uuid";         // record id, for the first_enc argmin
// TWO DIFFERENT EXCLUSIONS, and conflating them is a bug this census shipped twice
// upstream. NO_DESCEND means DO NOT WALK CHILDREN -- the parent cell is still emitted,
// because "this field exists and is an object on N records" is real schema. OMIT is
// different: `_meta` is engine provenance injected by `-meta on`, never corpus schema,
// so it is omitted ENTIRELY (ISSUE-20). Applying the omit rule to both silently drops
// real cells -- caught by the differential against the oracle, not by the goldens.
var CENSUS_NO_DESCEND = {};                     // emit the cell, do not walk children
var CENSUS_OMIT = { _meta: 1 };                 // never emit, never walk
// EXAMPLE VALUES: up to N distinct scalars per cell, so "what does this field actually
// contain?" is answerable without a second pass. This is the flexibility argument for
// doing the census in an aggregate: here it is a bounded set in the accumulator,
// costing one map probe per cell. Expressed as rows it would need a second GROUP BY
// over (type, path, val_type, value) -- another fan-out over the whole corpus. Capped
// hard: an unbounded distinct set is not mergeable in bounded space, and a census that
// can blow up on a high-cardinality field is worse than one that says "too many".
var CENSUS_EXAMPLES = 5;
// NUL as the key separator: impossible in any key this corpus produces. Written via
// fromCharCode rather than a literal escape so the source stays greppable and
// diffable -- an embedded raw NUL byte makes grep treat the file as binary.
var CENSUS_SEP = String.fromCharCode(0);

function census_agg_type(v) {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  var t = typeof v;
  if (t === "number") return "number";
  if (t === "string") return "string";
  if (t === "boolean") return "boolean";
  return "object";
}

// Fold one cell into the map: docs++, first_seen=MIN, last_seen=MAX.
function census_agg_cell(m, key, ts, v, enc) {
  var e = m[key];
  if (e === undefined) { m[key] = [1, ts, ts, census_agg_ex(null, v), enc]; return; }
  e[0]++;
  e[3] = census_agg_ex(e[3], v);
  // first_enc: argmin-as-MIN over "<ts>|<id>". ISO-8601 sorts lexically, so the
  // smallest composite carries the timestamp AND the id of the first record to show
  // this cell -- and MIN merges, which a real argmin would not. Same trick
  // census.sql++ uses; here it costs a string compare per cell rather than a concat,
  // because the composite is built once per RECORD in _update, not once per cell.
  if (enc !== undefined && (e[4] === undefined || enc < e[4])) e[4] = enc;
  if (ts !== undefined) {
    if (e[1] === undefined || ts < e[1]) e[1] = ts;
    if (e[2] === undefined || ts > e[2]) e[2] = ts;
  }
}

// Example-value set: an array of up to CENSUS_EXAMPLES distinct scalars, or the string
// "(many)" once it overflows. Only scalars -- an object or array "example" is a
// document, not an example, and would make the state unbounded.
function census_agg_ex(cur, v) {
  var t = typeof v;
  if (v === null || (t !== "string" && t !== "number" && t !== "boolean")) return cur;
  if (t === "string" && v.length > 64) return cur;          // not an example, a payload
  if (cur === "(many)") return cur;
  if (cur === null || cur === undefined) return [v];
  for (var i = 0; i < cur.length; i++) if (cur[i] === v) return cur;
  if (cur.length >= CENSUS_EXAMPLES) return "(many)";
  cur.push(v);
  return cur;
}

function census_agg_init() { return { c: {}, t: {} }; }

function census_agg_update(st, r) {
  if (r === undefined || r === null || typeof r !== "object" || Array.isArray(r)) return st;
  var m = st.c;
  var rt = r[CENSUS_TYPE_FIELD];
  rt = (typeof rt === "string") ? rt : "";
  var ts = r[CENSUS_TIME_FIELD];
  if (typeof ts !== "string") ts = undefined;
  var id = r[CENSUS_ID_FIELD];
  var enc = (ts !== undefined && typeof id === "string") ? (ts + "|" + id) : undefined;

  // the per-type denominator, from the same pass
  st.t[rt] = (st.t[rt] || 0) + 1;

  var prefix = rt + CENSUS_SEP;
  for (var k in r) {
    if (!Object.prototype.hasOwnProperty.call(r, k)) continue;
    if (CENSUS_OMIT[k]) continue;
    var v = r[k], vt = census_agg_type(v);
    census_agg_cell(m, prefix + k + CENSUS_SEP + vt, ts, v, enc);
    // depth 2: an object emits a cell for ITSELF (above) and one per child. Emitting
    // only the children is a bug this census shipped for a week upstream -- a field
    // that is an object on some records and a scalar on others then shows one
    // val_type and never looks polymorphic.
    if (vt === "object" && !CENSUS_NO_DESCEND[k]) {
      var kp = prefix + k + ".";
      for (var c in v) {
        if (!Object.prototype.hasOwnProperty.call(v, c)) continue;
        census_agg_cell(m, kp + c + CENSUS_SEP + census_agg_type(v[c]), ts, v[c], enc);
      }
    }
  }
  return st;
}

// Canonical ordering, so the same set never serializes two ways. The SET is already
// order-independent (<= CENSUS_EXAMPLES distinct values all survive; a further
// distinct value collapses it to "(many)"), but the ORDER was insertion order, which
// is scan order -- so two folds of identical data produced byte-different artifacts
// and a committed census churned in git for no reason. Sort by (type, value) because
// a cell can hold mixed scalars and JS would otherwise compare number-vs-string by
// coercion.
function census_agg_ex_sort(a) {
  if (!a || a === "(many)") return a;
  return a.slice().sort(function (x, y) {
    var tx = typeof x, ty = typeof y;
    if (tx !== ty) return tx < ty ? -1 : 1;
    return x < y ? -1 : x > y ? 1 : 0;
  });
}

// Example sets combine by union-then-cap, which keeps the monoid: "(many)" absorbs,
// and the cap means a merge can never grow the state without bound.
function census_agg_ex_merge(a, b) {
  if (a === "(many)" || b === "(many)") return "(many)";
  if (!a) return b; if (!b) return a;
  var out = a.slice();
  for (var i = 0; i < b.length; i++) {
    var seen = false;
    for (var j = 0; j < out.length; j++) if (out[j] === b[i]) { seen = true; break; }
    if (seen) continue;
    if (out.length >= CENSUS_EXAMPLES) return "(many)";
    out.push(b[i]);
  }
  return out;
}

// The monoid: SUM docs, MIN first_seen, MAX last_seen. Commutative and associative,
// so a window census folds into an accumulated one in any order.
function census_agg_merge(A, B) {
  var out = { c: {}, t: {} }, k;
  for (k in A.c) if (Object.prototype.hasOwnProperty.call(A.c, k)) out.c[k] = [A.c[k][0], A.c[k][1], A.c[k][2], A.c[k][3], A.c[k][4]];
  for (k in B.c) {
    if (!Object.prototype.hasOwnProperty.call(B.c, k)) continue;
    var e = out.c[k], s = B.c[k];
    if (e === undefined) { out.c[k] = [s[0], s[1], s[2], s[3], s[4]]; continue; }
    e[0] += s[0];
    e[3] = census_agg_ex_merge(e[3], s[3]);
    if (s[4] !== undefined && (e[4] === undefined || s[4] < e[4])) e[4] = s[4];
    if (s[1] !== undefined && (e[1] === undefined || s[1] < e[1])) e[1] = s[1];
    if (s[2] !== undefined && (e[2] === undefined || s[2] > e[2])) e[2] = s[2];
  }
  for (k in A.t) if (Object.prototype.hasOwnProperty.call(A.t, k)) out.t[k] = A.t[k];
  for (k in B.t) if (Object.prototype.hasOwnProperty.call(B.t, k)) out.t[k] = (out.t[k] || 0) + B.t[k];
  return out;
}

// Expand to rows at the very end -- ~hundreds of objects, not millions. `docs_in_type`
// is the denominator carried through so coverage is a read-time divide.
function census_agg_final(st) {
  var m = st.c, totals = st.t, out = [], k;
  for (k in m) {
    if (!Object.prototype.hasOwnProperty.call(m, k)) continue;
    var p = k.split(CENSUS_SEP), e = m[k];
    var row = { type: p[0], path: p[1], val_type: p[2], docs: e[0],
                docs_in_type: totals[p[0]] || 0 };
    if (e[1] !== undefined) row.first_seen = e[1];
    if (e[2] !== undefined) row.last_seen = e[2];
    if (e[3] !== undefined && e[3] !== null) row.examples = census_agg_ex_sort(e[3]);
    if (e[4] !== undefined) row.first_enc = e[4];
    out.push(row);
  }
  out.sort(function (x, y) {
    return x.type < y.type ? -1 : x.type > y.type ? 1
         : x.path < y.path ? -1 : x.path > y.path ? 1
         : x.val_type < y.val_type ? -1 : x.val_type > y.val_type ? 1 : 0;
  });
  return out;
}

// The live "census so far" view for the running-aggregates channel: cheap counts, not
// the whole cell dump at 10 Hz. `final` above is pure, so it would also work -- this
// is just smaller on the wire while a big scan is in flight.
function census_agg_snapshot(st) {
  var cells = 0, recs = 0, k;
  for (k in st.c) if (Object.prototype.hasOwnProperty.call(st.c, k)) cells++;
  for (k in st.t) if (Object.prototype.hasOwnProperty.call(st.t, k)) recs += st.t[k];
  return { cells: cells, records: recs };
}

exports.functions = [
  {
    name: "census_agg", kind: "aggregate",
    init: census_agg_init, update: census_agg_update, final: census_agg_final,
    merge: census_agg_merge, snapshot: census_agg_snapshot,
    examples: [
      // Two records of one type, a nested object, and a scalar/object polymorphic
      // field: the depth-2 walk, the parent-cell rule and MIN/MAX time folding.
      { desc: "two records: parent cells, depth-2 children, MIN/MAX time, example values",
        in: [ { type: "a", timestamp: "2026-01-02", m: { x: 1 }, s: "hi" },
              { type: "a", timestamp: "2026-01-01", m: { y: "z" }, s: 5 } ],
        out: [ { type: "a", path: "m", val_type: "object", docs: 2, docs_in_type: 2,
                 first_seen: "2026-01-01", last_seen: "2026-01-02" },
               { type: "a", path: "m.x", val_type: "number", docs: 1, docs_in_type: 2,
                 first_seen: "2026-01-02", last_seen: "2026-01-02", examples: [1] },
               { type: "a", path: "m.y", val_type: "string", docs: 1, docs_in_type: 2,
                 first_seen: "2026-01-01", last_seen: "2026-01-01", examples: ["z"] },
               { type: "a", path: "s", val_type: "number", docs: 1, docs_in_type: 2,
                 first_seen: "2026-01-01", last_seen: "2026-01-01", examples: [5] },
               { type: "a", path: "s", val_type: "string", docs: 1, docs_in_type: 2,
                 first_seen: "2026-01-02", last_seen: "2026-01-02", examples: ["hi"] },
               { type: "a", path: "timestamp", val_type: "string", docs: 2, docs_in_type: 2,
                 first_seen: "2026-01-01", last_seen: "2026-01-02",
                 examples: ["2026-01-01", "2026-01-02"] },   // sorted, not insertion order
               { type: "a", path: "type", val_type: "string", docs: 2, docs_in_type: 2,
                 first_seen: "2026-01-01", last_seen: "2026-01-02", examples: ["a"] } ] },
    ]
  }
];
