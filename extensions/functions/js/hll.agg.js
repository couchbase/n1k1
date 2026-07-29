// hll — approximate distinct count (HyperLogLog), as a MERGEABLE JS aggregate.
//
// This is the flagship for the merge() hook (DESIGN-census.md engine-ask #2): a
// count-distinct whose accumulator is a fixed array of m=64 registers (precision
// p=6). Each register keeps the largest "rank" (1 + trailing-zero run of the hashed
// value's high bits) routed to that bucket. Registers combine by ELEMENTWISE MAX --
// idempotent, commutative, associative -- so hll is a monoid: hll_merge takes the
// pairwise max, and combine(part(A), part(B)) == aggregate(A ∪ B). That's what lets a
// census fold distinct-cardinality across time windows or shards without rescanning.
//
// State is a plain array (round-trips as JSON between rows, like every JS aggregate).
// The estimate is bias-corrected (small-range linear counting when many registers are
// still empty). It's approximate by design -- trading exactness for a bounded,
// mergeable footprint -- so it has no inline goldens; the monoid law is proved in Go
// (glue/agg_merge_test.go).
var HLL_P = 6, HLL_M = 1 << HLL_P; // 64 registers

function hll_hash(v) {
  var s = JSON.stringify(v), h = 2166136261 >>> 0; // 32-bit FNV-1a
  for (var i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619) >>> 0; }
  return h >>> 0;
}
function hll_init() { var r = []; for (var i = 0; i < HLL_M; i++) r.push(0); return r; }
function hll_update(regs, v) {
  if (v === undefined || v === null) return regs;
  var h = hll_hash(v);
  var idx = h & (HLL_M - 1);                     // low p bits pick the bucket
  var w = (h >>> HLL_P) | (1 << (32 - HLL_P));   // high bits + sentinel (rank stays bounded)
  var rank = 1;
  while ((w & 1) === 0) { rank++; w >>>= 1; }    // 1 + trailing zeros
  if (rank > regs[idx]) regs[idx] = rank;
  return regs;
}
function hll_merge(a, b) {                        // elementwise max — the monoid op
  var out = [];
  for (var i = 0; i < HLL_M; i++) out.push(a[i] > b[i] ? a[i] : b[i]);
  return out;
}
function hll_final(regs) {
  var alpha = 0.709, sum = 0, zeros = 0;          // alpha_m for m=64
  for (var i = 0; i < HLL_M; i++) { sum += Math.pow(2, -regs[i]); if (regs[i] === 0) zeros++; }
  var est = alpha * HLL_M * HLL_M / sum;
  if (est <= 2.5 * HLL_M && zeros > 0) est = HLL_M * Math.log(HLL_M / zeros); // linear counting
  return Math.round(est);
}
