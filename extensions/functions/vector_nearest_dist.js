// version: v1.0
//
// vector_nearest_dist(vec, cents) -> [idx, dist]: the index of the nearest centroid in
// `cents` AND the true euclidean distance to it, in one pass -- for when the caller
// wants both (within-cluster ordering, outlier/spill reports, SemDeDup-style dedup by
// centroid distance) without recomputing. NULL/MISSING vec, empty cents, or a dimension
// mismatch -> null. Sibling of vector_nearest.js (index only); same argmin, same
// malformed-centroid tolerance.
function vector_nearest_dist(vec, cents) {
  if (!vec || !vec.length || !cents || !cents.length) return null;
  var best = -1, bestD = Infinity;
  for (var i = 0; i < cents.length; i++) {
    var c = cents[i];
    if (!c || c.length !== vec.length) continue; // skip malformed centroid
    var d = 0;
    for (var p = 0; p < vec.length; p++) {
      var t = vec[p] - c[p];
      d += t * t;
    }
    if (d < bestD) { bestD = d; best = i; }
  }
  return best < 0 ? null : [best, Math.sqrt(bestD)];
}

vector_nearest_dist.examples = [
  { in: [[1, 0], [[1, 0], [0, 1]]], out: [0, 0] },
  { in: [[0, 0], [[3, 4], [0, 1]]], out: [1, 1] },
];
