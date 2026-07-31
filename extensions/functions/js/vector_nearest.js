// version: v1.0
//
// vector_nearest(vec, cents) -> the index of the centroid in `cents` nearest to `vec`
// (squared euclidean -- monotone with euclidean, so the argmin is identical and we skip
// the sqrt). NULL/MISSING vec, empty cents, or a dimension mismatch -> null (never a
// guess). This is the k-means ASSIGNMENT step as a scalar UDF: the pure-SQL++ spelling
// (ARRAY_SORT over [dist, i] pairs -- see examples/kmeans/) works with no extensions,
// but the UDF is clearer to read, tolerant of ragged input, and one goja call per row
// instead of K array constructions. Sibling: vector_nearest_dist.js ([idx, dist]).
//
// The engine's VECTOR_DISTANCE is NOT usable for this argmin today: its (cbq-inherited)
// validation requires a STATIC query vector (constant / $param / WITH alias), and a
// comprehension variable ranging over the centroid list doesn't qualify. A scalar UDF
// (or plain SQL++ arithmetic) sidesteps that without engine changes.
function vector_nearest(vec, cents) {
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
  return best < 0 ? null : best;
}

vector_nearest.examples = [
  { in: [[1, 0], [[1, 0], [0, 1]]], out: 0 },
  { in: [[0.1, 0.9], [[1, 0], [0, 1]]], out: 1 },
];
