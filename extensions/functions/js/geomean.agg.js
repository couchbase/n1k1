// geomean — geometric mean, as a JS aggregate (3-callback protocol).
// State {logsum,n}; accumulate log(v) for numeric positives (log-sum is
// numerically stabler than a running product), then exp(mean) at the end.
function geomean_init()        { return { logsum: 0, n: 0 }; }
function geomean_update(s, v)  { if (typeof v === "number" && v > 0) { s.logsum += Math.log(v); s.n++; } return s; }
function geomean_final(s)      { return s.n ? Math.exp(s.logsum / s.n) : null; }
// Optional merge: {logsum,n} is additive, so geomean is a commutative monoid --
// defining geomean_merge makes it foldable across windows/shards (glue.CombineAggregate).
function geomean_merge(a, b)   { return { logsum: a.logsum + b.logsum, n: a.n + b.n }; }

// Inline goldens: the geometric mean of a value sequence (`in`) -> the final value.
var examples = [
  { desc: "geomean(1,10,100) = 10", in: [1, 10, 100], out: 10 },
  { in: [2, 8], out: 4 },
];
