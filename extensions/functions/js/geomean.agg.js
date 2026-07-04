// geomean — geometric mean, as a JS aggregate (3-callback protocol).
// State {logsum,n}; accumulate log(v) for numeric positives (log-sum is
// numerically stabler than a running product), then exp(mean) at the end.
function geomean_init()        { return { logsum: 0, n: 0 }; }
function geomean_update(s, v)  { if (typeof v === "number" && v > 0) { s.logsum += Math.log(v); s.n++; } return s; }
function geomean_final(s)      { return s.n ? Math.exp(s.logsum / s.n) : null; }
