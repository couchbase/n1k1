// series(start, stop[, step]) — a streaming table-valued source used in FROM.
// Emits {n} for each value start..stop (inclusive), one row at a time — no array
// is ever built, so `FROM series(1, 1000000) AS x` stays bounded-memory.
//   SELECT x.n, x.n*x.n AS sq FROM series(1, 10) AS x WHERE x.n % 2 = 0
function series(emit, start, stop, step) {
  step = step || 1;
  for (var n = start; n <= stop; n += step) emit({ n: n });
}

// Inline goldens: series(args) -> the emitted rows.
series.examples = [
  { in: [1, 3],       out: [{ n: 1 }, { n: 2 }, { n: 3 }] },
  { desc: "step 2",   in: [0, 5, 2], out: [{ n: 0 }, { n: 2 }, { n: 4 }] },
];
