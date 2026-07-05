// series(start, stop[, step]) — a streaming table-valued source used in FROM.
// Emits {n} for each value start..stop (inclusive), one row at a time — no array
// is ever built, so `FROM series(1, 1000000) AS x` stays bounded-memory.
//   SELECT x.n, x.n*x.n AS sq FROM series(1, 10) AS x WHERE x.n % 2 = 0
function series(emit, start, stop, step) {
  step = step || 1;
  for (var n = start; n <= stop; n += step) emit({ n: n });
}
