// add_two_numbers(a, b) -> a + b. The simplest possible UDF, per the ask.
function add_two_numbers(a, b) {
  return a + b;
}

add_two_numbers.examples = [
  { in: [2, 3],   out: 5 },
  { in: [-1, 1],  out: 0 },
];
