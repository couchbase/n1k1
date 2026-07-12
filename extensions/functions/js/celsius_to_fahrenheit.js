// celsius_to_fahrenheit(c) -> c*9/5 + 32. Shows plain numeric JS.
function celsius_to_fahrenheit(c) {
  return c * 9 / 5 + 32;
}

// Inline golden examples (self-documenting + verified by `.extensions test`).
celsius_to_fahrenheit.examples = [
  { in: [100], out: 212 },
  { in: [0],   out: 32  },
  { desc: "body temperature", in: [37], out: 98.6 },
];
