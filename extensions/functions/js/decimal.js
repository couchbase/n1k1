// decimal.js — exact fixed-point DECIMAL arithmetic, as a multi-export JS module.
//
// JS numbers (and SQL++ numbers) are float64, so `0.1 + 0.2 !== 0.3`. These functions do
// EXACT base-10 math on a BigInt coefficient + scale, so DECIMAL_ADD("0.1","0.2") is
// exactly "0.3". Inputs may be a string, a number (its shortest round-trip form is used),
// or an EJSON-tagged {"$numberDecimal":"..."} (so calls nest: DECIMAL_ADD(DECIMAL_MUL(a,b),c)).
// Results are returned EJSON-tagged {"$numberDecimal":"..."} — the VARIANT-decimal wire
// form (marshal:"variant"); DECIMAL_CMP returns a plain -1/0/1 (marshal:"json").
//
// This is the two-birds demo for DESIGN-variant.md §5.1/§5.2: a whole DECIMAL_* family in
// one namespace file, and VARIANT-typed values handled in a JS extension.

function pow10(n) {
  var r = BigInt(1), ten = BigInt(10);
  for (var i = 0; i < n; i++) r = r * ten;
  return r;
}

// parseDec -> { coeff: BigInt, scale: int } for a decimal that equals coeff / 10^scale.
function parseDec(x) {
  if (x !== null && typeof x === "object") {
    // Unwrap an EJSON-tagged decimal (so calls nest). Round-trip through JSON first so a
    // host-wrapped object (goja's wrapper over the engine value) becomes a plain JS
    // object with primitive fields.
    var m = JSON.parse(JSON.stringify(x));
    if (m !== null && typeof m === "object" && typeof m["$numberDecimal"] === "string") {
      x = m["$numberDecimal"];
    }
  }
  var s = String(x).trim();
  var neg = false;
  if (s.charAt(0) === "+") s = s.slice(1);
  else if (s.charAt(0) === "-") { neg = true; s = s.slice(1); }
  var dot = s.indexOf(".");
  var scale = 0, digits = s;
  if (dot >= 0) {
    scale = s.length - dot - 1;
    digits = s.slice(0, dot) + s.slice(dot + 1);
  }
  if (digits.length === 0 || !/^[0-9]+$/.test(digits)) {
    throw new Error("DECIMAL: not a decimal number: " + JSON.stringify(x));
  }
  var coeff = BigInt(digits);
  if (neg) coeff = -coeff;
  return { coeff: coeff, scale: scale };
}

// align two decimals to a common scale, returning their scaled BigInt coefficients.
function align(a, b) {
  var scale = a.scale > b.scale ? a.scale : b.scale;
  return { scale: scale, a: a.coeff * pow10(scale - a.scale), b: b.coeff * pow10(scale - b.scale) };
}

// format coeff / 10^scale as a canonical decimal string (trailing-zero-trimmed, no -0).
function format(coeff, scale) {
  var neg = coeff < BigInt(0);
  var d = (neg ? -coeff : coeff).toString();
  if (scale > 0) {
    while (d.length <= scale) d = "0" + d; // keep at least one integer digit
    var cut = d.length - scale;
    var frac = d.slice(cut).replace(/0+$/, "");
    d = frac.length ? d.slice(0, cut) + "." + frac : d.slice(0, cut);
  }
  if (d === "0") return "0"; // normalize -0
  return (neg ? "-" : "") + d;
}

function dec(coeff, scale) { return { "$numberDecimal": format(coeff, scale) }; }

function add(x, y) { var z = align(parseDec(x), parseDec(y)); return dec(z.a + z.b, z.scale); }
function sub(x, y) { var z = align(parseDec(x), parseDec(y)); return dec(z.a - z.b, z.scale); }
function mul(x, y) { var a = parseDec(x), b = parseDec(y); return dec(a.coeff * b.coeff, a.scale + b.scale); }
function cmp(x, y) { var z = align(parseDec(x), parseDec(y)); return z.a < z.b ? -1 : (z.a > z.b ? 1 : 0); }

// Each entry carries inline golden examples ({in: [args], out: expected}) — self-
// documenting AND verified by `.extensions test` (per SQL name, like a single-file UDF).
exports.functions = [
  {
    name: "DECIMAL_ADD", marshal: "variant", fn: add,
    examples: [
      { desc: "exact — a plain float + drifts to 0.30000000000000004",
        in: ["0.1", "0.2"], out: { "$numberDecimal": "0.3" } },
      { in: ["1", "2.50"], out: { "$numberDecimal": "3.5" } },
      { desc: "exact beyond 2^53", in: ["123456789012345678", "1"],
        out: { "$numberDecimal": "123456789012345679" } },
    ],
  },
  {
    name: "DECIMAL_SUB", marshal: "variant", fn: sub,
    examples: [{ in: ["1", "0.9"], out: { "$numberDecimal": "0.1" } }],
  },
  {
    name: "DECIMAL_MUL", marshal: "variant", fn: mul,
    examples: [
      { in: ["1.5", "1.5"], out: { "$numberDecimal": "2.25" } },
      { in: ["0.1", "0.1"], out: { "$numberDecimal": "0.01" } },
    ],
  },
  {
    name: "DECIMAL_CMP", marshal: "json", fn: cmp,
    examples: [
      { desc: "0.10 equals 0.1", in: ["0.10", "0.1"], out: 0 },
      { in: ["0.2", "0.1"], out: 1 },
      { in: ["0.1", "0.2"], out: -1 },
    ],
  },
];
