// builtin_ejson.js — SQL++ helpers to convert VARIANT values to & from the EJSON-tagged
// typed-JSON form (e.g. {"$numberDecimal":"0.3"}) that the DECIMAL_* family and other
// marshal:"variant" functions produce. Thin SQL++-facing wrappers over the host `ejson`
// helper (glue/ext_jsvm.go): EJSON_DECODE strips tags back to plain JSON, EJSON_DECIMAL /
// EJSON_UNWRAP go the other way / peel one tag. Shipped as a builtin module (embedders
// glob builtin_*.js).

exports.functions = [
  {
    name: "EJSON_DECODE", marshal: "json",
    fn: function (x) { return ejson.decode(x); }, // recursively strip tags -> plain JSON
    examples: [
      { desc: "a decimal tag -> a plain number", in: [{ "$numberDecimal": "0.3" }], out: 0.3 },
      { desc: "recursive, inside an object", in: [{ "amt": { "$numberDecimal": "1.50" }, "qty": 2 }], out: { "amt": 1.5, "qty": 2 } },
      { desc: "plain JSON passes through unchanged", in: [{ "a": 1, "b": [2, 3] }], out: { "a": 1, "b": [2, 3] } },
    ],
  },
  {
    name: "EJSON_DECIMAL", marshal: "json",
    fn: function (x) { return ejson.decimal(x); }, // value -> {"$numberDecimal": "value"}
    examples: [
      { in: ["0.3"], out: { "$numberDecimal": "0.3" } },
    ],
  },
  {
    name: "EJSON_UNWRAP", marshal: "json",
    fn: function (x) { return ejson.unwrap(x); }, // peel ONE tag -> its inner string
    examples: [
      { in: [{ "$numberDecimal": "0.3" }], out: "0.3" },
    ],
  },
];
