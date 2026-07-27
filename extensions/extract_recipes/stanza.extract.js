// stanza.extract.js — a JS EXTRACT RECIPE demonstrating the STREAMING imperative path
// (extractStream, DESIGN-extensions.md "Extract functions"). It parses a ".stanza"
// file: blank-line-delimited records, each a block of "key: value" lines. For example
//
//   name: alpha
//   role: web
//   weight: 3
//
//   name: beta
//   role: db
//
// yields two records {name, role, weight?}. This is a stateful framing native
// declarative specs can't express (records are grouped by BLANK LINES, and every data
// line is a key:value — there is no single "lead line" pattern), so JS owns it.
//
// Unlike extract(file, emit) (which gets the whole file text and buffers its records),
// extractStream reads the file INCREMENTALLY via file.readLine() and emits records that
// flow out one at a time with backpressure — so a huge multi-record file frames at
// bounded memory, and emit() returns false (letting the loop stop) once the consumer is
// done (e.g. a LIMIT is satisfied or the query is cancelled).

var match = { exts: [".stanza"], priority: 10 };

function extractStream(file, emit) {
  var rec = null;
  var line;
  while ((line = file.readLine()) !== null) {
    if (line.replace(/^\s+|\s+$/g, "") === "") { // blank line ends the current record.
      if (rec && !emit(rec)) { return; }         // emit()==false: consumer is done.
      rec = null;
      continue;
    }
    var i = line.indexOf(":");
    if (i < 0) { continue; } // not a key:value line — skip.
    if (!rec) { rec = {}; }
    var k = line.slice(0, i).replace(/^\s+|\s+$/g, "");
    var v = line.slice(i + 1).replace(/^\s+|\s+$/g, "");
    rec[k] = coerce(v);
  }
  if (rec) { emit(rec); } // a trailing record with no blank line before EOF.
}

// coerce gives values light type inference (int / float / bool), else keeps the string
// — so "weight: 3" is a JSON number and "WHERE weight > 1" compares numerically.
function coerce(v) {
  if (v === "true") { return true; }
  if (v === "false") { return false; }
  if (/^[+-]?\d+$/.test(v)) { return parseInt(v, 10); }
  if (/^[+-]?(\d+\.\d*|\.\d+)([eE][+-]?\d+)?$/.test(v)) { return parseFloat(v); }
  return v;
}

// Inline goldens: verified by `.extensions test`. Records are keyed "<prefix>#<n>".
var examples = [
  {
    desc: "two blank-line-delimited stanzas -> two typed records",
    in: "name: alpha\nrole: web\nweight: 3\n\nname: beta\nrole: db\n",
    out: [
      { name: "alpha", role: "web", weight: 3 },
      { name: "beta", role: "db" }
    ]
  }
];
