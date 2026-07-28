// toml2.extract.js — a JS EXTRACT RECIPE that parses TOML files, as a DEMO of the
// imperative extract(file, emit) path (DESIGN-extensions.md "Extract functions").
//
// n1k1 already supports TOML natively (in Go, for ".toml"); this recipe deliberately
// re-implements it in JS for a *different* extension, ".toml2", to prove that a
// user-supplied JS extract can own framing + parsing end-to-end and produce the SAME
// records as the native reader. Copy any .toml file to .toml2, load this recipe with
// `-ext <dir>`, and `FROM` it:
//
//   cp config.toml config.toml2
//   n1k1 -ext extensions/extract_recipes config.toml2 -c 'SELECT * FROM config'
//
// Unlike a describe()-only recipe (which returns a declarative spec the native framer
// runs), this recipe defines only extract(file, emit): a TOML file is one whole
// document, so "framing" is trivial (one frame = the file) and the interesting work is
// PARSING — done here, in JS.

// `match` (module scope): claim the brand-new ".toml2" extension. A registered
// recipe's claim is what makes such files records at all (records.IsRecordFile).
var match = { exts: [".toml2"], priority: 10 };

// extract(file, emit): parse the whole document and emit it as one record, keyed by
// the file stem — the one-doc-per-file convention native .toml/.json use. The emitted
// object is JSON-marshaled by the host (sorted keys, integer-valued floats without a
// trailing ".0"), so it lands on the same canonical JSON the native TOML reader emits.
function extract(file, emit) {
  emit(parseTOML(file.text), file.stem);
}

// ---- a compact TOML parser (a pragmatic subset: comments, key=value, [tables],
// ---- [[arrays of tables]], strings, ints, floats, bools, RFC3339 datetimes, and
// ---- single-line arrays — enough to round-trip typical config documents). ----

function parseTOML(text) {
  var root = {};
  var cur = root; // the table current bare keys attach to.
  var lines = text.split(/\r?\n/);
  for (var i = 0; i < lines.length; i++) {
    var line = stripComment(lines[i]).replace(/^\s+|\s+$/g, "");
    if (line === "") { continue; }

    var mm = line.match(/^\[\[\s*(.+?)\s*\]\]$/); // [[array of tables]]
    if (mm) { cur = pushArrayTable(root, mm[1]); continue; }

    mm = line.match(/^\[\s*(.+?)\s*\]$/); // [table]
    if (mm) { cur = ensureTable(root, mm[1]); continue; }

    var eq = findTopEq(line); // key = value
    if (eq < 0) { throw "toml2: expected 'key = value', got: " + lines[i]; }
    var key = unquote(line.slice(0, eq).replace(/^\s+|\s+$/g, ""));
    cur[key] = parseValue(line.slice(eq + 1).replace(/^\s+|\s+$/g, ""));
  }
  return root;
}

// stripComment drops a trailing '#' comment, but not a '#' inside a quoted string.
function stripComment(line) {
  var inS = null; // the open quote char, or null.
  for (var i = 0; i < line.length; i++) {
    var c = line[i];
    if (inS) {
      if (c === "\\" && inS === '"') { i++; continue; } // skip an escaped char.
      if (c === inS) { inS = null; }
    } else if (c === '"' || c === "'") {
      inS = c;
    } else if (c === "#") {
      return line.slice(0, i);
    }
  }
  return line;
}

// findTopEq returns the index of the first '=' not inside a quoted key.
function findTopEq(line) {
  var inS = null;
  for (var i = 0; i < line.length; i++) {
    var c = line[i];
    if (inS) { if (c === inS) { inS = null; } }
    else if (c === '"' || c === "'") { inS = c; }
    else if (c === "=") { return i; }
  }
  return -1;
}

// ensureTable navigates/creates the nested table named by a dotted key path.
function ensureTable(root, dotted) {
  var parts = splitDotted(dotted);
  var t = root;
  for (var i = 0; i < parts.length; i++) {
    var k = parts[i];
    if (t[k] === undefined || t[k] === null) { t[k] = {}; }
    t = t[k];
  }
  return t;
}

// pushArrayTable appends a fresh table to the array named by a dotted key path,
// creating the array (and any parent tables) as needed, and returns the new table.
function pushArrayTable(root, dotted) {
  var parts = splitDotted(dotted);
  var t = root;
  for (var i = 0; i < parts.length - 1; i++) {
    var k = parts[i];
    if (t[k] === undefined || t[k] === null) { t[k] = {}; }
    t = t[k];
  }
  var last = parts[parts.length - 1];
  if (!(t[last] instanceof Array)) { t[last] = []; }
  var obj = {};
  t[last].push(obj);
  return obj;
}

// splitDotted splits a dotted key path on top-level '.', honoring quoted segments.
function splitDotted(s) {
  var out = [], cur = "", inS = null;
  for (var i = 0; i < s.length; i++) {
    var c = s[i];
    if (inS) { if (c === inS) { inS = null; } else { cur += c; } }
    else if (c === '"' || c === "'") { inS = c; }
    else if (c === ".") { out.push(cur.replace(/^\s+|\s+$/g, "")); cur = ""; }
    else { cur += c; }
  }
  out.push(cur.replace(/^\s+|\s+$/g, ""));
  return out;
}

function unquote(s) {
  if (s.length >= 2 && (s[0] === '"' || s[0] === "'") && s[s.length - 1] === s[0]) {
    return s[0] === '"' ? unescapeBasic(s.slice(1, -1)) : s.slice(1, -1);
  }
  return s;
}

function unescapeBasic(s) {
  return s.replace(/\\(["\\ntr])/g, function (_, c) {
    return c === "n" ? "\n" : c === "t" ? "\t" : c === "r" ? "\r" : c;
  });
}

var RFC3339 = /^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$/;

function parseValue(s) {
  if (s === "") { throw "toml2: empty value"; }
  var c = s[0];
  if (c === '"' || c === "'") { return unquote(s); }
  if (c === "[") { return parseArray(s); }
  if (s === "true") { return true; }
  if (s === "false") { return false; }
  if (RFC3339.test(s)) { return s; } // keep datetimes as their RFC3339 string.
  // number: strip TOML's underscore digit separators.
  var n = s.replace(/_/g, "");
  if (/^[+-]?\d+$/.test(n)) { return parseInt(n, 10); }
  if (/^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$/.test(n)) { return parseFloat(n); }
  throw "toml2: unrecognized value: " + s;
}

// parseArray parses a single-line "[a, b, c]" of scalars/strings/arrays.
function parseArray(s) {
  var inner = s.replace(/^\[\s*|\s*\]$/g, "");
  if (inner === "") { return []; }
  var out = [], depth = 0, inQ = null, cur = "";
  for (var i = 0; i < inner.length; i++) {
    var ch = inner[i];
    if (inQ) { cur += ch; if (ch === inQ) { inQ = null; } continue; }
    if (ch === '"' || ch === "'") { inQ = ch; cur += ch; continue; }
    if (ch === "[") { depth++; cur += ch; continue; }
    if (ch === "]") { depth--; cur += ch; continue; }
    if (ch === "," && depth === 0) { out.push(parseValue(cur.replace(/^\s+|\s+$/g, ""))); cur = ""; continue; }
    cur += ch;
  }
  if (cur.replace(/^\s+|\s+$/g, "") !== "") { out.push(parseValue(cur.replace(/^\s+|\s+$/g, ""))); }
  return out;
}

// Inline goldens: verified by `.extensions test`. The `out` is the one emitted record.
var examples = [
  {
    desc: "a small TOML document -> one record (keys canonicalized by the host)",
    in: "name = \"alpha\"\nreplicas = 3\nenabled = true\ntags = [\"web\", \"prod\"]\n",
    out: [
      { name: "alpha", replicas: 3, enabled: true, tags: ["web", "prod"] }
    ]
  }
];
