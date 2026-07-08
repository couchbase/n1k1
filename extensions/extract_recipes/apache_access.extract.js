// apache_access.extract.js — a JS EXTRACT RECIPE for Apache/nginx "combined" access
// logs. It supplies only describe() (the cheap, once-per-file planning pass, run in
// goja); the per-row extract runs natively on records.SpecApply — no per-row JS.
//
// Drop this file into an -extractors dir; then `FROM <a matched access log>` yields
// typed rows: {ip, ts (int64 epoch-nanos), method, path, status, size, ...}.
//   SELECT l.status, COUNT(*) AS n FROM `access.log` l GROUP BY l.status
//
// See DESIGN-extensions.md "Extract functions" and records/spec.go for the shapes.

// `match` (module scope): which files this recipe claims. Same shape as
// records.ExtractMatch's json — exts AND/OR name-regexps, highest priority wins.
var match = {
  exts: [".log"],
  names: ["access[._-].*\\.log$", "(^|/)access\\.log$"],
  priority: 20
};

// describe(file) runs ONCE per matched file and returns a declarative ExtractSpec
// (records.ExtractSpec's json shape). file = { path, name, ext, head } — `head` is a
// decompressed head sample, here used to pick the timestamp timezone if present.
function describe(file) {
  // The Apache combined log format, as one anchored regexp with named captures.
  // Each (?P<name>...) becomes a typed field on the emitted record; `ts` is the
  // timestamp field, normalized to int64 epoch-nanos by the native extract layer.
  var pattern =
    '^(?P<ip>\\S+) \\S+ \\S+ \\[(?P<ts>[^\\]]+)\\] ' +
    '"(?P<method>\\S+) (?P<path>\\S+)[^"]*" ' +
    '(?P<status>\\d+) (?P<size>\\S+)';

  return {
    format: "apache_access_js",
    // One record per line (access logs don't span lines).
    framing: { kind: "line" },
    fields: { pattern: pattern },
    // Apache's "10/Oct/2000:13:55:36 -0700" — a Go reference-time layout string.
    // (Any layout that isn't an RFC3339/epoch_* tag is treated as a Go layout.)
    time: { field: "ts", layout: "02/Jan/2006:15:04:05 -0700" },
    // Access logs are wall-clock ordered; the native describe measures the real
    // sortedness/disorder from the sample, overriding this declared default.
    order: { by: "ts", sorted: "near" }
  };
}
