// couchbase_log.extract.js — a JS EXTRACT RECIPE for cbcollect_info's couchbase.log:
// tens of MB of ====-delimited command dumps (uname/date/ntpq/sysctl/...). It supplies
// only describe() (the cheap, once-per-file planning pass, run in goja); the per-record
// framing runs natively on records.SpecApply (section framing) — no per-row JS.
//
// Drop this file into an -extractors dir; then `FROM couchbase.log` yields one row per
// command section: {title (the command banner), text (its output)}.
//   SELECT c.title FROM `couchbase.log` c WHERE c.text LIKE "%error%"
//
// See DESIGN-extensions.md "Extract functions" and records/spec.go for the shapes.

// `match` (module scope): claim the cbcollect couchbase.log by name (any directory).
var match = {
  names: ["(^|/)couchbase\\.log$"],
  priority: 20
};

// describe(file) runs ONCE per matched file and returns a declarative ExtractSpec.
function describe(file) {
  return {
    format: "cbcollect_couchbase_log",
    // Section framing: each command dump is wrapped in ====-banner rules
    // (`====` / <command> / `====` / <output>). The command line between the banner
    // rules becomes `title`; the output up to the next banner becomes `text`.
    framing: { kind: "section", section: "^={10,}$" },
    // Command dumps carry no single sortable timestamp — an unsorted blob source.
    order: { sorted: "none" }
  };
}

// Inline goldens: a sample file (`in`) -> the framed rows (`out`), verified by
// `.extensions test`. Two ==== command sections become two {title, text} rows.
var examples = [
  {
    desc: "two command sections -> one row each",
    in: "==========\nuname -a\n==========\nLinux host 5.4.0\n==========\ndate\n==========\nMon Jan 1 2026\n",
    out: [
      { title: "uname -a", text: "Linux host 5.4.0" },
      { title: "date",     text: "Mon Jan 1 2026" }
    ]
  }
];
