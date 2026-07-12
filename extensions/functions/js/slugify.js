// slugify(s) -> a lowercase, dash-separated URL slug. Shows JS string/regexp
// builtins (a full ES runtime is available) returning a string value.
function slugify(s) {
  return String(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

slugify.examples = [
  { in: ["Hello, World!"],        out: "hello-world" },
  { in: ["  Trim -- Me  "],       out: "trim-me" },
  { in: ["CamelCase_and_123"],    out: "camelcase-and-123" },
];
