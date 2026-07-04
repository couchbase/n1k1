// slugify(s) -> a lowercase, dash-separated URL slug. Shows JS string/regexp
// builtins (a full ES runtime is available) returning a string value.
function slugify(s) {
  return String(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}
