// slugify(s) -> a lowercase, dash-separated URL slug. Shows JS string/regexp
// builtins (goja provides a full ES runtime) returning a string value.
function slugify(s) {
  return String(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}
