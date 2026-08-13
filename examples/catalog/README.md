# `catalog/` — a markdown knowledge catalog (frontmatter + outline + links)

Six markdown files under `default/concepts/`, in the shape Jekyll, Hugo, Obsidian and
Google's [OKF](https://github.com/GoogleCloudPlatform/knowledge-catalog) all share: a
`---`-fenced YAML frontmatter block, then markdown prose. It's a miniature data catalog —
tables, a dashboard, a policy — that links to itself.

Two layers make it queryable, and they compose:

| layer | where it lives | gives you |
|---|---|---|
| frontmatter split | the scan layer (`records/extract.go`, native) | `front` (parsed object) + `body`, and `front_error` when a block won't parse |
| structure inside the markdown | `builtin_markdown.js` (shipped UDFs) | `md_sections` (outline), `md_links` (link graph), `md_code` (fenced blocks) |

`text` is still the whole file, so `LIKE`/FTS queries are unaffected.

Everything below runs as-is, with no `-ext` flag — both layers ship in the binary.

## The catalog, by its own metadata

```sh
n1k1 -c 'SELECT c.front.title, c.front.`type`, c.front.owner, c.front.status
           FROM concepts AS c WHERE c.front IS NOT MISSING
          ORDER BY c.front.`type`, c.front.title' examples/catalog
```

```
┌───────────────────┬───────────┬───────────┬────────┐
│ title             │ type      │ owner     │ status │
├───────────────────┼───────────┼───────────┼────────┤
│ Revenue by region │ dashboard │ analytics │ stable │
│ Freshness policy  │ policy    │ data-eng  │ draft  │
│ Customers         │ table     │ growth    │ stable │
│ Orders            │ table     │ data-eng  │ stable │
└───────────────────┴───────────┴───────────┴────────┘
```

Nested and list values survive, so `WHERE "sales" IN c.front.tags` and
`c.front.generated.by = "dbt"` both work. (`type` and `status` are SQL++ reserved
words — backtick them, and single-quote the whole `-c` argument.)

## Corpus hygiene — the failure paths are queryable

`scratch_notes.md` has no frontmatter and `legacy_import.md` has an intentionally
unterminated `tags:` list. Neither fails the scan:

```sh
n1k1 -c 'SELECT c.filename,
                CASE WHEN c.front_error IS NOT MISSING
                     THEN "broken frontmatter" ELSE "no frontmatter" END AS problem
           FROM concepts AS c WHERE c.front IS MISSING ORDER BY c.filename' examples/catalog
```

```
legacy_import.md   broken frontmatter
scratch_notes.md   no frontmatter
```

## Outline — a nested structure, a flat predicate

`trail` is the breadcrumb of enclosing headings, so "every subsection" is one `WHERE`:

```sh
n1k1 -c 'SELECT c.filename, s.depth, s.trail
           FROM concepts AS c UNNEST md_sections(c.body) AS s
          WHERE s.depth = 2 ORDER BY c.filename' examples/catalog
```

```
freshness_policy.md    2  Rules > Exceptions
orders.md              2  Schema > Notes
revenue_by_region.md   2  Definition > Caveats
```

Pass `c.body` (line numbers relative to the body) or `c.text` (frontmatter skipped, line
numbers counted from the top of the file) — both work.

## Dead links — an anti-join over the link graph

A concept's id is its path minus `.md`, so a dangling `/concepts/…` link is a `NOT IN`.
`orders.md` deliberately points at a `refunds` concept that was never written:

```sh
n1k1 -c 'SELECT DISTINCT c.filename AS src, l.target AS dangling
           FROM concepts AS c UNNEST md_links(c.body) AS l
          WHERE l.kind = "absolute"
            AND SPLIT(l.target, "/")[2] || ".md" NOT IN (SELECT RAW filename FROM concepts)' examples/catalog
```

```
orders.md   /concepts/refunds
```

`md_links` classifies each target as `absolute` (a bundle-root `/…` ref, OKF's
recommended form), `relative`, or `external`.

## Code fences — is the SQL in your docs still true?

```sh
n1k1 -c 'SELECT c.filename, k.lang, k.line FROM concepts AS c
           UNNEST md_code(c.body) AS k WHERE k.lang = "sql" ORDER BY c.filename' examples/catalog
```

The three SQL blocks are real: the one in `orders.md` runs against the sibling `shop/`
dataset, and `revenue_by_region.md`'s runs against `warehouse/`. Extracting them and
checking they still parse (or still run) is the natural next step — a docs-rot test.

```sh
n1k1 -c 'SELECT status, COUNT(*) AS n, ROUND(SUM(total), 2) AS revenue
           FROM orders GROUP BY status' examples/shop
```
