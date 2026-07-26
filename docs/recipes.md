# SQL++ recipes — a SQL++ / SQL / jq Rosetta stone

_Slicing and dicing JSON: the same maneuver in three tools._

This guide is about doing JSON surgery in **SQL++** (the N1QL dialect n1k1 runs),
with each recipe cross-translated so you can lean on what you already know:

- **SQL++** — n1k1's query language: JSON is the native value model, and you query
  *collections* of it (a keyspace, or a literal array). **The runnable block.**
- **SQL** — a relational engine with JSON functions (PostgreSQL `jsonb`; SQLite
  `json1` noted where it differs).
- **jq** — the streaming JSON filter (`jqlang.org`), one value in, a stream out.

## Running the examples

**Every SQL++ block below is a complete statement you can copy and run** — each was
executed against n1k1 and its output shown as a `→` comment. Paste one into `-c`:

```sh
n1k1 -c 'SELECT ARRAY_SUM([1,2,3]) AS total'      # → {"total":6}
```

Blocks that read literal arrays (`FROM [ … ] AS x`) are self-contained. Blocks that
read `FROM orders` / `FROM customers` use the bundled shop dataset — pass it as the
data root:

```sh
n1k1 -c 'SELECT * FROM orders LIMIT 1' examples/shop
# {"type":"order","id":"1005","customer":"dave","total":22.0,"items":1,"status":"shipped","ts":"2026-01-06"}
n1k1 -c 'SELECT * FROM customers LIMIT 1' examples/shop
# {"type":"customer","id":"dave","name":"Dave Kim","city":"Austin","since":2020}
```

Or start a REPL (`n1k1 examples/shop`) and paste statements at the prompt.

---

## The three mental models

The tools solve the same problems from different starting points — internalizing
this makes every translation below obvious.

| | SQL++ | SQL | jq |
|---|---|---|---|
| **Unit of work** | a collection of documents | rows in a table | one JSON value, streamed |
| **JSON is** | the native value type | a `json`/`jsonb` column | the whole world |
| **"for each element"** | `FROM arr AS x` / `UNNEST` | `jsonb_array_elements(col)` | `.[]` (iterate) |
| **transform each** | `SELECT f …` or `ARRAY f FOR x IN … END` | `SELECT f FROM …` | `map(f)` |
| **keep some** | `WHERE cond` | `WHERE cond` | `select(cond)` |
| **chain steps** | nested subqueries / `LET` | subqueries / CTEs | `\|` (pipe) |
| **build an object** | `{"a": x}` | `jsonb_build_object('a', x)` | `{a: .x}` |
| **object ↔ pairs** | `OBJECT_PAIRS` / `OBJECT … FOR … END` | `jsonb_each` / `jsonb_object_agg` | `to_entries`/`from_entries` |

The punchline: **SQL++ and SQL treat every task as a query over a collection; jq
treats it as a stream rewrite.** jq's `map`/`select`/`group_by` are pipeline stages;
in SQL++ and SQL they are `SELECT`/`WHERE`/`GROUP BY`. What makes SQL++ special is
that it *also* has jq's JSON-surgery verbs (`ARRAY … FOR`, `OBJECT … FOR`, `WITHIN`,
`UNNEST`, `OBJECT_PAIRS`) — so you rarely have to choose between the set-query and
the document-surgery styles.

---

## 1. Access & navigation

### Field access — `.foo`, `.foo.bar`
```sql
SELECT RAW o.customer FROM orders o          -- → "dave", "alice", … (one per order)
```
> **SQL** `SELECT col->>'customer' FROM orders` (`->>` text, `->` jsonb) · **jq** `.customer`

Nested fields: SQL++ `x.a.b` · Postgres `col#>>'{a,b}'` (SQLite `col->>'$.a.b'`) ·
jq `.a.b`.

### Optional / missing field — `.foo?`
jq's `?` suppresses "cannot index" errors. SQL++ has no such error: a missing path
is the value **`MISSING`**, which simply drops out of results (see §11).
```sql
SELECT x.foo FROM [[1,2]] AS x               -- → {}   (foo is MISSING, omitted)
```
> **SQL** `SELECT col->'foo'` (NULL if absent) · **jq** `.foo?` (empty; no error)

### Array index — `.[0]`, `.[-1]`
SQL++ indexes are **0-based**, and a negative index counts from the end (like jq).
```sql
SELECT [10,20,30][0]  AS `first`,
       [10,20,30][-1] AS `last`,             -- → {"first":10,"last":30,"penult":20}
       [10,20,30][-2] AS penult
```
> **SQL** `col->0` (Postgres jsonb: 0-based, `-1` = last) · **jq** `.[0]`, `.[-1]`

⚠ An out-of-range *positive* index is `MISSING`; a negative index **wraps** to the
end, so guard `i >= 0` when computing indices (as `examples/queries/life.sql++`
does for board edges).

### Slice — `.[2:4]`
```sql
SELECT ["a","b","c","d","e"][2:4] AS s        -- → {"s":["c","d"]}
```
> **SQL** `jsonb_path_query(col,'$[2 to 3]')` (SQLite has no slice) · **jq** `.[2:4]`

---

## 2. Iterating & projecting

### Iterate — scan / emit each element
This is the pivot between the models: SQL++/SQL *scan* a collection, jq *streams*.
```sql
SELECT RAW x FROM [{"name":"JSON"},{"name":"XML"}] AS x
-- → {"name":"JSON"}
--   {"name":"XML"}
```
> **SQL** `SELECT value FROM jsonb_array_elements(col)` · **jq** `.[]`

### Project a field from each element
```sql
SELECT RAW x.name FROM [{"name":"JSON"},{"name":"XML"}] AS x   -- → "JSON", "XML"
```
> **SQL** `SELECT e->>'name' FROM jsonb_array_elements(col) e` · **jq** `.[] | .name`

### map — transform each element into an array
SQL++ offers two forms: a set query (`SELECT`), or the inline **array comprehension**
`ARRAY f FOR x IN arr END` when you want the result as one array value.
```sql
SELECT ARRAY v+1 FOR v IN [1,2,3] END AS r    -- → {"r":[2,3,4]}
```
> **SQL** `jsonb_agg(value::int+1) FROM jsonb_array_elements('[1,2,3]')` · **jq** `map(.+1)`

### Comma — multiple outputs
```sql
SELECT o.foo, o.bar FROM [{"foo":42,"bar":"x"}] AS o   -- → {"foo":42,"bar":"x"}
```
> **SQL** `SELECT col->'foo', col->'bar'` · **jq** `.foo, .bar` (a two-value stream)

### Collect — gather elements into an array — `[ … ]`
```sql
SELECT ARRAY_PREPEND(o.user, o.projects) AS r
FROM [{"user":"s","projects":["jq","wf"]}] AS o        -- → {"r":["s","jq","wf"]}
```
> **SQL** `col->'user' || col->'projects'` (jsonb `||` concatenates) · **jq** `[.user, .projects[]]`

### Object construction — `{ }`
```sql
SELECT {"user": o.user, "title": o.title} AS doc
FROM [{"user":"s","title":"JQ"}] AS o        -- → {"doc":{"title":"JQ","user":"s"}}
```
> **SQL** `jsonb_build_object('user', col->'user', 'title', col->'title')` · **jq** `{user, title}`

Shorthand: SQL++ `SELECT o.user` yields a column named `user`, and `{o.user}`
shorthand-names the key `user` too; jq `{user}` ≡ `{user: .user}`. Note that keys
*inside* a constructed object render **sorted** (canonical JSON) — `title` before
`user` above — whereas top-level `SELECT` columns keep their written order.

### Dynamic keys — key comes from the data
SQL++ uses an **object comprehension** whose key expression is dynamic. ⚠ Object
keys must be **strings** — wrap non-strings in `TO_STRING`.
```sql
SELECT OBJECT r.label : r.`value` FOR r IN [{"label":"a","value":1}] END AS o
-- → {"o":{"a":1}}   (`value` is a reserved word, so it's back-quoted)
```
> **SQL** `jsonb_object_agg(e->>'label', e->'value')` · **jq** `map({(.label): .value}) | add`

---

## 3. Filtering & selecting

### Keep elements matching a condition
```sql
SELECT RAW v FROM [1,5,3,0,7] AS v WHERE v >= 2    -- → 5, 3, 7
```
> **SQL** `… WHERE value::int >= 2` · **jq** `map(select(. >= 2))`

### has / missing a key
```sql
SELECT x.endpoint IS NOT MISSING AS has FROM [{"endpoint":1},{}] AS x
-- → {"has":true}
--   {"has":false}
```
> **SQL** `col ? 'endpoint'` (jsonb key-exists) · **jq** `has("endpoint")`

Missing a key: SQL++ `WHERE x.k IS MISSING` · Postgres `WHERE NOT (col ? 'k')` ·
jq `select(has("k")|not)`. Present-but-**null**: SQL++ `WHERE x.k IS NULL` (distinct
from `IS MISSING` — see §11).

### contains / startswith — substring & prefix
```sql
SELECT ARRAY s LIKE "foo%" FOR s IN ["fo","foo","foobar"] END AS r
-- → {"r":[false,true,true]}
```
> **SQL** `value LIKE 'foo%'` · **jq** `[.[]|startswith("foo")]`

Substring: SQL++ `CONTAINS(s,"bar")` or `POSITION(s,"bar")>=0` (0-based) ·
SQL `s LIKE '%bar%'` · jq `contains("bar")`.

### regex
```sql
SELECT REGEXP_CONTAINS("foo123", "[0-9]+") AS m    -- → {"m":true}
```
> **SQL** `'foo123' ~ '[0-9]+'` (SQLite needs the REGEXP ext) · **jq** `test("[0-9]+")`

---

## 4. Objects ↔ entries (keys, pairs, pivots)

### keys — list an object's field names
```sql
SELECT OBJECT_NAMES({"b":2,"a":1}) AS k       -- → {"k":["a","b"]}
```
> **SQL** `jsonb_object_keys(col)` (one row per key) · **jq** `keys` (sorted)

### to_entries — object → array of pairs
⚠ SQL++ `OBJECT_PAIRS` yields **`{name, val}`**; jq's pairs are `{key, value}`.
```sql
SELECT OBJECT_PAIRS({"a":1,"b":2}) AS p
-- → {"p":[{"name":"a","val":1},{"name":"b","val":2}]}
```
> **SQL** `jsonb_each(col)` · **jq** `to_entries` → `[{"key":"a","value":1},…]`

### from_entries — pairs → object
```sql
SELECT OBJECT p.name : p.val FOR p IN [{"name":"a","val":1}] END AS o   -- → {"o":{"a":1}}
```
> **SQL** `jsonb_object_agg(e->>'key', e->'value')` · **jq** `from_entries`

### Swap keys and values
```sql
SELECT OBJECT TO_STRING(p.val) : p.name FOR p IN OBJECT_PAIRS({"a":1,"b":2}) END AS o
-- → {"o":{"1":"a","2":"b"}}
```
> **SQL** `jsonb_object_agg(v#>>'{}', k) FROM jsonb_each(…) e(k,v)` · **jq** `to_entries|map({(.value):.key})|add`

### map_values — transform every value, keep keys
```sql
SELECT OBJECT p.name : p.val+1 FOR p IN OBJECT_PAIRS({"a":1,"b":2}) END AS o
-- → {"o":{"a":2,"b":3}}
```
> **SQL** `jsonb_object_agg(k, (v::int)+1) FROM jsonb_each(…) e(k,v)` · **jq** `map_values(.+1)`

### Add / remove / rename a field
```sql
SELECT OBJECT_ADD({"a":1}, "draft", true)          AS added,    -- {"a":1,"draft":true}
       OBJECT_REMOVE({"title":"x","a":1}, "title")  AS removed    -- {"a":1}
```
> **SQL** `col || '{"draft":true}'` (merge), `col - 'title'` (minus key) · **jq** `. + {…}`, `del(.title)`

Rename `.value` → `.slug` keeping everything else:
`OBJECT_ADD(OBJECT_REMOVE(o,"value"), "slug", o.value)`.

### Pivot: object → array of objects (key becomes a field)
```sql
SELECT ARRAY OBJECT_ADD(p.val, "slug", p.name) FOR p IN OBJECT_PAIRS({"x":{"n":1}}) END AS a
-- → {"a":[{"n":1,"slug":"x"}]}
```
> **SQL** `jsonb_agg(v || jsonb_build_object('slug',k))` · **jq** `to_entries|map(.value + {slug:.key})`

### Pivot: array of objects → object keyed by a field
```sql
SELECT OBJECT r.slug : r FOR r IN [{"slug":"x","n":1}] END AS o
-- → {"o":{"x":{"slug":"x","n":1}}}
```
> **SQL** `jsonb_object_agg(e->>'slug', e)` · **jq** `map({(.slug):.}) | add`

---

## 5. Aggregating & grouping

Here SQL's home turf — `GROUP BY` and aggregate functions — meets jq's specialized
verbs (`group_by`, `unique`, `add`, `max_by`).

### length / count
```sql
SELECT ARRAY_LENGTH([1,2,3]) AS arr, LENGTH("abc") AS str, OBJECT_LENGTH({"a":1}) AS obj
-- → {"arr":3,"str":3,"obj":1}
```
> **SQL** `jsonb_array_length('[1,2,3]')` · **jq** `length`

### add / sum / min / max / avg over an array
```sql
SELECT ARRAY_SUM([1,2,3]) AS s, ARRAY_MAX([1,2,3]) AS mx, ARRAY_AVG([1,2,3]) AS a
-- → {"s":6,"mx":3,"a":2}
```
> **SQL** `sum(value::int) FROM jsonb_array_elements('[1,2,3]')` · **jq** `add`

### Aggregate over a collection (real SQL territory)
```sql
SELECT COUNT(*) AS n, ROUND(SUM(o.total),2) AS revenue, ROUND(AVG(o.total),2) AS avg
FROM orders o                                 -- → {"n":20,"revenue":1949.36,"avg":97.47}
```
> **SQL** `SELECT count(*), sum(total), avg(total) FROM orders` · **jq** `[.[].total] | add`

### group_by — cluster, then aggregate
SQL++/SQL return one row per group; jq returns nested arrays.
```sql
SELECT o.status, COUNT(*) AS n, ROUND(SUM(o.total),2) AS revenue
FROM orders o GROUP BY o.status ORDER BY revenue DESC
-- → {"status":"shipped","n":16,"revenue":1758.73}
--   {"status":"pending","n":3,"revenue":163.23}
--   {"status":"cancelled","n":1,"revenue":27.4}
```
> **SQL** `… GROUP BY status ORDER BY 3 DESC` · **jq** `group_by(.status)` (→ array of arrays)

To collect each group's members like jq does, use `ARRAY_AGG`:
```sql
SELECT o.customer, ARRAY_AGG(o.id) AS order_ids FROM orders o GROUP BY o.customer
-- → one row per customer, e.g. {"customer":"dave","order_ids":["1005","1009","1013","1017"]}
```
> **SQL** `jsonb_agg(id) … GROUP BY customer` · **jq** `group_by(.customer) | map({…, ids:map(.id)})`

### unique / dedup
```sql
SELECT ARRAY_DISTINCT([1,2,5,3,5,3,1]) AS u    -- → {"u":[1,2,5,3]}
```
> **SQL** `SELECT DISTINCT value FROM jsonb_array_elements(…)` · **jq** `unique`

`ARRAY_DISTINCT` keeps first-seen order; wrap in `ARRAY_SORT` for jq's sorted result.
Across a collection: `SELECT DISTINCT o.customer FROM orders o` or
`ARRAY_AGG(DISTINCT o.customer)`.

### sort_by
```sql
SELECT o.id, o.total FROM orders o ORDER BY o.total DESC LIMIT 3
-- → {"id":"1020","total":389.99}, {"id":"1019","total":245.0}, {"id":"1003","total":210.00}
```
> **SQL** `… ORDER BY total DESC` · **jq** `sort_by(.total)`

### min_by / max_by — the extreme *record*
```sql
SELECT o.* FROM orders o ORDER BY o.total DESC LIMIT 1   -- the single priciest order
```
> **SQL** `SELECT * FROM orders ORDER BY total DESC LIMIT 1` · **jq** `max_by(.total)`

### Count occurrences / histogram (jq's `reduce … += 1`)
```sql
SELECT v AS `value`, COUNT(*) AS n FROM ["a","b","a"] AS v GROUP BY v
-- → {"value":"a","n":2}, {"value":"b","n":1}
```
> **SQL** `… GROUP BY value` · **jq** `reduce .[] as $x ({}; .[$x] += 1)`

For a distribution as a chart, n1k1 also ships native `histogram()` / `sparkline()`
aggregates — see `examples/queries/charts.sql++`.

### Find duplicates by key
```sql
SELECT o.id, COUNT(*) AS n FROM orders o GROUP BY o.id HAVING COUNT(*) > 1
-- (empty for shop — ids are unique)
```
> **SQL** `… GROUP BY id HAVING count(*) > 1` · **jq** `reduce .[].id … select(.value>1)`

---

## 6. Arrays

### flatten
⚠ SQL++ `ARRAY_FLATTEN(arr, depth)` needs an explicit depth (jq flattens fully; pass
a large depth for the same effect).
```sql
SELECT ARRAY_FLATTEN([1,[2],[[3]]], 2) AS f    -- → {"f":[1,2,3]}
```
> **SQL** (recursive unnest; no single builtin) · **jq** `flatten`

### reverse / range / append / concat
```sql
SELECT ARRAY_REVERSE([1,2,3]) AS rev,          -- [3,2,1]
       ARRAY_RANGE(2,4)       AS rng,          -- [2,3]
       ARRAY_APPEND([1,2],3)  AS app,          -- [1,2,3]
       ARRAY_CONCAT([1],[2])  AS cat           -- [1,2]
```
> **SQL** `array_…`, `a || b`, `generate_series(2,3)` · **jq** `reverse`, `[range(2;4)]`, `. + [x]`, `.a + .b`

### Every other element (even indices)
Index by a stepped range (`OBJECT_PAIRS` works only on objects, not arrays):
```sql
SELECT ARRAY a[i] FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(a), 2) END AS evens
FROM [["a","b","c","d"]] AS a                  -- → {"evens":["a","c"]}
```
> **SQL** `… WITH ORDINALITY t(v,i) WHERE i%2=1` · **jq** `to_entries|map(select(.key%2==0).value)`

### Chunk into fixed-size groups
⚠ Clamp the slice end with `LEAST(…, ARRAY_LENGTH(a))` — a slice whose end runs
*past* the array is `MISSING`, which would silently drop the final short chunk.
```sql
SELECT ARRAY a[i:LEAST(i+2, ARRAY_LENGTH(a))]
       FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(a), 2) END AS chunks
FROM [[1,2,3,4,5]] AS a                        -- → {"chunks":[[1,2],[3,4],[5]]}
```
> **SQL** window/`ntile` or `generate_series` with slicing · **jq** `[range(0;length;$n)] | map(.[.:.+$n])`

---

## 7. Strings & numbers

### split / join
```sql
SELECT SPLIT("a,b,c", ",") AS parts, CONCAT2("-", ["a","b","c"]) AS joined
-- → {"parts":["a","b","c"],"joined":"a-b-c"}
```
> **SQL** `string_to_array('a,b,c',',')`, `array_to_string(a,'-')` · **jq** `split(",")`, `join("-")`

### upcase / trim / titlecase
```sql
SELECT UPPER("abc") AS up, LTRIM("foobar","foo") AS trimmed, TITLE("hi there") AS titled
-- → {"up":"ABC","trimmed":"bar","titled":"Hi There"}
```
> **SQL** `upper(s)`, `ltrim(s,'foo')` · **jq** `ascii_upcase`, `ltrimstr("foo")`

### String interpolation
```sql
SELECT "total is " || TO_STRING(1+2) AS msg    -- → {"msg":"total is 3"}
```
> **SQL** `'total is ' || (1+2)::text` · **jq** `"total is \(.+1)"`

### index of a substring — `index("…")`
```sql
SELECT POSITION("a, b", ", ") AS i             -- → {"i":1}   ⚠ 0-based
```
> **SQL** `position(', ' in 'a, b') - 1` (Postgres `position` is 1-based) · **jq** `index(", ")`

### Numeric — floor / round / sqrt / integer-divide
```sql
SELECT FLOOR(3.7) AS fl, ROUND(3.14159,2) AS rnd, SQRT(9) AS sq, IDIV(7,2) AS idiv
-- → {"fl":3,"rnd":3.14,"sq":3,"idiv":3}
```
> **SQL** `floor(x)`, `round(x,2)`, `sqrt(x)`, `div(a,b)` · **jq** `floor`, `sqrt`, `(./b|floor)`

### type — name the JSON type
⚠ SQL++ `TYPE()` returns lowercase, and distinguishes **`"missing"`** from `"null"`.
```sql
SELECT ARRAY TYPE(v) FOR v IN [0,false,[],{},null,"x"] END AS t
-- → {"t":["number","boolean","array","object","null","string"]}
```
> **SQL** `jsonb_typeof(value)` · **jq** `map(type)` (jq has no `missing`)

---

## 8. Recursion & deep search

SQL++'s answer to jq's `..` (recurse) / `walk` is the **`WITHIN`** collection
operator (descend into any nested value) and the quantifiers
`ANY … WITHIN … SATISFIES`. Postgres reaches for `jsonpath` (`$..`).

### Recurse into everything — `..`
```sql
SELECT ARRAY v FOR v WITHIN {"a":0,"b":[1]} END AS descendants
-- → {"descendants":[0,[1],1]}
```
> **SQL** `jsonb_path_query(col, '$..*')` · **jq** `..`

### Does value X appear anywhere in the tree?
```sql
SELECT ANY v WITHIN {"a":{"b":5}} SATISFIES v = 5 END AS found   -- → {"found":true}
```
> **SQL** `jsonb_path_exists(col, '$..* ? (@ == 5)')` · **jq** `[.. | select(. == 5)] | length > 0`

### Find every object with a given id, at any depth
```sql
SELECT ARRAY v FOR v WITHIN {"a":{"id":"x"},"b":[{"id":"y"}]} WHEN v.id = "y" END AS hits
-- → {"hits":[{"id":"y"}]}
```
> **SQL** `jsonb_path_query(col, '$..* ? (@.id == "y")')` · **jq** `[.. | objects | select(.id=="y")]`

---

## 9. Reshaping records (the meaty ones)

### UNNEST — explode a nested array into rows (jq `.items[]`)
The workhorse for line-items, tags, events. SQL++ has a dedicated `UNNEST` join that
pairs each element with its parent's fields.
```sql
SELECT o.id, t AS tag FROM [{"id":1,"tags":["a","b"]}] AS o UNNEST o.tags AS t
-- → {"id":1,"tag":"a"}
--   {"id":1,"tag":"b"}
```
> **SQL** `… , jsonb_array_elements_text(o->'tags') t` (lateral cross join) · **jq** `.[] | .id as $id | .tags[] | {id:$id, tag:.}`

### CSV-ish array-of-rows → objects
```sql
SELECT r[0] AS name, r[1] AS url, TO_NUMBER(r[2]) AS category
FROM [["hdr","hdr","hdr"],["n","u","3"]][1:] AS r    -- → {"name":"n","url":"u","category":3}
```
> **SQL** `(r->>2)::int FROM jsonb_array_elements(col) WITH ORDINALITY … WHERE i>1` · **jq** `.[1:] | map({name:.[0], …})`

(n1k1 can also read a `.csv` file as a keyspace of objects directly — see
`DESIGN-data.md`.)

### Reshape + rename + sort (jq's Twitter-DM recipe)
```sql
SELECT d.text, d.sender.screen_name AS from_name
FROM [{"text":"hi","sender":{"screen_name":"amy"},"ts":2},
      {"text":"yo","sender":{"screen_name":"bo"},"ts":1}] AS d
ORDER BY d.ts                                  -- → {"text":"yo","from_name":"bo"}, {"text":"hi","from_name":"amy"}
```
> **SQL** `col#>>'{sender,screen_name}' … ORDER BY col->>'ts'` · **jq** `[.[] | {text, from:.sender.screen_name}] | sort_by(.date)`

### Merge two arrays, flagging one (jq's `team` + `formerly`)
```sql
SELECT ARRAY_CONCAT(d.team,
       ARRAY OBJECT_ADD(m,"formerly",true) FOR m IN d.formerly END) AS everyone
FROM [{"team":[{"n":"a"}],"formerly":[{"n":"b"}]}] AS d
-- → {"everyone":[{"n":"a"},{"formerly":true,"n":"b"}]}
```
> **SQL** `(col->'team') || (SELECT jsonb_agg(m||'{"formerly":true}') …)` · **jq** `[.team, (.formerly | map(.+{formerly:true}))] | flatten`

---

## 10. Conditionals & defaults

### if / then / else
```sql
SELECT CASE WHEN 5 > 3 THEN "big" ELSE "small" END AS c    -- → {"c":"big"}
```
> **SQL** `CASE WHEN 5 > 3 THEN 'big' ELSE 'small' END` · **jq** `if .>3 then "big" else "small" end`

### Default for a missing/null value — `//`
```sql
SELECT IFMISSINGORNULL(x.foo, "default") AS v FROM [{}] AS x    -- → {"v":"default"}
```
> **SQL** `COALESCE(col->>'foo', 'default')` · **jq** `.foo // "default"`

SQL++ splits the concept SQL's `COALESCE` and jq's `//` blur: `IFMISSING`, `IFNULL`,
`IFMISSINGORNULL` (and `MISSINGIF`/`NULLIF`) let you treat *absent* and
*present-but-null* differently.

### try / catch
jq guards type errors with `try f catch g`. SQL++ has no type errors to catch —
mismatched operations yield `NULL`/`MISSING` rather than aborting — so the pattern
is a `CASE`/`IF*` guard, e.g. `CASE WHEN TYPE(x)="object" THEN x.a END`.

---

## 11. The SQL++ superpower: MISSING vs NULL

The one concept with no jq or standard-SQL equivalent, and worth understanding
because it changes how filters and defaults behave.

- **`MISSING`** — the field/element **is not there**. SQL models this as `NULL`; jq
  models it as an error (needing `?`).
- **`NULL`** — the field **is present with the JSON value `null`**.

n1k1 keeps them distinct end to end. A `MISSING` field is **omitted** from output
objects entirely; a `NULL` field is rendered:
```sql
SELECT MISSINGIF(3,3) AS gone, NULLIF(3,3) AS nulled
-- → {"nulled":null}      -- "gone" vanished; "nulled" stayed as null
```
Consequences you rely on:
- `x.foo IS MISSING` ≠ `x.foo IS NULL`. Use `IS VALUED` for "present and not null".
- Aggregates and array builders **skip `MISSING`** (and `NULL`), so a sparse field
  aggregates cleanly.
- Projecting a missing field just drops that key — which is why jq's `.foo?` needs no
  translation.

---

## Function cheat-sheet

| Goal | SQL++ | SQL (PostgreSQL) | jq |
|---|---|---|---|
| iterate array | `FROM a AS x` / `UNNEST` | `jsonb_array_elements` | `.[]` |
| map | `ARRAY f FOR x IN a END` | `jsonb_agg(f)` | `map(f)` |
| filter | `WHERE c` | `WHERE c` | `select(c)` |
| object keys | `OBJECT_NAMES(o)` | `jsonb_object_keys` | `keys` |
| obj → pairs | `OBJECT_PAIRS(o)` → `{name,val}` | `jsonb_each` | `to_entries` |
| pairs → obj | `OBJECT k:v FOR … END` | `jsonb_object_agg` | `from_entries` |
| add field | `OBJECT_ADD(o,k,v)` | `o \|\| '{…}'` | `. + {k:v}` |
| remove field | `OBJECT_REMOVE(o,k)` | `o - 'k'` | `del(.k)` |
| length | `ARRAY_LENGTH`/`LENGTH`/`OBJECT_LENGTH` | `jsonb_array_length` | `length` |
| sum array | `ARRAY_SUM(a)` | `sum(…)` | `add` |
| dedup | `ARRAY_DISTINCT(a)` / `DISTINCT` | `DISTINCT` | `unique` |
| sort | `ORDER BY k` | `ORDER BY k` | `sort_by(.k)` |
| group | `GROUP BY k` | `GROUP BY k` | `group_by(.k)` |
| flatten | `ARRAY_FLATTEN(a, n)` | (recursive) | `flatten` |
| recurse | `… WITHIN v` | `jsonb_path_query($..)` | `..` |
| split / join | `SPLIT` / `CONCAT2` | `string_to_array` | `split`/`join` |
| type of | `TYPE(v)` | `jsonb_typeof` | `type` |
| default | `IFMISSINGORNULL(v,d)` | `COALESCE` | `// d` |
| substring idx | `POSITION(x,s)` (0-based) | `position()` (1-based) | `index(s)` |
| interpolate | `"…" \|\| TO_STRING(x)` | `\|\|` | `"\(.x)"` |

---

## Gotchas worth pinning

- **0-based `SUBSTR`/`POSITION`.** `SUBSTR("hello",1,3)` → `"ell"`;
  `POSITION("hello","ll")` → `2`. (SQL's are 1-based.)
- **`OBJECT_PAIRS` yields `{name, val}`**, not jq's `{key, value}`.
- **Object keys must be strings** — wrap non-string keys in `TO_STRING`.
- **Negative array index wraps** to the end (`a[-1]` = last); an out-of-range
  positive index is `MISSING`. Guard `i >= 0` when indices are computed.
- **A slice past the end is `MISSING`, not a clamped short array** — `[1,2,3][1:9]`
  is `MISSING`. Clamp the end with `LEAST(end, ARRAY_LENGTH(a))`.
- **`OBJECT_PAIRS`/`OBJECT_NAMES` work on objects only** (an array → `NULL`); index
  arrays positionally instead.
- **`ARRAY_FLATTEN` needs an explicit depth** (jq flattens fully).
- **Reserved words need backticks** — `` `value` ``, `` `last` ``, `` `type` ``
  as identifiers.
- **`UNION`/`UNION ALL` align by field name**, not position — alias every column so
  branches line up (see `examples/queries/life.sql++`).
- **A subquery returns an array**: scalarize with `(SELECT RAW … )[0]`.

---

## See also

- The jq manual (`jqlang.org/manual`) and Remy Sharp's jq recipes
  (`remysharp.com/drafts/jq-recipes`) — the sources this Rosetta stone translates.
- `examples/queries/*.sql++` — runnable SQL++ showpieces (Conway's Life, Mandelbrot,
  unicode charts) that exercise array comprehensions, `WITH RECURSIVE`, and window
  functions.
- `examples/README.md` — the shop/logs/metrics/… datasets and how to point n1k1 at
  them.
- Couchbase N1QL function reference — n1k1 speaks the same dialect (parser + planner
  are the `couchbase/query` fork), so the full function library applies.
