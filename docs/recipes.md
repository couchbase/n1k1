# SQL++ recipes — a SQL++ / SQL / jq Rosetta stone

_Slicing and dicing JSON: the same maneuver across seven tools._

> **This file is generated** from `docs/recipes.yaml` by `docs/recipes_build.py`.
> Edit the `.yaml`, not this `.md`. An interactive HTML version with toggleable
> dialect columns lives at `docs/recipes.html`. And because the source is a plain
> YAML sequence of records, **n1k1 can query it directly**:
>
> ```sh
> n1k1 -c 'SELECT r.section, COUNT(*) AS n FROM recipes r GROUP BY r.section' docs/recipes.yaml
> ```

This guide is about doing JSON surgery in **SQL++** (the N1QL dialect n1k1 runs),
cross-translated so you can lean on what you already know. Each recipe shows SQL++
first (the runnable column), then SQL (PostgreSQL `jsonb`), then jq — and the HTML
view adds **DuckDB, JavaScript, Python, and MongoDB** columns you can toggle on.

## Running the examples

**Every SQL++ block is a complete statement you can copy and run** — each was
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
```

## The three mental models

| | SQL++ | SQL | jq |
|---|---|---|---|
| **Unit of work** | a collection of documents | rows in a table | one JSON value, streamed |
| **JSON is** | the native value type | a `json`/`jsonb` column | the whole world |
| **"for each element"** | `FROM arr AS x` / `UNNEST` | `jsonb_array_elements(col)` | `.[]` |
| **transform each** | `SELECT f …` / `ARRAY f FOR x IN … END` | `SELECT f FROM …` | `map(f)` |
| **build an object** | `{"a": x}` | `jsonb_build_object('a', x)` | `{a: .x}` |
| **object ↔ pairs** | `OBJECT_PAIRS` / `OBJECT … FOR … END` | `jsonb_each` / `jsonb_object_agg` | `to_entries`/`from_entries` |

**SQL++ and SQL treat every task as a query over a collection; jq treats it as a
stream rewrite.** DuckDB is SQL with list comprehensions; JS/Python are imperative
map/filter; MongoDB is a document pipeline (its twin of jq's pipe *and* SQL's
`GROUP BY`). What makes SQL++ special is that it has jq's JSON-surgery verbs
(`ARRAY … FOR`, `OBJECT … FOR`, `WITHIN`, `UNNEST`, `OBJECT_PAIRS`) *inside* a set
query — so you rarely have to choose.

## 1. Querying a collection

### Pick fields from each record
_Source data:_
```
orders — the shop keyspace (20 docs), e.g.
{"id":"1005","customer":"dave","total":22.0,"status":"shipped","ts":"2026-01-06"}
```
```sql
SELECT id, status, total
FROM orders
-- → {"id":"1005","status":"shipped","total":22.0}, … (one per order)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT id, status, total FROM orders` |
| **SQL (DuckDB)** | `SELECT id, status, total FROM orders` |
| **JavaScript** | `orders.map(o => ({id: o.id, status: o.status, total: o.total}))` |
| **Python** | `[{"id": o["id"], "status": o["status"], "total": o["total"]} for o in orders]` |
| **MongoDB** | `db.orders.find({}, {id: 1, status: 1, total: 1, _id: 0})` |
| **jq** | `.[] \| {id, status, total}` |

</details>

### Keep matching records
_Over the shop `orders` / `customers` keyspaces._
```sql
SELECT id, total
FROM orders
WHERE total > 100
-- → the orders with total over 100
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT id, total FROM orders WHERE total > 100` |
| **SQL (DuckDB)** | `SELECT id, total FROM orders WHERE total > 100` |
| **JavaScript** | `orders.filter(o => o.total > 100)` |
| **Python** | `[o for o in orders if o["total"] > 100]` |
| **MongoDB** | `db.orders.find({total: {$gt: 100}})` |
| **jq** | `map(select(.total > 100))` |

</details>

### Sort, then take the top N
_Over the shop `orders` / `customers` keyspaces._
```sql
SELECT id, total
FROM orders
ORDER BY total DESC
LIMIT 3
-- → {"id":"1020","total":389.99}, {"id":"1019","total":245.0}, {"id":"1003","total":210.00}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT id, total FROM orders ORDER BY total DESC LIMIT 3` |
| **SQL (DuckDB)** | `SELECT id, total FROM orders ORDER BY total DESC LIMIT 3` |
| **JavaScript** | `orders.toSorted((a, b) => b.total - a.total).slice(0, 3)` |
| **Python** | `sorted(orders, key=lambda o: -o["total"])[:3]` |
| **MongoDB** | `db.orders.find().sort({total: -1}).limit(3)` |
| **jq** | `sort_by(.total) \| reverse \| .[:3]` |

</details>

### The single largest record
jq's max_by returns the whole record with the largest key; in SQL it's ORDER BY … LIMIT 1.
_Over the shop `orders` / `customers` keyspaces._
```sql
SELECT orders.*
FROM orders
ORDER BY total DESC
LIMIT 1
-- → the priciest order (the whole row)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT * FROM orders ORDER BY total DESC LIMIT 1` |
| **SQL (DuckDB)** | `SELECT MAX_BY(orders, total) FROM orders` |
| **JavaScript** | `orders.reduce((m, o) => o.total > m.total ? o : m)` |
| **Python** | `max(orders, key=lambda o: o["total"])` |
| **MongoDB** | `db.orders.find().sort({total: -1}).limit(1)` |
| **jq** | `max_by(.total)` |

</details>

### Count and total a collection
_Over the shop `orders` / `customers` keyspaces._
```sql
SELECT COUNT(*)             AS n,
       ROUND(SUM(total), 2) AS revenue,
       ROUND(AVG(total), 2) AS avg
FROM orders
-- → {"n":20,"revenue":1949.36,"avg":97.47}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT COUNT(*) AS n, ROUND(SUM(total), 2) AS revenue, ROUND(AVG(total), 2) AS avg FROM orders` |
| **SQL (DuckDB)** | `SELECT COUNT(*) AS n, ROUND(SUM(total), 2) AS revenue, ROUND(AVG(total), 2) AS avg FROM orders` |
| **JavaScript** | `({n: orders.length, revenue: orders.reduce((s, o) => s + o.total, 0)})` |
| **Python** | `{"n": len(orders), "revenue": sum(o["total"] for o in orders)}` |
| **MongoDB** | `db.orders.aggregate([{$group: {_id: null, n: {$sum: 1}, revenue: {$sum: "$total"}, avg: {$avg: "$total"}}}])` |
| **jq** | `{n: length, revenue: (map(.total) \| add)}` |

</details>

### Group, then aggregate each group
_Over the shop `orders` / `customers` keyspaces._
```sql
SELECT status,
       COUNT(*) AS n,
       ROUND(SUM(total), 2) AS revenue
FROM orders
GROUP BY status
ORDER BY revenue DESC
-- → {"status":"shipped","n":16,"revenue":1758.73}, {"status":"pending",…}, {"status":"cancelled",…}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT status, COUNT(*) AS n, ROUND(SUM(total),2) AS revenue FROM orders GROUP BY status ORDER BY revenue DESC` |
| **SQL (DuckDB)** | `SELECT status, COUNT(*) AS n, ROUND(SUM(total),2) AS revenue FROM orders GROUP BY status ORDER BY revenue DESC` |
| **JavaScript** | `Object.groupBy(orders, o => o.status)   // then reduce each bucket` |
| **Python** | `df.groupby("status").total.agg(["count", "sum"])   # pandas` |
| **MongoDB** | `db.orders.aggregate([
  {$group: {_id: "$status", n: {$sum: 1}, revenue: {$sum: "$total"}}},
  {$sort: {revenue: -1}}])` |
| **jq** | `group_by(.status)   # → array of arrays` |

</details>

### RAW — one bare value per row (not an object)
A normal SELECT yields objects; SELECT RAW (jq's .[] | .field) yields the bare value —
handy for one column, an IN-list, or feeding another query.
_Over the shop `orders` / `customers` keyspaces._
```sql
SELECT RAW customer
FROM orders
-- → "dave", "alice", …  (bare strings, not {"customer":…})
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **JavaScript** | `orders.map(o => o.customer)` |
| **Python** | `[o["customer"] for o in orders]` |
| **MongoDB** | `db.orders.distinct("customer")  // bare values (also dedups)` |
| **jq** | `.[] \| .customer` |

</details>

## 2. Reaching into a value

### Field access, including nested
_Source data:_
```
doc = {"id": "o1", "customer": {"name": "Dave", "city": "Austin"}}
```
```sql
WITH doc AS ({"id":"o1","customer":{"name":"Dave","city":"Austin"}})
SELECT doc.customer.city
FROM doc
-- → {"city":"Austin"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT doc->'customer'->>'city' FROM doc -- ->> text, -> jsonb` |
| **SQL (DuckDB)** | `SELECT doc->'$.customer.city' FROM doc` |
| **JavaScript** | `doc.customer.city` |
| **Python** | `doc["customer"]["city"]` |
| **MongoDB** | `db.c.find({}, {"customer.city": 1})` |
| **jq** | `.customer.city` |

</details>

### A missing field is MISSING (not an error)
jq's ? suppresses errors on non-objects; SQL++ has no error — a missing path is MISSING
and simply drops out of the result (see "MISSING vs NULL" at the end).
_Source data:_
```
doc = {"bar": 1}
```
```sql
WITH doc AS ({"bar":1})
SELECT doc.foo
FROM doc
-- → {}   (foo is MISSING → the key is omitted)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT doc->'foo' -- NULL if absent FROM doc` |
| **SQL (DuckDB)** | `SELECT doc->'$.foo' FROM doc` |
| **JavaScript** | `doc.foo               // undefined` |
| **Python** | `doc.get("foo")        # None` |
| **MongoDB** | `// a missing field is simply absent from the result` |
| **jq** | `.foo?                 # no error even on a non-object` |

</details>

### Array index — 0-based, negatives from the end
_Source data:_
```
doc = {"nums": [10, 20, 30]}
```
```sql
WITH doc AS ({"nums":[10,20,30]})
SELECT doc.nums[0]  AS `first`,
       doc.nums[-1] AS `last`,
       doc.nums[-2] AS penult
FROM doc
-- → {"first":10,"last":30,"penult":20}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT doc->'nums'->0 AS "first", doc->'nums'->-1 AS "last", doc->'nums'->-2 AS penult FROM doc -- jsonb: 0-based, -1 = last` |
| **SQL (DuckDB)** | `SELECT doc.nums[1] AS first, doc.nums[-1] AS last, doc.nums[-2] AS penult FROM doc -- ⚠ 1-based` |
| **JavaScript** | `doc.nums[0]; doc.nums.at(-1); doc.nums.at(-2)` |
| **Python** | `doc["nums"][0]; doc["nums"][-1]; doc["nums"][-2]` |
| **MongoDB** | `{first: {$arrayElemAt: ["$nums", 0]}, last: {$arrayElemAt: ["$nums", -1]}, penult: {$arrayElemAt: ["$nums", -2]}}` |
| **jq** | `.nums[0], .nums[-1], .nums[-2]` |

</details>

### Slice a range
_Source data:_
```
doc = {"letters": ["a", "b", "c", "d", "e"]}
```
```sql
WITH doc AS ({"letters":["a","b","c","d","e"]})
SELECT doc.letters[2:4] AS s
FROM doc
-- → {"s":["c","d"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_PATH_QUERY_ARRAY(doc->'letters', '$[2 to 3]') AS s FROM doc` |
| **SQL (DuckDB)** | `SELECT doc.letters[3:4] AS s FROM doc -- ⚠ 1-based, inclusive` |
| **JavaScript** | `doc.letters.slice(2, 4)` |
| **Python** | `doc["letters"][2:4]` |
| **MongoDB** | `{$slice: ["$letters", 2, 2]}` |
| **jq** | `.letters[2:4]` |

</details>

## 3. Transforming arrays

### map — transform every element
SQL++ has two forms: a scan (SELECT … FROM nums) or the inline array comprehension
ARRAY … FOR … END that returns one array value.
_Source data:_
```
doc = {"nums": [1, 2, 3]}
```
```sql
WITH doc AS ({"nums":[1,2,3]})
SELECT ARRAY v + 1 FOR v IN doc.nums END AS r
FROM doc
-- → {"r":[2,3,4]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_AGG(v::int + 1) AS r FROM doc, JSONB_ARRAY_ELEMENTS(doc->'nums') AS v` |
| **SQL (DuckDB)** | `SELECT [x + 1 for x in doc.nums] AS r FROM doc` |
| **JavaScript** | `doc.nums.map(x => x + 1)` |
| **Python** | `[x + 1 for x in doc["nums"]]` |
| **MongoDB** | `{$map: {input: "$nums", as: "v", in: {$add: ["$$v", 1]}}}` |
| **jq** | `.nums \| map(. + 1)` |

</details>

### filter — keep matching elements
_Source data:_
```
doc = {"nums": [1, 5, 3, 0, 7]}
```
```sql
WITH doc AS ({"nums":[1,5,3,0,7]})
SELECT ARRAY v FOR v IN doc.nums WHEN v >= 2 END AS r
FROM doc
-- → {"r":[5,3,7]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_AGG(v) AS r FROM doc, JSONB_ARRAY_ELEMENTS(doc->'nums') AS v WHERE v::int >= 2` |
| **SQL (DuckDB)** | `SELECT [x for x in doc.nums if x >= 2] AS r FROM doc` |
| **JavaScript** | `doc.nums.filter(x => x >= 2)` |
| **Python** | `[x for x in doc["nums"] if x >= 2]` |
| **MongoDB** | `{$filter: {input: "$nums", cond: {$gte: ["$$this", 2]}}}` |
| **jq** | `.nums \| map(select(. >= 2))` |

</details>

### Sum, min, max, average
_Source data:_
```
doc = {"nums": [1, 2, 3]}
```
```sql
WITH doc AS ({"nums":[1,2,3]})
SELECT ARRAY_SUM(doc.nums) AS s,
       ARRAY_MAX(doc.nums) AS mx,
       ARRAY_AVG(doc.nums) AS a
FROM doc
-- → {"s":6,"mx":3,"a":2}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT SUM(v::int) AS s, MAX(v::int) AS mx, AVG(v::int) AS a FROM doc, JSONB_ARRAY_ELEMENTS(doc->'nums') AS v` |
| **SQL (DuckDB)** | `SELECT LIST_SUM(doc.nums) AS s, LIST_MAX(doc.nums) AS mx, LIST_AVG(doc.nums) AS a FROM doc` |
| **JavaScript** | `doc.nums.reduce((a, b) => a + b, 0); Math.max(...doc.nums)` |
| **Python** | `sum(doc["nums"]); max(doc["nums"]); statistics.mean(doc["nums"])` |
| **MongoDB** | `{$sum: "$nums"}; {$max: "$nums"}; {$avg: "$nums"}` |
| **jq** | `.nums \| add; .nums \| max; (.nums \| add / length)` |

</details>

### Length / count of elements
_Source data:_
```
doc = {"nums": [1, 2, 3]}
```
```sql
WITH doc AS ({"nums":[1,2,3]})
SELECT ARRAY_LENGTH(doc.nums) AS n
FROM doc
-- → {"n":3}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_ARRAY_LENGTH(doc->'nums') AS n FROM doc` |
| **SQL (DuckDB)** | `SELECT LEN(doc.nums) AS n FROM doc` |
| **JavaScript** | `doc.nums.length` |
| **Python** | `len(doc["nums"])` |
| **MongoDB** | `{$size: "$nums"}` |
| **jq** | `.nums \| length` |

</details>

### Unique / dedup
ARRAY_DISTINCT keeps first-seen order; wrap in ARRAY_SORT for jq's sorted result.
_Source data:_
```
doc = {"nums": [1, 2, 5, 3, 5, 3, 1]}
```
```sql
WITH doc AS ({"nums":[1,2,5,3,5,3,1]})
SELECT ARRAY_DISTINCT(doc.nums) AS u
FROM doc
-- → {"u":[1,2,5,3]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_AGG(DISTINCT v) AS u FROM doc, JSONB_ARRAY_ELEMENTS(doc->'nums') AS v` |
| **SQL (DuckDB)** | `SELECT LIST_DISTINCT(doc.nums) AS u FROM doc` |
| **JavaScript** | `[...new Set(doc.nums)]` |
| **Python** | `list(dict.fromkeys(doc["nums"]))` |
| **MongoDB** | `{$setUnion: ["$nums", []]}   // unordered` |
| **jq** | `.nums \| unique   # sorted` |

</details>

### Flatten nested arrays
⚠ SQL++ ARRAY_FLATTEN needs an explicit depth; jq flattens fully.
_Source data:_
```
doc = {"nested": [1, [2], [[3]]]}
```
```sql
WITH doc AS ({"nested":[1,[2],[[3]]]})
SELECT ARRAY_FLATTEN(doc.nested, 2) AS f
FROM doc
-- → {"f":[1,2,3]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `-- recursive unnest; no single builtin` |
| **SQL (DuckDB)** | `SELECT FLATTEN(doc.nested) AS f FROM doc -- one level` |
| **JavaScript** | `doc.nested.flat(2)` |
| **Python** | `# no deep builtin; recurse` |
| **MongoDB** | `{$reduce: {input: "$nested", initialValue: [], in: {$concatArrays: ["$$value", "$$this"]}}}  // one level` |
| **jq** | `.nested \| flatten` |

</details>

### reverse · range · append · concat
```sql
SELECT ARRAY_REVERSE([1,2,3]) AS rev,
       ARRAY_RANGE(2, 4)      AS rng,
       ARRAY_APPEND([1,2], 3) AS app,
       ARRAY_CONCAT([1], [2]) AS cat
-- → {"rev":[3,2,1],"rng":[2,3],"app":[1,2,3],"cat":[1,2]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT ARRAY_APPEND(ARRAY[1,2], 3), ARRAY[1] \|\| ARRAY[2], GENERATE_SERIES(2, 3) -- (no array reverse builtin)` |
| **SQL (DuckDB)** | `SELECT LIST_REVERSE(a) AS rev, RANGE(2,4) AS rng, LIST_APPEND(a,x) AS app, LIST_CONCAT(a,b) AS cat` |
| **JavaScript** | `a.toReversed(); [...Array(2).keys()].map(i=>i+2); [...a, x]; a.concat(b)` |
| **Python** | `a[::-1]; list(range(2,4)); a + [x]; a + b` |
| **MongoDB** | `{$reverseArray}; {$range:[2,4]}; {$concatArrays}` |
| **jq** | `reverse; [range(2;4)]; . + [x]; .a + .b` |

</details>

### Every other element
Index by a stepped range (OBJECT_PAIRS works on objects only, not arrays).
_Source data:_
```
doc = {"letters": ["a", "b", "c", "d"]}
```
```sql
WITH doc AS ({"letters":["a","b","c","d"]})
SELECT ARRAY doc.letters[i]
       FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(doc.letters), 2) END AS evens
FROM doc
-- → {"evens":["a","c"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_AGG(v) AS evens FROM doc, JSONB_ARRAY_ELEMENTS(doc->'letters') WITH ORDINALITY t(v, i) WHERE i % 2 = 1` |
| **SQL (DuckDB)** | `SELECT LIST_SLICE(doc.letters, 1, 4, 2) AS evens FROM doc -- 1-based, step 2` |
| **JavaScript** | `doc.letters.filter((_, i) => i % 2 === 0)` |
| **Python** | `doc["letters"][::2]` |
| **MongoDB** | `{$map: {input: {$range: [0, {$size: "$letters"}, 2]}, in: {$arrayElemAt: ["$letters", "$$this"]}}}` |
| **jq** | `.letters \| [range(0; length; 2) as $i \| .[$i]]` |

</details>

### Chunk into fixed-size groups
⚠ Clamp the slice end with LEAST(…, ARRAY_LENGTH(a)) — a slice past the end is MISSING
and would drop the last short chunk.
_Source data:_
```
doc = {"nums": [1, 2, 3, 4, 5]}
```
```sql
WITH doc AS ({"nums":[1,2,3,4,5]})
SELECT ARRAY doc.nums[i:LEAST(i + 2, ARRAY_LENGTH(doc.nums))]
       FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(doc.nums), 2) END AS chunks
FROM doc
-- → {"chunks":[[1,2],[3,4],[5]]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `-- window / ntile, or generate_series with slicing` |
| **SQL (DuckDB)** | `SELECT [doc.nums[i+1 : i+2] for i in RANGE(0, LEN(doc.nums), 2)] AS chunks FROM doc` |
| **JavaScript** | `Array.from({length: Math.ceil(doc.nums.length/2)}, (_, i) => doc.nums.slice(i*2, i*2+2))` |
| **Python** | `[doc["nums"][i:i+2] for i in range(0, len(doc["nums"]), 2)]` |
| **jq** | `.nums \| [range(0; length; 2) as $i \| .[$i:$i+2]]` |

</details>

## 4. Iterating & collecting

### Visit each element
_Source data:_
```
doc = {"people": [{"name": "JSON"}, {"name": "XML"}]}
```
```sql
WITH doc AS ({"people":[{"name":"JSON"},{"name":"XML"}]})
SELECT p.*
FROM doc
UNNEST doc.people AS p
-- → {"name":"JSON"} then {"name":"XML"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT p FROM doc, JSONB_ARRAY_ELEMENTS(doc->'people') AS p` |
| **SQL (DuckDB)** | `SELECT UNNEST(doc.people) FROM doc` |
| **JavaScript** | `for (const p of doc.people) { … }` |
| **Python** | `for p in doc["people"]: ...` |
| **MongoDB** | `db.people.find()` |
| **jq** | `.people[]` |

</details>

### One field from each element
_Source data:_
```
doc = {"people": [{"name": "JSON"}, {"name": "XML"}]}
```
```sql
WITH doc AS ({"people":[{"name":"JSON"},{"name":"XML"}]})
SELECT RAW p.name
FROM doc
UNNEST doc.people AS p
-- → "JSON", "XML"
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT p->>'name' FROM doc, JSONB_ARRAY_ELEMENTS(doc->'people') AS p` |
| **SQL (DuckDB)** | `SELECT p.name FROM doc, UNNEST(doc.people) AS t(p)` |
| **JavaScript** | `doc.people.map(p => p.name)` |
| **Python** | `[p["name"] for p in doc["people"]]` |
| **MongoDB** | `db.people.find({}, {name: 1, _id: 0})` |
| **jq** | `.people[] \| .name` |

</details>

### Collect values into one array
_Source data:_
```
doc = {"user": "s", "projects": ["jq", "wf"]}
```
```sql
WITH doc AS ({"user":"s","projects":["jq","wf"]})
SELECT ARRAY_PREPEND(doc.user, doc.projects) AS r
FROM doc
-- → {"r":["s","jq","wf"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT doc->'user' \|\| doc->'projects' AS r FROM doc` |
| **SQL (DuckDB)** | `SELECT LIST_PREPEND(doc.user, doc.projects) AS r FROM doc` |
| **JavaScript** | `[doc.user, ...doc.projects]` |
| **Python** | `[doc["user"], *doc["projects"]]` |
| **MongoDB** | `{$concatArrays: [["$user"], "$projects"]}` |
| **jq** | `[.user, .projects[]]` |

</details>

## 5. Building & reshaping objects

### Build an object
Keys inside a constructed object render sorted (canonical JSON): title before user.
_Source data:_
```
doc = {"user": "s", "title": "JQ"}
```
```sql
WITH doc AS ({"user":"s","title":"JQ"})
SELECT {"user": doc.user, "title": doc.title} AS o
FROM doc
-- → {"o":{"title":"JQ","user":"s"}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_BUILD_OBJECT('user', doc->'user', 'title', doc->'title') AS o FROM doc` |
| **SQL (DuckDB)** | `SELECT {'user': doc.user, 'title': doc.title} AS o -- a STRUCT FROM doc` |
| **JavaScript** | `({user: doc.user, title: doc.title})` |
| **Python** | `{"user": doc["user"], "title": doc["title"]}` |
| **MongoDB** | `{$project: {user: 1, title: 1, _id: 0}}` |
| **jq** | `{user, title}` |

</details>

### "List an object's keys"
_Source data:_
```
doc = {"obj": {"b": 2, "a": 1}}
```
```sql
WITH doc AS ({"obj":{"b":2,"a":1}})
SELECT OBJECT_NAMES(doc.obj) AS k
FROM doc
-- → {"k":["a","b"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_KEYS(doc->'obj') AS k FROM doc` |
| **SQL (DuckDB)** | `SELECT JSON_KEYS(doc.obj) AS k FROM doc` |
| **JavaScript** | `Object.keys(doc.obj)` |
| **Python** | `list(doc["obj"].keys())` |
| **MongoDB** | `{$map: {input: {$objectToArray: "$obj"}, in: "$$this.k"}}` |
| **jq** | `.obj \| keys   # sorted` |

</details>

### Object → array of pairs
⚠ SQL++ OBJECT_PAIRS yields {name, val}; jq yields {key, value}.
_Source data:_
```
doc = {"obj": {"a": 1, "b": 2}}
```
```sql
WITH doc AS ({"obj":{"a":1,"b":2}})
SELECT OBJECT_PAIRS(doc.obj) AS p
FROM doc
-- → {"p":[{"name":"a","val":1},{"name":"b","val":2}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_EACH(doc->'obj') AS p FROM doc` |
| **SQL (DuckDB)** | `SELECT MAP_ENTRIES(doc.obj) AS p FROM doc` |
| **JavaScript** | `Object.entries(doc.obj)   // [["a",1],["b",2]]` |
| **Python** | `list(doc["obj"].items())` |
| **MongoDB** | `{$objectToArray: "$obj"}   // [{k,v}]` |
| **jq** | `.obj \| to_entries   # [{key,value}]` |

</details>

### Pairs → object
_Source data:_
```
doc = {"pairs": [{"name": "a", "val": 1}]}
```
```sql
WITH doc AS ({"pairs":[{"name":"a","val":1}]})
SELECT OBJECT p.name : p.val FOR p IN doc.pairs END AS o
FROM doc
-- → {"o":{"a":1}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(p->>'name', p->'val') AS o FROM doc, JSONB_ARRAY_ELEMENTS(doc->'pairs') AS p` |
| **SQL (DuckDB)** | `SELECT MAP_FROM_ENTRIES(doc.pairs) AS o FROM doc` |
| **JavaScript** | `Object.fromEntries(doc.pairs.map(p => [p.name, p.val]))` |
| **Python** | `{p["name"]: p["val"] for p in doc["pairs"]}` |
| **MongoDB** | `{$arrayToObject: "$pairs"}` |
| **jq** | `.pairs \| from_entries` |

</details>

### Object with a key from the data
⚠ Keys must be strings; `value` is a reserved word (back-quoted).
_Source data:_
```
doc = {"recs": [{"label": "a", "value": 1}]}
```
```sql
WITH doc AS ({"recs":[{"label":"a","value":1}]})
SELECT OBJECT r.label : r.`value` FOR r IN doc.recs END AS o
FROM doc
-- → {"o":{"a":1}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(r->>'label', r->'value') AS o FROM doc, JSONB_ARRAY_ELEMENTS(doc->'recs') AS r` |
| **SQL (DuckDB)** | `SELECT MAP_FROM_ENTRIES([{'k': r.label, 'v': r.value} for r in doc.recs]) AS o FROM doc` |
| **JavaScript** | `Object.fromEntries(doc.recs.map(r => [r.label, r.value]))` |
| **Python** | `{r["label"]: r["value"] for r in doc["recs"]}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: "$recs", in: {k: "$$this.label", v: "$$this.value"}}}}` |
| **jq** | `.recs \| map({(.label): .value}) \| add` |

</details>

### Transform every value, keep keys
_Source data:_
```
doc = {"obj": {"a": 1, "b": 2}}
```
```sql
WITH doc AS ({"obj":{"a":1,"b":2}})
SELECT OBJECT p.name : p.val + 1 FOR p IN OBJECT_PAIRS(doc.obj) END AS o
FROM doc
-- → {"o":{"a":2,"b":3}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(k, (v::int)+1) AS o FROM doc, JSONB_EACH(doc->'obj') AS e(k, v)` |
| **SQL (DuckDB)** | `SELECT MAP_FROM_ENTRIES([{'k': e.key, 'v': e.value + 1} for e in MAP_ENTRIES(doc.obj)]) AS o FROM doc` |
| **JavaScript** | `Object.fromEntries(Object.entries(doc.obj).map(([k, v]) => [k, v + 1]))` |
| **Python** | `{k: v + 1 for k, v in doc["obj"].items()}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: {$objectToArray: "$obj"}, in: {k: "$$this.k", v: {$add: ["$$this.v", 1]}}}}}` |
| **jq** | `.obj \| map_values(. + 1)` |

</details>

### Swap keys and values
_Source data:_
```
doc = {"obj": {"a": 1, "b": 2}}
```
```sql
WITH doc AS ({"obj":{"a":1,"b":2}})
SELECT OBJECT TO_STRING(p.val) : p.name FOR p IN OBJECT_PAIRS(doc.obj) END AS o
FROM doc
-- → {"o":{"1":"a","2":"b"}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(v#>>'{}', k) AS o FROM doc, JSONB_EACH(doc->'obj') AS e(k, v)` |
| **SQL (DuckDB)** | `SELECT MAP_FROM_ENTRIES([{'k': e.value::VARCHAR, 'v': e.key} for e in MAP_ENTRIES(doc.obj)]) AS o FROM doc` |
| **JavaScript** | `Object.fromEntries(Object.entries(doc.obj).map(([k, v]) => [v, k]))` |
| **Python** | `{str(v): k for k, v in doc["obj"].items()}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: {$objectToArray: "$obj"}, in: {k: {$toString: "$$this.v"}, v: "$$this.k"}}}}` |
| **jq** | `.obj \| to_entries \| map({(.value\|tostring): .key}) \| add` |

</details>

### Add / remove a field
_Source data:_
```
doc = {"obj": {"title": "x", "a": 1}}
```
```sql
WITH doc AS ({"obj":{"title":"x","a":1}})
SELECT OBJECT_ADD(doc.obj, "draft", true) AS added,
       OBJECT_REMOVE(doc.obj, "title")    AS removed
FROM doc
-- → {"added":{"a":1,"draft":true,"title":"x"},"removed":{"a":1}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT doc->'obj' \|\| '{"draft":true}' AS added, doc->'obj' - 'title' AS removed FROM doc` |
| **SQL (DuckDB)** | `SELECT STRUCT_INSERT(doc.obj, draft := true) AS added, doc.obj.* EXCLUDE (title) AS removed FROM doc` |
| **JavaScript** | `({...doc.obj, draft: true})   \|   (({title, ...rest}) => rest)(doc.obj)` |
| **Python** | `{**doc["obj"], "draft": True}   \|   {k: v for k, v in doc["obj"].items() if k != "title"}` |
| **MongoDB** | `{$mergeObjects: ["$$ROOT", {draft: true}]}   \|   {$unset: "title"}` |
| **jq** | `.obj + {draft: true}   \|   (.obj \| del(.title))` |

</details>

### Object → array of objects (key becomes a field)
_Source data:_
```
doc = {"obj": {"x": {"n": 1}}}
```
```sql
WITH doc AS ({"obj":{"x":{"n":1}}})
SELECT ARRAY OBJECT_ADD(p.val, "slug", p.name)
       FOR p IN OBJECT_PAIRS(doc.obj) END AS a
FROM doc
-- → {"a":[{"n":1,"slug":"x"}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_AGG(v \|\| JSONB_BUILD_OBJECT('slug', k)) AS a FROM doc, JSONB_EACH(doc->'obj') AS e(k, v)` |
| **SQL (DuckDB)** | `SELECT [STRUCT_INSERT(e.value, slug := e.key) for e in MAP_ENTRIES(doc.obj)] AS a FROM doc` |
| **JavaScript** | `Object.entries(doc.obj).map(([k, v]) => ({...v, slug: k}))` |
| **Python** | `[{**v, "slug": k} for k, v in doc["obj"].items()]` |
| **MongoDB** | `{$map: {input: {$objectToArray: "$obj"}, in: {$mergeObjects: ["$$this.v", {slug: "$$this.k"}]}}}` |
| **jq** | `.obj \| to_entries \| map(.value + {slug: .key})` |

</details>

### Array of objects → object keyed by a field
_Source data:_
```
doc = {"recs": [{"slug": "x", "n": 1}]}
```
```sql
WITH doc AS ({"recs":[{"slug":"x","n":1}]})
SELECT OBJECT r.slug : r FOR r IN doc.recs END AS o
FROM doc
-- → {"o":{"x":{"n":1,"slug":"x"}}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(r->>'slug', r) AS o FROM doc, JSONB_ARRAY_ELEMENTS(doc->'recs') AS r` |
| **SQL (DuckDB)** | `SELECT MAP_FROM_ENTRIES([{'k': r.slug, 'v': r} for r in doc.recs]) AS o FROM doc` |
| **JavaScript** | `Object.fromEntries(doc.recs.map(r => [r.slug, r]))` |
| **Python** | `{r["slug"]: r for r in doc["recs"]}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: "$recs", in: {k: "$$this.slug", v: "$$this"}}}}` |
| **jq** | `.recs \| map({(.slug): .}) \| add` |

</details>

## 6. Aggregating a collection

### "Collect each group's members"
_Over the shop `orders` / `customers` keyspaces._
```sql
SELECT customer, ARRAY_AGG(id) AS order_ids
FROM orders
GROUP BY customer
-- → one row per customer, e.g. {"customer":"dave","order_ids":["1005","1009","1013","1017"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT customer, JSONB_AGG(id) AS order_ids FROM orders GROUP BY customer` |
| **SQL (DuckDB)** | `SELECT customer, LIST(id) AS order_ids FROM orders GROUP BY customer` |
| **JavaScript** | `Object.groupBy(orders, o => o.customer)   // then map ids` |
| **Python** | `{c: [o["id"] for o in g] for c, g in groupby(sorted(orders, key=k), k)}` |
| **MongoDB** | `db.orders.aggregate([{$group: {_id: "$customer", order_ids: {$push: "$id"}}}])` |
| **jq** | `group_by(.customer) \| map({customer: .[0].customer, ids: map(.id)})` |

</details>

### Count occurrences (histogram)
_Source data:_
```
doc = {"tags": ["a", "b", "a"]}
```
```sql
WITH doc AS ({"tags":["a","b","a"]})
SELECT v AS `value`, COUNT(*) AS n
FROM doc
UNNEST doc.tags AS v
GROUP BY v
-- → {"value":"a","n":2}, {"value":"b","n":1}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT v AS value, COUNT(*) AS n FROM doc, JSONB_ARRAY_ELEMENTS_TEXT(doc->'tags') AS v GROUP BY v` |
| **SQL (DuckDB)** | `SELECT v AS value, COUNT(*) AS n FROM doc, UNNEST(doc.tags) AS t(v) GROUP BY v` |
| **JavaScript** | `doc.tags.reduce((m, x) => (m[x] = (m[x] \|\| 0) + 1, m), {})` |
| **Python** | `collections.Counter(doc["tags"])` |
| **MongoDB** | `[{$unwind: "$tags"}, {$group: {_id: "$tags", n: {$sum: 1}}}]` |
| **jq** | `.tags \| reduce .[] as $x ({}; .[$x] += 1)` |

</details>

### Find duplicates by key
_Over the shop `orders` / `customers` keyspaces._
```sql
SELECT id, COUNT(*) AS n
FROM orders
GROUP BY id
HAVING COUNT(*) > 1
-- → (empty for shop — order ids are unique)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT id, COUNT(*) AS n FROM orders GROUP BY id HAVING COUNT(*) > 1` |
| **SQL (DuckDB)** | `SELECT id, COUNT(*) AS n FROM orders GROUP BY id HAVING COUNT(*) > 1` |
| **JavaScript** | `// group by id, then keep buckets with length > 1` |
| **Python** | `[k for k, c in Counter(o["id"] for o in orders).items() if c > 1]` |
| **MongoDB** | `[{$group: {_id: "$id", n: {$sum: 1}}}, {$match: {n: {$gt: 1}}}]` |
| **jq** | `reduce .[].id as $x ({}; .[$x] += 1) \| to_entries[] \| select(.value > 1)` |

</details>

## 7. Strings & numbers

### split / join
```sql
SELECT SPLIT("a,b,c", ",")         AS parts,
       CONCAT2("-", ["a","b","c"]) AS joined
-- → {"parts":["a","b","c"],"joined":"a-b-c"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT STRING_TO_ARRAY('a,b,c', ',') AS parts, ARRAY_TO_STRING(a, '-') AS joined` |
| **SQL (DuckDB)** | `SELECT STRING_SPLIT('a,b,c', ',') AS parts, ARRAY_TO_STRING(['a','b','c'], '-') AS joined` |
| **JavaScript** | `"a,b,c".split(","); ["a","b","c"].join("-")` |
| **Python** | `"a,b,c".split(","); "-".join(["a","b","c"])` |
| **MongoDB** | `{$split: ["a,b,c", ","]}; {$reduce: …}   // no direct join` |
| **jq** | `split(","); join("-")` |

</details>

### upcase / trim-prefix / titlecase
```sql
SELECT UPPER("abc")           AS up,
       LTRIM("foobar", "foo") AS trimmed,
       TITLE("hi there")      AS titled
-- → {"up":"ABC","trimmed":"bar","titled":"Hi There"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT UPPER(s) AS up, LTRIM(s, 'foo') AS trimmed, INITCAP(s) AS titled` |
| **SQL (DuckDB)** | `SELECT UPPER(s), LTRIM(s, 'foo')` |
| **JavaScript** | `s.toUpperCase(); s.replace(/^foo/, "")` |
| **Python** | `s.upper(); s.removeprefix("foo"); s.title()` |
| **MongoDB** | `{$toUpper: "$s"}; {$ltrim: {input: "$s", chars: "foo"}}` |
| **jq** | `ascii_upcase; ltrimstr("foo")` |

</details>

### String interpolation
```sql
SELECT "total is " || TO_STRING(1 + 2) AS msg
-- → {"msg":"total is 3"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT 'total is ' \|\| (1 + 2)::text AS msg` |
| **SQL (DuckDB)** | `SELECT PRINTF('total is %d', 1 + 2) AS msg` |
| **JavaScript** | ``total is ${1 + 2}`` |
| **Python** | `f"total is {1 + 2}"` |
| **MongoDB** | `{$concat: ["total is ", {$toString: {$add: [1, 2]}}]}` |
| **jq** | `"total is \(. + 1)"` |

</details>

### Index of a substring
⚠ SQL++ POSITION is 0-based; Postgres/DuckDB are 1-based.
```sql
SELECT POSITION("a, b", ", ") AS i
-- → {"i":1}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT POSITION(', ' in 'a, b') - 1 AS i` |
| **SQL (DuckDB)** | `SELECT STRPOS('a, b', ', ') - 1 AS i` |
| **JavaScript** | `"a, b".indexOf(", ")` |
| **Python** | `"a, b".find(", ")` |
| **MongoDB** | `{$indexOfCP: ["a, b", ", "]}` |
| **jq** | `index(", ")` |

</details>

### floor / round / sqrt / integer-divide
```sql
SELECT FLOOR(3.7)        AS fl,
       ROUND(3.14159, 2) AS rnd,
       SQRT(9)           AS sq,
       IDIV(7, 2)        AS idiv
-- → {"fl":3,"rnd":3.14,"sq":3,"idiv":3}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT FLOOR(x) AS fl, ROUND(x, 2) AS rnd, SQRT(x) AS sq, DIV(7, 2) AS idiv` |
| **SQL (DuckDB)** | `SELECT FLOOR(x) AS fl, ROUND(x, 2) AS rnd, SQRT(x) AS sq, 7 // 2 AS idiv` |
| **JavaScript** | `Math.floor(x); +x.toFixed(2); Math.sqrt(x); Math.trunc(7/2)` |
| **Python** | `math.floor(x); round(x, 2); math.sqrt(x); 7 // 2` |
| **MongoDB** | `{$floor}; {$round: [x, 2]}; {$sqrt}; {$trunc: {$divide: [7, 2]}}` |
| **jq** | `floor; sqrt; (7 / 2 \| floor)` |

</details>

### Name the JSON type
⚠ SQL++ TYPE() is lowercase and distinguishes "missing" from "null".
_Source data:_
```
doc = {"vals": [0, false, [], {}, null, "x"]}
```
```sql
WITH doc AS ({"vals":[0,false,[],{},null,"x"]})
SELECT ARRAY TYPE(v) FOR v IN doc.vals END AS t
FROM doc
-- → {"t":["number","boolean","array","object","null","string"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_AGG(JSONB_TYPEOF(v)) AS t FROM doc, JSONB_ARRAY_ELEMENTS(doc->'vals') AS v` |
| **SQL (DuckDB)** | `SELECT LIST_TRANSFORM(doc.vals, x -> TYPEOF(x)) AS t FROM doc` |
| **JavaScript** | `doc.vals.map(x => Array.isArray(x) ? "array" : typeof x)` |
| **Python** | `[type(x).__name__ for x in doc["vals"]]` |
| **MongoDB** | `{$type: "$x"}` |
| **jq** | `.vals \| map(type)` |

</details>

## 8. Recursion & deep search

SQL++'s answer to jq's .. is the WITHIN operator (descend into any nested value) plus ANY … WITHIN … SATISFIES. Postgres uses jsonpath ($..). DuckDB/JS/Python/Mongo have no clean one-liner — you recurse.

### Recurse into every descendant
SQL++'s answer to jq's .. is the WITHIN operator (descend into any nested value).
Postgres uses jsonpath ($..); DuckDB/JS/Python/Mongo have no one-liner — you recurse.
_Source data:_
```
doc = {"tree": {"a": 0, "b": [1]}}
```
```sql
WITH doc AS ({"tree":{"a":0,"b":[1]}})
SELECT ARRAY v FOR v WITHIN doc.tree END AS descendants
FROM doc
-- → {"descendants":[0,[1],1]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_PATH_QUERY_ARRAY(doc->'tree', '$..*') AS descendants FROM doc` |
| **JavaScript** | `function* walk(x){ yield x; if (x && typeof x=="object") for (const v of Object.values(x)) yield* walk(v) }` |
| **Python** | `# recursive generator over dict/list values` |
| **jq** | `[.tree \| ..]` |

</details>

### Does a value appear anywhere in the tree?
_Source data:_
```
doc = {"tree": {"a": {"b": 5}}}
```
```sql
WITH doc AS ({"tree":{"a":{"b":5}}})
SELECT ANY v WITHIN doc.tree SATISFIES v = 5 END AS found
FROM doc
-- → {"found":true}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_PATH_EXISTS(doc->'tree', '$..* ? (@ == 5)') AS found FROM doc` |
| **JavaScript** | `[...walk(doc.tree)].includes(5)` |
| **Python** | `any(v == 5 for v in walk(doc["tree"]))` |
| **jq** | `[.tree \| .. \| select(. == 5)] \| length > 0` |

</details>

### Find every object with a given id, at any depth
_Source data:_
```
doc = {"tree": {"a": {"id": "x"}, "b": [{"id": "y"}]}}
```
```sql
WITH doc AS ({"tree":{"a":{"id":"x"},"b":[{"id":"y"}]}})
SELECT ARRAY v FOR v WITHIN doc.tree WHEN v.id = "y" END AS hits
FROM doc
-- → {"hits":[{"id":"y"}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_PATH_QUERY_ARRAY(doc->'tree', '$..* ? (@.id == "y")') AS hits FROM doc` |
| **JavaScript** | `[...walk(doc.tree)].filter(v => v && v.id === "y")` |
| **Python** | `[v for v in walk(doc["tree"]) if isinstance(v, dict) and v.get("id") == "y"]` |
| **jq** | `[.tree \| .. \| objects \| select(.id == "y")]` |

</details>

## 9. Reshaping records

### UNNEST — explode a nested array into rows
The workhorse for line-items, tags, events. SQL++ UNNEST pairs each element with its
parent's fields.
_Source data:_
```
doc = {"docs": [{"id": 1, "tags": ["a", "b"]}]}
```
```sql
WITH doc AS ({"docs":[{"id":1,"tags":["a","b"]}]})
SELECT o.id, t AS tag
FROM doc
UNNEST doc.docs AS o
UNNEST o.tags AS t
-- → {"id":1,"tag":"a"} then {"id":1,"tag":"b"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT o->>'id', t AS tag FROM doc, JSONB_ARRAY_ELEMENTS(doc->'docs') AS o, JSONB_ARRAY_ELEMENTS_TEXT(o->'tags') AS t` |
| **SQL (DuckDB)** | `SELECT o.id, UNNEST(o.tags) AS tag FROM doc, UNNEST(doc.docs) AS s(o)` |
| **JavaScript** | `doc.docs.flatMap(o => o.tags.map(t => ({id: o.id, tag: t})))` |
| **Python** | `[{"id": o["id"], "tag": t} for o in doc["docs"] for t in o["tags"]]` |
| **MongoDB** | `[{$unwind: "$tags"}, {$project: {id: 1, tag: "$tags"}}]` |
| **jq** | `.docs[] \| {id, tag: .tags[]}` |

</details>

### Reshape + rename + sort
_Source data:_
```
doc = {"dms": [{"text": "hi", "sender": {"screen_name": "amy"}, "ts": 2},
               {"text": "yo", "sender": {"screen_name": "bo"},  "ts": 1}]}
```
```sql
WITH doc AS ({"dms":[{"text":"hi","sender":{"screen_name":"amy"},"ts":2},{"text":"yo","sender":{"screen_name":"bo"},"ts":1}]})
SELECT d.text, d.sender.screen_name AS from_name
FROM doc
UNNEST doc.dms AS d
ORDER BY d.ts
-- → {"text":"yo","from_name":"bo"} then {"text":"hi","from_name":"amy"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT d->>'text', d#>>'{sender,screen_name}' AS from_name FROM doc, JSONB_ARRAY_ELEMENTS(doc->'dms') AS d ORDER BY d->>'ts'` |
| **SQL (DuckDB)** | `SELECT d.text, d.sender.screen_name AS from_name FROM doc, UNNEST(doc.dms) AS s(d) ORDER BY d.ts` |
| **JavaScript** | `doc.dms.map(d => ({text: d.text, from: d.sender.screen_name})).sort((a,b)=>a.ts-b.ts)` |
| **Python** | `sorted(({"text": d["text"], "from": d["sender"]["screen_name"]} for d in doc["dms"]), key=…)` |
| **MongoDB** | `[{$project: {text: 1, from: "$sender.screen_name"}}, {$sort: {ts: 1}}]` |
| **jq** | `[.dms[] \| {text, from: .sender.screen_name}] \| sort_by(.ts)` |

</details>

### Merge two arrays, flagging one
_Source data:_
```
doc = {"team": [{"n": "a"}], "formerly": [{"n": "b"}]}
```
```sql
WITH doc AS ({"team":[{"n":"a"}],"formerly":[{"n":"b"}]})
SELECT ARRAY_CONCAT(doc.team,
       ARRAY OBJECT_ADD(m, "formerly", true) FOR m IN doc.formerly END) AS everyone
FROM doc
-- → {"everyone":[{"n":"a"},{"formerly":true,"n":"b"}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT (doc->'team') \|\| (SELECT JSONB_AGG(m \|\| '{"formerly":true}') FROM JSONB_ARRAY_ELEMENTS(doc->'formerly') m) AS everyone FROM doc` |
| **SQL (DuckDB)** | `SELECT LIST_CONCAT(doc.team, [STRUCT_INSERT(m, formerly := true) for m in doc.formerly]) AS everyone FROM doc` |
| **JavaScript** | `[...doc.team, ...doc.formerly.map(m => ({...m, formerly: true}))]` |
| **Python** | `doc["team"] + [{**m, "formerly": True} for m in doc["formerly"]]` |
| **MongoDB** | `{$concatArrays: ["$team", {$map: {input: "$formerly", in: {$mergeObjects: ["$$this", {formerly: true}]}}}]}` |
| **jq** | `[.team, (.formerly \| map(. + {formerly: true}))] \| flatten` |

</details>

## 10. Conditionals & defaults

### if / then / else
```sql
SELECT CASE WHEN 5 > 3 THEN "big" ELSE "small" END AS c
-- → {"c":"big"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT CASE WHEN 5 > 3 THEN 'big' ELSE 'small' END AS c` |
| **SQL (DuckDB)** | `SELECT IF(5 > 3, 'big', 'small') AS c` |
| **JavaScript** | `5 > 3 ? "big" : "small"` |
| **Python** | `"big" if 5 > 3 else "small"` |
| **MongoDB** | `{$cond: [{$gt: [5, 3]}, "big", "small"]}` |
| **jq** | `if . > 3 then "big" else "small" end` |

</details>

### Default for a missing/null value
SQL++ splits what COALESCE and jq's // blur: IFMISSING / IFNULL / IFMISSINGORNULL treat
absent and present-but-null differently.
_Source data:_
```
doc = {}
```
```sql
WITH doc AS ({})
SELECT IFMISSINGORNULL(doc.foo, "default") AS v
FROM doc
-- → {"v":"default"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT COALESCE(doc->>'foo', 'default') AS v FROM doc` |
| **SQL (DuckDB)** | `SELECT COALESCE(doc.foo, 'default') AS v FROM doc` |
| **JavaScript** | `doc.foo ?? "default"` |
| **Python** | `doc.get("foo", "default")` |
| **MongoDB** | `{$ifNull: ["$foo", "default"]}` |
| **jq** | `.foo // "default"` |

</details>

### "Type guard (jq's try/catch)"
jq guards type errors with try/catch; SQL++ has no type errors — a bad op yields
NULL/MISSING — so guard with CASE.
_Source data:_
```
doc = {"vals": [{"a": 1}, "str"]}
```
```sql
WITH doc AS ({"vals":[{"a":1},"str"]})
SELECT ARRAY (CASE WHEN TYPE(x) = "object" THEN x.a END) FOR x IN doc.vals END AS a
FROM doc
-- → {"a":[1,null]}   (the string has no .a → null)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **JavaScript** | `doc.vals.map(x => (x && typeof x === "object" && !Array.isArray(x)) ? x.a : null)` |
| **Python** | `[x.get("a") if isinstance(x, dict) else None for x in doc["vals"]]` |
| **MongoDB** | `{$cond: [{$eq: [{$type: "$x"}, "object"]}, "$x.a", null]}` |
| **jq** | `.vals \| map(try .a catch null)` |

</details>


## Gotchas worth pinning

- **0-based `SUBSTR`/`POSITION`** — `SUBSTR("hello",1,3)` → `"ell"`. (SQL's are 1-based; DuckDB lists/strings are 1-based too.)
- **`OBJECT_PAIRS` yields `{name, val}`**, not jq's `{key, value}`.
- **Object keys must be strings** — wrap non-string keys in `TO_STRING`.
- **Constructed-object keys render sorted** (canonical JSON); top-level `SELECT` columns keep their written order.
- **Negative array index wraps** to the end (`a[-1]` = last); an out-of-range positive index — and a slice whose end runs past the array — is `MISSING`. Clamp with `LEAST(end, ARRAY_LENGTH(a))`; guard `i >= 0` on computed indices.
- **`OBJECT_PAIRS`/`OBJECT_NAMES` work on objects only** (an array → `NULL`); index arrays positionally.
- **`ARRAY_FLATTEN` needs an explicit depth** (jq flattens fully).
- **Reserved words need backticks** — `` `value` ``, `` `last` ``, `` `type` `` as identifiers.
- **`UNION`/`UNION ALL` align by field name**, not position — alias every column.

## MISSING vs NULL — the SQL++ superpower

The one concept with no jq or standard-SQL equivalent. **`MISSING`** = the field
isn't there (SQL models this as `NULL`; jq as an error needing `?`). **`NULL`** =
the field is present with value `null`. n1k1 keeps them distinct: a `MISSING` field
is *omitted* from output, a `NULL` field is rendered. So `x.foo IS MISSING` ≠
`x.foo IS NULL` (use `IS VALUED` for "present and not null"), and aggregates/array
builders skip `MISSING` — which is why jq's `.foo?` needs no translation.

## See also

- The jq manual (`jqlang.org/manual`) and Remy Sharp's jq recipes (`remysharp.com/drafts/jq-recipes`).
- `examples/queries/*.sql++` — runnable SQL++ showpieces (Conway's Life, Mandelbrot, unicode charts).
- Couchbase N1QL function reference — n1k1 speaks the same dialect.

