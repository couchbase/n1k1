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
| **DuckDB** | `SELECT id, status, total FROM orders` |
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
| **DuckDB** | `SELECT id, total FROM orders WHERE total > 100` |
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
| **DuckDB** | `SELECT id, total FROM orders ORDER BY total DESC LIMIT 3` |
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
| **DuckDB** | `SELECT MAX_BY(orders, total) FROM orders` |
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
| **SQL (Postgres)** | `SELECT COUNT(*), ROUND(SUM(total),2), ROUND(AVG(total),2) FROM orders` |
| **DuckDB** | `SELECT COUNT(*), ROUND(SUM(total),2), ROUND(AVG(total),2) FROM orders` |
| **JavaScript** | `({n: orders.length, revenue: orders.reduce((s, o) => s + o.total, 0)})` |
| **Python** | `{"n": len(orders), "revenue": sum(o["total"] for o in orders)}` |
| **MongoDB** | `db.orders.aggregate([{$group: {_id: null, n: {$sum: 1}, revenue: {$sum: "$total"}, avg: {$avg: "$total"}}}])` |
| **jq** | `{n: length, revenue: (map(.total) \| add)}` |

</details>

### Group, then aggregate each group
_Over the shop `orders` / `customers` keyspaces._
```sql
SELECT status, COUNT(*) AS n,
       ROUND(SUM(total), 2) AS revenue
FROM orders
GROUP BY status
ORDER BY revenue DESC
-- → {"status":"shipped","n":16,"revenue":1758.73}, {"status":"pending",…}, {"status":"cancelled",…}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT status, COUNT(*), ROUND(SUM(total),2) FROM orders GROUP BY status ORDER BY 3 DESC` |
| **DuckDB** | `SELECT status, COUNT(*), ROUND(SUM(total),2) FROM orders GROUP BY status ORDER BY 3 DESC` |
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
| **SQL (Postgres)** | `SELECT customer FROM orders -- a single-column result set` |
| **DuckDB** | `SELECT customer FROM orders` |
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
-- → {"city":"Austin"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT col->'customer'->>'city' -- ->> text, -> jsonb` |
| **DuckDB** | `SELECT doc->'$.customer.city'` |
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
-- → {}   (foo is MISSING → the key is omitted)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT col->'foo' -- NULL if absent` |
| **DuckDB** | `SELECT doc->'$.foo'` |
| **JavaScript** | `doc.foo               // undefined` |
| **Python** | `doc.get("foo")        # None` |
| **MongoDB** | `// a missing field is simply absent from the result` |
| **jq** | `.foo?                 # no error even on a non-object` |

</details>

### Array index — 0-based, negatives from the end
_Source data:_
```
nums = [10, 20, 30]
```
```sql
WITH nums AS ([10,20,30])
SELECT nums[0]  AS `first`,
       nums[-1] AS `last`,
       nums[-2] AS penult
-- → {"first":10,"last":30,"penult":20}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT col->0, col->-1 -- jsonb: 0-based, -1 = last` |
| **DuckDB** | `SELECT nums[1], nums[-1] -- ⚠ DuckDB lists are 1-based` |
| **JavaScript** | `nums[0]; nums.at(-1)` |
| **Python** | `nums[0]; nums[-1]` |
| **MongoDB** | `{$arrayElemAt: ["$nums", -1]}` |
| **jq** | `.[0], .[-1]` |

</details>

### Slice a range
_Source data:_
```
letters = ["a", "b", "c", "d", "e"]
```
```sql
WITH letters AS (["a","b","c","d","e"])
SELECT letters[2:4] AS s
-- → {"s":["c","d"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_PATH_QUERY(col, '$[2 to 3]') -- SQLite has no slice` |
| **DuckDB** | `SELECT letters[3:4] -- ⚠ 1-based, inclusive` |
| **JavaScript** | `letters.slice(2, 4)` |
| **Python** | `letters[2:4]` |
| **MongoDB** | `{$slice: ["$letters", 2, 2]}` |
| **jq** | `.[2:4]` |

</details>

## 3. Transforming arrays

### map — transform every element
SQL++ has two forms: a scan (SELECT … FROM nums) or the inline array comprehension
ARRAY … FOR … END that returns one array value.
_Source data:_
```
nums = [1, 2, 3]
```
```sql
WITH nums AS ([1,2,3])
SELECT ARRAY v + 1 FOR v IN nums END AS r
-- → {"r":[2,3,4]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_AGG(value::int + 1) FROM JSONB_ARRAY_ELEMENTS(col)` |
| **DuckDB** | `SELECT [x + 1 for x in nums]` |
| **JavaScript** | `nums.map(x => x + 1)` |
| **Python** | `[x + 1 for x in nums]` |
| **MongoDB** | `{$map: {input: "$nums", as: "v", in: {$add: ["$$v", 1]}}}` |
| **jq** | `map(. + 1)` |

</details>

### filter — keep matching elements
_Source data:_
```
nums = [1, 5, 3, 0, 7]
```
```sql
WITH nums AS ([1,5,3,0,7])
SELECT RAW v
FROM nums AS v
WHERE v >= 2
-- → 5, 3, 7
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT value FROM JSONB_ARRAY_ELEMENTS(col) WHERE value::int >= 2` |
| **DuckDB** | `SELECT [x for x in nums if x >= 2]` |
| **JavaScript** | `nums.filter(x => x >= 2)` |
| **Python** | `[x for x in nums if x >= 2]` |
| **MongoDB** | `{$filter: {input: "$nums", cond: {$gte: ["$$this", 2]}}}` |
| **jq** | `map(select(. >= 2))` |

</details>

### Sum, min, max, average
_Source data:_
```
nums = [1, 2, 3]
```
```sql
WITH nums AS ([1,2,3])
SELECT ARRAY_SUM(nums) AS s,
       ARRAY_MAX(nums) AS mx,
       ARRAY_AVG(nums) AS a
-- → {"s":6,"mx":3,"a":2}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT SUM(value::int), MAX(value::int), AVG(value::int) FROM JSONB_ARRAY_ELEMENTS(col)` |
| **DuckDB** | `SELECT LIST_SUM(nums), LIST_MAX(nums), LIST_AVG(nums)` |
| **JavaScript** | `nums.reduce((a, b) => a + b, 0); Math.max(...nums)` |
| **Python** | `sum(nums); max(nums); statistics.mean(nums)` |
| **MongoDB** | `{$sum: "$nums"}; {$max: "$nums"}; {$avg: "$nums"}` |
| **jq** | `add; max; (add / length)` |

</details>

### Length / count of elements
_Source data:_
```
nums = [1, 2, 3]
```
```sql
WITH nums AS ([1,2,3])
SELECT ARRAY_LENGTH(nums) AS n
-- → {"n":3}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_ARRAY_LENGTH(col)` |
| **DuckDB** | `SELECT LEN(nums)` |
| **JavaScript** | `nums.length` |
| **Python** | `len(nums)` |
| **MongoDB** | `{$size: "$nums"}` |
| **jq** | `length` |

</details>

### Unique / dedup
ARRAY_DISTINCT keeps first-seen order; wrap in ARRAY_SORT for jq's sorted result.
_Source data:_
```
nums = [1, 2, 5, 3, 5, 3, 1]
```
```sql
WITH nums AS ([1,2,5,3,5,3,1])
SELECT ARRAY_DISTINCT(nums) AS u
-- → {"u":[1,2,5,3]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT DISTINCT value FROM JSONB_ARRAY_ELEMENTS(col)` |
| **DuckDB** | `SELECT LIST_DISTINCT(nums)` |
| **JavaScript** | `[...new Set(nums)]` |
| **Python** | `list(dict.fromkeys(nums))` |
| **MongoDB** | `{$setUnion: ["$nums", []]}   // unordered` |
| **jq** | `unique   # sorted` |

</details>

### Flatten nested arrays
⚠ SQL++ ARRAY_FLATTEN needs an explicit depth; jq flattens fully.
_Source data:_
```
nested = [1, [2], [[3]]]
```
```sql
WITH nested AS ([1,[2],[[3]]])
SELECT ARRAY_FLATTEN(nested, 2) AS f
-- → {"f":[1,2,3]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `-- recursive unnest; no single builtin` |
| **DuckDB** | `SELECT FLATTEN(nested) -- one level` |
| **JavaScript** | `nested.flat(2)` |
| **Python** | `# no deep builtin; recurse` |
| **MongoDB** | `{$reduce: {input: "$nested", initialValue: [], in: {$concatArrays: ["$$value", "$$this"]}}}  // one level` |
| **jq** | `flatten` |

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
| **DuckDB** | `SELECT LIST_REVERSE(a), RANGE(2,4), LIST_APPEND(a,x), LIST_CONCAT(a,b)` |
| **JavaScript** | `a.toReversed(); [...Array(2).keys()].map(i=>i+2); [...a, x]; a.concat(b)` |
| **Python** | `a[::-1]; list(range(2,4)); a + [x]; a + b` |
| **MongoDB** | `{$reverseArray}; {$range:[2,4]}; {$concatArrays}` |
| **jq** | `reverse; [range(2;4)]; . + [x]; .a + .b` |

</details>

### Every other element
Index by a stepped range (OBJECT_PAIRS works on objects only, not arrays).
_Source data:_
```
letters = ["a", "b", "c", "d"]
```
```sql
WITH letters AS (["a","b","c","d"])
SELECT ARRAY letters[i]
       FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(letters), 2) END AS evens
-- → {"evens":["a","c"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT v FROM JSONB_ARRAY_ELEMENTS(col) WITH ORDINALITY t(v, i) WHERE i % 2 = 1` |
| **DuckDB** | `SELECT LIST_SLICE(letters, 1, 4, 2) -- 1-based, step 2` |
| **JavaScript** | `letters.filter((_, i) => i % 2 === 0)` |
| **Python** | `letters[::2]` |
| **MongoDB** | `{$map: {input: {$range: [0, {$size: "$letters"}, 2]}, in: {$arrayElemAt: ["$letters", "$$this"]}}}` |
| **jq** | `[range(0; length; 2) as $i \| .[$i]]` |

</details>

### Chunk into fixed-size groups
⚠ Clamp the slice end with LEAST(…, ARRAY_LENGTH(a)) — a slice past the end is MISSING
and would drop the last short chunk.
_Source data:_
```
nums = [1, 2, 3, 4, 5]
```
```sql
WITH nums AS ([1,2,3,4,5])
SELECT ARRAY nums[i:LEAST(i + 2, ARRAY_LENGTH(nums))]
       FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(nums), 2) END AS chunks
-- → {"chunks":[[1,2],[3,4],[5]]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `-- window / ntile, or generate_series with slicing` |
| **DuckDB** | `SELECT [nums[i+1 : i+2] for i in RANGE(0, LEN(nums), 2)]` |
| **JavaScript** | `Array.from({length: Math.ceil(nums.length/2)}, (_, i) => nums.slice(i*2, i*2+2))` |
| **Python** | `[nums[i:i+2] for i in range(0, len(nums), 2)]` |
| **jq** | `[range(0; length; 2) as $i \| .[$i:$i+2]]` |

</details>

## 4. Iterating & collecting

### Visit each element
_Source data:_
```
people = [{"name": "JSON"}, {"name": "XML"}]
```
```sql
WITH people AS ([{"name":"JSON"},{"name":"XML"}])
SELECT p.*
FROM people AS p
-- → {"name":"JSON"} then {"name":"XML"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT value FROM JSONB_ARRAY_ELEMENTS(col)` |
| **DuckDB** | `SELECT UNNEST(people, recursive := false)` |
| **JavaScript** | `for (const p of people) { … }` |
| **Python** | `for p in people: ...` |
| **MongoDB** | `db.people.find()` |
| **jq** | `.[]` |

</details>

### One field from each element
_Source data:_
```
people = [{"name": "JSON"}, {"name": "XML"}]
```
```sql
WITH people AS ([{"name":"JSON"},{"name":"XML"}])
SELECT RAW p.name
FROM people AS p
-- → "JSON", "XML"
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT e->>'name' FROM JSONB_ARRAY_ELEMENTS(col) e` |
| **DuckDB** | `SELECT p.name FROM (SELECT UNNEST(people) p)` |
| **JavaScript** | `people.map(p => p.name)` |
| **Python** | `[p["name"] for p in people]` |
| **MongoDB** | `db.people.find({}, {name: 1, _id: 0})` |
| **jq** | `.[] \| .name` |

</details>

### Collect values into one array
_Source data:_
```
doc = {"user": "s", "projects": ["jq", "wf"]}
```
```sql
WITH doc AS ({"user":"s","projects":["jq","wf"]})
SELECT ARRAY_PREPEND(doc.user, doc.projects) AS r
-- → {"r":["s","jq","wf"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT col->'user' \|\| col->'projects' -- jsonb \|\| concatenates` |
| **DuckDB** | `SELECT LIST_PREPEND(doc.user, doc.projects)` |
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
-- → {"o":{"title":"JQ","user":"s"}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_BUILD_OBJECT('user', col->'user', 'title', col->'title')` |
| **DuckDB** | `SELECT {'user': doc.user, 'title': doc.title} -- a STRUCT` |
| **JavaScript** | `({user: doc.user, title: doc.title})` |
| **Python** | `{"user": doc["user"], "title": doc["title"]}` |
| **MongoDB** | `{$project: {user: 1, title: 1, _id: 0}}` |
| **jq** | `{user, title}` |

</details>

### "List an object's keys"
_Source data:_
```
obj = {"b": 2, "a": 1}
```
```sql
WITH obj AS ({"b":2,"a":1})
SELECT OBJECT_NAMES(obj) AS k
-- → {"k":["a","b"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_KEYS(col) -- one row per key` |
| **DuckDB** | `SELECT JSON_KEYS(obj)` |
| **JavaScript** | `Object.keys(obj)` |
| **Python** | `list(obj.keys())` |
| **MongoDB** | `{$map: {input: {$objectToArray: "$obj"}, in: "$$this.k"}}` |
| **jq** | `keys   # sorted` |

</details>

### Object → array of pairs
⚠ SQL++ OBJECT_PAIRS yields {name, val}; jq yields {key, value}.
_Source data:_
```
obj = {"a": 1, "b": 2}
```
```sql
WITH obj AS ({"a":1,"b":2})
SELECT OBJECT_PAIRS(obj) AS p
-- → {"p":[{"name":"a","val":1},{"name":"b","val":2}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_EACH(col) -- rows (key, value)` |
| **DuckDB** | `SELECT MAP_ENTRIES(obj)` |
| **JavaScript** | `Object.entries(obj)   // [["a",1],["b",2]]` |
| **Python** | `list(obj.items())` |
| **MongoDB** | `{$objectToArray: "$obj"}   // [{k,v}]` |
| **jq** | `to_entries   # [{key,value}]` |

</details>

### Pairs → object
_Source data:_
```
pairs = [{"name": "a", "val": 1}]
```
```sql
WITH pairs AS ([{"name":"a","val":1}])
SELECT OBJECT p.name : p.val FOR p IN pairs END AS o
-- → {"o":{"a":1}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(e->>'key', e->'value')` |
| **DuckDB** | `SELECT MAP_FROM_ENTRIES(pairs)` |
| **JavaScript** | `Object.fromEntries(pairs.map(p => [p.name, p.val]))` |
| **Python** | `{p["name"]: p["val"] for p in pairs}` |
| **MongoDB** | `{$arrayToObject: "$pairs"}` |
| **jq** | `from_entries` |

</details>

### Object with a key from the data
⚠ Keys must be strings; `value` is a reserved word (back-quoted).
_Source data:_
```
recs = [{"label": "a", "value": 1}]
```
```sql
WITH recs AS ([{"label":"a","value":1}])
SELECT OBJECT r.label : r.`value` FOR r IN recs END AS o
-- → {"o":{"a":1}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(e->>'label', e->'value')` |
| **DuckDB** | `SELECT MAP_FROM_ENTRIES([{'k': r.label, 'v': r.value} for r in recs])` |
| **JavaScript** | `Object.fromEntries(recs.map(r => [r.label, r.value]))` |
| **Python** | `{r["label"]: r["value"] for r in recs}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: "$recs", in: {k: "$$this.label", v: "$$this.value"}}}}` |
| **jq** | `map({(.label): .value}) \| add` |

</details>

### Transform every value, keep keys
_Source data:_
```
obj = {"a": 1, "b": 2}
```
```sql
WITH obj AS ({"a":1,"b":2})
SELECT OBJECT p.name : p.val + 1 FOR p IN OBJECT_PAIRS(obj) END AS o
-- → {"o":{"a":2,"b":3}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(k, (v::int)+1) FROM JSONB_EACH(obj) e(k,v)` |
| **DuckDB** | `SELECT MAP_FROM_ENTRIES([{'k': e.key, 'v': e.value + 1} for e in MAP_ENTRIES(obj)])` |
| **JavaScript** | `Object.fromEntries(Object.entries(obj).map(([k, v]) => [k, v + 1]))` |
| **Python** | `{k: v + 1 for k, v in obj.items()}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: {$objectToArray: "$obj"}, in: {k: "$$this.k", v: {$add: ["$$this.v", 1]}}}}}` |
| **jq** | `map_values(. + 1)` |

</details>

### Swap keys and values
_Source data:_
```
obj = {"a": 1, "b": 2}
```
```sql
WITH obj AS ({"a":1,"b":2})
SELECT OBJECT TO_STRING(p.val) : p.name FOR p IN OBJECT_PAIRS(obj) END AS o
-- → {"o":{"1":"a","2":"b"}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(v#>>'{}', k) FROM JSONB_EACH(obj) e(k,v)` |
| **DuckDB** | `SELECT MAP_FROM_ENTRIES([{'k': e.value::VARCHAR, 'v': e.key} for e in MAP_ENTRIES(obj)])` |
| **JavaScript** | `Object.fromEntries(Object.entries(obj).map(([k, v]) => [v, k]))` |
| **Python** | `{str(v): k for k, v in obj.items()}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: {$objectToArray: "$obj"}, in: {k: {$toString: "$$this.v"}, v: "$$this.k"}}}}` |
| **jq** | `to_entries \| map({(.value): .key}) \| add` |

</details>

### Add / remove a field
_Source data:_
```
obj = {"title": "x", "a": 1}
```
```sql
WITH obj AS ({"title":"x","a":1})
SELECT OBJECT_ADD(obj, "draft", true) AS added,
       OBJECT_REMOVE(obj, "title")    AS removed
-- → {"added":{"a":1,"draft":true,"title":"x"},"removed":{"a":1}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT col \|\| '{"draft":true}' AS added, col - 'title' AS removed` |
| **DuckDB** | `SELECT STRUCT_INSERT(obj, draft := true) AS added, obj.* EXCLUDE (title)` |
| **JavaScript** | `({...obj, draft: true})   \|   (({title, ...rest}) => rest)(obj)` |
| **Python** | `{**obj, "draft": True}   \|   {k: v for k, v in obj.items() if k != "title"}` |
| **MongoDB** | `{$mergeObjects: ["$$ROOT", {draft: true}]}   \|   {$unset: "title"}` |
| **jq** | `. + {draft: true}   \|   del(.title)` |

</details>

### Object → array of objects (key becomes a field)
_Source data:_
```
obj = {"x": {"n": 1}}
```
```sql
WITH obj AS ({"x":{"n":1}})
SELECT ARRAY OBJECT_ADD(p.val, "slug", p.name)
       FOR p IN OBJECT_PAIRS(obj) END AS a
-- → {"a":[{"n":1,"slug":"x"}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_AGG(v \|\| JSONB_BUILD_OBJECT('slug', k))` |
| **DuckDB** | `SELECT [STRUCT_INSERT(e.value, slug := e.key) for e in MAP_ENTRIES(obj)]` |
| **JavaScript** | `Object.entries(obj).map(([k, v]) => ({...v, slug: k}))` |
| **Python** | `[{**v, "slug": k} for k, v in obj.items()]` |
| **MongoDB** | `{$map: {input: {$objectToArray: "$obj"}, in: {$mergeObjects: ["$$this.v", {slug: "$$this.k"}]}}}` |
| **jq** | `to_entries \| map(.value + {slug: .key})` |

</details>

### Array of objects → object keyed by a field
_Source data:_
```
recs = [{"slug": "x", "n": 1}]
```
```sql
WITH recs AS ([{"slug":"x","n":1}])
SELECT OBJECT r.slug : r FOR r IN recs END AS o
-- → {"o":{"x":{"n":1,"slug":"x"}}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_OBJECT_AGG(e->>'slug', e)` |
| **DuckDB** | `SELECT MAP_FROM_ENTRIES([{'k': r.slug, 'v': r} for r in recs])` |
| **JavaScript** | `Object.fromEntries(recs.map(r => [r.slug, r]))` |
| **Python** | `{r["slug"]: r for r in recs}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: "$recs", in: {k: "$$this.slug", v: "$$this"}}}}` |
| **jq** | `map({(.slug): .}) \| add` |

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
| **SQL (Postgres)** | `SELECT customer, JSONB_AGG(id) FROM orders GROUP BY customer` |
| **DuckDB** | `SELECT customer, LIST(id) FROM orders GROUP BY customer` |
| **JavaScript** | `Object.groupBy(orders, o => o.customer)   // then map ids` |
| **Python** | `{c: [o["id"] for o in g] for c, g in groupby(sorted(orders, key=k), k)}` |
| **MongoDB** | `db.orders.aggregate([{$group: {_id: "$customer", order_ids: {$push: "$id"}}}])` |
| **jq** | `group_by(.customer) \| map({customer: .[0].customer, ids: map(.id)})` |

</details>

### Count occurrences (histogram)
_Source data:_
```
tags = ["a", "b", "a"]
```
```sql
WITH tags AS (["a","b","a"])
SELECT v AS `value`, COUNT(*) AS n
FROM tags AS v
GROUP BY v
-- → {"value":"a","n":2}, {"value":"b","n":1}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT value, COUNT(*) FROM JSONB_ARRAY_ELEMENTS_TEXT(col) GROUP BY value` |
| **DuckDB** | `SELECT v, COUNT(*) FROM (SELECT UNNEST(tags) v) GROUP BY v` |
| **JavaScript** | `tags.reduce((m, x) => (m[x] = (m[x] \|\| 0) + 1, m), {})` |
| **Python** | `collections.Counter(tags)` |
| **MongoDB** | `[{$unwind: "$tags"}, {$group: {_id: "$tags", n: {$sum: 1}}}]` |
| **jq** | `reduce .[] as $x ({}; .[$x] += 1)` |

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
| **SQL (Postgres)** | `SELECT id, COUNT(*) FROM orders GROUP BY id HAVING COUNT(*) > 1` |
| **DuckDB** | `SELECT id, COUNT(*) FROM orders GROUP BY id HAVING COUNT(*) > 1` |
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
| **SQL (Postgres)** | `SELECT STRING_TO_ARRAY('a,b,c', ','), ARRAY_TO_STRING(a, '-')` |
| **DuckDB** | `SELECT STRING_SPLIT('a,b,c', ','), ARRAY_TO_STRING(['a','b','c'], '-')` |
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
| **SQL (Postgres)** | `SELECT UPPER(s), LTRIM(s, 'foo'), INITCAP(s)` |
| **DuckDB** | `SELECT UPPER(s), LTRIM(s, 'foo')` |
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
| **SQL (Postgres)** | `SELECT 'total is ' \|\| (1 + 2)::text` |
| **DuckDB** | `SELECT PRINTF('total is %d', 1 + 2)` |
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
| **SQL (Postgres)** | `SELECT POSITION(', ' in 'a, b') - 1` |
| **DuckDB** | `SELECT STRPOS('a, b', ', ') - 1` |
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
| **SQL (Postgres)** | `SELECT FLOOR(x), ROUND(x, 2), SQRT(x), DIV(7, 2)` |
| **DuckDB** | `SELECT FLOOR(x), ROUND(x, 2), SQRT(x), 7 // 2` |
| **JavaScript** | `Math.floor(x); +x.toFixed(2); Math.sqrt(x); Math.trunc(7/2)` |
| **Python** | `math.floor(x); round(x, 2); math.sqrt(x); 7 // 2` |
| **MongoDB** | `{$floor}; {$round: [x, 2]}; {$sqrt}; {$trunc: {$divide: [7, 2]}}` |
| **jq** | `floor; sqrt; (7 / 2 \| floor)` |

</details>

### Name the JSON type
⚠ SQL++ TYPE() is lowercase and distinguishes "missing" from "null".
_Source data:_
```
vals = [0, false, [], {}, null, "x"]
```
```sql
WITH vals AS ([0,false,[],{},null,"x"])
SELECT ARRAY TYPE(v) FOR v IN vals END AS t
-- → {"t":["number","boolean","array","object","null","string"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_TYPEOF(value)` |
| **DuckDB** | `SELECT TYPEOF(v), JSON_TYPE(j)` |
| **JavaScript** | `typeof x; Array.isArray(x)` |
| **Python** | `type(x).__name__` |
| **MongoDB** | `{$type: "$x"}` |
| **jq** | `type   # (jq has no "missing")` |

</details>

## 8. Recursion & deep search

SQL++'s answer to jq's .. is the WITHIN operator (descend into any nested value) plus ANY … WITHIN … SATISFIES. Postgres uses jsonpath ($..). DuckDB/JS/Python/Mongo have no clean one-liner — you recurse.

### Recurse into every descendant
SQL++'s answer to jq's .. is the WITHIN operator (descend into any nested value).
Postgres uses jsonpath ($..); DuckDB/JS/Python/Mongo have no one-liner — you recurse.
_Source data:_
```
tree = {"a": 0, "b": [1]}
```
```sql
WITH tree AS ({"a":0,"b":[1]})
SELECT ARRAY v FOR v WITHIN tree END AS descendants
-- → {"descendants":[0,[1],1]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_PATH_QUERY(col, '$..*')` |
| **JavaScript** | `function* walk(x){ yield x; if (x && typeof x=="object") for (const v of Object.values(x)) yield* walk(v) }` |
| **Python** | `# recursive generator over dict/list values` |
| **jq** | `..` |

</details>

### Does a value appear anywhere in the tree?
_Source data:_
```
tree = {"a": {"b": 5}}
```
```sql
WITH tree AS ({"a":{"b":5}})
SELECT ANY v WITHIN tree SATISFIES v = 5 END AS found
-- → {"found":true}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_PATH_EXISTS(col, '$..* ? (@ == 5)')` |
| **JavaScript** | `[...walk(tree)].includes(5)` |
| **Python** | `any(v == 5 for v in walk(tree))` |
| **jq** | `[.. \| select(. == 5)] \| length > 0` |

</details>

### Find every object with a given id, at any depth
_Source data:_
```
tree = {"a": {"id": "x"}, "b": [{"id": "y"}]}
```
```sql
WITH tree AS ({"a":{"id":"x"},"b":[{"id":"y"}]})
SELECT ARRAY v FOR v WITHIN tree WHEN v.id = "y" END AS hits
-- → {"hits":[{"id":"y"}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT JSONB_PATH_QUERY(col, '$..* ? (@.id == "y")')` |
| **JavaScript** | `[...walk(tree)].filter(v => v && v.id === "y")` |
| **Python** | `[v for v in walk(tree) if isinstance(v, dict) and v.get("id") == "y"]` |
| **jq** | `[.. \| objects \| select(.id == "y")]` |

</details>

## 9. Reshaping records

### UNNEST — explode a nested array into rows
The workhorse for line-items, tags, events. SQL++ UNNEST pairs each element with its
parent's fields.
_Source data:_
```
docs = [{"id": 1, "tags": ["a", "b"]}]
```
```sql
WITH docs AS ([{"id":1,"tags":["a","b"]}])
SELECT o.id, t AS tag
FROM docs AS o
UNNEST o.tags AS t
-- → {"id":1,"tag":"a"} then {"id":1,"tag":"b"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT o->>'id', t FROM JSONB_ARRAY_ELEMENTS(col) o, JSONB_ARRAY_ELEMENTS_TEXT(o->'tags') t` |
| **DuckDB** | `SELECT id, UNNEST(tags) AS tag FROM docs` |
| **JavaScript** | `docs.flatMap(o => o.tags.map(t => ({id: o.id, tag: t})))` |
| **Python** | `[{"id": o["id"], "tag": t} for o in docs for t in o["tags"]]` |
| **MongoDB** | `[{$unwind: "$tags"}, {$project: {id: 1, tag: "$tags"}}]` |
| **jq** | `.[] \| {id, tag: .tags[]}` |

</details>

### Reshape + rename + sort
_Source data:_
```
dms = [{"text": "hi", "sender": {"screen_name": "amy"}, "ts": 2},
       {"text": "yo", "sender": {"screen_name": "bo"},  "ts": 1}]
```
```sql
WITH dms AS ([{"text":"hi","sender":{"screen_name":"amy"},"ts":2},{"text":"yo","sender":{"screen_name":"bo"},"ts":1}])
SELECT d.text, d.sender.screen_name AS from_name
FROM dms AS d
ORDER BY d.ts
-- → {"text":"yo","from_name":"bo"} then {"text":"hi","from_name":"amy"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT col->>'text', col#>>'{sender,screen_name}' FROM dms ORDER BY col->>'ts'` |
| **DuckDB** | `SELECT text, sender.screen_name AS from_name FROM dms ORDER BY ts` |
| **JavaScript** | `dms.map(d => ({text: d.text, from: d.sender.screen_name})).sort((a,b)=>a.ts-b.ts)` |
| **Python** | `sorted(({"text": d["text"], "from": d["sender"]["screen_name"]} for d in dms), key=…)` |
| **MongoDB** | `[{$project: {text: 1, from: "$sender.screen_name"}}, {$sort: {ts: 1}}]` |
| **jq** | `[.[] \| {text, from: .sender.screen_name}] \| sort_by(.ts)` |

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
-- → {"everyone":[{"n":"a"},{"formerly":true,"n":"b"}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT (col->'team') \|\| (SELECT JSONB_AGG(m \|\| '{"formerly":true}') FROM JSONB_ARRAY_ELEMENTS(col->'formerly') m)` |
| **DuckDB** | `SELECT LIST_CONCAT(team, [STRUCT_INSERT(m, formerly := true) for m in formerly])` |
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
| **SQL (Postgres)** | `SELECT CASE WHEN 5 > 3 THEN 'big' ELSE 'small' END` |
| **DuckDB** | `SELECT IF(5 > 3, 'big', 'small')` |
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
-- → {"v":"default"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT COALESCE(col->>'foo', 'default')` |
| **DuckDB** | `SELECT COALESCE(doc.foo, 'default')` |
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
vals = [{"a": 1}, "str"]
```
```sql
WITH vals AS ([{"a":1},"str"])
SELECT CASE WHEN TYPE(x) = "object" THEN x.a END AS a
FROM vals AS x
-- → {"a":1} then {}   (the string has no .a → MISSING)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **JavaScript** | `try { x.a } catch { null }` |
| **Python** | `x.get("a") if isinstance(x, dict) else None` |
| **MongoDB** | `{$cond: [{$eq: [{$type: "$x"}, "object"]}, "$x.a", null]}` |
| **jq** | `try .a catch null` |

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

