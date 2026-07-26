# SQL++ recipes — a SQL++ / SQL / jq Rosetta stone

_Slicing and dicing JSON: the same maneuver across seven tools._

> **This file is generated** from `docs/recipes.toml` by `docs/build_recipes.py`.
> Edit the `.toml`, not this `.md`. An interactive HTML version with toggleable
> dialect columns lives at `docs/recipes.html`.

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
n1k1 -c 'SELECT * FROM customers LIMIT 1' examples/shop
# {"type":"customer","id":"dave","name":"Dave Kim","city":"Austin","since":2020}
```

## The three mental models

| | SQL++ | SQL | jq |
|---|---|---|---|
| **Unit of work** | a collection of documents | rows in a table | one JSON value, streamed |
| **JSON is** | the native value type | a `json`/`jsonb` column | the whole world |
| **"for each element"** | `FROM arr AS x` / `UNNEST` | `jsonb_array_elements(col)` | `.[]` |
| **transform each** | `SELECT f …` / `ARRAY f FOR x IN … END` | `SELECT f FROM …` | `map(f)` |
| **keep some** | `WHERE cond` | `WHERE cond` | `select(cond)` |
| **build an object** | `{"a": x}` | `jsonb_build_object('a', x)` | `{a: .x}` |
| **object ↔ pairs** | `OBJECT_PAIRS` / `OBJECT … FOR … END` | `jsonb_each` / `jsonb_object_agg` | `to_entries`/`from_entries` |

**SQL++ and SQL treat every task as a query over a collection; jq treats it as a
stream rewrite.** DuckDB, JavaScript, Python, and MongoDB each land somewhere on
that spectrum — DuckDB is SQL with list comprehensions, JS/Python are imperative
map/filter, MongoDB is a document pipeline (its twin of jq's pipe *and* SQL's
`GROUP BY`). What makes SQL++ special is that it has jq's JSON-surgery verbs
(`ARRAY … FOR`, `OBJECT … FOR`, `WITHIN`, `UNNEST`, `OBJECT_PAIRS`) *inside* a set
query — so you rarely have to choose.

## 1. Access & navigation

### Field access — .foo, .foo.bar
```sql
SELECT RAW o.customer FROM orders o
-- → "dave", "alice", … (one per order)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT col->>'customer' FROM orders` |
| **DuckDB** | `SELECT customer FROM orders` |
| **JavaScript** | `orders.map(o => o.customer)` |
| **Python** | `[o["customer"] for o in orders]` |
| **MongoDB** | `db.orders.find({}, {customer: 1, _id: 0})` |
| **jq** | `.customer` |

</details>

### Optional / missing field
jq's ? suppresses errors on non-objects; SQL++ has no error — a missing path is MISSING and just drops out.
```sql
SELECT x.foo FROM [{"bar":1}] AS x
-- → {}   (foo is MISSING, so the key is omitted)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT col->'foo'      -- NULL if absent` |
| **DuckDB** | `SELECT col->'$.foo'  -- NULL if absent` |
| **JavaScript** | `[{bar:1}].map(x => x.foo)      // [undefined]` |
| **Python** | `[x.get("foo") for x in [{"bar":1}]]   # [None]` |
| **MongoDB** | `// a missing field is simply absent from the result` |
| **jq** | `.foo      # → null   (.foo? to suppress errors on non-objects)` |

</details>

### Array index — 0-based, negatives from the end
```sql
SELECT [10,20,30][0]  AS `first`,
       [10,20,30][-1] AS `last`,
       [10,20,30][-2] AS penult
-- → {"first":10,"last":30,"penult":20}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `col->0, col->-1        -- jsonb: 0-based, -1 = last` |
| **DuckDB** | `[10,20,30][1], [10,20,30][-1]   -- ⚠ DuckDB lists are 1-based` |
| **JavaScript** | `a[0]; a.at(-1)` |
| **Python** | `a[0]; a[-1]` |
| **MongoDB** | `{$arrayElemAt: ["$a", 0]}, {$arrayElemAt: ["$a", -1]}` |
| **jq** | `.[0], .[-1]` |

</details>

### Slice — arr[2:4]
```sql
SELECT ["a","b","c","d","e"][2:4] AS s
-- → {"s":["c","d"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_path_query(col, '$[2 to 3]')   -- SQLite has no slice` |
| **DuckDB** | `['a','b','c','d','e'][3:4]   -- ⚠ 1-based, inclusive → ['c','d']` |
| **JavaScript** | `a.slice(2, 4)` |
| **Python** | `a[2:4]` |
| **MongoDB** | `{$slice: ["$a", 2, 2]}` |
| **jq** | `.[2:4]` |

</details>

## 2. Iterating & projecting

The pivot between the models: SQL++/SQL/DuckDB scan a collection, JS/Python loop, MongoDB pipelines, jq streams.

### Iterate — one output per element
```sql
SELECT RAW x FROM [{"name":"JSON"},{"name":"XML"}] AS x
-- → {"name":"JSON"} then {"name":"XML"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT value FROM jsonb_array_elements(col)` |
| **DuckDB** | `SELECT unnest([{'name':'JSON'},{'name':'XML'}], recursive := false)` |
| **JavaScript** | `for (const x of arr) { … }` |
| **Python** | `for x in arr: ...` |
| **MongoDB** | `db.c.find()      // each document` |
| **jq** | `.[]` |

</details>

### Project a field from each element
```sql
SELECT RAW x.name FROM [{"name":"JSON"},{"name":"XML"}] AS x
-- → "JSON", "XML"
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT e->>'name' FROM jsonb_array_elements(col) e` |
| **DuckDB** | `SELECT x.name FROM (SELECT unnest(arr) x)` |
| **JavaScript** | `arr.map(x => x.name)` |
| **Python** | `[x["name"] for x in arr]` |
| **MongoDB** | `db.c.find({}, {name: 1, _id: 0})` |
| **jq** | `.[] \| .name` |

</details>

### map — transform each element into an array
SQL++ array comprehension ARRAY f FOR x IN arr END returns one array value; a SELECT scans instead.
```sql
SELECT ARRAY v+1 FOR v IN [1,2,3] END AS r
-- → {"r":[2,3,4]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_agg(value::int + 1) FROM jsonb_array_elements('[1,2,3]')` |
| **DuckDB** | `[x + 1 for x in [1,2,3]]      -- list comprehension` |
| **JavaScript** | `[1,2,3].map(x => x + 1)` |
| **Python** | `[x + 1 for x in [1,2,3]]` |
| **MongoDB** | `{$map: {input: [1,2,3], as: "v", in: {$add: ["$$v", 1]}}}` |
| **jq** | `map(.+1)` |

</details>

### Collect elements into an array — [ … ]
```sql
SELECT ARRAY_PREPEND(o.user, o.projects) AS r
FROM [{"user":"s","projects":["jq","wf"]}] AS o
-- → {"r":["s","jq","wf"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `col->'user' \|\| col->'projects'    -- jsonb \|\| concatenates` |
| **DuckDB** | `list_prepend(user, projects)` |
| **JavaScript** | `[o.user, ...o.projects]` |
| **Python** | `[o["user"], *o["projects"]]` |
| **MongoDB** | `{$concatArrays: [["$user"], "$projects"]}` |
| **jq** | `[.user, .projects[]]` |

</details>

### Object construction — { }
Keys inside a constructed object render sorted (canonical JSON): title before user.
```sql
SELECT {"user": o.user, "title": o.title} AS doc
FROM [{"user":"s","title":"JQ"}] AS o
-- → {"doc":{"title":"JQ","user":"s"}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_build_object('user', col->'user', 'title', col->'title')` |
| **DuckDB** | `{'user': o.user, 'title': o.title}      -- a STRUCT` |
| **JavaScript** | `({ user: o.user, title: o.title })` |
| **Python** | `{"user": o["user"], "title": o["title"]}` |
| **MongoDB** | `{$project: {user: 1, title: 1, _id: 0}}` |
| **jq** | `{user, title}` |

</details>

### Dynamic keys — key comes from the data
Object comprehension with a dynamic key expression. ⚠ Keys must be strings; `value` is a reserved word (back-quoted).
```sql
SELECT OBJECT r.label : r.`value` FOR r IN [{"label":"a","value":1}] END AS o
-- → {"o":{"a":1}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_object_agg(e->>'label', e->'value')` |
| **DuckDB** | `map_from_entries([{'k': r.label, 'v': r.value}])` |
| **JavaScript** | `Object.fromEntries(arr.map(r => [r.label, r.value]))` |
| **Python** | `{r["label"]: r["value"] for r in arr}` |
| **MongoDB** | `{$arrayToObject: [[{k: "$label", v: "$value"}]]}` |
| **jq** | `map({(.label): .value}) \| add` |

</details>

## 3. Filtering & selecting

### Keep elements matching a condition
```sql
SELECT RAW v FROM [1,5,3,0,7] AS v WHERE v >= 2
-- → 5, 3, 7
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT value FROM jsonb_array_elements('[1,5,3,0,7]') WHERE value::int >= 2` |
| **DuckDB** | `[x for x in [1,5,3,0,7] if x >= 2]` |
| **JavaScript** | `[1,5,3,0,7].filter(x => x >= 2)` |
| **Python** | `[x for x in [1,5,3,0,7] if x >= 2]` |
| **MongoDB** | `{$filter: {input: [1,5,3,0,7], cond: {$gte: ["$$this", 2]}}}` |
| **jq** | `map(select(. >= 2))` |

</details>

### has / missing a key
```sql
SELECT x.endpoint IS NOT MISSING AS has FROM [{"endpoint":1},{}] AS x
-- → {"has":true} then {"has":false}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `col ? 'endpoint'          -- jsonb key-exists` |
| **DuckDB** | `json_exists(col, '$.endpoint')` |
| **JavaScript** | `"endpoint" in x` |
| **Python** | `"endpoint" in x` |
| **MongoDB** | `{endpoint: {$exists: true}}` |
| **jq** | `has("endpoint")` |

</details>

### contains / startswith — substring & prefix
```sql
SELECT ARRAY s LIKE "foo%" FOR s IN ["fo","foo","foobar"] END AS r
-- → {"r":[false,true,true]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `value LIKE 'foo%'` |
| **DuckDB** | `starts_with(s, 'foo')` |
| **JavaScript** | `s.startsWith("foo")` |
| **Python** | `s.startswith("foo")` |
| **MongoDB** | `{$regexMatch: {input: "$s", regex: "^foo"}}` |
| **jq** | `startswith("foo")` |

</details>

### regex match
```sql
SELECT REGEXP_CONTAINS("foo123", "[0-9]+") AS m
-- → {"m":true}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `'foo123' ~ '[0-9]+'` |
| **DuckDB** | `regexp_matches('foo123', '[0-9]+')` |
| **JavaScript** | `/[0-9]+/.test("foo123")` |
| **Python** | `bool(re.search(r"[0-9]+", "foo123"))` |
| **MongoDB** | `{$regexMatch: {input: "foo123", regex: "[0-9]+"}}` |
| **jq** | `test("[0-9]+")` |

</details>

## 4. Objects ↔ entries (keys, pairs, pivots)

### keys — list an object's field names
```sql
SELECT OBJECT_NAMES({"b":2,"a":1}) AS k
-- → {"k":["a","b"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_object_keys(col)      -- one row per key` |
| **DuckDB** | `json_keys('{"b":2,"a":1}')` |
| **JavaScript** | `Object.keys({b:2, a:1})` |
| **Python** | `list({"b":2,"a":1}.keys())` |
| **MongoDB** | `{$map: {input: {$objectToArray: "$obj"}, in: "$$this.k"}}` |
| **jq** | `keys      # sorted` |

</details>

### to_entries — object → array of pairs
⚠ SQL++ OBJECT_PAIRS yields {name, val}; jq yields {key, value}.
```sql
SELECT OBJECT_PAIRS({"a":1,"b":2}) AS p
-- → {"p":[{"name":"a","val":1},{"name":"b","val":2}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_each(col)      -- rows (key, value)` |
| **DuckDB** | `map_entries(MAP {'a':1, 'b':2})   -- [{key,value}]` |
| **JavaScript** | `Object.entries({a:1, b:2})      // [["a",1],["b",2]]` |
| **Python** | `list({"a":1,"b":2}.items())` |
| **MongoDB** | `{$objectToArray: "$obj"}      // [{k,v}]` |
| **jq** | `to_entries      # [{key,value}]` |

</details>

### from_entries — pairs → object
```sql
SELECT OBJECT p.name : p.val FOR p IN [{"name":"a","val":1}] END AS o
-- → {"o":{"a":1}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_object_agg(e->>'key', e->'value')` |
| **DuckDB** | `map_from_entries([{'k':'a', 'v':1}])` |
| **JavaScript** | `Object.fromEntries([["a", 1]])` |
| **Python** | `dict([("a", 1)])` |
| **MongoDB** | `{$arrayToObject: [[{k: "a", v: 1}]]}` |
| **jq** | `from_entries` |

</details>

### Swap keys and values
```sql
SELECT OBJECT TO_STRING(p.val) : p.name FOR p IN OBJECT_PAIRS({"a":1,"b":2}) END AS o
-- → {"o":{"1":"a","2":"b"}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_object_agg(v#>>'{}', k) FROM jsonb_each(obj) e(k,v)` |
| **DuckDB** | `map_from_entries([{'k': e.value::VARCHAR, 'v': e.key} for e in map_entries(m)])` |
| **JavaScript** | `Object.fromEntries(Object.entries(o).map(([k,v]) => [v, k]))` |
| **Python** | `{str(v): k for k, v in o.items()}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: {$objectToArray:"$o"}, in: {k: {$toString:"$$this.v"}, v: "$$this.k"}}}}` |
| **jq** | `to_entries \| map({(.value): .key}) \| add` |

</details>

### map_values — transform every value, keep keys
```sql
SELECT OBJECT p.name : p.val+1 FOR p IN OBJECT_PAIRS({"a":1,"b":2}) END AS o
-- → {"o":{"a":2,"b":3}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_object_agg(k, (v::int)+1) FROM jsonb_each(obj) e(k,v)` |
| **DuckDB** | `map_from_entries([{'k': e.key, 'v': e.value + 1} for e in map_entries(m)])` |
| **JavaScript** | `Object.fromEntries(Object.entries(o).map(([k,v]) => [k, v+1]))` |
| **Python** | `{k: v + 1 for k, v in o.items()}` |
| **MongoDB** | `{$arrayToObject: {$map: {input: {$objectToArray:"$o"}, in: {k: "$$this.k", v: {$add:["$$this.v",1]}}}}}` |
| **jq** | `map_values(.+1)` |

</details>

### Add / remove a field
```sql
SELECT OBJECT_ADD({"a":1}, "draft", true)          AS added,
       OBJECT_REMOVE({"title":"x","a":1}, "title")   AS removed
-- → {"added":{"a":1,"draft":true},"removed":{"a":1}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `col \|\| '{"draft":true}'   \|   col - 'title'` |
| **DuckDB** | `struct_insert(s, draft := true)   \|   s.* EXCLUDE (title)` |
| **JavaScript** | `({...o, draft:true})   \|   (({title, ...rest}) => rest)(o)` |
| **Python** | `{**o, "draft":True}   \|   {k:v for k,v in o.items() if k != "title"}` |
| **MongoDB** | `{$mergeObjects:["$$ROOT",{draft:true}]}   \|   {$unset:"title"}` |
| **jq** | `. + {draft:true}   \|   del(.title)` |

</details>

### Pivot: object → array of objects (key becomes a field)
```sql
SELECT ARRAY OBJECT_ADD(p.val, "slug", p.name) FOR p IN OBJECT_PAIRS({"x":{"n":1}}) END AS a
-- → {"a":[{"n":1,"slug":"x"}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_agg(v \|\| jsonb_build_object('slug', k))` |
| **DuckDB** | `[struct_insert(e.value, slug := e.key) for e in map_entries(m)]` |
| **JavaScript** | `Object.entries(o).map(([k,v]) => ({...v, slug:k}))` |
| **Python** | `[{**v, "slug":k} for k, v in o.items()]` |
| **MongoDB** | `{$map: {input: {$objectToArray:"$o"}, in: {$mergeObjects:["$$this.v",{slug:"$$this.k"}]}}}` |
| **jq** | `to_entries \| map(.value + {slug:.key})` |

</details>

### Pivot: array of objects → object keyed by a field
```sql
SELECT OBJECT r.slug : r FOR r IN [{"slug":"x","n":1}] END AS o
-- → {"o":{"x":{"slug":"x","n":1}}}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_object_agg(e->>'slug', e)` |
| **DuckDB** | `map_from_entries([{'k': r.slug, 'v': r} for r in arr])` |
| **JavaScript** | `Object.fromEntries(arr.map(r => [r.slug, r]))` |
| **Python** | `{r["slug"]: r for r in arr}` |
| **MongoDB** | `{$arrayToObject: {$map: {input:"$arr", in: {k:"$$this.slug", v:"$$this"}}}}` |
| **jq** | `map({(.slug): .}) \| add` |

</details>

## 5. Aggregating & grouping

SQL's home turf — GROUP BY and aggregates — meets jq's group_by / unique / add / max_by. MongoDB's aggregation pipeline is the natural fit here.

### length / count
```sql
SELECT ARRAY_LENGTH([1,2,3]) AS arr, LENGTH("abc") AS str, OBJECT_LENGTH({"a":1}) AS obj
-- → {"arr":3,"str":3,"obj":1}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_array_length('[1,2,3]')` |
| **DuckDB** | `len([1,2,3]); length('abc')` |
| **JavaScript** | `[1,2,3].length; "abc".length; Object.keys(o).length` |
| **Python** | `len([1,2,3]); len("abc"); len(o)` |
| **MongoDB** | `{$size:"$arr"}; {$strLenCP:"$s"}` |
| **jq** | `length` |

</details>

### add / sum / min / max / avg over an array
```sql
SELECT ARRAY_SUM([1,2,3]) AS s, ARRAY_MAX([1,2,3]) AS mx, ARRAY_AVG([1,2,3]) AS a
-- → {"s":6,"mx":3,"a":2}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `sum(value::int) FROM jsonb_array_elements('[1,2,3]')` |
| **DuckDB** | `list_sum([1,2,3]); list_max([1,2,3]); list_avg([1,2,3])` |
| **JavaScript** | `arr.reduce((a,b) => a+b, 0); Math.max(...arr)` |
| **Python** | `sum(arr); max(arr); statistics.mean(arr)` |
| **MongoDB** | `{$sum:"$a"}; {$max:"$a"}; {$avg:"$a"}` |
| **jq** | `add; max; (add/length)` |

</details>

### Aggregate over a collection
```sql
SELECT COUNT(*) AS n, ROUND(SUM(o.total),2) AS revenue, ROUND(AVG(o.total),2) AS avg
FROM orders o
-- → {"n":20,"revenue":1949.36,"avg":97.47}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT count(*), sum(total), avg(total) FROM orders` |
| **DuckDB** | `SELECT count(*), sum(total), avg(total) FROM orders` |
| **JavaScript** | `orders.reduce((a,o) => a + o.total, 0)` |
| **Python** | `sum(o["total"] for o in orders)` |
| **MongoDB** | `db.orders.aggregate([{$group:{_id:null, n:{$sum:1}, revenue:{$sum:"$total"}, avg:{$avg:"$total"}}}])` |
| **jq** | `[.[].total] \| add` |

</details>

### group_by — cluster, then aggregate
```sql
SELECT o.status, COUNT(*) AS n, ROUND(SUM(o.total),2) AS revenue
FROM orders o GROUP BY o.status ORDER BY revenue DESC
-- → {"status":"shipped","n":16,"revenue":1758.73}, {"status":"pending",…}, {"status":"cancelled",…}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT status, count(*), sum(total) FROM orders GROUP BY status ORDER BY 3 DESC` |
| **DuckDB** | `SELECT status, count(*), sum(total) FROM orders GROUP BY status ORDER BY 3 DESC` |
| **JavaScript** | `Object.groupBy(orders, o => o.status)      // then reduce each bucket` |
| **Python** | `df.groupby("status").total.agg(["count","sum"])      # pandas` |
| **MongoDB** | `db.orders.aggregate([{$group:{_id:"$status", n:{$sum:1}, revenue:{$sum:"$total"}}}, {$sort:{revenue:-1}}])` |
| **jq** | `group_by(.status)      # → array of arrays` |

</details>

### Collect each group's members
```sql
SELECT o.customer, ARRAY_AGG(o.id) AS order_ids FROM orders o GROUP BY o.customer
-- → one row per customer, e.g. {"customer":"dave","order_ids":["1005","1009","1013","1017"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT customer, jsonb_agg(id) FROM orders GROUP BY customer` |
| **DuckDB** | `SELECT customer, list(id) FROM orders GROUP BY customer` |
| **JavaScript** | `Object.groupBy(orders, o => o.customer)      // then map ids` |
| **Python** | `{c: [o["id"] for o in g] for c, g in groupby(sorted(orders,key=k), k)}` |
| **MongoDB** | `db.orders.aggregate([{$group:{_id:"$customer", order_ids:{$push:"$id"}}}])` |
| **jq** | `group_by(.customer) \| map({customer:.[0].customer, ids:map(.id)})` |

</details>

### unique / dedup
ARRAY_DISTINCT keeps first-seen order; wrap in ARRAY_SORT for jq's sorted result.
```sql
SELECT ARRAY_DISTINCT([1,2,5,3,5,3,1]) AS u
-- → {"u":[1,2,5,3]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT DISTINCT value FROM jsonb_array_elements('[…]')` |
| **DuckDB** | `list_distinct([1,2,5,3,5,3,1])` |
| **JavaScript** | `[...new Set([1,2,5,3,5,3,1])]` |
| **Python** | `list(dict.fromkeys([1,2,5,3,5,3,1]))` |
| **MongoDB** | `{$setUnion: ["$a", []]}      // unordered` |
| **jq** | `unique      # sorted` |

</details>

### sort_by
```sql
SELECT o.id, o.total FROM orders o ORDER BY o.total DESC LIMIT 3
-- → {"id":"1020","total":389.99}, {"id":"1019","total":245.0}, {"id":"1003","total":210.00}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT id, total FROM orders ORDER BY total DESC` |
| **DuckDB** | `SELECT id, total FROM orders ORDER BY total DESC` |
| **JavaScript** | `orders.toSorted((a,b) => b.total - a.total)` |
| **Python** | `sorted(orders, key=lambda o: -o["total"])` |
| **MongoDB** | `db.orders.aggregate([{$sort:{total:-1}}])` |
| **jq** | `sort_by(.total)` |

</details>

### min_by / max_by — the extreme record
```sql
SELECT o.* FROM orders o ORDER BY o.total DESC LIMIT 1
-- → the single priciest order (whole row)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT * FROM orders ORDER BY total DESC LIMIT 1` |
| **DuckDB** | `SELECT max_by(orders, total) FROM orders` |
| **JavaScript** | `orders.reduce((m,o) => o.total > m.total ? o : m)` |
| **Python** | `max(orders, key=lambda o: o["total"])` |
| **MongoDB** | `db.orders.aggregate([{$sort:{total:-1}}, {$limit:1}])` |
| **jq** | `max_by(.total)` |

</details>

### Count occurrences / histogram
```sql
SELECT v AS `value`, COUNT(*) AS n FROM ["a","b","a"] AS v GROUP BY v
-- → {"value":"a","n":2}, {"value":"b","n":1}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT value, count(*) FROM jsonb_array_elements_text('["a","b","a"]') GROUP BY value` |
| **DuckDB** | `SELECT v, count(*) FROM (SELECT unnest(['a','b','a']) v) GROUP BY v` |
| **JavaScript** | `arr.reduce((m,x) => (m[x]=(m[x]\|\|0)+1, m), {})` |
| **Python** | `collections.Counter(["a","b","a"])` |
| **MongoDB** | `db.c.aggregate([{$unwind:"$v"}, {$group:{_id:"$v", n:{$sum:1}}}])` |
| **jq** | `reduce .[] as $x ({}; .[$x] += 1)` |

</details>

### Find duplicates by key
```sql
SELECT o.id, COUNT(*) AS n FROM orders o GROUP BY o.id HAVING COUNT(*) > 1
-- → (empty for shop — ids are unique)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT id, count(*) FROM orders GROUP BY id HAVING count(*) > 1` |
| **DuckDB** | `SELECT id, count(*) FROM orders GROUP BY id HAVING count(*) > 1` |
| **JavaScript** | `// group by id, then keep buckets with length > 1` |
| **Python** | `[k for k, c in Counter(o["id"] for o in orders).items() if c > 1]` |
| **MongoDB** | `db.orders.aggregate([{$group:{_id:"$id", n:{$sum:1}}}, {$match:{n:{$gt:1}}}])` |
| **jq** | `reduce .[].id as $x ({}; .[$x]+=1) \| to_entries[] \| select(.value>1)` |

</details>

## 6. Arrays

### flatten
⚠ SQL++ ARRAY_FLATTEN(arr, depth) needs an explicit depth; jq flattens fully.
```sql
SELECT ARRAY_FLATTEN([1,[2],[[3]]], 2) AS f
-- → {"f":[1,2,3]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `-- recursive unnest; no single builtin` |
| **DuckDB** | `flatten([[1],[2],[3]])      -- one level only` |
| **JavaScript** | `[1,[2],[[3]]].flat(2)` |
| **Python** | `# no deep builtin; recurse, or itertools.chain for one level` |
| **MongoDB** | `{$reduce:{input:"$a", initialValue:[], in:{$concatArrays:["$$value","$$this"]}}}   // one level` |
| **jq** | `flatten` |

</details>

### reverse / range / append / concat
```sql
SELECT ARRAY_REVERSE([1,2,3]) AS rev, ARRAY_RANGE(2,4) AS rng,
       ARRAY_APPEND([1,2],3) AS app, ARRAY_CONCAT([1],[2]) AS cat
-- → {"rev":[3,2,1],"rng":[2,3],"app":[1,2,3],"cat":[1,2]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `a \|\| b; generate_series(2,3)` |
| **DuckDB** | `list_reverse(a); range(2,4); list_append(a,x); list_concat(a,b)` |
| **JavaScript** | `a.toReversed(); [...Array(2).keys()].map(i=>i+2); [...a,x]; a.concat(b)` |
| **Python** | `a[::-1]; list(range(2,4)); a+[x]; a+b` |
| **MongoDB** | `{$reverseArray:"$a"}; {$range:[2,4]}; {$concatArrays:["$a",[x]]}` |
| **jq** | `reverse; [range(2;4)]; . + [x]; .a + .b` |

</details>

### Every other element (even indices)
Index by a stepped range — OBJECT_PAIRS works only on objects, not arrays.
```sql
SELECT ARRAY a[i] FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(a), 2) END AS evens
FROM [["a","b","c","d"]] AS a
-- → {"evens":["a","c"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_agg(value) … WITH ORDINALITY t(value,i) WHERE i%2 = 1` |
| **DuckDB** | `list_slice(['a','b','c','d'], 1, 4, 2)      -- 1-based, step 2` |
| **JavaScript** | `arr.filter((_, i) => i % 2 === 0)` |
| **Python** | `arr[::2]` |
| **MongoDB** | `{$map:{input:{$range:[0,{$size:"$a"},2]}, in:{$arrayElemAt:["$a","$$this"]}}}` |
| **jq** | `[range(0; length; 2) as $i \| .[$i]]` |

</details>

### Chunk into fixed-size groups
⚠ Clamp the slice end with LEAST(…, ARRAY_LENGTH(a)) — a slice past the end is MISSING, dropping the final short chunk.
```sql
SELECT ARRAY a[i:LEAST(i+2, ARRAY_LENGTH(a))]
       FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(a), 2) END AS chunks
FROM [[1,2,3,4,5]] AS a
-- → {"chunks":[[1,2],[3,4],[5]]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `-- window / ntile, or generate_series with slicing` |
| **DuckDB** | `[a[i+1 : i+2] for i in range(0, len(a), 2)]` |
| **JavaScript** | `Array.from({length: Math.ceil(a.length/2)}, (_,i) => a.slice(i*2, i*2+2))` |
| **Python** | `[a[i:i+2] for i in range(0, len(a), 2)]` |
| **jq** | `[range(0; length; 2) as $i \| .[$i:$i+2]]` |

</details>

## 7. Strings & numbers

### split / join
```sql
SELECT SPLIT("a,b,c", ",") AS parts, CONCAT2("-", ["a","b","c"]) AS joined
-- → {"parts":["a","b","c"],"joined":"a-b-c"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `string_to_array('a,b,c', ','); array_to_string(a, '-')` |
| **DuckDB** | `string_split('a,b,c', ','); array_to_string(['a','b','c'], '-')` |
| **JavaScript** | `"a,b,c".split(","); ["a","b","c"].join("-")` |
| **Python** | `"a,b,c".split(","); "-".join(["a","b","c"])` |
| **MongoDB** | `{$split:["a,b,c", ","]}; {$reduce:…}   // no direct join` |
| **jq** | `split(","); join("-")` |

</details>

### upcase / trim-prefix / titlecase
```sql
SELECT UPPER("abc") AS up, LTRIM("foobar","foo") AS trimmed, TITLE("hi there") AS titled
-- → {"up":"ABC","trimmed":"bar","titled":"Hi There"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `upper(s); ltrim(s, 'foo'); initcap(s)` |
| **DuckDB** | `upper(s); ltrim(s, 'foo')` |
| **JavaScript** | `s.toUpperCase(); s.replace(/^foo/, "")` |
| **Python** | `s.upper(); s.removeprefix("foo"); s.title()` |
| **MongoDB** | `{$toUpper:"$s"}; {$ltrim:{input:"$s", chars:"foo"}}` |
| **jq** | `ascii_upcase; ltrimstr("foo")` |

</details>

### String interpolation
```sql
SELECT "total is " || TO_STRING(1+2) AS msg
-- → {"msg":"total is 3"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `'total is ' \|\| (1+2)::text` |
| **DuckDB** | `printf('total is %d', 1+2)` |
| **JavaScript** | ``total is ${1+2}`` |
| **Python** | `f"total is {1+2}"` |
| **MongoDB** | `{$concat:["total is ", {$toString:{$add:[1,2]}}]}` |
| **jq** | `"total is \(.+1)"` |

</details>

### index of a substring
⚠ SQL++ POSITION is 0-based; Postgres/DuckDB are 1-based.
```sql
SELECT POSITION("a, b", ", ") AS i
-- → {"i":1}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `position(', ' in 'a, b') - 1` |
| **DuckDB** | `strpos('a, b', ', ') - 1` |
| **JavaScript** | `"a, b".indexOf(", ")` |
| **Python** | `"a, b".find(", ")` |
| **MongoDB** | `{$indexOfCP:["a, b", ", "]}` |
| **jq** | `index(", ")` |

</details>

### Numeric — floor / round / sqrt / integer-divide
```sql
SELECT FLOOR(3.7) AS fl, ROUND(3.14159,2) AS rnd, SQRT(9) AS sq, IDIV(7,2) AS idiv
-- → {"fl":3,"rnd":3.14,"sq":3,"idiv":3}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `floor(x); round(x,2); sqrt(x); div(7,2)` |
| **DuckDB** | `floor(x); round(x,2); sqrt(x); 7 // 2` |
| **JavaScript** | `Math.floor(x); +x.toFixed(2); Math.sqrt(x); Math.trunc(7/2)` |
| **Python** | `math.floor(x); round(x,2); math.sqrt(x); 7 // 2` |
| **MongoDB** | `{$floor:"$x"}; {$round:["$x",2]}; {$sqrt:"$x"}; {$trunc:{$divide:[7,2]}}` |
| **jq** | `floor; sqrt; (7/2 \| floor)` |

</details>

### type — name the JSON type
⚠ SQL++ TYPE() is lowercase and distinguishes "missing" from "null".
```sql
SELECT ARRAY TYPE(v) FOR v IN [0,false,[],{},null,"x"] END AS t
-- → {"t":["number","boolean","array","object","null","string"]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_typeof(value)` |
| **DuckDB** | `typeof(v); json_type(j)` |
| **JavaScript** | `typeof x; Array.isArray(x)` |
| **Python** | `type(x).__name__` |
| **MongoDB** | `{$type:"$x"}` |
| **jq** | `type      # (jq has no "missing")` |

</details>

## 8. Recursion & deep search

SQL++'s answer to jq's .. is the WITHIN operator (descend into any nested value) plus ANY … WITHIN … SATISFIES. Postgres uses jsonpath ($..). DuckDB/JS/Python/Mongo have no clean one-liner — you recurse.

### Recurse into every descendant — ..
```sql
SELECT ARRAY v FOR v WITHIN {"a":0,"b":[1]} END AS descendants
-- → {"descendants":[0,[1],1]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_path_query(col, '$..*')` |
| **JavaScript** | `function* walk(x){ yield x; if (x && typeof x=="object") for (const v of Object.values(x)) yield* walk(v) }` |
| **Python** | `# recursive generator over dict/list values` |
| **jq** | `..` |

</details>

### Does value X appear anywhere in the tree?
```sql
SELECT ANY v WITHIN {"a":{"b":5}} SATISFIES v = 5 END AS found
-- → {"found":true}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_path_exists(col, '$..* ? (@ == 5)')` |
| **JavaScript** | `[...walk(o)].includes(5)` |
| **Python** | `any(v == 5 for v in walk(o))` |
| **jq** | `[.. \| select(. == 5)] \| length > 0` |

</details>

### Find every object with a given id, at any depth
```sql
SELECT ARRAY v FOR v WITHIN {"a":{"id":"x"},"b":[{"id":"y"}]} WHEN v.id = "y" END AS hits
-- → {"hits":[{"id":"y"}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `jsonb_path_query(col, '$..* ? (@.id == "y")')` |
| **JavaScript** | `[...walk(o)].filter(v => v && v.id === "y")` |
| **Python** | `[v for v in walk(o) if isinstance(v, dict) and v.get("id") == "y"]` |
| **jq** | `[.. \| objects \| select(.id == "y")]` |

</details>

## 9. Reshaping records

### UNNEST — explode a nested array into rows
The workhorse for line-items, tags, events. SQL++ UNNEST pairs each element with its parent's fields.
```sql
SELECT o.id, t AS tag FROM [{"id":1,"tags":["a","b"]}] AS o UNNEST o.tags AS t
-- → {"id":1,"tag":"a"} then {"id":1,"tag":"b"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `… , jsonb_array_elements_text(o->'tags') t      -- lateral cross join` |
| **DuckDB** | `SELECT id, unnest(tags) AS tag FROM t` |
| **JavaScript** | `arr.flatMap(o => o.tags.map(t => ({id: o.id, tag: t})))` |
| **Python** | `[{"id":o["id"], "tag":t} for o in arr for t in o["tags"]]` |
| **MongoDB** | `db.c.aggregate([{$unwind:"$tags"}, {$project:{id:1, tag:"$tags"}}])` |
| **jq** | `.[] \| {id, tag: .tags[]}` |

</details>

### Array-of-rows (CSV-ish) → objects
```sql
SELECT r[0] AS name, r[1] AS url, TO_NUMBER(r[2]) AS category
FROM [["hdr","hdr","hdr"],["n","u","3"]][1:] AS r
-- → {"name":"n","url":"u","category":3}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `(r->>2)::int FROM jsonb_array_elements(col) WITH ORDINALITY t(r,i) WHERE i > 1` |
| **DuckDB** | `SELECT r[1] AS name, r[2] AS url, r[3]::int AS category FROM rows[2:]` |
| **JavaScript** | `rows.slice(1).map(r => ({name:r[0], url:r[1], category:+r[2]}))` |
| **Python** | `[{"name":r[0], "url":r[1], "category":int(r[2])} for r in rows[1:]]` |
| **MongoDB** | `—      // load via a driver / mongoimport` |
| **jq** | `.[1:] \| map({name:.[0], url:.[1], category:(.[2]\|tonumber)})` |

</details>

### Reshape + rename + sort
```sql
SELECT d.text, d.sender.screen_name AS from_name
FROM [{"text":"hi","sender":{"screen_name":"amy"},"ts":2},
      {"text":"yo","sender":{"screen_name":"bo"},"ts":1}] AS d
ORDER BY d.ts
-- → {"text":"yo","from_name":"bo"} then {"text":"hi","from_name":"amy"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `SELECT col->>'text', col#>>'{sender,screen_name}' FROM dms ORDER BY col->>'ts'` |
| **DuckDB** | `SELECT text, sender.screen_name AS from_name FROM dms ORDER BY ts` |
| **JavaScript** | `dms.map(d => ({text:d.text, from:d.sender.screen_name})).sort((a,b)=>a.ts-b.ts)` |
| **Python** | `sorted(({"text":d["text"], "from":d["sender"]["screen_name"]} for d in dms), key=…)` |
| **MongoDB** | `db.dms.aggregate([{$project:{text:1, from:"$sender.screen_name"}}, {$sort:{ts:1}}])` |
| **jq** | `[.[] \| {text, from:.sender.screen_name}] \| sort_by(.date)` |

</details>

### Merge two arrays, flagging one
```sql
SELECT ARRAY_CONCAT(d.team,
       ARRAY OBJECT_ADD(m,"formerly",true) FOR m IN d.formerly END) AS everyone
FROM [{"team":[{"n":"a"}],"formerly":[{"n":"b"}]}] AS d
-- → {"everyone":[{"n":"a"},{"formerly":true,"n":"b"}]}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `(col->'team') \|\| (SELECT jsonb_agg(m \|\| '{"formerly":true}') FROM jsonb_array_elements(col->'formerly') m)` |
| **DuckDB** | `list_concat(team, [struct_insert(m, formerly := true) for m in formerly])` |
| **JavaScript** | `[...d.team, ...d.formerly.map(m => ({...m, formerly:true}))]` |
| **Python** | `d["team"] + [{**m, "formerly":True} for m in d["formerly"]]` |
| **MongoDB** | `{$concatArrays:["$team", {$map:{input:"$formerly", in:{$mergeObjects:["$$this",{formerly:true}]}}}]}` |
| **jq** | `[.team, (.formerly \| map(.+{formerly:true}))] \| flatten` |

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
| **SQL (Postgres)** | `CASE WHEN 5 > 3 THEN 'big' ELSE 'small' END` |
| **DuckDB** | `if(5 > 3, 'big', 'small')` |
| **JavaScript** | `5 > 3 ? "big" : "small"` |
| **Python** | `"big" if 5 > 3 else "small"` |
| **MongoDB** | `{$cond: [{$gt:[5,3]}, "big", "small"]}` |
| **jq** | `if . > 3 then "big" else "small" end` |

</details>

### Default for a missing/null value
SQL++ splits what COALESCE and jq's // blur: IFMISSING / IFNULL / IFMISSINGORNULL treat absent and present-but-null differently.
```sql
SELECT IFMISSINGORNULL(x.foo, "default") AS v FROM [{}] AS x
-- → {"v":"default"}
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **SQL (Postgres)** | `COALESCE(col->>'foo', 'default')` |
| **DuckDB** | `COALESCE(x.foo, 'default')` |
| **JavaScript** | `x.foo ?? "default"` |
| **Python** | `x.get("foo", "default")` |
| **MongoDB** | `{$ifNull: ["$foo", "default"]}` |
| **jq** | `.foo // "default"` |

</details>

### Type guard (jq's try/catch)
jq guards type errors with try/catch; SQL++ has no type errors — a bad op yields NULL/MISSING — so guard with CASE.
```sql
SELECT CASE WHEN TYPE(x) = "object" THEN x.a END AS a FROM [{"a":1}, "str"] AS x
-- → {"a":1} then {}   (the string has no .a → MISSING)
```
<details><summary>other dialects</summary>

| | |
|---|---|
| **JavaScript** | `try { x.a } catch { null }` |
| **Python** | `x.get("a") if isinstance(x, dict) else None` |
| **MongoDB** | `{$cond:[{$eq:[{$type:"$x"},"object"]}, "$x.a", null]}` |
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
- **A subquery returns an array** — scalarize with `(SELECT RAW … )[0]`.

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
- `examples/README.md` — the shop/logs/metrics datasets.
- Couchbase N1QL function reference — n1k1 speaks the same dialect.

