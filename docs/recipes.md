# SQL++ recipes — a jq / SQL++ / SQL Rosetta stone

_Slicing and dicing JSON: the same maneuver in three tools._

If you already reach for **jq** to hack JSON on the command line, or you know
**SQL** and wish it spoke JSON natively, this guide translates between them and
**SQL++** (the N1QL dialect n1k1 runs). Every recipe below is shown three ways:

- **jq** — the streaming JSON filter (`jqlang.org`), one value in, a stream out.
- **SQL++** — n1k1's query language: JSON is the native value model, and you query
  *collections* of it (a keyspace, or a literal array).
- **SQL** — a relational engine with JSON functions. Examples use **PostgreSQL**
  `jsonb`; SQLite's `json1` equivalents are noted where they differ.

Every SQL++ snippet here was run against n1k1 and its output shown. Self-contained
ones use a literal array as the data source so you can paste them directly:

```sh
n1k1 -c 'SELECT ARRAY_SUM([1,2,3]) AS total'      # → {"total":6}
```

The ones that read `FROM orders` / `FROM customers` use the bundled shop dataset:

```sh
n1k1 -c 'SELECT * FROM orders LIMIT 1' examples/shop
# {"type":"order","id":"1005","customer":"dave","total":22.0,"items":1,"status":"shipped","ts":"2026-01-06"}
n1k1 -c 'SELECT * FROM customers LIMIT 1' examples/shop
# {"type":"customer","id":"dave","name":"Dave Kim","city":"Austin","since":2020}
```

---

## The three mental models

The tools solve the same problems from different starting points — internalizing
this makes every translation below obvious.

| | jq | SQL++ | SQL |
|---|---|---|---|
| **Unit of work** | one JSON value, streamed | a collection of documents | rows in a table |
| **JSON is** | the whole world | the native value type | a `json`/`jsonb` column |
| **"for each element"** | `.[]` (iterate) | `FROM arr AS x` / `UNNEST` | `jsonb_array_elements(col)` |
| **transform each** | `map(f)` | `SELECT f FROM …` or `ARRAY f FOR x IN … END` | `SELECT f FROM …` |
| **keep some** | `select(cond)` | `WHERE cond` | `WHERE cond` |
| **chain steps** | `\|` (pipe) | nested subqueries / `LET` | subqueries / CTEs |
| **build an object** | `{a: .x}` | `{"a": x}` | `jsonb_build_object('a', x)` |
| **object ↔ pairs** | `to_entries`/`from_entries` | `OBJECT_PAIRS` / `OBJECT … FOR … END` | `jsonb_each` / `jsonb_object_agg` |

The punchline: **jq treats every task as a stream rewrite; SQL++ and SQL treat it
as a query over a collection.** jq's `map`/`select`/`group_by` are pipeline stages;
in SQL++ and SQL they are `SELECT`/`WHERE`/`GROUP BY`. What makes SQL++ special is
that it *also* has jq's JSON-surgery verbs (`ARRAY … FOR`, `OBJECT … FOR`, `WITHIN`,
`UNNEST`, `OBJECT_PAIRS`) — so you rarely have to choose.

---

## 1. Access & navigation

### Identity — pass the value through
```
jq     .
SQL++  SELECT RAW x FROM [42] AS x          -- → 42
SQL    SELECT '42'::jsonb;
```

### Field access — `.foo`, `.foo.bar`
```
jq     .customer                 {"customer":"dave",…} → "dave"
SQL++  SELECT RAW o.customer FROM orders o
SQL    SELECT col->>'customer' FROM orders;         -- ->> text, -> jsonb
```
Nested: jq `.a.b`, SQL++ `x.a.b`, Postgres `col#>>'{a,b}'` (SQLite
`col->>'$.a.b'`).

### Optional / missing field — `.foo?`
jq's `?` suppresses "cannot index" errors. SQL++ has no such error: a missing path
is the value **`MISSING`**, which simply drops out of results (see §11).
```
jq     .foo?                     [1,2] → (empty; no error)
SQL++  SELECT x.foo FROM [[1,2]] AS x            -- → {} (foo is MISSING, omitted)
SQL    SELECT col->'foo';        -- NULL if absent
```

### Array index — `.[0]`, `.[-1]`
SQL++ indexes are **0-based**, and a negative index counts from the end (like jq).
```
jq     .[0]                      [{"name":"JSON"},{"name":"XML"}] → {"name":"JSON"}
SQL++  SELECT RAW a[0] FROM [[{"name":"JSON"},{"name":"XML"}]] AS a
SQL    SELECT col->0;            -- Postgres jsonb: 0-based, -1 = last
```
```
jq     .[-1]                     [10,20,30] → 30
SQL++  SELECT [10,20,30][-1] AS `last`, [10,20,30][-2] AS penult   -- {"last":30,"penult":20}
```
⚠ An out-of-range *positive* index is `MISSING`; a negative index **wraps** to the
end, so guard `i >= 0` when computing indices (as `examples/queries/life.sql++`
does for board edges).

### Slice — `.[2:4]`
```
jq     .[2:4]                    ["a","b","c","d","e"] → ["c","d"]
SQL++  SELECT ["a","b","c","d","e"][2:4] AS s        -- {"s":["c","d"]}
SQL    SELECT jsonb_path_query(col, '$[2 to 3]');    -- Postgres; SQLite has no slice
```

---

## 2. Iterating & projecting

### Iterate — `.[]` emits each element
This is the pivot between the models: jq *streams* elements, SQL++/SQL *scan* a
collection.
```
jq     .[]                       [{"name":"JSON"},{"name":"XML"}] → two values
SQL++  SELECT RAW x FROM [{"name":"JSON"},{"name":"XML"}] AS x
SQL    SELECT value FROM jsonb_array_elements('[…]'::jsonb);
```

### Pipe / project a field from each — `.[] | .name`
```
jq     .[] | .name               → "JSON", "XML"
SQL++  SELECT RAW x.name FROM [{"name":"JSON"},{"name":"XML"}] AS x
SQL    SELECT e->>'name' FROM jsonb_array_elements(col) e;
```

### map — transform each element into an array
jq's `map(f)` is `[ .[] | f ]`. SQL++ offers two forms: a set query (`SELECT`), or
the inline **array comprehension** `ARRAY f FOR x IN arr END` when you want the
result as one array value.
```
jq     map(.+1)                  [1,2,3] → [2,3,4]
SQL++  SELECT ARRAY v+1 FOR v IN [1,2,3] END AS r     -- {"r":[2,3,4]}
SQL    SELECT jsonb_agg(value::int + 1) FROM jsonb_array_elements('[1,2,3]') ;
```

### Comma — multiple outputs
```
jq     .foo, .bar                {"foo":42,"bar":"x"} → 42, "x"
SQL++  SELECT o.foo, o.bar FROM [{"foo":42,"bar":"x"}] AS o   -- one row, two columns
SQL    SELECT col->'foo', col->'bar';
```

### Collect — gather a stream into an array — `[ … ]`
```
jq     [.user, .projects[]]      {"user":"s","projects":["jq","wf"]} → ["s","jq","wf"]
SQL++  SELECT ARRAY_PREPEND(o.user, o.projects) AS r
       FROM [{"user":"s","projects":["jq","wf"]}] AS o        -- {"r":["s","jq","wf"]}
SQL    SELECT col->'user' || col->'projects';   -- jsonb || concatenates
```

### Object construction — `{ }`
```
jq     {user, title: .title}     {"user":"s","title":"JQ"} → {"user":"s","title":"JQ"}
SQL++  SELECT o.user, o.title FROM [{"user":"s","title":"JQ"}] AS o
       -- or explicitly: SELECT {"user": o.user, "title": o.title} AS doc FROM …
SQL    SELECT jsonb_build_object('user', col->'user', 'title', col->'title');
```
Note `{shorthand}`: jq `{user}` ≡ `{user: .user}`; SQL++ `SELECT o.user` yields a
column named `user`, and `{o.user}` shorthand-names the key `user` too.

### Dynamic keys — key comes from the data
jq builds `{ (.k): .v }`; SQL++ uses an **object comprehension** whose key
expression is dynamic. ⚠ Object keys must be **strings** — wrap non-strings in
`TO_STRING`.
```
jq     map({(.label): .value}) | add     [{"label":"a","value":1}] → {"a":1}
SQL++  SELECT OBJECT r.label : r.value FOR r IN [{"label":"a","value":1}] END AS o
SQL    SELECT jsonb_object_agg(e->>'label', e->'value') FROM jsonb_array_elements(col) e;
```

---

## 3. Filtering & selecting

### select — keep elements matching a condition
```
jq     map(select(. >= 2))       [1,5,3,0,7] → [5,3,7]
SQL++  SELECT RAW v FROM [1,5,3,0,7] AS v WHERE v >= 2
SQL    SELECT value FROM jsonb_array_elements('[1,5,3,0,7]') WHERE value::int >= 2;
```

### has / missing a key
```
jq     map(has("endpoint"))              [{"endpoint":1},{}] → [true,false]
SQL++  SELECT x.endpoint IS NOT MISSING AS has FROM [{"endpoint":1},{}] AS x
SQL    SELECT col ? 'endpoint';          -- Postgres jsonb key-exists operator
```
Find objects *missing* a key: jq `select(has("k")|not)`, SQL++
`WHERE x.k IS MISSING`, Postgres `WHERE NOT (col ? 'k')`. Objects where a key is
present but **null**: jq `select(has("k") and .k==null)`, SQL++
`WHERE x.k IS NULL` (distinct from `IS MISSING` — see §11).

### contains / startswith — substring & prefix
```
jq     [.[]|startswith("foo")]   ["fo","foo","foobar"] → [false,true,true]
SQL++  SELECT ARRAY s LIKE "foo%" FOR s IN ["fo","foo","foobar"] END AS r
SQL    SELECT value LIKE 'foo%' FROM jsonb_array_elements_text('[…]');
```
`contains("bar")` on a string → SQL++ `CONTAINS(s, "bar")` or `POSITION(s,"bar")>=0`
(0-based), SQL `s LIKE '%bar%'`.

### regex
```
jq     test("[0-9]+")            "foo123" → true
SQL++  SELECT REGEXP_CONTAINS("foo123", "[0-9]+") AS m         -- {"m":true}
SQL    SELECT 'foo123' ~ '[0-9]+';       -- Postgres; SQLite needs REGEXP ext
```

---

## 4. Objects ↔ entries (keys, pairs, pivots)

### keys — list an object's field names
```
jq     keys                      {"b":2,"a":1} → ["a","b"]   (sorted)
SQL++  SELECT OBJECT_NAMES({"b":2,"a":1}) AS k                -- {"k":["a","b"]}
SQL    SELECT jsonb_object_keys(col);    -- one row per key
```

### to_entries — object → array of pairs
⚠ jq's pairs are `{key, value}`; SQL++ `OBJECT_PAIRS` yields **`{name, val}`**.
```
jq     to_entries               {"a":1,"b":2} → [{"key":"a","value":1},…]
SQL++  SELECT OBJECT_PAIRS({"a":1,"b":2}) AS p   -- [{"name":"a","val":1},{"name":"b","val":2}]
SQL    SELECT jsonb_agg(jsonb_build_object('key',k,'value',v)) FROM jsonb_each(col) AS e(k,v);
```

### from_entries — pairs → object
```
jq     from_entries             [{"key":"a","value":1}] → {"a":1}
SQL++  SELECT OBJECT p.name : p.val FOR p IN [{"name":"a","val":1}] END AS o
SQL    SELECT jsonb_object_agg(e->>'key', e->'value') FROM jsonb_array_elements(col) e;
```

### Swap keys and values
```
jq     to_entries|map({(.value):.key})|add     {"a":1,"b":2} → {"1":"a","2":"b"}
SQL++  SELECT OBJECT TO_STRING(p.val) : p.name FOR p IN OBJECT_PAIRS({"a":1,"b":2}) END AS o
SQL    SELECT jsonb_object_agg(v#>>'{}', k) FROM jsonb_each('{"a":1,"b":2}') AS e(k,v);
```

### map_values — transform every value, keep keys
```
jq     map_values(.+1)          {"a":1,"b":2} → {"a":2,"b":3}
SQL++  SELECT OBJECT p.name : p.val+1 FOR p IN OBJECT_PAIRS({"a":1,"b":2}) END AS o
SQL    SELECT jsonb_object_agg(k, (v::int)+1) FROM jsonb_each('{"a":1,"b":2}') AS e(k,v);
```

### Add / remove / rename a field
```
jq     . + {"draft":true}       {"a":1} → {"a":1,"draft":true}
SQL++  SELECT OBJECT_ADD({"a":1}, "draft", true) AS o
SQL    SELECT col || '{"draft":true}';           -- jsonb merge
```
```
jq     del(.title)              {"title":"x","a":1} → {"a":1}
SQL++  SELECT OBJECT_REMOVE({"title":"x","a":1}, "title") AS o
SQL    SELECT col - 'title';                      -- jsonb minus key
```
Rename `.value` → `.slug` while keeping everything else: SQL++
`OBJECT_ADD(OBJECT_REMOVE(o,"value"), "slug", o.value)`.

### Pivot: object → array of objects (key becomes a field)
```
jq     to_entries|map(.value + {slug:.key})   {"x":{"n":1}} → [{"n":1,"slug":"x"}]
SQL++  SELECT ARRAY OBJECT_ADD(p.val, "slug", p.name) FOR p IN OBJECT_PAIRS({"x":{"n":1}}) END AS a
SQL    SELECT jsonb_agg(v || jsonb_build_object('slug',k)) FROM jsonb_each(col) AS e(k,v);
```

### Pivot: array of objects → object keyed by a field
```
jq     map({(.slug):.}) | add   [{"slug":"x","n":1}] → {"x":{"slug":"x","n":1}}
SQL++  SELECT OBJECT r.slug : r FOR r IN [{"slug":"x","n":1}] END AS o
SQL    SELECT jsonb_object_agg(e->>'slug', e) FROM jsonb_array_elements(col) e;
```

---

## 5. Aggregating & grouping

Here jq's specialized verbs (`group_by`, `unique`, `add`, `max_by`) meet SQL's
home turf — `GROUP BY` and aggregate functions.

### length / count
```
jq     length                   [1,2,3] → 3   ("abc" → 3, {"a":1} → 1)
SQL++  SELECT ARRAY_LENGTH([1,2,3]) AS n       -- LENGTH("abc"), OBJECT_LENGTH({…})
SQL    SELECT jsonb_array_length('[1,2,3]');
```

### add / sum / min / max / avg over an array
```
jq     add                      [1,2,3] → 6
SQL++  SELECT ARRAY_SUM([1,2,3]) AS s, ARRAY_MAX([1,2,3]) AS mx, ARRAY_AVG([1,2,3]) AS a
SQL    SELECT sum(value::int) FROM jsonb_array_elements('[1,2,3]');
```

### Aggregate over a collection (real SQL territory)
```
jq     [.[].total] | add        (sum a field across records)
SQL++  SELECT SUM(o.total) AS revenue, COUNT(*) AS n, ROUND(AVG(o.total),2) AS avg FROM orders o
SQL    SELECT sum(total), count(*), round(avg(total),2) FROM orders;
```

### group_by — cluster, then aggregate
jq returns nested arrays; SQL++/SQL return one row per group. Against the shop
data:
```
jq     group_by(.status)                (→ array of arrays)
SQL++  SELECT o.status, COUNT(*) AS n, ROUND(SUM(o.total),2) AS revenue
       FROM orders o GROUP BY o.status ORDER BY revenue DESC
SQL    SELECT status, count(*), round(sum(total),2) FROM orders GROUP BY status ORDER BY 3 DESC;
```
```
{"status":"shipped","n":16,"revenue":1758.73}
{"status":"pending","n":3,"revenue":163.23}
{"status":"cancelled","n":1,"revenue":27.4}
```
To collect each group's members like jq does, use `ARRAY_AGG`:
```
jq     group_by(.customer) | map({customer:.[0].customer, ids:map(.id)})
SQL++  SELECT o.customer, ARRAY_AGG(o.id) AS order_ids FROM orders o GROUP BY o.customer
SQL    SELECT customer, jsonb_agg(id) FROM orders GROUP BY customer;
```

### unique / dedup
```
jq     unique                   [1,2,5,3,5,3,1] → [1,2,3,5]
SQL++  SELECT ARRAY_DISTINCT([1,2,5,3,5,3,1]) AS u     -- or SELECT DISTINCT over a scan
SQL    SELECT DISTINCT value FROM jsonb_array_elements('[…]') ORDER BY 1;
```
`ARRAY_DISTINCT` keeps first-seen order; wrap in `ARRAY_SORT` for jq's sorted
result. Across a collection: `SELECT DISTINCT o.customer FROM orders o` /
`ARRAY_AGG(DISTINCT o.customer)`.

### sort_by
```
jq     sort_by(.total)          [{"total":4},{"total":2}] → [{"total":2},{"total":4}]
SQL++  SELECT o.id, o.total FROM orders o ORDER BY o.total DESC
SQL    SELECT id, total FROM orders ORDER BY total DESC;
```

### min_by / max_by — the extreme *record*
```
jq     max_by(.total)           (the whole object with the largest .total)
SQL++  SELECT o.* FROM orders o ORDER BY o.total DESC LIMIT 1
SQL    SELECT * FROM orders ORDER BY total DESC LIMIT 1;
```

### Count occurrences / histogram (jq's `reduce … += 1`)
```
jq     reduce .[] as $x ({}; .[$x] += 1)     ["a","b","a"] → {"a":2,"b":1}
SQL++  SELECT v AS value, COUNT(*) AS n FROM ["a","b","a"] AS v GROUP BY v
SQL    SELECT value, count(*) FROM jsonb_array_elements_text('["a","b","a"]') GROUP BY value;
```
For a value distribution as a chart, n1k1 also ships the native `histogram()` and
`sparkline()` aggregates — see `examples/queries/charts.sql++`.

### Find duplicates by key
```
jq     [reduce .[].id as $x ({}; .[$x]+=1) | to_entries[] | select(.value>1)]
SQL++  SELECT o.id, COUNT(*) AS n FROM orders o GROUP BY o.id HAVING COUNT(*) > 1
SQL    SELECT id, count(*) FROM orders GROUP BY id HAVING count(*) > 1;
```

---

## 6. Arrays

### flatten
```
jq     flatten                  [1,[2],[[3]]] → [1,2,3]
SQL++  SELECT ARRAY_FLATTEN([1,[2],[[3]]], 2) AS f    -- depth arg is explicit
SQL    (recursive unnest; no single builtin)
```
⚠ SQL++ `ARRAY_FLATTEN(arr, depth)` requires a depth (jq flattens fully; pass a
large depth for the same effect).

### reverse / range / append / concat
```
jq     reverse                  [1,2,3] → [3,2,1]        SQL++  ARRAY_REVERSE([1,2,3])
jq     [range(2;4)]             → [2,3]                  SQL++  ARRAY_RANGE(2,4)
jq     . + [4]                  [1,2,3] → [1,2,3,4]      SQL++  ARRAY_APPEND([1,2,3], 4)
jq     [.a] + [.b]              (concat arrays)          SQL++  ARRAY_CONCAT(a, b)
```

### Every other element (even indices)
Index by a stepped range rather than jq's `to_entries` trick (`OBJECT_PAIRS` works
only on objects, not arrays):
```
jq     to_entries|map(select(.key%2==0).value)   [a,b,c,d] → [a,c]
SQL++  SELECT ARRAY a[i] FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(a), 2) END AS evens
       FROM [["a","b","c","d"]] AS a          -- {"evens":["a","c"]}
SQL    SELECT jsonb_agg(value) FROM jsonb_array_elements(col) WITH ORDINALITY t(value,i) WHERE i%2=1;
```

### Chunk into fixed-size groups
⚠ Clamp the slice end with `LEAST(…, ARRAY_LENGTH(a))` — a slice whose end runs
*past* the array is `MISSING`, which would silently drop the final short chunk.
```
jq     . as $x | [range(0;$x|length;$n)] | map($x[.:.+$n])
SQL++  SELECT ARRAY a[i:LEAST(i+2, ARRAY_LENGTH(a))] FOR i IN ARRAY_RANGE(0, ARRAY_LENGTH(a), 2) END
       AS chunks FROM [[1,2,3,4,5]] AS a        -- {"chunks":[[1,2],[3,4],[5]]}
SQL    (window/ntile or generate_series with slicing)
```

---

## 7. Strings & numbers

### split / join
```
jq     split(",")               "a,b,c" → ["a","b","c"]     SQL++  SPLIT("a,b,c", ",")
jq     join("-")                ["a","b","c"] → "a-b-c"     SQL++  CONCAT2("-", ["a","b","c"])
SQL    string_to_array('a,b,c', ','),  array_to_string(arr, '-')
```

### String interpolation
```
jq     "total is \(.+1)"        2 → "total is 3"
SQL++  SELECT "total is " || TO_STRING(1+2) AS msg          -- {"msg":"total is 3"}
SQL    SELECT 'total is ' || (1+2)::text;
```

### upcase / trim
```
jq     ascii_upcase             "abc" → "ABC"      SQL++  UPPER("abc")   (LOWER, TITLE)
jq     ltrimstr("foo")          "foobar" → "bar"   SQL++  LTRIM, TRIM, RTRIM
SQL    upper('abc'),  ltrim(s, 'foo')
```

### index of a substring — `index("…")`
```
jq     index(", ")              "a, b" → 1
SQL++  SELECT POSITION("a, b", ", ") AS i     -- {"i":1}   ⚠ 0-based
SQL    SELECT position(', ' in 'a, b') - 1;   -- Postgres position is 1-based
```

### Numeric — floor / round / sqrt
```
jq     floor / sqrt / (.*100|round/100)
SQL++  FLOOR(x), ROUND(x, 2), SQRT(x), CEIL(x), ABS(x), IDIV(a,b) (integer divide)
SQL    floor(x), round(x, 2), sqrt(x), div(a,b)
```

### type — name the JSON type
⚠ SQL++ `TYPE()` returns lowercase, and distinguishes **`"missing"`** from
`"null"` (jq has no missing).
```
jq     map(type)                [0,false,[],{},null,"x"] → ["number","boolean","array","object","null","string"]
SQL++  SELECT ARRAY TYPE(v) FOR v IN [0,false,[],{},null,"x"] END AS t
SQL    SELECT jsonb_typeof(value) FROM jsonb_array_elements(col);   -- "number","boolean",…
```

---

## 8. Recursion & deep search

jq's `..` (recurse) and `walk` traverse arbitrarily-nested structure. SQL++'s
answer is the **`WITHIN`** collection operator (descend into any nested value) and
the quantifiers `ANY … WITHIN … SATISFIES`. Postgres reaches for `jsonpath` (`$..`).

### Recurse into everything — `..`
```
jq     ..                       {"a":0,"b":[1]} → {"a":0,"b":[1]}, 0, [1], 1
SQL++  SELECT ARRAY v FOR v WITHIN {"a":0,"b":[1]} END AS all_descendants
SQL    SELECT jsonb_path_query('{…}', '$..*');
```

### Does value X appear anywhere in the tree?
```
jq     [.. | select(. == 5)] | length > 0
SQL++  SELECT ANY v WITHIN {"a":{"b":5}} SATISFIES v = 5 END AS found   -- {"found":true}
SQL    SELECT jsonb_path_exists('{"a":{"b":5}}', '$..* ? (@ == 5)');
```

### Find every object with a given id, at any depth
```
jq     [.. | objects | select(.id == "0:16")]
SQL++  SELECT ARRAY v FOR v WITHIN doc WHEN v.id = "0:16" END AS hits FROM … AS doc
SQL    SELECT jsonb_path_query(col, '$..* ? (@.id == "0:16")');
```

---

## 9. Reshaping records (the meaty ones)

### UNNEST — explode a nested array into rows (jq `.items[]`)
The workhorse for line-items, tags, events. jq flattens with `.[]`; SQL++ has a
dedicated `UNNEST` join that pairs each element with its parent's fields.
```
jq     .[] | .id as $id | .tags[] | {id:$id, tag:.}
SQL++  SELECT o.id, t AS tag FROM [{"id":1,"tags":["a","b"]}] AS o UNNEST o.tags AS t
SQL    SELECT o->>'id', t FROM jsonb_array_elements(col) o,
              jsonb_array_elements_text(o->'tags') t;      -- lateral cross join
```
```
{"id":1,"tag":"a"}
{"id":1,"tag":"b"}
```

### CSV-ish array-of-rows → objects
```
jq     .[1:] | map({name:.[0], url:.[1], category:(.[2]|tonumber)})
SQL++  SELECT r[0] AS name, r[1] AS url, TO_NUMBER(r[2]) AS category
       FROM [["hdr","hdr","hdr"],["n","u","3"]][1:] AS r
SQL    SELECT r->>0, r->>1, (r->>2)::int FROM jsonb_array_elements(col) WITH ORDINALITY … WHERE i>1;
```
(n1k1 can also just read a `.csv` file as a keyspace of objects directly — see
`DESIGN-data.md`.)

### Reshape + rename + sort (jq's Twitter-DM recipe)
```
jq     [.[] | {text, from:.sender.screen_name}] | sort_by(.date)
SQL++  SELECT d.text, d.sender.screen_name AS from_name FROM dms d ORDER BY d.created_at
SQL    SELECT col->>'text', col#>>'{sender,screen_name}' FROM dms ORDER BY col->>'created_at';
```

### Merge two arrays, flagging one (jq's `team` + `formerly`)
```
jq     [.team, (.formerly | map(.+{formerly:true}))] | flatten
SQL++  SELECT ARRAY_CONCAT(d.team,
              ARRAY OBJECT_ADD(m,"formerly",true) FOR m IN d.formerly END) AS everyone
       FROM [{"team":[{"n":"a"}],"formerly":[{"n":"b"}]}] AS d
SQL    SELECT (col->'team') || (SELECT jsonb_agg(m||'{"formerly":true}') FROM jsonb_array_elements(col->'formerly') m);
```

---

## 10. Conditionals & defaults

### if / then / else
```
jq     if .>3 then "big" else "small" end     5 → "big"
SQL++  SELECT CASE WHEN 5 > 3 THEN "big" ELSE "small" END AS c   -- {"c":"big"}
SQL    SELECT CASE WHEN 5 > 3 THEN 'big' ELSE 'small' END;
```

### Default for a missing/null value — `//`
```
jq     .foo // "default"        {} → "default"
SQL++  SELECT IFMISSINGORNULL(x.foo, "default") AS v FROM [{}] AS x   -- {"v":"default"}
SQL    SELECT COALESCE(col->>'foo', 'default');
```
SQL++ splits the concept jq's `//` and SQL's `COALESCE` blur: `IFMISSING`,
`IFNULL`, `IFMISSINGORNULL` (and `MISSINGIF`/`NULLIF`) let you treat *absent* and
*present-but-null* differently.

### try / catch
jq guards type errors with `try f catch g`. SQL++ has no type errors to catch —
mismatched operations yield `NULL`/`MISSING` rather than aborting — so the pattern
is a `CASE`/`IF*` guard, e.g. `CASE WHEN TYPE(x)="object" THEN x.a END`.

---

## 11. The SQL++ superpower: MISSING vs NULL

The one concept with no jq or standard-SQL equivalent, and worth understanding
because it changes how filters and defaults behave.

- **`MISSING`** — the field/element **is not there**. jq models this as an error
  (needing `?`); SQL models it as `NULL`.
- **`NULL`** — the field **is present with the JSON value `null`**.

n1k1 keeps them distinct end to end. A `MISSING` field is **omitted** from output
objects entirely; a `NULL` field is rendered:
```
n1k1 -c 'SELECT MISSINGIF(3,3) AS gone, NULLIF(3,3) AS nulled'
# {"nulled":null}                    -- "gone" vanished; "nulled" stayed as null
```
Consequences you rely on:
- `x.foo IS MISSING` ≠ `x.foo IS NULL`. Use `IS VALUED` for "present and not null".
- Aggregates and array builders **skip `MISSING`** (and `NULL`), so a sparse field
  aggregates cleanly.
- Projecting a missing field just drops that key — which is why jq's `.foo?` needs
  no translation.

---

## Function cheat-sheet

| Goal | jq | SQL++ | PostgreSQL |
|---|---|---|---|
| iterate array | `.[]` | `FROM a AS x` / `UNNEST` | `jsonb_array_elements` |
| map | `map(f)` | `ARRAY f FOR x IN a END` | `jsonb_agg(f)` |
| filter | `select(c)` | `WHERE c` | `WHERE c` |
| object keys | `keys` | `OBJECT_NAMES(o)` | `jsonb_object_keys` |
| obj → pairs | `to_entries` | `OBJECT_PAIRS(o)` → `{name,val}` | `jsonb_each` |
| pairs → obj | `from_entries` | `OBJECT k:v FOR … END` | `jsonb_object_agg` |
| add field | `. + {k:v}` | `OBJECT_ADD(o,k,v)` | `o \|\| '{…}'` |
| remove field | `del(.k)` | `OBJECT_REMOVE(o,k)` | `o - 'k'` |
| length | `length` | `ARRAY_LENGTH`/`LENGTH`/`OBJECT_LENGTH` | `jsonb_array_length` |
| sum array | `add` | `ARRAY_SUM(a)` | `sum(…)` |
| dedup | `unique` | `ARRAY_DISTINCT(a)` / `DISTINCT` | `DISTINCT` |
| sort | `sort_by(.k)` | `ORDER BY k` | `ORDER BY k` |
| group | `group_by(.k)` | `GROUP BY k` | `GROUP BY k` |
| flatten | `flatten` | `ARRAY_FLATTEN(a, n)` | (recursive) |
| recurse | `..` | `… WITHIN v` | `jsonb_path_query($..)` |
| split / join | `split`/`join` | `SPLIT` / `CONCAT2` | `string_to_array` |
| type of | `type` | `TYPE(v)` | `jsonb_typeof` |
| default | `// d` | `IFMISSINGORNULL(v,d)` | `COALESCE` |
| substring idx | `index(s)` | `POSITION(x,s)` (0-based) | `position()` (1-based) |
| interpolate | `"\(.x)"` | `"…" \|\| TO_STRING(x)` | `\|\|` |

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
- **`UNION`/`UNION ALL` align by field name**, not position — alias every column
  so branches line up (see `examples/queries/life.sql++`).
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
- Couchbase N1QL function reference — n1k1 speaks the same dialect (parser +
  planner are the `couchbase/query` fork), so the full function library applies.
