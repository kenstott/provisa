# Array types in semantic SQL — design

Status: design only, 2026-10-09. No product code. Base: origin/main `3db8281dd`.

Evidence tags: `[code]` read in the repo at the cited line; `[doc]` vendor page fetched today
(links in section 12); `[probe]` run locally today (section 12); `[mem]` from memory, not
fetched, treat as unverified.

## 0. Answer to the central question

Provisa is in the value path on two read routes only: a DIRECT read from a source without
arrays, and a read served by a self-only engine without arrays (MySQL, SQL Server, Oracle and
the like). There it parses JSON text once per batch. Measured on this laptop, 1M rows of
5-element text arrays: 0.49 s to Arrow lists, 0.82 to 1.27 s to Python lists `[probe]`. On
every other route an engine or the source produces the array.

The larger cost is not in Provisa. It is (a) an element filter on a JSON-encoded column in a
database that has no index for it, and (b) the same filter through a federating engine over a
live source, where the predicate is not pushed down and the engine pulls the column. Section 4
names the two measurements.

## 1. What is true today

| Fact | Where |
|---|---|
| IR vocabulary has no array type; 15 scalar names | `provisa/core/ir_types.py:48-69` `[code]` |
| Trino `array/row/map` and DuckDB `list/struct/map` map to `text` | `ir_types.py:115-117,151-153` `[code]` |
| `to_ir` strips at `(`, so only Trino's `array(T)` reaches that map. `VARCHAR[]`, `INTEGER[][]`, `_int4`, `ARRAY`, `Array(String)`, `ARRAY<STRING>`, `array<string>` all raise | `ir_types.py:244` `[code]`, `[probe]` |
| PostgreSQL-wire introspection reads `information_schema.columns.data_type`, gets the marker `ARRAY`, and stores `text` | `provisa/api/admin/introspect.py:72,1254-1263` `[code]` |
| DuckDB reports `VARCHAR[]`, `INTEGER[][]`, `INTEGER[3]` in `information_schema` | `[probe]` |
| GraphQL already turns Trino's `array(varchar)` into `[String!]` and nested into `[[Int!]!]`; no other spelling | `provisa/compiler/type_map.py:214-218` `[code]`, `[probe]` |
| gRPC schema generation has the same Trino-only branch | `provisa/grpc/proto_gen.py:85-96` `[code]` |
| pgwire maps `INTEGER[]` and `VARCHAR[]` to array wire types, any other `[]` to JSON, and types a list column from the first non-null row | `provisa/pgwire/server.py:325-326,401-402,406-431` `[code]` |
| Flight catalog lists `array`, `map`, `row` as utf8 | `provisa/api/flight/catalog.py:64-66` `[code]` |
| Arrow landing types: no list type | `provisa/core/ir_arrow.py:35-51` `[code]` |
| Every result is `list[tuple]` of Python values; row surfaces (pgwire, Bolt, gRPC, GraphQL, JSON:API) consume those | `provisa/executor/result.py:55-73` `[code]` |
| Arrow-native engines (DuckDB, Snowflake, BigQuery, pg over ADBC) hand Flight record batches with no Python rows | `provisa/federation/duckdb_runtime.py:1215-1254`, `snowflake_runtime.py:413-455`, `bigquery_runtime.py:258-289`, `pg_runtime.py:993-1020` `[code]` |
| Row engines reach Flight through `arrow_batches_from_rows`, schema taken from the first batch | `provisa/federation/runtime_support.py:163-200` `[code]` |
| Value transforms on landing are SQL expressions by rule | `ir_types.py:157-176` `[code]` |
| The JSON land path already parses a JSON column per row in Python before insert | `provisa/federation/materialize_exec.py:135-138` `[code]` |
| Semantic SQL is PostgreSQL dialect, transpiled by sqlglot | `provisa/transpiler/transpile.py:11-38` `[code]` |
| Column `data_type` is one string on the column | `provisa/core/models.py:822` `[code]` |
| Masks validate by base type; string masks accept `varchar/char/text/varbinary` | `provisa/security/masking.py:43-60` `[code]` |

sqlglot 30.9.0 cannot be left to translate array operations on its own `[probe]`:

| Input (PostgreSQL) | Target | Output | Verdict |
|---|---|---|---|
| `tags \|\| ARRAY['y']` | trino | `CONCAT(CAST(tags AS VARCHAR), CAST(ARRAY['y'] AS VARCHAR))` | wrong answer, silently |
| `ARRAY[1,2,3][1]` | mysql, tsql, oracle, sqlite | `ARRAY(1, 2, 3)[0]` | not valid SQL there |
| `ARRAY_AGG(a ORDER BY a)` | databricks | `COLLECT_LIST(a)` | order dropped, silently |
| `ARRAY_AGG(a ORDER BY a)` | mysql | `GROUP_CONCAT(...)` | a string, not an array |
| `x = ANY(tags)` | mysql, tsql, oracle, sqlite | unchanged | not valid on a JSON column |

So array operations are rewritten by a table Provisa owns (section 5), before sqlglot
generation, in the same compile stage that already transpiles, and kept with the governed plan.

## 2. The type

### 2.1 Syntax

Model spelling (the IR name stored in `data_type`): `array<T>`, where `T` is an IR scalar name,
optionally sized (`numeric(18,2)`), or another `array<...>`. Examples: `array<text>`,
`array<numeric(18,2)>`, `array<array<bigint>>`.

Semantic SQL spelling is PostgreSQL's, because semantic SQL is PostgreSQL dialect: `text[]`,
`bigint[][]`, `ARRAY[...]`, `CAST(x AS text[])`.

`to_ir` stops splitting at `(` and parses a type into a small value (name, element, precision,
scale). Spellings accepted on input, all normalised to `array<T>`:

| Spelling | Reported by |
|---|---|
| `T[]`, `T[][]`, `T[n]` | PostgreSQL `format_type`, DuckDB `[probe]` |
| `_int4`, `_text` (leading underscore) | PostgreSQL `udt_name` |
| `array(T)` | Trino `[doc]` |
| `Array(T)`, `Array(Nullable(T))` | ClickHouse `[doc]` |
| `ARRAY<T>` | BigQuery, Databricks, Hive `[doc]` |
| `ARRAY(T)` | Snowflake structured `[doc]` |

An element type outside the vocabulary raises, as `to_ir` does today. No default.

### 2.2 Semantics

| Property | Rule |
|---|---|
| Element types | every scalar IR type, `json`, and `array` |
| Homogeneous | yes, one element type |
| Order | preserved; position is meaningful; duplicates kept |
| Element NULL | allowed |
| NULL vs empty | distinct on every surface: a NULL array is not `[]` |
| Nested | array of arrays, ragged: inner arrays may differ in length and may be NULL. Not PostgreSQL's multidimensional array |
| Index base | 1 (PostgreSQL, DuckDB, Trino, ClickHouse agree `[doc]`); translated for 0-based backends |
| Out-of-range or index below 1 | NULL, on every backend |
| Maximum depth | 3, refused above by the type parser (`model.array_depth_exceeded`) |
| Size | no model limit; store limits apply (CockroachDB advises under 1 MB `[doc]`, Redshift SUPER 16 MB `[doc]`, Snowflake 128 MB `[doc]`) |
| Fixed-size arrays (DuckDB `T[3]`, Arrow FixedSizeList) | registered as `array<T>`; the length is not kept |

### 2.3 struct and map: out

| Left out | Cost |
|---|---|
| struct / row / tuple | BigQuery repeated records, Parquet and Iceberg nested columns, Trino `row`, ClickHouse `Tuple` cannot be typed field by field |
| map | Trino/DuckDB/ClickHouse `map` columns stay opaque |

Bridge: `array<json>`. An array of structs registers as `array<json>`, so it can be unnested and
its elements read with the JSON path functions the model already has (REQ-980). A lone struct or
map registers as `json` instead of today's `text`. That closes most of the gap without a second
composite type.

### 2.4 How a source column becomes `array<T>`

| Source / engine | What introspection reports | Rule |
|---|---|---|
| PostgreSQL, Greenplum, YugabyteDB, openGauss | `data_type='ARRAY'` today | introspect with `format_type(atttypid, atttypmod)`; `integer[]` → `array<integer>`. Declared dimensions are not enforced by PostgreSQL `[doc]`, so depth is always registered as 1; the registrar may declare `array<array<T>>` |
| CockroachDB | same wire path | depth 1 only: "does not support nested arrays" `[doc]` |
| Redshift | `super` | `json`; the registrar may declare `array<T>` |
| Trino engine catalog | `array(varchar)`, `array(array(integer))` | parsed recursively; `array(row(...))` → `array<json>` |
| DuckDB (files, Parquet, attached databases) | `VARCHAR[]`, `INTEGER[][]`, `STRUCT(...)[]` | parsed recursively; struct element → `json` |
| ClickHouse | `Array(String)`, `Array(Array(UInt8))` | parsed; `Nullable` wrapper dropped |
| Snowflake | `ARRAY` (elements VARIANT) or `ARRAY(T)` | `ARRAY` → `array<json>`, the registrar may declare `array<text>`; `ARRAY(T)` → `array<T>` |
| BigQuery | `ARRAY<STRING>`, `ARRAY<STRUCT<...>>` | parsed; struct element → `json`. No array of arrays `[doc]` |
| Databricks, Hive, HiveServer2 | `array<string>`, `array<array<int>>` | parsed |
| MySQL family, SQL Server family, Oracle, SQLite, Exasol, SAP HANA | no array type; `json` or text | never inferred. The registrar declares `array<T>` on a `json`/text column, the way an epoch unit is declared (REQ-1908, `ir_types.py:180-187` `[code]`). A structural data-quality check validates it (section 6) |
| OpenAPI, GraphQL remote, gRPC remote, Kafka (Avro, JSON Schema) | `type: array, items`, `[String!]`, `repeated`, Avro `array` | scalar items → `array<T>`; object items stay the child-table / JSON path they are today (`provisa/openapi/mapper.py:220-234`, `provisa/kafka/source.py:184,208` `[code]`) |
| MongoDB, Elasticsearch, Neo4j, Redis | no schema for it | declared by the registrar (`provisa/mongodb/source.py:41` maps `array` to `VARCHAR` today `[code]`) |

## 3. Per-backend matrix

Classes used in the rest of the document:

- **N** native, nested ragged arrays allowed.
- **N1** native, one level only.
- **J+** JSON-encoded, an index can serve element search.
- **J** JSON-encoded, no index for element search.
- **?** not verified today; lands as JSON text, whole-value read only, every array operation
  refused by name until the row is verified.

| Backend | Class | Native array | Nested | Notes and limits | Index for element search |
|---|---|---|---|---|---|
| DuckDB | N | `T[]` LIST | yes, ragged: `[['duck','goose'],['frog']]` `[doc]` | 1-based; NULL elements; list comparison is positional `[doc]` | none |
| Trino | N | `array(T)` | yes (`flatten` takes `array(array(T))`) `[doc]` | `[]` is 1-based and fails out of range; `element_at` returns NULL `[doc]` | none |
| ClickHouse | N | `Array(T)` | yes, "T can be any data type, including an array" `[doc]` | 1-based `[doc]`; out-of-range returns the type default, not NULL `[mem]`; `Array` itself cannot be `Nullable` `[mem]`, so NULL vs empty needs an encoding decision (section 11, D7) | bloom-filter skip index for `has()` `[mem]` |
| Databricks | N | `ARRAY<T>` | yes `[doc]` | `[]` 0-based `[doc]`; `COLLECT_LIST` has no order | none |
| Hive / HiveServer2 | N | `ARRAY<T>` | yes `[doc]` | 0-based `[mem]`; `size`, `array_contains`, `posexplode` `[mem]` | none |
| Snowflake | N | `ARRAY` of VARIANT; structured `ARRAY(T)` in standard and Iceberg tables `[doc]` | yes `[doc]` | 0-based; holds both SQL NULL and JSON null `[doc]`; 128 MB `[doc]` | search optimization `[mem]` |
| PostgreSQL, Greenplum, YugabyteDB, openGauss | N1 | `T[]` | multidimensional only, and "must have matching extents for each dimension" `[doc]` — not array of arrays | 1-based; declared size and dimensions not enforced `[doc]` | GIN `array_ops`: `&&`, `@>`, `<@`, `=` `[doc]`. `x = ANY(col)` is not in that list `[doc]` |
| CockroachDB | N1 | `T[]` | no `[doc]` | keep values under 1 MB `[doc]` | inverted index: `@>`, `<@`, `&&` `[doc]` |
| BigQuery | N1 | `ARRAY<T>` | "Arrays of arrays aren't allowed" `[doc]` | a NULL array written to a table "is converted to an empty array"; a result array with NULL elements raises `[doc]`; array equality not supported `[mem]` | search index `[mem]` |
| Redshift | N (variant) | `SUPER` | yes, no regularity required `[doc]` | 16 MB `[doc]`; PartiQL navigation, 0-based `[mem]` | none |
| MySQL 8.0.17+ | J+ | none; `JSON` | JSON nesting | sqlglot: "Arrays are not supported by MySQL" `[probe]` | multi-valued index on `CAST(col AS type ARRAY)`: serves `MEMBER OF`, `JSON_CONTAINS`, `JSON_OVERLAPS`; one such key part per index; not covering; no range scan; an empty array has no index entry `[doc]` |
| TiDB | J+ | none; `JSON` | JSON nesting | uses IndexMerge only `[doc]` | multi-valued index, same three functions `[doc]` |
| Oracle (JSON type) | J+ | VARRAY / nested table object types, not used `[mem]` | JSON nesting | | multivalue function-based index for `json_exists`, "especially as elements of a JSON array" `[doc]`; release that introduced it not confirmed today |
| SQL Server 2025, Azure SQL, Fabric SQL database | J+ | none; native `json` type `[doc]` | JSON nesting | `JSON_CONTAINS`, `JSON_ARRAYAGG` `[doc]` | `CREATE JSON INDEX ... WITH (OPTIMIZE_FOR_ARRAY_SEARCH = ON)`, needs a clustered primary key, one per column `[doc]` |
| SQL Server 2016–2022, Synapse | J | none; JSON in `nvarchar`, "indexed as plain text" `[doc]` | JSON nesting | `OPENJSON`, `JSON_VALUE`, `JSON_QUERY` `[doc]` | none for elements |
| Fabric Warehouse | ? | | | which JSON functions the warehouse endpoint has was not checked today | |
| MariaDB | J | none; `JSON` is "an alias for LONGTEXT" with a `JSON_VALID` check `[doc]` | JSON nesting | `JSON_TABLE` from 10.6 `[mem]` | none found `[doc]` |
| SingleStore | J | none; `JSON` | JSON nesting | `JSON_ARRAY_CONTAINS_<type>` `[mem]` | not verified |
| SQLite (source only, never an engine) | J | none; JSON as text, JSONB blob from 3.45 `[doc]` | `json_array(json_array(1,2))` `[doc]` | `json_each` key is the 0-based position `[doc]` | none documented `[doc]` |
| Exasol | J | none, and no JSON type; JSON in `VARCHAR` `[doc]` | JSON nesting | `JSON_VALUE`; `JSON_EXTRACT ... EMITS` with `#` in the path to split an array into rows `[doc]`; no constructor found | none |
| SAP HANA | ? | `ARRAY` in column tables only ("cannot create row table having array type", community answer, not the SQL reference) | not verified | the SQL reference page did not render for the fetch | |
| Db2, Teradata, Vertica, Firebird, MonetDB, SAP ASE, SQL Anywhere, generic `sqlalchemy` | ? | not checked | | Vertica and Teradata have array types `[mem]` | |
| pgwire adapters (govdata/AskAmerica, Supabase wrappers stubs) | N1 | as PostgreSQL | | the adapter decides what it exposes | as PostgreSQL |
| API and file sources | n/a | rows land in the engine's cache or a replica (`provisa/api_source/engine_cache.py:188` `[code]`) | | class of the engine that holds them | |

JSON functions for the encoded form:

| Backend | Construct | Length | Element `a[i]` | Contains | Unnest with position | Aggregate |
|---|---|---|---|---|---|---|
| MySQL, TiDB | `JSON_ARRAY(...)` | `JSON_LENGTH(c)` | `JSON_EXTRACT(c, '$[i-1]')` | `v MEMBER OF (c)` `[doc]` | `JSON_TABLE(c, '$[*]' COLUMNS (ord FOR ORDINALITY, v T PATH '$'))` | `JSON_ARRAYAGG` |
| MariaDB | `JSON_ARRAY` | `JSON_LENGTH` | `JSON_EXTRACT` | `JSON_CONTAINS` | `JSON_TABLE` (10.6+) | `JSON_ARRAYAGG` (10.5+) |
| SQL Server | `JSON_ARRAY` (2022+) | `(SELECT COUNT(*) FROM OPENJSON(c))` | `JSON_VALUE(c, '$[i-1]')` | `EXISTS (SELECT 1 FROM OPENJSON(c) WHERE value = v)`; `JSON_CONTAINS` on 2025 `[doc]` | `CROSS APPLY OPENJSON(c)`, `[key]` is the 0-based position | `JSON_ARRAYAGG` (2025, Azure) `[doc]` |
| Oracle | `JSON_ARRAY` | `JSON_VALUE(c, '$.size()')` | `JSON_VALUE(c, '$[i-1]')` | `JSON_EXISTS(c, '$[*]?(@ == $v)' PASSING ...)` | `JSON_TABLE(c, '$[*]' COLUMNS (ord FOR ORDINALITY, v PATH '$'))` | `JSON_ARRAYAGG` |
| SQLite | `json_array` | `json_array_length` | `c ->> '$[i-1]'` | `EXISTS (SELECT 1 FROM json_each(c) WHERE value = v)` | `json_each(c)`, `key` + 1 | `json_group_array` |
| Exasol | none | none found | `JSON_VALUE(c, '$[i-1]')` | via `JSON_EXTRACT` rows | `JSON_EXTRACT(c, '$[#]') EMITS (...)` | none |
| PostgreSQL `jsonb` (nested only) | `jsonb_build_array` | `jsonb_array_length` | `c -> (i-1)` | `c @> '[v]'` (GIN `jsonb_ops` `[doc]`) | `jsonb_array_elements(c) WITH ORDINALITY` | `jsonb_agg` |
| BigQuery `JSON` (nested only) | `JSON_ARRAY` | `ARRAY_LENGTH(JSON_QUERY_ARRAY(c))` | `c[i-1]` | via `UNNEST` | `UNNEST(JSON_QUERY_ARRAY(c)) WITH OFFSET` | `TO_JSON(ARRAY_AGG(...))` |

Rows for SQLite, Exasol, the MySQL index functions and the SQL Server function list are `[doc]`;
the remaining function spellings are `[mem]` and each is pinned by a test against the real
backend before its operation ships.

## 4. Where each translation happens, and who runs it

### 4.1 The three translations

| Translation | When | Runs in | Form |
|---|---|---|---|
| Landing a replica | at replication | the store | native array where the store class is N, or N1 at depth 1; otherwise the encoded form (section 6). Element reshaping is a SQL expression in the landing statement (`value_transform`, `ir_types.py:222-233`), never a Python callable. Stores loaded from staged Parquet (Databricks, Snowflake, BigQuery) receive Arrow `list<T>` and need no expression |
| Operation rewrite | at compile, once, kept with the governed plan | Provisa's compile stage | a rule table keyed by (operation, backend class); pass-through on N, JSON functions on J |
| Final projection | at compile | the serving engine | when the serving engine is class N and a projected array column is encoded in a live source it reads, the outermost projection casts it: DuckDB `CAST(c AS JSON)::T[]`, Trino `CAST(json_parse(c) AS array(T))`. The engine returns a real array |
| Result decode | at result delivery | Provisa | only when nothing upstream can produce an array (routes 3 and 5 below). Columns to decode are taken from the plan's result shape, never from looking at values |

There is one pipeline. The rewrite is a step inside the existing transpile stage of
`_govern_and_route` / `_execute_plan`; decode is a step on the result stream both terminals
already return (`provisa/executor/result.py`).

### 4.2 Per-route table

| # | Route | Array operations run in | Typed value produced by | Does Provisa touch array values | Cost |
|---|---|---|---|---|---|
| 1 | ENGINE, engine class N or N1, column native in the store or source | engine | engine | no more than today: Flight passes record batches; row surfaces already build Python values | none added |
| 2 | ENGINE, engine class N, column encoded in a live-attached source | engine, after its own cast | engine (final projection) | no | the engine parses JSON; an element filter is not pushed to the source, so the engine pulls the whole column. **Measure (M2)** |
| 3 | ENGINE, self-only engine of class J (MySQL, SQL Server, Oracle engines) | engine, JSON functions | Provisa | yes, decode | as route 5 |
| 4 | DIRECT, source class N or N1 | source | driver | no more than today | none added |
| 5 | DIRECT, source class J | source, JSON functions | Provisa | yes, decode | **Measure (M1)** |
| 6 | API route | rows land in the engine cache, then route 1 or 2 | engine | no | as route 1 or 2 |
| 7 | Response cache hit | none | stored already typed | no | none added |

### 4.3 How decode works, by surface

| Surface kind | Decode | Per-cell Python | Measured, 1M rows × 5 text elements `[probe]` |
|---|---|---|---|
| Arrow: Flight, JDBC over Flight, the DuckDB extension | the batch's JSON text column, as one Arrow string array, is cast to `list<T>` in one vectorised call. In-process DuckDB over the Arrow batch: `CAST(c AS JSON)::T[]`; nested and `DECIMAL(p,s)` elements verified | none | 0.49 s. `pyarrow.json` alternative: 0.97 s |
| JSON out: GraphQL, REST, JSON:API, MCP, NDJSON | parse to Python lists with `orjson` (already a dependency, `pyproject.toml:21`). Later: splice the JSON text into the response unparsed where the encoded spelling of the element type equals the wire spelling | one C call per array cell | 0.82 s (`orjson`), 1.27 s (`json`) |
| Row wires: pgwire, Bolt, gRPC | parse to Python lists; the wire encoder then walks them, as it does for a native array | one C call per array cell | same |

Row surfaces receive Python values for every column today (`result.py:62`), and pgwire runs a
Python converter per value (`provisa/pgwire/server.py:380-388` `[code]`). For scale: turning a
native Arrow list column into Python lists costs 3.05 s per 1M rows on the same data, against
0.42 s for a text column `[probe]`. Decoding the encoded form is cheaper than the conversion a
native array column already pays on a row surface. The vectorised path is the one that matters
for Flight, where today no Python value exists at all.

### 4.4 Verdict and the two measurements

| Question | Answer |
|---|---|
| Does array support put Provisa in the value path where it was not before? | Only on Arrow surfaces of routes 3 and 5, and there it is one vectorised cast per batch |
| Is the cost bounded? | Linear in bytes of JSON, streamed per batch, no whole-result buffering |
| What is the real exposure? | Element search on an encoded column: no index on class J, and no pushdown on route 2 |

- **M1, decode cost.** MySQL 8.4 source, 5M rows, `labels JSON` with 5 text elements, plus the
  same payload in a `TEXT` column as baseline. `SELECT id, labels` DIRECT over Flight and over
  pgwire. Report rows/s and Provisa CPU-seconds per request for both columns. Proposed gate:
  array column within 1.5× the baseline's CPU on Flight, 2× on pgwire.
- **M2, element filter.** Same table, `WHERE 'X' = ANY(labels)` at 0.1% selectivity, five ways:
  MySQL DIRECT without index; MySQL DIRECT with a multi-valued index; DuckDB engine over the
  live MySQL attach; replica in PostgreSQL with a GIN index; the child-table join. Report
  latency, rows read from the source, and bytes moved to the engine.

Both run locally first, then on the perf-bench VM.

## 5. Operations

Semantic spelling is PostgreSQL's. `e` is an element, `a`, `b` arrays, `i` a 1-based position.

| Operation | Semantic SQL | N: DuckDB / Trino / ClickHouse | N: Snowflake / Databricks / Hive | N1: PostgreSQL family / BigQuery | J |
|---|---|---|---|---|---|
| Constructor | `ARRAY[e1, e2]`, `ARRAY[]::T[]` | `[..]` / `ARRAY[..]` / `[..]` | `ARRAY_CONSTRUCT(..)` / `ARRAY(..)` | `ARRAY[..]` / `[..]` | `JSON_ARRAY(..)`; Exasol refused |
| Element | `a[i]` | `a[i]` / `element_at(a, i)` / guarded `a[i]` | `GET(a, i-1)` / `try_element_at(a, i)` | `a[i]` / `a[SAFE_OFFSET(i-1)]` | JSON path `$[i-1]`, cast to `T` |
| Length | `cardinality(a)` | `len` / `cardinality` / `length` | `ARRAY_SIZE` / `size` | `cardinality` / `ARRAY_LENGTH` | JSON length function |
| Contains | `e = ANY(a)` | `list_contains` / `contains` / `has` | `ARRAY_CONTAINS(e::variant, a)` / `array_contains` | PostgreSQL: `a @> ARRAY[e]`, so GIN can serve it `[doc]`; BigQuery: `e IN UNNEST(a)` | `MEMBER OF` / `EXISTS (... OPENJSON / json_each ...)` / `JSON_EXISTS` |
| All elements | `e = ALL(a)` | `list` lambda / `all_match` / `arrayAll` | lambda forms | `= ALL` / `NOT EXISTS (.. UNNEST ..)` | `NOT EXISTS` over the unnest form |
| Contains all | `a @> b` | `list_has_all` / `cardinality(array_except(b, a)) = 0` / `hasAll` | `ARRAY_SIZE(ARRAY_EXCEPT(b, a)) = 0` / same | `@>` / unnest form | MySQL `JSON_CONTAINS`; others refused |
| Overlap | `a && b` | `list_has_any` / `arrays_overlap` / `hasAny` | `ARRAYS_OVERLAP` / `arrays_overlap` | `&&` / unnest form | MySQL `JSON_OVERLAPS`; others refused |
| Concatenate | `a \|\| b` | `list_concat` / `a \|\| b` (emitted by Provisa, not sqlglot) / `arrayConcat` | `ARRAY_CAT` / `concat` | `\|\|` / `ARRAY_CONCAT` | MySQL `JSON_MERGE_PRESERVE`; others refused |
| Slice | `a[i:j]` | `a[i:j]` / `slice(a, i, j-i+1)` / `arraySlice` | `ARRAY_SLICE(a, i-1, j)` / `slice` | `a[i:j]` / subquery over `UNNEST WITH OFFSET` | refused |
| Unnest | `unnest(a) WITH ORDINALITY AS u(e, ord)` | native; ClickHouse `ARRAY JOIN` with `arrayEnumerate` | `LATERAL FLATTEN` (`index + 1`) / `posexplode` (`pos + 1`) | native / `UNNEST(a) WITH OFFSET` (`+ 1`) | `JSON_TABLE ... FOR ORDINALITY` / `OPENJSON` (`key + 1`) / `json_each` (`key + 1`) / Exasol `EMITS` without position: ordinality refused |
| Aggregate | `array_agg(x ORDER BY k)` | native | Snowflake `ARRAY_AGG(x) WITHIN GROUP (ORDER BY k)`; Databricks/Hive with `ORDER BY`: refused first, sorted-struct rewrite later | native | `JSON_ARRAYAGG`; with `ORDER BY` refused where the function has no ordering |
| Equality | `a = b` | native | native | PostgreSQL native; BigQuery refused | MySQL: JSON value comparison. Others: text of Provisa-written canonical JSON for text, integer, boolean, uuid elements; refused for other element types and for columns Provisa did not write |
| Ordering | `a < b`, `ORDER BY a` | native | Databricks native; Snowflake refused | PostgreSQL native; BigQuery refused | refused |
| To JSON | `to_jsonb(a)` | engine's to-JSON | same | same | identity |
| From JSON | `CAST(j AS T[])` | section 4.1 casts | `j::ARRAY(T)` / `from_json` | PostgreSQL: `ARRAY(SELECT ... jsonb_array_elements_text ...)` | identity plus the structural check |
| Position of | `array_position(a, e)` | native | native | native / unnest form | unnest form |

Rules:

- Index base is 1. A literal index is folded at compile; an expression index is emitted as
  `(expr) - 1` on 0-based backends.
- Out of range is NULL on every backend. Trino uses `element_at`, BigQuery `SAFE_OFFSET`,
  ClickHouse a guard on `length(a)`.
- A refusal is a named error raised at compile, in the family the read refusals use: code
  `query.array_operation_unsupported` with parameters operation, backend, column. No operation
  falls through to sqlglot's default rendering; the rule table is total over (operation, class),
  and a missing cell is a refusal, held by a unit test that enumerates the table.
- Class `?` backends refuse every operation in the table; a whole-value read works.

Nested arrays, what composes:

| Operation on `array<array<T>>` | N | N1 (stored as `jsonb` / `JSON`) | J |
|---|---|---|---|
| `a[i]` gives `array<T>`, then any depth-1 operation | yes | yes, via JSON path | yes |
| `a[i][j]` | yes | yes | yes |
| `cardinality(a)`, `cardinality(a[i])` | yes | yes | yes |
| `unnest(a)` one level per call | yes | yes | yes |
| `e = ANY(a)` where `e` is an array (needs array equality) | yes | refused | refused |
| `flatten(a)` one level | Trino `flatten` `[doc]`, DuckDB `flatten`, Databricks `flatten`; ClickHouse refused (`arrayFlatten` flattens all levels `[mem]`) | refused | refused |
| `array_agg` of an array column | yes | PostgreSQL: refused (it builds a multidimensional array and fails on ragged input); result typed `array<array<T>>` needs `jsonb_agg` | `JSON_ARRAYAGG` of the JSON value |

## 6. The encoded form

Recommendation: a JSON array. Not a shorter custom encoding.

| Argument | JSON array | Custom (delimited, or PostgreSQL's `{a,b}` text) |
|---|---|---|
| Functions in the database for length, element, contains, unnest | every class J row in section 3 | string split at best; none for element access with escaping |
| Index for element search | MySQL, TiDB, Oracle, SQL Server 2025 `[doc]` | none |
| Escaping | defined by the format; every backend's JSON function produces and reads it | a bespoke rule each backend would need written in SQL |
| NULL element, empty array, nesting | `null`, `[]`, nested arrays | each needs an invented spelling |
| Size | native JSON column types store a binary form (MySQL `JSON`, SQL Server 2025 `json` `[doc]`, Oracle `JSON`, PostgreSQL `jsonb`); in text it costs about 3 bytes per text element over a delimiter | smaller in text columns only |

Physical column for IR `array<T>` on a class J store is the store's physical type for IR `json`
(REQ-980): the landing reuses that path.

Spelling inside the array, written by Provisa at landing with no insignificant whitespace:

| Element type | Spelling | Why |
|---|---|---|
| NULL element | `null` | |
| text, uuid | JSON string; uuid lower-case with hyphens | |
| smallint, integer, bigint | JSON number | 64-bit integers survive MySQL, SQLite, PostgreSQL JSON; cast to the declared type on read |
| boolean | `true` / `false` | |
| float, double | JSON number; NaN and infinities refused at landing (`replica.array_element_unrepresentable`) | JSON has no spelling for them |
| numeric(p,s) | JSON **string** of the exact digits, `"12345678901234567.89"` | a JSON number is read as a double by several backends; REQ-1961 requires the declared exact decimal on every read path. Decode to `decimal128(p,s)` verified `[probe]` |
| numeric, no precision | JSON number | REQ-1961: undeclared numeric is floating point |
| date | `"YYYY-MM-DD"` | |
| timestamp | `"YYYY-MM-DDTHH:MM:SS.ffffff"`, UTC, no offset | the IR timestamp is UTC-naive (`ir_types.py:168-172`) |
| time | `"HH:MM:SS.ffffff"` | |
| interval | ISO 8601 duration string | the IR's canonical text (`ir_types.py:202`) |
| bytea | lower-case hex string, no prefix | every backend has a hex decoder; base64 does not have one everywhere |
| json | the JSON value in place | |
| nested array | a nested JSON array; a NULL inner array is `null` | |
| the column value | SQL NULL for a NULL array; `[]` for empty; the JSON text `null` is not a valid value | keeps NULL and empty apart |

A source column the registrar declared as `array<T>` (section 2.4) was not written by Provisa.
It is read with the same casts, and a structural data-quality check states whether every value
is a JSON array whose elements read as `T`. A value that is not fails the read with the
backend's own cast error; nothing is coerced.

## 7. Surfaces and wires

| Surface | Depth 1 | Nested | NULL vs empty | Notes |
|---|---|---|---|---|
| GraphQL | `[T]`, elements nullable (today's `[T!]` at `type_map.py:218` changes) | `[[T]]` | yes | filter input `ArrayFilter`: `contains`, `contains_all`, `contains_any`, `is_empty`, `length`, `is_null`. Mutations take list inputs |
| REST, JSON:API, NDJSON | JSON array | nested JSON array | yes | OpenAPI: `type: array, items` (`provisa/api/rest/openapi_spec.py`) |
| pgwire | the element type's array OID: `_bool` 1000, `_bytea` 1001, `_int2` 1005, `_int4` 1007, `_text` 1009, `_int8` 1016, `_float4` 1021, `_float8` 1022, `_timestamp` 1115, `_date` 1182, `_time` 1183, `_interval` 1187, `_numeric` 1231, `_uuid` 2951, `_jsonb` 3807 `[mem]`, read from `pg_type` at build, not hand-typed. Text format `{a,"b c",NULL}` with the quoting rules of the PostgreSQL page `[doc]`; binary format: dimension count 1, has-null flag, element OID, length, lower bound 1, then length-prefixed elements `[mem]` | `jsonb` (OID 3802), value as JSON text | yes | see 7.1. Catalog: `pg_attribute.atttypid` the array OID, `attndims` 1; `pg_type` rows with `typelem` / `typarray` (columns exist, `provisa/pgwire/catalog_columns_data.py:320-321` `[code]`). Type from the declaration, never from the first row |
| Flight / Arrow | `list<T>` with child field named `item`, nullable | `list<list<T>>` | yes: Arrow keeps a null list apart from an empty one `[doc]` | DuckDB names the child `l` (`list<l: int64>` `[probe]`), pyarrow `item`. One schema cast at the Flight boundary (metadata only) makes a read's schema equal the catalog's, the REQ-1961 rule. Never `large_list` or fixed-size |
| JDBC and catalog listings | `java.sql.Types.ARRAY`, type name `array<T>` | type name `array<array<T>>` | yes | driver code not read today; listing must equal what a read returns |
| Cypher / Bolt | PackStream list | nested list | yes | Cypher positions are 0-based; the translator converts to the semantic 1-based form (`provisa/cypher/translator.py`). `UNWIND` is `unnest`, `IN` is `= ANY` |
| gRPC | a wrapper message per element type, field `optional` | wrapper of wrappers | yes | bare `repeated` cannot carry NULL vs empty, a NULL element, or nesting; today's generator emits bare `repeated` for Trino spellings (`provisa/grpc/proto_gen.py:85-96`) |
| MCP | JSON Schema `type: array` with `items` | nested | yes | `provisa/api/mcp/model_tool_specs.py` |
| DuckDB extension (airport) | DuckDB `T[]` from Arrow list | `T[][]` | yes | `_arrow_type_to_ir` at `provisa/api/airport/server.py:928` gains the list case |
| Row rules | a rule may use any section 5 operation, e.g. `current_setting('team') = ANY(teams)` | same | | compiled through the same rewrite; on class J it is a scan per read; the rule validator states the backend class |
| Masking | whole value only at first: constant NULL or `[]`. Regex, truncate and fake on an array column are refused at model load by name (`validate_masking_rule`, `masking.py:87`) | same | | element-wise masks are a later piece: lambda on N, unnest and re-aggregate on N1 and J. A fake stays computed by the serving engine (REQ-1494) |
| Column grants | per column, as any column; no element-level grant | | | |
| Results grid, typed filters (REQ-1937) | array filter: contains value, is empty, is not empty, length comparisons; the value checklist lists distinct elements | cell shown as JSON; filter is empty / is not empty only | | the server-paged viewer sends `e = ANY(col)` |
| Search index | the type name `array<T>` is indexed with the field | | | values are not indexed |
| Data-quality checks | new checks: length between, elements in a value set, no NULL elements, and the structural check of section 6; PostgreSQL: `array_ndims(col) = 1` for a depth-1 declaration | | | external checkers read through pgwire and see PostgreSQL arrays |
| Profiler | length distribution, distinct element count | | | |
| Fakes and synthetic data | element kind plus a length distribution, computed in the serving engine (REQ-1494) | later | | refused by name until built |
| Lineage, metadata export | column-level unchanged; export as array with element type (`provisa/api/metadata_export/atlas.py`, `openmetadata.py`) | | | |
| Kafka sink, webhooks, subscriptions | JSON array in the payload | nested | yes | |
| Embedding columns | unchanged; `embedding` stays its own declaration (`models.py:858`), not `array<float>` | | | |

### 7.1 pgwire and nested arrays

PostgreSQL has multidimensional arrays, not arrays of arrays: sub-arrays "must have matching
extents for each dimension", a ragged value is a "malformed array literal" `[doc]`, and a
dimension cannot be NULL while its siblings are not. `array<array<T>>` in this design is ragged
and allows NULL inner arrays, so most of its values have no PostgreSQL array form.

Sending a multidimensional array when a value happens to be rectangular and JSON otherwise
would make the column's wire type depend on the rows. So:

- a nested array column is advertised as `jsonb` and sent as JSON text, always;
- as a PostgreSQL-family store or engine, a nested array column is a `jsonb` column;
- a PostgreSQL source column that holds multidimensional values registers at depth 1 by default
  (PostgreSQL does not record dimensions `[doc]`); the registrar declares `array<array<T>>` to
  read it nested, and the read projects `to_jsonb(col)`;
- a depth-1 declaration over a column that holds a 2-D value is a data fault: the `array_ndims`
  check reports it; engines reading it fail the cast.

Semantic `a[1]` on a nested array returns the first inner array. In PostgreSQL the same
expression on a 2-D array returns NULL. Storing nested arrays as `jsonb` avoids that
difference: the rewrite is `col -> 0`.

## 8. Governance of the change

V1: no migrations, no shims.

| Question | Answer |
|---|---|
| Control-plane schema change | none: `data_type` is already a string (`models.py:822`) |
| A column registered as `text` today that is natively an array | the stored type is whatever registration recorded. On the next re-registration or schema reconcile, introspection returns `array<T>` and the model records it. There is no reading of text as an array in between |
| Its replica | derived state: rebuilt with the new column type, not altered in place |
| Things declared against the text reading (a regex mask on it, a row rule using `LIKE`, a view casting it) | fail model validation at that re-registration, each named with the column and the reason. Nothing is rewritten for the user |
| Repository model files and demos | a demo column that is natively an array changes type in the commit that ships piece 1 |
| The cast of array columns to text in views (`type_map.py:130` comment) | removed for arrays in the same commit |

Requirement text to amend: REQ-980 ("Composite/array types ... remain collapsed to `text`"),
REQ-846 (IR vocabulary), REQ-1961 (extend "the type a catalog lists is the type a read returns"
to element type and depth), REQ-1937 (array filter), REQ-1494 (fakes on arrays). No requirement
was filed by this design.

Guards and tests the build will trip, to read before starting:

| Guard / test | Why |
|---|---|
| `tests/unit/test_ir_types.py`, `tests/unit/test_discovery_through_driver.py` | pin `ARRAY`/array → `text` and the collapsed set |
| `tests/unit/pgwire/test_server.py`, `test_extended_protocol_portals.py`, `test_catalog.py` | pin `INTEGERARRAY`/`STRINGARRAY`, first-row inference, list → JSON |
| `tests/unit/test_proto_gen.py` | pins bare `repeated` |
| `tests/unit/test_ops_domain.py` | mentions the collapsed spellings |
| `tests/unit/test_no_per_request_sql_derivation.py` (REQ-1877) | the rewrite and the decode column set must be derived once and kept with the governed plan; a per-request parse fails this guard |
| the engine-kinds equality test (`provisa/core/engine_kinds.py:14-16`) | the array class per engine kind needs the same held-equal test, so a new engine kind cannot ship without a class |
| import layering | type parsing lives in `provisa/core`; the per-backend class and rule table live in the engine layer, which core may not import |
| complexity and length ratchet | `provisa/pgwire/server.py` and `ir_types.py` should not grow: array wire codec and the type parser go in new modules |
| the "never a silent default" rule | an unknown element type, a missing rule-table cell and an unrepresentable element all raise by name |

## 9. Build order

Each piece ships alone. L = unit tests run locally (DuckDB in-process and SQLite files need no
container). C = container or credential lanes in CI.

| # | Piece | Value on its own | Tests |
|---|---|---|---|
| 1 | Type vocabulary: parse and print `array<T>` incl. nested and sized elements; `to_ir` for every spelling in 2.1; Arrow `list<T>`; registration records `array<T>` (PostgreSQL via `format_type`, DuckDB, Trino); catalogs list it (Flight, pgwire `pg_type`/`pg_attribute`, GraphQL, JDBC type name) | registration stops raising on DuckDB/ClickHouse/BigQuery spellings; catalogs state the real type | L |
| 2 | Whole-value read, depth 1, on DuckDB / PostgreSQL / Trino: Arrow list with the `item` cast; pgwire array OIDs, text and binary, typed from the declaration; GraphQL and JSON lists | a list column arrives as a list on every wire | L for DuckDB and the codecs; C for PostgreSQL, Trino |
| 3 | Operations on the same three backends: the rule table, 1-based access, NULL out of range, named refusals, the table-is-total test | arrays usable in semantic SQL, row rules and views | L snapshot tests of emitted SQL, L execution on DuckDB; C for PostgreSQL, Trino |
| 4 | Encoded form: landing into a class J store, canonical spelling, structural check, the final-projection cast (route 2), result decode (routes 3 and 5), rules for MySQL and SQL Server; M1 and M2 | arrays on stores without them | L for spelling, decode, and route 2 with a SQLite file under the DuckDB engine; C for MySQL, SQL Server |
| 5 | Nested: `jsonb` on PostgreSQL family, pgwire `jsonb`, nested rules, depth cap | `array<array<T>>` end to end | L on DuckDB; C on PostgreSQL |
| 6 | Surfaces: GraphQL `ArrayFilter`, grid filters (REQ-1937), gRPC wrappers, MCP, Bolt and the Cypher index conversion, JSON:API | arrays filterable from every client | L; Playwright in CI |
| 7 | Governance: row-rule validation with array predicates, whole-value masks and the refusals, element checks in data quality, profiler | governed arrays | L |
| 8 | Remaining backends: ClickHouse, Snowflake, BigQuery, Databricks, Oracle, MariaDB, SQLite source rules, Exasol; each `?` row verified or left refused | breadth | C, warehouse-credential lanes |
| 9 | Element-wise masks, fakes and synthetic arrays | parity with scalar columns | L on DuckDB; C |
| 10 | Mail sources move their list columns to `array<text>` (section 10) | lists on the wire, `= ANY` filters | L |

## 10. The mail sources

Not verified in code: on `feat/gmail-source` only settings and OAuth grants exist
(`provisa/google_workspace/settings.py`, `provisa/api_source/oauth_grants.py`); the canonical
schema is taken from the lead's description (list columns as JSON text, child tables for joins).

| Step | Effect |
|---|---|
| The column's declared type changes from JSON text to `array<text>` | the stored payload does not change: the canonical encoded form of `array<text>` is a JSON array of strings |
| In a class N or N1 engine cache or replica it lands as a native array | `'INBOX' = ANY(label_ids)`, lists on GraphQL / Arrow / pgwire |
| Child tables stay | they remain the join path with their own keys and indexes; the array is the same fact in list form: a child table's rows equal `unnest(col) WITH ORDINALITY` |

To keep that move free of any change in meaning, the mail schema should hold to four things
now: the JSON text is an array of scalar strings (an address list of `{name, address}` objects
is `array<json>` or stays child-table only); an empty list is `[]` and an absent field is SQL
NULL, decided once; the child table carries the element position; nothing shipped (view, row
rule, mask) uses a text operation such as `LIKE` on the JSON text.

## 11. Decisions for the maintainer

Recommendation first in each row.

| # | Decision | Choices |
|---|---|---|
| D1 | Encoded form | **JSON array**; a shorter custom encoding |
| D2 | Nested arrays on PostgreSQL family and on pgwire | **`jsonb` column and `jsonb` wire type**; PostgreSQL multidimensional arrays with ragged values refused; no nested arrays on those backends |
| D3 | struct and map | **out, with `array<json>` and `json` as the bridge**; add a struct type now |
| D4 | Index base | **1, PostgreSQL's**; 0 |
| D5 | DIRECT read from a source without arrays | **Provisa decodes at the result boundary (0.5 to 1.3 s per 1M rows measured)**; route such reads through the federation engine so it casts; refuse typed surfaces on that route |
| D6 | A JSON/text column in a source without arrays | **the registrar may declare it `array<T>`, validated by a check, never inferred**; only natively-array columns can be arrays |
| D7 | Stores that cannot keep NULL apart from empty, or NULL elements, natively (BigQuery `[doc]`, ClickHouse `[mem]`) | **native only when the model declares the column and its elements not null, otherwise the encoded form**; always native and document the loss; always encoded |
| D8 | gRPC | **wrapper messages**; bare `repeated`, losing NULL vs empty and nesting |
| D9 | Columns registered as text that are natively arrays | **change at the next re-registration or reconcile, dependents fail validation by name**; per-column opt-in |
| D10 | Masks on array columns at first | **whole-value constant only, the rest refused by name**; element-wise from the start |
| D11 | Depth cap | **3**; 2; none |
| D12 | Element nullability | **always nullable; `not null` only as the column-level declaration D7 needs**; a `not null` element qualifier in the type |

## 12. Probe and sources

Local probe, 2026-10-09, macOS laptop, pyarrow 24.0.0, duckdb 1.5.5, sqlglot 30.9.0, 1M rows of
`["INBOX","UNREAD","Label_n","CATEGORY_UPDATES","a \"q\" b"]`:

| Step | Seconds |
|---|---|
| `json.loads` per cell | 1.27 |
| `orjson.loads` per cell | 0.82 |
| `json.loads` per cell, then `pa.array(..., list<string>)` | 2.01 |
| `pyarrow.json.read_json` with explicit schema (includes a Python-side join of the buffer) | 0.97 |
| DuckDB in-process `CAST(c AS JSON)::VARCHAR[]` over the Arrow table, to Arrow | 0.49 |
| `to_pylist()` of the resulting list column | 3.05 |
| `to_pylist()` of the text column | 0.42 |

DuckDB decode of `[[1,2],[3],[],null]` to `BIGINT[][]` and of `["12345678901234567.89","0.10"]`
to `DECIMAL(20,2)[]` returned the exact values.

Vendor pages fetched:

- PostgreSQL arrays: https://www.postgresql.org/docs/current/arrays.html
- PostgreSQL GIN operator classes: https://www.postgresql.org/docs/current/gin.html
- DuckDB LIST: https://duckdb.org/docs/current/sql/data_types/list.html
- Trino array functions: https://trino.io/docs/current/functions/array.html
- ClickHouse Array: https://clickhouse.com/docs/sql-reference/data-types/array
- BigQuery data types, Array type: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types
- Snowflake semi-structured types: https://docs.snowflake.com/en/sql-reference/data-types-semistructured
- Snowflake structured types: https://docs.snowflake.com/en/sql-reference/data-types-structured
- Databricks ARRAY: https://docs.databricks.com/aws/en/sql/language-manual/data-types/array-type
- MySQL 8.4 multi-valued indexes: https://dev.mysql.com/doc/refman/8.4/en/create-index.html
- TiDB index selection: https://docs.pingcap.com/tidb/stable/choose-index/
- MariaDB JSON type: https://mariadb.com/kb/en/json-data-type/
- SQL Server JSON: https://learn.microsoft.com/en-us/sql/relational-databases/json/json-data-sql-server
- SQL Server CREATE JSON INDEX: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-json-index-transact-sql
- Oracle indexes for JSON: https://docs.oracle.com/en/database/oracle/oracle-database/23/adjsn/indexes-for-json-data.html
- SQLite JSON: https://www.sqlite.org/json1.html
- Exasol JSON_EXTRACT: https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/json_extract.htm
- CockroachDB ARRAY: https://docs.cockroachlabs.com/docs/stable/array
- Redshift SUPER: https://docs.aws.amazon.com/redshift/latest/dg/r_SUPER_type.html
- Hive types: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
- Arrow columnar format: https://arrow.apache.org/docs/format/Columnar.html

Not verified today: SAP HANA (reference page did not render), Fabric Warehouse, SingleStore,
Db2, Teradata, Vertica, Firebird, MonetDB, SAP ASE, SQL Anywhere; the ClickHouse `Nullable`
and out-of-range behaviour; BigQuery array equality; the PostgreSQL binary array layout and the
OID list; the JDBC driver's array handling; every function spelling tagged `[mem]`.
