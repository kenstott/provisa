# Arrays, structs and maps as JSON columns with a schema — design

Status: design only, 2026-10-09. Version 2. No product code. Base: origin/main `3db8281dd`.
Version 1, the typed `array<T>` design this replaces, stays on record beside it:
`docs/arch/array-types-design.md`.

Ruling this version follows (maintainer, 2026-10-09): there is no typed array in semantic SQL,
only JSON columns. A datasource translates its native arrays, structs and maps into JSON. A JSON
column may carry a JSON Schema, generated from the source's native type or declared by a
registrar. The schema is an input to the one SQL translation, which uses it to cast in and out.
The tradeoffs of JSON-only are accepted (maintainer: "we can accept (and train) on design
tradeoffs, but this does fill a hole in semantic sql"). Section 9 states each one once, in the
form the user documentation takes.

Two rules run through the document:

- A named refusal is for what a backend cannot do correctly. Never for what it does slowly.
- A JSON operation that is correct but slow runs. The product's part is to say so: in the
  documentation, and in the query plan (section 10).

Evidence tags: `[code]` read in the repo at the cited line; `[doc]` vendor page fetched today;
`[probe]` run locally today; `[mem]` from memory, not fetched, unverified. Sources in section 16.

## 0. The claim, and where "always" stops

Claim: with the column's schema available to the translation, a statement is always translated
to the source's own query and returns values in the expected type, and Provisa touches no value.

| Part of the claim | Holds | Stops at |
|---|---|---|
| Translated to a native query | every backend that has either JSON functions or a native array/struct form for the operation | backends with neither, and operations a backend lacks (section 4.3): refused by name at compile |
| Results in the expected type | an extracted scalar or an unnested row is cast in the generated SQL to the type the schema states, so the engine returns it typed | the whole column value: on SQL wires it is JSON text, by the ruling |
| Exact decimals | when the JSON holds them as strings; the datasource translation writes them that way | a JSON **number** in a source's own JSON column: DuckDB and SQLite read it as a double (`12345678901234567.89` comes back `…567.68`) `[probe]`. Such an element is `number`, floating point, and is said to be |
| Native speed and indexes | DIRECT reads and native-engine reads of a natively-array column, for the recognised operations (section 4.2) | a federating engine over a live source: whether the engine pushes the native predicate to the source is the engine's choice. The statement still runs; the plan says so (section 10) |
| Schema is right | generated schemas are true by construction: the same translation writes the JSON | the source's type changing after registration: detected at reconcile, column refused by name (section 5.4) |
| Provisa touches no value | every SQL route and wire (section 8) | GraphQL and JSON:API parse a JSON column's text today (`provisa/executor/serialize.py:67-70` `[code]`); section 7 replaces that with splicing the engine's text |
| No per-request derivation (REQ-1877) | the schema is model data, read when the plan is compiled and kept with the governed plan | — |

The bottleneck case of the first version is gone: a DIRECT read from a source without arrays
returns JSON text and it stays JSON text. Nothing is decoded.

## 1. What "json is first-class" covers today

| Area | Today | Where |
|---|---|---|
| IR type | `json`; `jsonb` aliases to it; Trino and DuckDB overlays | `provisa/core/ir_types.py:68,83-84,110,150` `[code]` |
| Native array / list / row / struct / map | registered as `text`, not `json`. Only Trino's spelling reaches that map; `VARCHAR[]`, `_int4`, `ARRAY`, `Array(String)`, `ARRAY<STRING>` raise in `to_ir` | `ir_types.py:115-117,151-153,244`; `provisa/api/admin/introspect.py:72,1261` `[code]`, `[probe]` |
| Physical type per store | PostgreSQL `JSON` (not `jsonb`), MySQL `JSON`, SQLite `JSON` (text), SQL Server `NVARCHAR(max)`, DuckDB `JSON`, Trino `JSON`, Redshift `JSON`; Oracle: raises `UnsupportedCompilationError` | SQLAlchemy generic JSON through `to_physical` `[probe]` |
| | BigQuery `JSON`; Snowflake `VARIANT`; ClickHouse `Nullable(String)`; Databricks `STRING`; Fabric/Synapse warehouse `VARCHAR(8000)` | `bigquery_store.py:60`, `snowflake_store.py:70`, `clickhouse_store.py:56`, `databricks_store.py:66`, `mssql_warehouse_runtime.py:79` `[code]` |
| Arrow landing | string | `provisa/core/ir_arrow.py:50` `[code]` |
| Landing a JSON value | parsed per row in Python before insert, so SQLAlchemy does not double-encode | `provisa/federation/materialize_exec.py:135-138` `[code]` |
| JSON path columns | `Column.path` compiles to `->>` / `json_extract_scalar` | `provisa/core/models.py:838`, `provisa/compiler/sql_gen.py:324` `[code]` |
| DIRECT route with a JSON path | allowed on PostgreSQL only; every other direct source is sent to the engine: "query uses JSON path extraction, unsupported for direct" | `provisa/transpiler/router.py:273-283` `[code]` |
| JSON constructors | hand rewrites for Trino and DuckDB only: `json_build_object`, `json_agg`/`JSON_ARRAYAGG`, `to_json` | `provisa/transpiler/transpile.py:44-48,119-257` `[code]` |
| GraphQL | `JSON` scalar; typed sub-fields through `object_fields` on the column | `provisa/compiler/type_map.py:191-192`, `models.py:809-816,845` `[code]` |
| JSON Schema → GraphQL | exists for tracked functions (REQ-304): one level of scalar properties, or an array of such objects; schema inferred from a pasted sample in the UI (REQ-305) | `provisa/compiler/actions_schema.py:77-106`; `provisa-ui/src/pages/CommandsPage.tsx` `[code]` |
| pgwire | `JSON` and `JSONB` wire types; `STRUCT`, `MAP` and unknown `[]` types sent as JSON | `provisa/pgwire/server.py:355-358,401-402` `[code]` |
| Flight catalog | `json`, `jsonb` listed as utf8 | `provisa/api/flight/catalog.py:61-62` `[code]` |
| GraphQL result | any string starting with `{` or `[` is parsed as JSON, whatever the column's type | `provisa/executor/serialize.py:67-70` `[code]` |

What sqlglot 30.9.0 does with PostgreSQL JSON syntax `[probe]`:

| Input | Result |
|---|---|
| `c -> 0`, `c ->> 0`, `c ->> 'a'` | translated for every dialect, with defects: T-SQL gets `ISNULL(JSON_QUERY(..), JSON_VALUE(..))` for both operators; ClickHouse gets `JSONExtractString` for `->` (a string, not JSON); Oracle gets `JSON_EXTRACT`, which Oracle does not have `[mem]`; Snowflake gets `PARSE_JSON(c)` around a column that is already VARIANT |
| `c #>> '{a,1}'`, `jsonb_array_length(c)`, `c @> '[..]'`, `jsonb_array_elements(c)`, `jsonb_build_array(..)`, `jsonb_agg(..)`, `to_jsonb(..)` | passed through unchanged to every dialect: not valid there |
| `JSON_VALUE`, `JSON_QUERY`, `JSON_EXISTS` | passed through unchanged; valid only where the backend has the standard function |

What is missing for "JSON for all engines and direct query sources":

1. A translation Provisa owns for the JSON operations, per dialect. Today: PostgreSQL native, two
   engines hand-patched, DIRECT refused elsewhere.
2. Native arrays, structs and maps registered as `json` with a to-JSON cast, instead of `text`.
3. A schema on the column.
4. Physical type gaps: PostgreSQL lands `JSON`, which has no index support (`jsonb` supports
   GIN, btree and hash; `json` does not `[doc]`); Fabric/Synapse lands `VARCHAR(8000)`; Oracle
   has no mapping.
5. Type taken from the declaration at the GraphQL boundary, not from the first character of a
   string.

## 2. The model

| Field on the column | Meaning |
|---|---|
| `data_type` | `json` |
| `json_schema` | the JSON Schema of the value. Present on every column generated from a native type; optional on a source's own JSON column |
| `native_type` | the source's type as introspected (`text[]`, `array(row(a integer, b varchar))`, `ARRAY<STRING>`), or absent when the source column is itself JSON. Says whether the native form (section 4.2) applies, and is what drift is checked against |

Schema subset used (JSON Schema 2020-12 keywords):

| Native | Schema |
|---|---|
| array / list of T | `{"type":"array","items":<T>}`; nested arrays nest `items` |
| struct / row / composite | `{"type":"object","properties":{...},"additionalProperties":false}` |
| map(K, V) | `{"type":"object","additionalProperties":<V>}`; keys are strings in JSON |
| text, varchar | `{"type":"string"}` |
| smallint, integer, bigint | `{"type":"integer"}` |
| boolean | `{"type":"boolean"}` |
| float, double | `{"type":"number"}` |
| numeric(p,s) | `{"type":"string","format":"decimal","x-precision":p,"x-scale":s}` |
| numeric, no precision | `{"type":"number"}` (REQ-1961: undeclared numeric is floating point) |
| date / timestamp / time | `{"type":"string","format":"date"}` / `"date-time"` / `"time"` |
| interval | `{"type":"string","format":"duration"}` |
| uuid | `{"type":"string","format":"uuid"}` |
| binary | `{"type":"string","contentEncoding":"base16"}` |
| nullable anything | `"type":[<t>,"null"]` |

One mechanism, not two. Today the shape of a JSON column is described twice: `object_fields`
(name, type, alias, description, `visible_to`, nested; `models.py:809-816`) and, for functions,
`return_schema`. `json_schema` becomes the one description of shape. The governance attributes
`object_fields` carries (alias, description, `visible_to` per sub-field) stay, keyed by JSON
path. `_json_schema_to_gql_type` (`actions_schema.py:77`) is the converter to extend (nesting,
arrays of scalars, formats) and use for columns and functions alike.

Storage and UI: `json_schema` and `native_type` are stored with the column, as `object_fields`
is (`provisa/core/schema_org.py:341` `[code]`). The column editor shows the schema, read-only
when generated, editable when declared, with the paste-a-sample inference the commands page
already has (REQ-305).

## 3. The datasource translation

A native array, struct or map column is registered as `json`. Every reference to it in generated
SQL is the source's own to-JSON expression, executed by the source or engine. Provisa executes
nothing.

| Backend | Introspected spelling | To-JSON expression | Notes |
|---|---|---|---|
| PostgreSQL family | `format_type()` → `text[]`, composite name | `to_jsonb(c)` | "Arrays and composites are converted recursively to arrays and objects (multidimensional arrays become arrays of arrays in JSON)" `[doc]`. Multidimensional arrays stop being a special case |
| DuckDB | `VARCHAR[]`, `STRUCT(..)`, `MAP(..)` `[probe]` | `to_json(c)` | lists, nested lists, structs, maps verified `[probe]` |
| Trino | `array(T)`, `row(..)`, `map(K,V)` | `CAST(c AS JSON)` | ARRAY, MAP, ROW convert when their element types do; scalar list fetched: BOOLEAN, integers, REAL, DOUBLE, VARCHAR `[doc]` |
| ClickHouse | `Array(T)`, `Tuple(..)`, `Map(K,V)` | `toJSONString(c)` `[mem]` | |
| Snowflake | `ARRAY`, `OBJECT`, `VARIANT`, structured types | none needed: already semi-structured; `TO_JSON(c)` for text `[mem]` | |
| BigQuery | `ARRAY<T>`, `STRUCT<..>` | `TO_JSON(c, stringify_wide_numbers => TRUE)` | accepts ARRAY and STRUCT `[doc]` |
| Databricks | `array<T>`, `struct<..>`, `map<K,V>` | `to_json(c)` `[mem]` | |
| Hive / HiveServer2 | `array<T>`, `struct`, `map` | not verified | whole-value read refused until a to-JSON form is confirmed |
| Redshift | `super` | `JSON_SERIALIZE(c)` `[mem]` | |
| CockroachDB | `T[]` | `to_jsonb(c)` `[mem]` | no nested arrays `[doc]` |
| MySQL family, SQL Server family, Oracle, SQLite, Exasol | no native arrays | none: the column is JSON or JSON text already | |
| API and file sources (OpenAPI, GraphQL remote, gRPC remote, Kafka, MongoDB, Elasticsearch) | arrays and objects in the payload | none: already JSON; the schema comes from the source's own spec where it has one (OpenAPI `items`, GraphQL list types, Avro) | `provisa/openapi/mapper.py`, `provisa/kafka/source.py:184-208` `[code]` |

Exact types inside the JSON. Each backend prints temporal and decimal values its own way, and a
client reads the JSON text, so the translation fixes the spelling before the to-JSON call. It is
generated from the schema, recursively, as SQL:

| Element type | Spelling written | Why not the backend's default |
|---|---|---|
| numeric(p,s) | JSON string, exact digits | DuckDB writes a JSON number with exact digits but reads a JSON number back through a double `[probe]`; SQLite reads it as `real` `[probe]`; PostgreSQL `jsonb` keeps numbers as `numeric` `[doc]`. A string is exact on all of them |
| timestamp | `"YYYY-MM-DDTHH:MM:SS.ffffff"`, UTC | DuckDB writes `"2026-10-09 12:34:56.123456"` with a space `[probe]` |
| date, time, uuid, interval | ISO 8601 text | |
| binary | lower-case hex | DuckDB writes `"\\x00ff"` `[probe]` |
| float, double | JSON number | DuckDB writes `NaN` unquoted, which is not JSON `[probe]`; `jsonb` rejects NaN and infinity `[doc]`. See D9 |
| integer up to 64 bits | JSON number | exact through DuckDB (`9007199254740993` verified `[probe]`), SQLite `[probe]` |

Form: cast the native value to the same shape with those element types as text, then to JSON.
DuckDB: `to_json(list_transform(c, x -> strftime(x, ...)))` or a shape cast
`CAST(c AS STRUCT(a VARCHAR, b VARCHAR[]))`. PostgreSQL: `to_jsonb(c::text[])` for a numeric
array. Columns whose elements are text, integer or boolean need no pre-cast.

Replicas. A replica of such a column is landed by the same expression in the landing statement:
the store receives JSON and stores its physical JSON type. The replica's column keeps the same
`json_schema`; `native_type` is absent there, so reads of the replica use the JSON form.

## 4. The portable JSON operations

### 4.1 What semantic SQL offers

Semantic SQL is PostgreSQL dialect. Offer the SQL/JSON standard functions, which PostgreSQL has
`[doc]`, and accept PostgreSQL's operators as other spellings of the same internal operations.
JSON positions are 0-based, as in JSON path and in PostgreSQL's `->`.

| Internal operation | Standard spelling | PostgreSQL spelling also accepted | Result type |
|---|---|---|---|
| element or field, as JSON | `JSON_QUERY(c, '$[0]')` | `c -> 0`, `c -> 'a'` | json |
| scalar, typed | `JSON_VALUE(c, '$[0]' RETURNING T)`; `RETURNING` may be omitted and is then the schema's type | `c ->> 0` (text, as PostgreSQL) | T |
| length | — | `jsonb_array_length(c)` | integer |
| contains value | `JSON_EXISTS(c, '$[*] ? (@ == $v)' PASSING x AS v)` | `c @> '["x"]'`, `c ? 'x'` | boolean |
| each element, with position | `JSON_TABLE(c, '$[*]' COLUMNS (ord FOR ORDINALITY, v T PATH '$'))` | `jsonb_array_elements(c) WITH ORDINALITY`, `jsonb_array_elements_text` | rows |
| path exists | `JSON_EXISTS(c, path)` | `c ? 'key'` | boolean |
| construct array / object | `JSON_ARRAY(..)`, `JSON_OBJECT(..)` | `jsonb_build_array`, `jsonb_build_object` | json |
| aggregate into an array | `JSON_ARRAYAGG(x ORDER BY k)` | `jsonb_agg(x ORDER BY k)` | json |
| map keys | — | `jsonb_object_keys(c)` | rows |

Every one is rewritten by a rule table Provisa owns, keyed by (operation, backend, JSON form or
native form), before sqlglot generates SQL. A missing cell is a refusal. A unit test enumerates
the table so no cell falls through to sqlglot's pass-through.

### 4.2 Native form, when `native_type` is set

When the statement runs where the column is natively an array, struct or map, the operation is
translated to the native operation and no JSON is built. Positions shift by one where the
backend counts from 1. This is what keeps the source's own indexes usable.

| Backend | element `[i]` | field | length | contains `v` | each with position | index that serves contains |
|---|---|---|---|---|---|---|
| PostgreSQL family | `c[i+1]` | `(c).f` | `cardinality(c)` | `c @> ARRAY[v]` | `unnest(c) WITH ORDINALITY` | GIN `array_ops`: `&&`, `@>`, `<@`, `=` `[doc]`. `v = ANY(c)` is not served, so it is not emitted |
| CockroachDB | same | — | `array_length(c,1)` | `c @> ARRAY[v]` | `unnest` | inverted index `[doc]` |
| DuckDB | `c[i+1]` | `c.f`, `c[k]` | `len(c)` | `list_contains(c, v)` | `unnest` with `generate_subscripts` | none |
| Trino | `element_at(c, i+1)` | `c.f`, `element_at(c, k)` | `cardinality(c)` | `contains(c, v)` | `UNNEST(c) WITH ORDINALITY` `[doc]` | none |
| ClickHouse | `c[i+1]`, guarded by length | tuple element, `c[k]` | `length(c)` | `has(c, v)` | `ARRAY JOIN` with `arrayEnumerate` | bloom-filter skip index `[mem]` |
| Snowflake | `GET(c, i)` | `c:f` | `ARRAY_SIZE(c)` | `ARRAY_CONTAINS(v::variant, c)` | `LATERAL FLATTEN` | search optimization `[mem]` |
| BigQuery | `c[SAFE_OFFSET(i)]` | `c.f` | `ARRAY_LENGTH(c)` | `v IN UNNEST(c)` | `UNNEST(c) WITH OFFSET` | search index `[mem]` |
| Databricks | `try_element_at(c, i+1)` | `c.f`, `c[k]` | `size(c)` | `array_contains(c, v)` | `posexplode(c)` | none |

How the rewrite is arranged so it cannot be wrong: every reference to a native-origin column
starts as its to-JSON expression (section 3), which is always correct. A recognised pattern,
operation over that expression, is then replaced by the native form. Anything not recognised
keeps the to-JSON expression. Aggregate into an array and construct stay in JSON form on every
backend.

A native form is used only where it returns what the JSON form would: `c ->> 0` on a
`timestamp[]` returns the canonical text of section 3, so its native form is the formatted
element, not a bare `c[1]::text`.

### 4.3 JSON form, and refusals

For a column that is JSON in the backend (a source's own JSON column, or a replica).

| Backend | scalar typed | element as JSON | length | contains | each with position | aggregate |
|---|---|---|---|---|---|---|
| PostgreSQL `jsonb` | `JSON_VALUE(.. RETURNING T)` `[doc]` | `->` | `jsonb_array_length` `[doc]` | `@>`, GIN `[doc]` | `jsonb_array_elements .. WITH ORDINALITY`; `JSON_TABLE` `[doc]` | `jsonb_agg` |
| DuckDB | `(c ->> '$[i]')::T` | `->` | `json_array_length` `[doc]` | `json_contains` `[doc]` | `json_each`, key is the position `[doc]` | `json_group_array` `[doc]` |
| Trino | `json_value(.. RETURNING T)` `[doc]` | `json_query` `[doc]` | `json_array_length` `[doc]` | `json_array_contains` `[doc]` | `json_table` `[doc]` | `CAST(array_agg(..) AS JSON)` |
| MySQL, TiDB | `JSON_VALUE(.. RETURNING T)`; default is `VARCHAR(512)` `[doc]` | `JSON_EXTRACT` | `JSON_LENGTH` | `v MEMBER OF (c)`; multi-valued index `[doc]` | `JSON_TABLE .. FOR ORDINALITY` | `JSON_ARRAYAGG` |
| MariaDB | `JSON_VALUE` + cast | `JSON_EXTRACT` | `JSON_LENGTH` | `JSON_CONTAINS` | `JSON_TABLE` (10.6+) `[mem]` | `JSON_ARRAYAGG` |
| SQL Server 2016+ | `CAST(JSON_VALUE(..) AS T)` | `JSON_QUERY` `[doc]` | `(SELECT COUNT(*) FROM OPENJSON(c))` | `EXISTS (.. OPENJSON ..)`; `JSON_CONTAINS` and JSON index on 2025 `[doc]` | `CROSS APPLY OPENJSON(c)`, `[key]` | `JSON_ARRAYAGG` (2025, Azure) `[doc]`; refused earlier |
| Oracle | `JSON_VALUE(.. RETURNING T)` | `JSON_QUERY` | `JSON_VALUE(c, '$.size()')` | `JSON_EXISTS`; multivalue index `[doc]` | `JSON_TABLE .. FOR ORDINALITY` | `JSON_ARRAYAGG` |
| SQLite (source only) | `CAST(c ->> '$[i]' AS T)` | `->` `[doc]` | `json_array_length` `[doc]` | `EXISTS (.. json_each ..)` | `json_each`, `key` `[doc]` | `json_group_array` `[doc]` |
| Snowflake | `GET(c, i)::T` | `GET` | `ARRAY_SIZE` | `ARRAY_CONTAINS` | `LATERAL FLATTEN` | `ARRAY_AGG .. WITHIN GROUP` |
| BigQuery `JSON` | `CAST(JSON_VALUE(..) AS T)` `[doc]` | `JSON_QUERY` | `ARRAY_LENGTH(JSON_QUERY_ARRAY(c))` `[doc]` | via `UNNEST(JSON_QUERY_ARRAY(c))` | same, `WITH OFFSET` | `TO_JSON(ARRAY_AGG(..))` |
| ClickHouse (String) | `JSONExtract(c, i+1, 'T')` `[mem]` | `JSONExtractRaw` | `JSONLength` | `has(JSONExtract(c, 'Array(T)'), v)` | `ARRAY JOIN` over the extract | `toJSONString(groupArray(..))` |
| Databricks (STRING) | `c:[i]::T` | `c:[i]` | `size(from_json(c, ..))` | `array_contains(from_json(c, ..), v)` | `posexplode(from_json(c, ..))` | `to_json(collect_list(..))`; with `ORDER BY` refused |
| Exasol | `JSON_VALUE` + cast `[doc]` | refused | refused | via `JSON_EXTRACT .. EMITS` rows `[doc]` | `JSON_EXTRACT(c, '$[#]') EMITS (..)`; position refused | refused |

Cells without a tag are `[mem]`; each is pinned by a test against the real backend before its
operation ships there.

Refused by name (`query.json_operation_unsupported`, parameters operation, backend, column),
at compile. Each of these is something the backend cannot do, not something it does slowly:

- any operation on a backend with no verified JSON functions and no native form: SAP HANA,
  Fabric Warehouse, SingleStore, Db2, Teradata, Vertica, Firebird, MonetDB, SAP ASE, SQL
  Anywhere, the generic `sqlalchemy` kind, until each is verified. A whole-value read of a JSON
  or text column still works there.
- the cells marked refused above.
- comparing or ordering whole JSON values where the backend has no JSON comparison (text
  comparison of JSON is not offered as a substitute).
- an array operator of PostgreSQL applied to a JSON column (`v = ANY(c)`, `unnest(c)`,
  `c[1]`): refused, naming the JSON operation to use.

### 4.4 Index and pushdown facts the rule table carries

Backend classes (carried over from version 1) and what serves an element filter. The plan notes
of section 10 and the documentation are generated from these cells.

| Class | Backends | Element filter (`contains`) | Index kind |
|---|---|---|---|
| Native, nested | DuckDB, Trino, Databricks, Hive | native operation, scan | none |
| | ClickHouse | `has`, scan | bloom-filter skip index `[mem]` |
| | Snowflake, Redshift (`SUPER`) | native, scan | Snowflake search optimization `[mem]` |
| Native, one level | PostgreSQL, Greenplum, YugabyteDB, openGauss | `c @> ARRAY[v]` | GIN `array_ops` `[doc]` |
| | CockroachDB | `c @> ARRAY[v]` | inverted index `[doc]` |
| | BigQuery | `v IN UNNEST(c)`, scan | search index `[mem]` |
| JSON, indexable | PostgreSQL `jsonb` | `@>` | GIN `jsonb_ops` / `jsonb_path_ops` `[doc]` |
| | MySQL 8.0.17+, TiDB | `MEMBER OF`, `JSON_CONTAINS`, `JSON_OVERLAPS` | multi-valued index; an empty array has no entry; no range scan `[doc]` |
| | Oracle | `JSON_EXISTS` | multivalue function-based index `[doc]` |
| | SQL Server 2025, Azure SQL | `JSON_CONTAINS` | JSON index with `OPTIMIZE_FOR_ARRAY_SEARCH` `[doc]` |
| JSON, no index | MariaDB, SQL Server 2016–2022, Synapse, SQLite, Exasol, DuckDB JSON, Trino JSON, ClickHouse String, Databricks STRING | JSON functions, scan | none |
| Not verified | SAP HANA, Fabric Warehouse, SingleStore, Db2, Teradata, Vertica, Firebird, MonetDB, SAP ASE, SQL Anywhere, generic `sqlalchemy` | operations refused by name | |

Pushdown through a federating engine over an attached source is recorded per (engine kind,
source kind, operation). Every cell starts as "not pushed down" and is changed only by a test
that reads the engine's plan for that pair; none was verified today.

## 5. Casts in and out, driven by the schema

### 5.1 Out

| Expression | Generated |
|---|---|
| `JSON_VALUE(c, '$[0]')` on items `{"type":"string","format":"date-time"}` | JSON form: the backend's typed extraction to `timestamp`. Native form: `c[1]`, already a timestamp |
| `JSON_TABLE(c, '$[*]' COLUMNS (v PATH '$'))` | each column typed from the schema at its path |
| a `decimal` element | JSON form: the string cast to `DECIMAL(p,s)` in the engine. Native form: the element itself |
| the whole column | the to-JSON expression of section 3; type `json` |

The result's column types are stated by the plan from the schema. No surface types a column
from the rows it receives.

### 5.2 In

| Expression | Generated |
|---|---|
| `JSON_VALUE(c, '$.due') > '2026-01-01'` | the literal cast to the element's type: `> TIMESTAMP '2026-01-01'` |
| contains with a parameter | native form: `c @> ARRAY[$1::text]`; MySQL: `CAST(? AS CHAR) MEMBER OF (c)` |
| a literal that does not read as the element's type | refused at compile (`query.json_literal_type`), not sent to fail at the source |
| writes to a native-origin column | the JSON value given is cast to the native type by the source: PostgreSQL `ARRAY(SELECT jsonb_array_elements_text($1))::text[]`, DuckDB `from_json($1, '["VARCHAR"]')`, Trino `CAST(json_parse(?) AS array(varchar))` |

### 5.3 What the schema cannot do

- State a type the JSON does not hold. A registrar who declares `format: decimal` over a JSON
  number on a backend that reads numbers through a double is refused at declaration
  (`model.json_schema_inexact`).
- Describe a heterogeneous array beyond `items: {}`; elements are then `json` and extraction
  needs an explicit `RETURNING`.

### 5.4 Drift

`native_type` is recorded at registration. Schema reconcile re-introspects; where the source's
type no longer equals it, the column is marked and every read that references it is refused by
name (`model.json_schema_drift`: table, column, registered type, current type) until it is
re-registered. Between reconciles a changed type surfaces as the source's own cast error, since
every cast is explicit in the SQL. No cast is adjusted to fit.

### 5.5 REQ-1877

The schema and `native_type` are part of the compiled context a governed plan is anchored to.
The rewrite runs in the transpile stage, once, and its output is kept with the plan. A schema
edit bumps the schema version, which already invalidates kept plans.

## 6. Validation

| Schema | Status |
|---|---|
| Generated from a native type | true by construction: the translation that writes the JSON is generated from it |
| Declared over a source's own JSON column | descriptive. Enforced where it is used: a value that does not fit fails the engine's cast with the engine's error |
| Checked on demand | a data-quality check whose SQL is generated from the schema and run by the engine (type tests per path; MySQL has `JSON_SCHEMA_VALID` `[mem]`). Full or sampled is the check's setting. Nothing is validated row by row in Provisa |

## 7. Wires and surfaces

| Surface | A JSON column arrives as | Schema used for |
|---|---|---|
| pgwire | `jsonb` (OID 3802), JSON text | the catalog lists `jsonb`; schema shown as the column comment |
| Flight / Arrow, JDBC, DuckDB extension | utf8 holding JSON text; field metadata carries the schema | catalog type `json`, the same as a read returns (REQ-1961 rule) |
| Extracted scalars and `JSON_TABLE` columns, all SQL wires | their own typed columns | typed by the schema |
| GraphQL | a typed list or object built from the schema by the extended `_json_schema_to_gql_type`; without a schema the `JSON` scalar | field types, sub-selection, `where` inputs: `contains`, `length`, typed by `items` |
| GraphQL, REST, JSON:API response | the engine's JSON text spliced into the response unparsed (`orjson.Fragment`; `orjson` is a dependency, `pyproject.toml:21`). Sub-field selection and per-path visibility are applied in SQL, so the text is already what the reader may see | — |
| gRPC | `string` field holding JSON | field comment |
| MCP | JSON value; the column's schema is in the tool's schema | tool input and output schemas |
| Cypher / Bolt | see D8 | list functions map to the section 4 operations |
| Row rules | any section 4 operation; native form on native columns, so an index can serve the rule | literal typing |
| Masks | whole value: constant NULL. By path: a mask on a JSON path, rebuilt in SQL. Fake kinds on a path follow REQ-1494 in the engine | which mask kinds fit a path's type |
| Column grants and per-path visibility | column `visible_to`; per-path `visible_to` as `object_fields` has today | paths |
| Results grid (REQ-1937) | a cell renders the JSON; for an array of scalars the filter offers contains, is empty, length | operator set from `items` |
| Search index | type `json` and the schema's field names | |
| Data-quality | the conformance check of section 6; length and element checks | |
| Profiler | length distribution, distinct elements, through `JSON_TABLE` | |
| Fakes and synthetic data | generated in the engine as JSON from the schema | |
| Metadata export | type `json` with the schema attached | |

## 8. Per-route table: who touches a value

| Route | Column natively array/struct/map | Column JSON in the backend |
|---|---|---|
| ENGINE, native engine or store | engine runs native operations; engine builds JSON text only where the whole value is projected | engine runs JSON functions |
| ENGINE over a live-attached source | engine's native operations over the attached column; pushdown to the source is the engine's choice | engine's JSON functions over the pulled column |
| DIRECT | source runs native operations and its own to-JSON | source runs its JSON functions |
| API route | rows land in the engine cache as JSON (`provisa/api_source/engine_cache.py:188`); then as ENGINE | same |
| Response cache hit | stored result | stored result |
| **Provisa** | passes bytes | passes bytes |

Remaining touches, both existing and both removable: the JSON land path parses each JSON value
in Python before insert (`materialize_exec.py:135-138`); GraphQL result assembly parses JSON
strings (`serialize.py:67-70`). Section 3 (landing by SQL expression) and section 7 (splice)
remove them.

Measurements, as numbers for the documentation (neither is a gate):

- **M1.** Does any route still decode in Provisa? No: a pgwire, JDBC or Arrow client receives
  `json` and the bytes pass through on every route above. What M1 measures instead is the price
  of the ruling at the source: `SELECT id, labels` DIRECT from PostgreSQL where `labels` is
  `text[]`, 5M rows, over Flight and pgwire, `to_jsonb(labels)` against the same column read as
  a native array. Report the source's CPU and the bytes on the wire.
- **M2.** Contains at 0.1% selectivity on the same table: PostgreSQL DIRECT with a GIN index
  (native form); DuckDB engine over the live attach; MySQL `JSON` with and without a
  multi-valued index; a replica as `jsonb` with GIN; the child-table join. Report latency and
  rows read. These are the figures section 9's guidance quotes.

## 9. The tradeoffs, as the documentation states them

Each is stated once. The right-hand column is the recommended practice.

| Tradeoff | What the user sees | Practice |
|---|---|---|
| A JSON column is a string to SQL clients | a JDBC, BI, Arrow or pgwire client receives the whole column as JSON text, typed `json`. It does not receive a list or a struct | select the parts wanted as columns: `JSON_VALUE(c, '$.a')` arrives typed; `JSON_TABLE` gives one typed row per element. BI tools then see ordinary columns |
| An element filter may not use an index | `contains`, `JSON_EXISTS` and filters on unnested rows run as a scan on backends with no index for them (section 4.4, column "index"). They return the right answer | for a list that is filtered or joined often, register a child table (one row per element, with the position) and join it; or keep a replica on an engine that indexes JSON (PostgreSQL `jsonb` with GIN, MySQL multi-valued index) |
| A filter may not be pushed down | when a federating engine reads a live source, a JSON or array predicate may be evaluated in the engine after it pulls the column | query the source directly where routing allows; or replicate the table |
| Unnesting a large JSON column is work | `JSON_TABLE` over every row parses every value in the backend | filter first, unnest after; or the child table |
| Exactness inside JSON | text, integers, booleans, dates, timestamps, uuids and decimals with a declared precision are exact: the last four travel as strings. A JSON number is floating point | declare precision on numeric columns (REQ-1961); in a source's own JSON, store money as strings |
| Whole values do not compare | two JSON arrays cannot be ordered, and compare for equality only where the backend compares JSON | compare extracted parts |
| Some operations are missing on some backends | the statement is refused at compile, naming the operation, the backend and the column (section 4.3) | run it through an engine that has the operation, or replicate the table |
| The source serialises | a native array read whole is converted to JSON text by the source | M1 gives the cost; select parts where the whole value is not needed |

On record, what a typed array would have given and this design does not: a list type on SQL
wires; binary transfer of whole arrays; PostgreSQL array syntax (`= ANY`, `unnest`, `c[1]`) on
these columns; element statistics for planners over replicas.

## 10. The query plan points out slow JSON operations

Requirement (maintainer, 2026-10-09): the plan states, for a statement that uses a JSON
operation, when that operation will not use an index or will not be pushed down on the backend
it is routed to, naming the column, the operation and the backend, with the documented
alternative. It is advice. It never blocks or changes the statement.

### 10.1 Surfaces that exist

| Surface | Where | Carries today |
|---|---|---|
| Explain endpoint | `provisa/api/data/endpoint_dev.py:340-372` → `analyze_sql` (`provisa/executor/explain.py:414-463`) `[code]` | `route`, `route_reason`, `dialect`, `sources`, `optimizations`, `sql`, `semantic_sql`, `plan` (engine's EXPLAIN, normalised), `mermaid` |
| SQL editor plan view | `provisa-ui/src/pages/SqlPage.tsx:24,167,309` `[code]` | renders that response |
| MCP tool `explain_sql` | `provisa/api/mcp/tools.py:867` `[code]` | the same response |
| The plan object | `optimizations`, `warnings` on `_Plan` (`provisa/pgwire/_pipeline.py:198,1077`) `[code]` | kept with the governed plan |
| Statement warnings (REQ-1350) | `provisa/core/statement_warnings.py:36-45` `[code]` | `{code, params, message}` on every surface's warning channel |
| `EXPLAIN` typed over pgwire, GraphQL explain | not found today | |

### 10.2 Design

- The rule table (section 4.4) carries, per (operation, backend, form), two facts as data:
  `index` (the index kind that can serve it, or none) and `pushdown` (whether a federating
  engine of this kind passes the operation to an attached source of that kind). The notes and
  the documentation tables are both generated from it.
- When the transpile stage picks a rule for an operation, it appends a note where `index` is
  none or `pushdown` is false for the route chosen. Route and backend are known there. The
  notes are a field of the plan (`advice`), derived once and kept with the governed plan, as
  `optimizations` is. Nothing is derived per request (REQ-1877).
- A note has the shape statement warnings already have, so it is localised from the same
  catalog and is machine-readable:

| Code | Params | English text |
|---|---|---|
| `plan.json_no_index` | `column`, `operation`, `backend`, `alternative` | "`contains` on `orders.tags` runs as a scan on mysql: no index serves it there. Alternative: a child table, or a replica on an engine that indexes JSON." |
| `plan.json_index_possible` | `column`, `operation`, `backend`, `index_kind` | "`contains` on `orders.tags` can use a GIN index on postgresql if one exists. The engine plan below shows whether it did." |
| `plan.json_not_pushed_down` | `column`, `operation`, `engine`, `source` | "`contains` on `orders.tags` is evaluated by duckdb after reading the column from mysql; it is not pushed to the source. Alternative: read the source directly, or replicate the table." |
| `plan.json_serialised_by_source` | `column`, `backend` | "`orders.tags` is a native array on postgresql and is returned whole as JSON text. Select the parts needed to avoid the conversion." |

- `analyze_sql` returns them as `advice: [{code, params, message}]`, and the diagram shows each
  beside the Provisa nodes it already draws above the engine's plan. The engine's own EXPLAIN
  remains the statement of what happened (a `Seq Scan` or an index scan); the note states what
  the backend is able to do. Provisa does not look at the source's indexes.
- Notes appear in the plan. Whether they also ride the warning channel of every execution is
  decision D7.

## 11. Governance of the change

V1: no migrations, no shims.

| Question | Answer |
|---|---|
| A column registered as `text` today that is natively an array | at the next re-registration or reconcile it becomes `json` with a generated schema and `native_type`. Its replica is rebuilt |
| Things declared against the text reading (regex mask, `LIKE` in a row rule or view) | fail model validation then, each named |
| `object_fields` | its shape part is replaced by `json_schema`; model files in the repository change in the same commit |
| Requirement text to amend | REQ-980 (composites "remain collapsed to text"), REQ-846, REQ-304/305 (converter and inference shared with columns), REQ-1961 (catalog type = read type, extended to the schema), REQ-1937. No requirement was filed by this design |

Guards and tests the build will trip (found by content search, to read before starting):
`tests/unit/test_ir_types.py`, `test_discovery_through_driver.py`, `test_ops_domain.py` (array →
text); `tests/unit/pgwire/test_server.py`, `test_extended_protocol_portals.py`, `test_catalog.py`
(list wire types, first-row inference); `tests/unit/test_function_gen.py`,
`test_schema_service.py` (the JSON Schema converter); `test_no_per_request_sql_derivation.py`
(REQ-1877); the engine-kinds equality test (the rule table needs the same held-equal test per
engine kind); import layering (schema handling in `provisa/core`, rule table in the engine and
transpiler layers); the length ratchet on `transpile.py` and `pgwire/server.py`.

## 12. Build order

L = unit tests run locally (DuckDB in-process, SQLite files). C = container or credential lanes
in CI.

| # | Piece | Value alone | Tests |
|---|---|---|---|
| 1 | `json_schema` on the column: model, loader, admin API, column editor with sample inference; `_json_schema_to_gql_type` extended and used for columns; schema in catalogs | typed GraphQL for existing JSON columns; the mail sources' list columns get their schema | L |
| 2 | Registration: native array/struct/map → `json` + generated schema + `native_type`; `to_ir` parses every native spelling; whole-value read through the to-JSON expression with exact spellings. DuckDB, PostgreSQL, Trino | registration stops raising or storing `text`; JSON on every wire | L for DuckDB; C |
| 3 | The rule table with its index and pushdown facts, JSON form, for PostgreSQL, DuckDB, Trino; typed `JSON_VALUE` and `JSON_TABLE` from the schema; literal casts; the route rule at `router.py:273` replaced by the table; plan notes in `analyze_sql`, the SQL editor and the MCP tool | JSON operations in semantic SQL, DIRECT where the source can; the plan says what will scan | L snapshot and DuckDB execution; C |
| 4 | Native form for PostgreSQL, DuckDB, Trino; M1 and M2 | source indexes serve JSON-spelled predicates | L; C; perf-bench |
| 5 | JSON form for MySQL, MariaDB, SQL Server, SQLite source; physical type fixes (PostgreSQL `jsonb`, Fabric, Oracle) | sources without arrays, DIRECT | L for SQLite; C |
| 6 | Landing by SQL expression instead of the per-row parse; GraphQL splice; type from the declaration at `serialize.py` | no value touched anywhere | L |
| 7 | Surfaces: GraphQL `where` on JSON arrays, grid filters, gRPC, MCP, Bolt | filtering from every client | L; Playwright in CI |
| 8 | Governance: per-path visibility on the schema, masks by path, conformance check, drift refusal | governed JSON | L |
| 9 | Remaining backends: ClickHouse, Snowflake, BigQuery, Databricks, Oracle, Exasol, each unverified kind, each pushdown cell | breadth | C |

Documentation, written with the piece that makes it true (doc-writer agent):

| Page | Sections | With piece |
|---|---|---|
| `docs/modeling.md` | "JSON columns": what a native array, struct or map registers as; the schema, generated and declared; editing it; drift | 1, 2 |
| `docs/sources.md` | per source kind: native types that become JSON, the spelling of exact values, what is not available | 2, 5, 9 |
| new `docs/json.md` | the operations with one example each; typed extraction; unnesting; the tradeoff table of section 9 with its practices; "when to use a child table"; the per-backend table of what uses an index, what is pushed down and what is refused, generated from the rule table; M1 and M2 figures | 3, 4 |
| `docs/engines.md` | per engine: JSON physical type, index kinds, pushdown to attached sources | 3, 5, 9 |
| `docs/pgwire.md`, `docs/api-reference.md` | a JSON column on each wire; what BI and JDBC clients receive and how to select typed parts | 2, 7 |
| SQL editor help, `docs/admin.md` | reading the plan notes | 3 |
| `docs/security.md` | visibility and masks by JSON path | 8 |
| `docs/glossary.md` | JSON column, schema, native type, child table | 1 |

Common to this design and version 1: the per-dialect JSON function table, the native spelling
parser, the exact spellings, M2.

## 13. Decisions for the maintainer

Only what needs a choice. The accepted tradeoffs of section 9 are not listed. Recommendation
first.

| # | Decision | Choices |
|---|---|---|
| D1 | Spelling in semantic SQL | **SQL/JSON standard functions, with PostgreSQL's operators accepted as the same operations**; PostgreSQL operators only; standard only |
| D2 | Decimals with a declared precision inside JSON | **strings, `format: decimal`** (exact on every backend); JSON numbers (inexact through DuckDB and SQLite `[probe]`) |
| D3 | Shape description | **one `json_schema` per column, `object_fields` reduced to per-path governance attributes**; keep both, `object_fields` generated from the schema |
| D4 | A declared schema | **descriptive, with an engine-run conformance check and the engine's cast error at use**; also a CHECK constraint on replicas where the store has one |
| D5 | PostgreSQL physical type for `json` | **`jsonb`** (GIN, containment); `JSON` as today |
| D6 | Drift | **refuse reads of the column by name until re-registered**; regenerate the schema automatically at reconcile |
| D7 | Plan notes | **in the plan only**; also on the warning channel of every execution |
| D8 | Cypher / Bolt | **the JSON text as a string, as on the other wires**; parse to lists at that surface, the one place Provisa would touch a value |
| D9 | NaN and infinities in float elements | **the translation raises by name (`json.non_finite_number`)**; write them as strings |

## 14. Defects that stand on their own

Independent of this design. None filed.

| # | Defect | Where | Reproduced |
|---|---|---|---|
| 1 | `to_ir` raises `unknown SQL type` for native array spellings: DuckDB `VARCHAR[]`, `INTEGER[][]`; PostgreSQL `_int4`, `ARRAY`; ClickHouse `Array(String)`; BigQuery `ARRAY<STRING>`; Databricks `array<string>`. The DuckDB overlay's keys `list`/`struct`/`map` never match what DuckDB reports (`VARCHAR[]`, `[probe]` of `information_schema`). Only Trino's `array(T)` is mapped | `provisa/core/ir_types.py:151-153,244-255` | yes, by calling `to_ir` |
| 2 | pgwire types a list column from the first non-null row: integers → int array, strings → text array, anything else → JSON. An empty first list, or a first row whose list is empty, gives JSON while a later statement gives an array. Declared `[]` types other than `INTEGER[]` and `VARCHAR[]` are sent as JSON | `provisa/pgwire/server.py:406-431,325-326,401-402` | read in code, not run |
| 3 | The Arrow list child field is named `l` by DuckDB and `item` by pyarrow, so the same list column has two schemas depending on the route that produced it | observed from `duckdb` 1.5.5 `to_arrow_table()`; rows-to-Arrow path at `provisa/federation/runtime_support.py:140-160` uses pyarrow inference | yes for the two names; not on a Flight read end to end |
| 4 | GraphQL result assembly parses any string beginning with `{` or `[` as JSON, whatever the column's declared type; a `text` value such as `[draft]` is attempted and a text value that is valid JSON is returned as an object | `provisa/executor/serialize.py:67-72` | read in code, not run |
| 5 | A DIRECT read that uses a JSON path is sent to the engine for every dialect except PostgreSQL, including dialects with JSON functions (MySQL, SQL Server, SQLite) | `provisa/transpiler/router.py:273-283` | read in code |
| 6 | IR `json` lands as `JSON` on PostgreSQL (no index, no containment), `VARCHAR(8000)` on Fabric/Synapse (length limit), and has no Oracle mapping (`to_physical("json","oracle")` raises `UnsupportedCompilationError`) | `provisa/core/ir_types.py:68,308-320`; `provisa/federation/mssql_warehouse_runtime.py:79` | yes for PostgreSQL and Oracle, by calling `to_physical`; Fabric read in code |
| 7 | sqlglot output accepted unchecked for array and JSON operations: Trino `a \|\| b` on arrays becomes a string concatenation; Databricks `array_agg(x ORDER BY k)` loses its order; PostgreSQL JSON functions other than `->`/`->>` pass through unchanged to every dialect | `provisa/transpiler/transpile.py:41-48` and the generic `transpile` | yes, by transpiling |
| 8 | The JSON land path parses each JSON value in Python per row, against the set-based rule stated in `ir_types.py:157-160` | `provisa/federation/materialize_exec.py:135-138` | read in code |

## 15. The mail sources

Their canonical schema is not in code yet (`feat/gmail-source` holds settings and OAuth grants
only); this follows the lead's description: list columns as JSON text, child tables for joins.

Under this design nothing moves. The list columns are already JSON columns. Piece 1 gives each
its schema (`{"type":"array","items":{"type":"string"}}`), which types them in GraphQL and
drives filters. The child tables stay as the indexed join path, which is the practice section 9
recommends. To hold now: the JSON is an array of scalar strings (an address list of objects gets
an object `items` schema); an empty list is `[]` and an absent field is SQL NULL; the child
table carries the element position.

## 16. Probes and sources

Local probes, 2026-10-09, pyarrow 24.0.0, duckdb 1.5.5, sqlglot 30.9.0, SQLite 3.42.0:

| Probe | Result |
|---|---|
| DuckDB `to_json` of `DECIMAL(20,2)[]`, `TIMESTAMP[]`, `BLOB[]`, nested list, struct, map, `DOUBLE[]` with NaN | `[12345678901234567.89,0.10]`, `["2026-10-09 12:34:56.123456"]`, `["\\x00ff"]`, `[[1,2],[3]]`, `{"a":1,"b":["2026-01-01"]}`, `{"k":1}`, `[1.5,NaN]` |
| DuckDB JSON number → `DECIMAL(20,2)` | `12345678901234567.68` (inexact); from a JSON string: exact |
| DuckDB JSON `[9007199254740993]` → `BIGINT[]` | exact |
| SQLite `json_extract('[12345678901234567.89]', '$[0]')` | `1.2345678901234568e+16`, type `real`; 64-bit integer exact |
| `to_ir` on `VARCHAR[]`, `_int4`, `ARRAY`, `Array(String)`, `ARRAY<STRING>`, `array<string>` | raises; Trino `array(varchar)` → `text` |
| `to_physical("json", d)` | postgresql/mysql/sqlite/duckdb/trino/redshift `JSON`; mssql `NVARCHAR(max)`; oracle raises |
| sqlglot JSON transpilation | section 1 table |

Vendor pages fetched:

- PostgreSQL JSON functions: https://www.postgresql.org/docs/current/functions-json.html
- PostgreSQL JSON types: https://www.postgresql.org/docs/current/datatype-json.html
- PostgreSQL arrays: https://www.postgresql.org/docs/current/arrays.html
- PostgreSQL GIN: https://www.postgresql.org/docs/current/gin.html
- DuckDB JSON functions: https://duckdb.org/docs/current/data/json/json_functions.html
- DuckDB LIST: https://duckdb.org/docs/current/sql/data_types/list.html
- Trino JSON functions: https://trino.io/docs/current/functions/json.html
- Trino array functions: https://trino.io/docs/current/functions/array.html
- MySQL JSON search functions: https://dev.mysql.com/doc/refman/8.4/en/json-search-functions.html
- MySQL multi-valued indexes: https://dev.mysql.com/doc/refman/8.4/en/create-index.html
- TiDB index selection: https://docs.pingcap.com/tidb/stable/choose-index/
- MariaDB JSON type: https://mariadb.com/kb/en/json-data-type/
- SQL Server JSON: https://learn.microsoft.com/en-us/sql/relational-databases/json/json-data-sql-server
- SQL Server CREATE JSON INDEX: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-json-index-transact-sql
- Oracle indexes for JSON: https://docs.oracle.com/en/database/oracle/oracle-database/23/adjsn/indexes-for-json-data.html
- SQLite JSON: https://www.sqlite.org/json1.html
- Exasol JSON_EXTRACT: https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/json_extract.htm
- BigQuery JSON functions: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
- BigQuery data types: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types
- ClickHouse Array: https://clickhouse.com/docs/sql-reference/data-types/array
- Snowflake semi-structured types: https://docs.snowflake.com/en/sql-reference/data-types-semistructured
- Databricks ARRAY: https://docs.databricks.com/aws/en/sql/language-manual/data-types/array-type
- CockroachDB ARRAY: https://docs.cockroachlabs.com/docs/stable/array
- Redshift SUPER: https://docs.aws.amazon.com/redshift/latest/dg/r_SUPER_type.html

Not verified today: every cell tagged `[mem]`; Trino's cast to JSON for DECIMAL, DATE and
TIMESTAMP elements (the fetched list names only boolean, integers, REAL, DOUBLE, VARCHAR; the
pre-cast to text of section 3 does not depend on it); Hive's to-JSON form; SAP HANA and the
long-tail engine kinds; how the Bolt surface carries a JSON column today; the JDBC driver; the
mail sources' canonical schema (not in code on `feat/gmail-source`); whether a federating
engine pushes a native array predicate to its attached source.
