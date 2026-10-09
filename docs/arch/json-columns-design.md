# Nested data as JSON columns, with hidden array attributes — design

Status: final design, 2026-10-09. Answers REQ-1965 as rewritten on origin/main the same day. No
product code. Supersedes the earlier text of this file and `json-columns-v3-note.md`. On record
beside it: `array-types-design.md` (typed arrays, not adopted) and
`array-of-primitives-addendum.md` (an array type on the wire, not adopted).

The design in six lines:

- Semantic SQL has no array, struct or map type. Nested data is a JSON column, listed as JSON by
  every catalog and wire, on every engine and every source read directly.
- A datasource hands a native array, struct or map over as JSON, by a cast the source or engine
  executes.
- A column carries three hidden attributes, recorded from the source at registration: array
  depth (integer), innermost element type, native type.
- One portable set of JSON operations, translated per backend from one rule table. For an array
  of a primitive the attributes drive casts in and out and the native form.
- A refusal by name is for what a backend cannot do correctly, never for what it does slowly.
- What is correct but slow runs, and the query plan says so.

Evidence tags: `[code]` read in the repo at the cited line; `[doc]` vendor page fetched today;
`[probe]` run locally today; `[mem]` from memory, not fetched, unverified. Sources in section 16.

## 0. The claim, and where "always" stops

Claim: with the attributes available to the translation, a statement is always translated to the
source's own query and returns values in the expected type, and Provisa converts no value.

| Part of the claim | Holds | Stops at |
|---|---|---|
| Translated to a native query | every backend that has either JSON functions or a native array form for the operation | backends with neither, and operations a backend lacks (section 4.3): refused by name at compile |
| Results in the expected type | an innermost element of an array of a primitive, extracted or unnested, is cast in the generated SQL to the element type | the whole column value is JSON text on SQL wires; inside a struct, map or array of structs nothing is typed and the user writes the cast |
| Exact decimals | a decimal element with a declared precision is written into the JSON as a string and cast back | inside a struct or map, where a decimal is a JSON number: DuckDB and SQLite read one through a double (`12345678901234567.89` comes back `…567.68`) `[probe]` |
| Native speed and indexes | reads at the source of a natively-array column, for element, length, contains and unnest (section 4.2) | a federating engine over a live source: pushdown is the engine's choice. The statement runs; the plan says so (section 10) |
| The attributes are right | recorded from the source's own type at registration | the source's type changing afterwards: reads of the column refused by name until it is registered again (section 5.4) |
| Provisa converts no value | once a source or engine has produced a value, on every route (section 8) | API and stream sources, where Provisa's own caller fetches the payload; two existing touches removed by piece 6 (section 8) |
| No per-request derivation (REQ-1877) | the attributes are model data, read when the plan is compiled and kept with the governed plan | — |

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
3. The three hidden attributes on the column.
4. Physical type gaps: PostgreSQL lands `JSON`, which has no index support (`jsonb` supports
   GIN, btree and hash; `json` does not `[doc]`); Fabric/Synapse lands `VARCHAR(8000)`; Oracle
   has no mapping.
5. At the GraphQL boundary, JSON parsing decided by the column's declared type, not by the first
   character of a string (a defect on its own, section 14).

## 2. The model

### 2.1 The three hidden attributes

| Attribute | Value | Read by |
|---|---|---|
| array depth | integer. 0: not an array of a primitive. 1: array of a primitive. 2: array of arrays of a primitive. And so on | every translator: which casts apply, whether a native form exists |
| element type | the semantic SQL type of the innermost element, with precision and scale where it has them (REQ-1961 spelling): `text`, `bigint`, `numeric(18,2)`, `timestamp`. Empty at depth 0 | cast out, cast in, the element pre-cast of the datasource cast |
| native type | the source's type in its own spelling: `varchar(20)[]`, `timestamptz[]`, `array(row(a integer, b varchar))`, `STRUCT<..>` | the three casts that depend on the exact spelling (section 5.3), the to-JSON cast for depth-0 native columns, drift |

| Source column | depth | element type | native type |
|---|---|---|---|
| array of one primitive type, any nesting | ≥ 1 | the primitive | recorded |
| struct, map, array of structs, array of mixed types | 0 | — | recorded |
| JSON at its source (MySQL `JSON`, PostgreSQL `jsonb`, a text column holding JSON, an API payload) | none of the three | | |
| any scalar column | not a JSON column; outside this design | | |

Rules:

- Visible type: `data_type` is `json` for all of the above. Registration suggests `json`, never
  text, and never raises on a native nested type.
- The attributes come from a native type only. A registrar does not declare them on a column
  that is JSON at its source.
- Stored with the column, beside `data_type` (`provisa/core/models.py:822` `[code]`), as
  `object_fields` is (`provisa/core/schema_org.py:341` `[code]`). V1: the table definition
  changes; no migration.
- Hidden: not offered in the registration form, not listed by catalogs. A read-only line in the
  column detail ("array of text, depth 1, native `text[]`") is allowed for support.
- Whether a column is natively nested and so needs the to-JSON cast is: native type recorded.

`Column.object_fields` (`models.py:809-816,845`) is not touched. It declares typed sub-fields of
an object column for GraphQL, with alias, description and `visible_to` per sub-field, and keeps
doing that. GraphQL serves a JSON column as the opaque `JSON` scalar. There is no JSON Schema.

### 2.2 Does any backend have an array of more than one element type

No supported backend has an array type parameterised by several element types. Arrays are
homogeneous: DuckDB lists "must all have the same underlying type" `[doc]`; Trino `ARRAY` is "an
array of the given component type" `[doc]`; PostgreSQL, ClickHouse `Array(T)`, BigQuery
`ARRAY<T>`, Databricks and Hive `ARRAY<T>` take one element type `[doc]`.

| Nearest thing | What it is | Registers as |
|---|---|---|
| Snowflake `ARRAY` | "Each value in a semi-structured array is of type VARIANT" `[doc]`: elements may differ at run time | depth 0, plain JSON. Structured `ARRAY(T)` is depth 1 |
| Redshift `SUPER` | arrays of scalar or complex values "without having any restrictions for regularity" `[doc]` | JSON at its source |
| ClickHouse `Array(Variant(T1, T2))` | a `Variant` value is "either type T1 or T2 ... or none of them" `[doc]`; arrays of variants are shown `[doc]` | depth 0 |
| DuckDB `UNION` | a tagged union holding "one of multiple alternative values" `[doc]`; a list of unions is not stated on the page | depth 0 |
| Trino `ROW`, ClickHouse `Tuple`, PostgreSQL composite | fixed positions, each with its own type; Trino: "allows mixed types" `[doc]` | depth 0 (a struct) |
| PostgreSQL `anyarray` | a pseudo-type for function signatures, not a column type `[mem]` | not a column |
| JSON arrays | heterogeneous everywhere | JSON at its source |

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
| MySQL family, SQL Server family, Oracle, SQLite, Exasol | no native arrays | none: the column is JSON or JSON text already; none of the three attributes | |
| API and file sources (OpenAPI, GraphQL remote, gRPC remote, Kafka, MongoDB, Elasticsearch) | arrays and objects in the payload | none: already JSON; none of the three attributes | `provisa/openapi/mapper.py`, `provisa/kafka/source.py:184-208` `[code]` |

Exact types inside the JSON. Each backend prints temporal and decimal values its own way, and a
client reads the JSON text, so the translation fixes the spelling before the to-JSON call. It is
generated from the element type, for columns of depth 1 or more, as SQL:

| Element type | Spelling written | Why not the backend's default |
|---|---|---|
| numeric(p,s) | JSON string, exact digits | DuckDB writes a JSON number with exact digits but reads a JSON number back through a double `[probe]`; SQLite reads it as `real` `[probe]`; PostgreSQL `jsonb` keeps numbers as `numeric` `[doc]`. A string is exact on all of them |
| timestamp | `"YYYY-MM-DDTHH:MM:SS.ffffff"`, UTC | DuckDB writes `"2026-10-09 12:34:56.123456"` with a space `[probe]` |
| date, time, uuid, interval | ISO 8601 text | |
| binary | lower-case hex | DuckDB writes `"\\x00ff"` `[probe]` |
| float, double | JSON number | DuckDB writes `NaN` unquoted, which is not JSON `[probe]`; `jsonb` rejects NaN and infinity `[doc]`. Refused by name: `json.non_finite_number` |
| integer up to 64 bits | JSON number | exact through DuckDB (`9007199254740993` verified `[probe]`), SQLite `[probe]` |

Form: cast the native value to the same shape with those element types as text, then to JSON.
DuckDB: `to_json(list_transform(c, x -> strftime(x, ...)))` or a shape cast
`CAST(c AS STRUCT(a VARCHAR, b VARCHAR[]))`. PostgreSQL: `to_jsonb(c::text[])` for a numeric
array. Columns whose elements are text, integer or boolean need no pre-cast.

A column of depth 0 with a native type (struct, map, array of structs, mixed array) gets the
plain to-JSON expression with no pre-cast: nothing inside it is typed. Its numbers are JSON
numbers and its timestamps are in the backend's own spelling.

A timestamp element keeps its instant: the pre-cast applies the native type's own transform
(a timestamp with time zone is taken to UTC, `provisa/core/ir_types.py:164-172` `[code]`) before
it is written.

Replicas. A replica of such a column is landed by the same expression in the landing statement:
the store receives JSON and stores its physical JSON type (`jsonb` on PostgreSQL). The replica's
column keeps depth and element type, so casts in and out still apply; reads of a replica use the
JSON form.

## 4. The portable JSON operations

### 4.1 What semantic SQL offers

Semantic SQL is PostgreSQL dialect. Offer the SQL/JSON standard functions, which PostgreSQL has
`[doc]`, and accept PostgreSQL's operators as other spellings of the same internal operations.
JSON positions are 0-based, as in JSON path and in PostgreSQL's `->`.

| Internal operation | Standard spelling | PostgreSQL spelling also accepted | Result type |
|---|---|---|---|
| element or field, as JSON | `JSON_QUERY(c, '$[0]')` | `c -> 0`, `c -> 'a'` | json |
| scalar, typed | `JSON_VALUE(c, '$[0]' RETURNING T)`; for an innermost element of an array of a primitive `RETURNING` may be omitted and is then the element type | `c ->> 0` (text, as PostgreSQL) | T |
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

### 4.2 Native form: depth 1 or more, read at the source

When the statement runs on the source and the column there is natively an array of a primitive,
element access, length, contains and unnest are translated to the native operation and no JSON
is built. The "field" column of the table is not used by this design: a struct or map is depth 0
and always goes through the to-JSON expression. Depth above 1 is section 6. Positions shift by one where the
backend counts from 1. This is what keeps the source's own indexes usable.

| Backend | element `[i]` | field | length | contains `v` | each with position | index that serves contains |
|---|---|---|---|---|---|---|
| PostgreSQL family | `c[i+1]` | `(c).f` | `cardinality(c)` | `c @> ARRAY[v]` | none: always the JSON form, `jsonb_array_elements(to_jsonb(c)) WITH ORDINALITY` (ruling, section 6.2) | GIN `array_ops`: `&&`, `@>`, `<@`, `=` `[doc]`. `v = ANY(c)` is not served, so it is not emitted |
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

## 5. Casts in and out, driven by the attributes

Applies to columns of depth 1 or more. `T` is the element type.

### 5.1 Out

| Expression | Generated |
|---|---|
| `JSON_VALUE(c, '$[0]')` on a depth-1 `timestamp` array | JSON form: the backend's typed extraction to `to_physical(T, dialect)`. Native form: `c[1]`, with the native type's transform |
| `JSON_TABLE(c, '$[*]' COLUMNS (v PATH '$'))` | `v` typed `T` |
| a `numeric(p,s)` element | JSON form: the string cast to `DECIMAL(p,s)` in the engine. Native form: the element itself |
| the whole column | the to-JSON expression of section 3; type `json` |

The plan states the result's column types from the attributes. No surface types a column from
the rows it receives.

### 5.2 In

| Expression | Generated |
|---|---|
| `JSON_VALUE(c, '$[0]') > '2026-01-01'` | the literal cast to `T` |
| contains with a parameter | native form: the parameter cast to the column's native element type; MySQL: `CAST(? AS CHAR) MEMBER OF (c)` |
| a literal that does not read as `T` | refused at compile (`query.json_literal_type`) |
| a write to a natively-array column | the JSON given is cast to the native type by the source: PostgreSQL `ARRAY(SELECT jsonb_array_elements_text($1))::<native type>`, DuckDB `from_json`, Trino `CAST(json_parse(?) AS <native type>)` |

### 5.3 Where the native type, not the element type, decides the cast

| Case | Why |
|---|---|
| timestamp with time zone against timestamp without | both are semantic `timestamp`; the UTC transform is keyed by the native type and applied to the wrong kind it shifts the value |
| PostgreSQL array operators | both sides must be the same array type: `varchar[] @> text[]` has no operator `[mem]`; the literal side is cast to the native element type |
| writes | the target of the cast is the native type (`char(10)[]`, DuckDB `INTEGER[3]`) |

### 5.4 Drift

Schema reconcile re-introspects. Where the source's type no longer equals the recorded native
type, the column is marked and every read that references it is refused by name
(`model.json_column_drift`: table, column, recorded type, current type) until it is registered
again. Between reconciles a changed type surfaces as the source's own cast error, since every
cast is explicit in the SQL. No cast is adjusted to fit.

### 5.5 How the attributes reach the translation, and REQ-1877

Per-column types already travel in the governance context as `(name, type)` pairs
(`gov_ctx.all_columns`, read at `provisa/compiler/stage2.py:488,517,558` `[code]`); the
attributes travel beside them. Nothing under `provisa/transpiler` reads column types today, and
the rewrite needs the routed backend, known only after routing. So stage 2 records on the plan
the attributes of each JSON column the statement references, and the rule-table rewrite in the
transpile stage reads that map with the dialect and whether the address read is the source or a
replica. Derived once, kept with the governed plan. Re-registering a column bumps the schema
version, which already invalidates kept plans.

## 6. Depth above 1

### 6.1 Which backends state depth in the type

| Backend | Depth in the type | Recorded depth |
|---|---|---|
| DuckDB | `INTEGER[][]` `[probe]` | as stated |
| Trino | `array(array(T))` `[doc]` | as stated |
| ClickHouse | `Array(Array(T))` `[doc]` | as stated |
| Databricks, Hive | `array<array<T>>` `[doc]` | as stated |
| Snowflake structured | `ARRAY(ARRAY(T))`: the element may be a structured ARRAY `[doc]` | as stated |
| BigQuery | "Arrays of arrays aren't allowed" `[doc]`; nesting needs a struct between, which is an array of structs | 1 at most; the struct form is depth 0 |
| CockroachDB | "does not support nested arrays" `[doc]` | 1 |
| PostgreSQL, Greenplum, YugabyteDB, openGauss | no. The type records no dimensions; `pg_attribute.attndims` is "not enforced, so any nonzero value effectively means it's an array" `[doc]` | always 1 |

### 6.2 PostgreSQL: a multidimensional value in a depth-1 column

| Read | What happens |
|---|---|
| whole value | `to_jsonb` writes "arrays of arrays" `[doc]`: correct JSON, deeper than the recorded depth says |
| JSON form (replica, or any expression kept as to-JSON) | follows the JSON: `$[0]` is an inner array, so `JSON_VALUE` of it is not a scalar and gives NULL; each yields inner arrays |
| native form, element / length / contains | PostgreSQL's own rules: a 2-D array subscripted once gives NULL, as the JSON form does; `@>` looks at innermost elements `[mem]` |

Ruling (maintainer, 2026-10-09): on the PostgreSQL family unnest is always done through JSON,
`to_jsonb` first and then the JSON each. There is no native unnest there, so the rows an unnest
returns never depend on the dimensions of the value. Element access, length and contains keep
their native form. The `array_ndims(c) = 1` data-quality check stays available to a registrar
who wants to know whether a column holds such values.

### 6.3 How operations address a level

The JSON path states the level. `d` is the recorded depth.

| Operation | Addressing | Typed |
|---|---|---|
| element | `$[i]` is a level-1 element; `$[i][j]` level 2 | cast to `T` when the path has `d` array steps; with fewer steps the result is JSON (`JSON_QUERY`). `JSON_VALUE` with fewer than `d` steps is refused at compile (`query.json_path_not_scalar`: column, depth) |
| length | `jsonb_array_length(c)` is the outer length; of an inner array: `jsonb_array_length(c -> i)` | integer |
| contains | `JSON_EXISTS(c, '$[*] ? (@ == $v)')` at depth 1; `'$[*][*] ? (@ == $v)'` asks whether any inner array holds `v` | `$v` cast to `T` when the path has `d` wildcard steps |
| unnest | `JSON_TABLE(c, '$[*]' ...)` gives one row per level-1 element (an inner array at `d` > 1); `NESTED PATH '$[*]'` or `'$[*][*]'` goes down a level, each level with its own `FOR ORDINALITY` | innermost column typed `T` |
| construct, aggregate | JSON form on every backend; nested by nesting the calls | json |

### 6.4 Native form at depth above 1

| Backend | element `[i][j]` | contains at the innermost level | unnest |
|---|---|---|---|
| DuckDB | `c[i+1][j+1]` | `list_contains(flatten(c), v)` `[mem]` | `unnest(c)` one level per call |
| Trino | `element_at(element_at(c, i+1), j+1)` | `contains(flatten(c), v)`; `flatten` takes `array(array(T))` `[doc]` | `UNNEST` once per level, each `WITH ORDINALITY` |
| ClickHouse | `c[i+1][j+1]`, guarded by lengths | `has(arrayFlatten(c), v)` `[mem]` | `ARRAY JOIN` per level |
| Databricks | `try_element_at(try_element_at(c, i+1), j+1)` | `array_contains(flatten(c), v)` `[mem]` | `posexplode` per level |
| Snowflake structured | `GET(GET(c, i), j)` | `ARRAY_CONTAINS(v, ARRAY_FLATTEN(c))` `[mem]` | `FLATTEN` per level |
| PostgreSQL family, CockroachDB, BigQuery | none: recorded depth never exceeds 1 | | |

Each rule is the depth-1 rule applied once per level, so the rule table has no separate rows per
depth: an entry carries `max_native_depth` (1 for the PostgreSQL family, CockroachDB and
BigQuery; unbounded elsewhere), and above it the operation uses the to-JSON expression. No index
serves a contains at depth above 1 on any backend listed; the plan note says so.

## 7. Wires and surfaces

| Surface | A JSON column arrives as |
|---|---|
| pgwire | `jsonb` (OID 3802), JSON text; the catalog lists `jsonb` |
| Flight / Arrow, JDBC, DuckDB extension | utf8 holding JSON text; catalog type `json`, the same as a read returns (REQ-1961 rule) |
| Extracted scalars and `JSON_TABLE` columns, all SQL wires | their own typed columns |
| GraphQL | the opaque `JSON` scalar, as today (`provisa/compiler/type_map.py:191-192`); `object_fields` sub-fields as today |
| GraphQL, REST, JSON:API response | JSON. Where the column has no `object_fields` sub-selection to resolve, the engine's JSON text is passed into the response unparsed (`orjson.Fragment`; `orjson` is a dependency, `pyproject.toml:21`); otherwise parsed as today |
| gRPC | `string` field holding JSON |
| MCP | JSON value |
| Cypher / Bolt | a JSON string; list functions in the query map to the section 4 operations |
| Row rules | any section 4 operation; native form at the source, so an index can serve the rule |
| Masks | whole value: constant NULL. Regex, truncate and fake kinds do not apply to a JSON column |
| Column grants | column `visible_to`; sub-field `visible_to` of `object_fields` as today |
| Results grid (REQ-1937) | a cell renders the JSON; is-empty and is-not-empty filters. Typed columns from `JSON_VALUE` / `JSON_TABLE` filter as any typed column |
| Search index, metadata export | type `json` |
| Data-quality, profiler | checks and measures written with the section 4 operations |

The hidden attributes appear on no surface.

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
| Exactness inside JSON | in an array of a primitive, elements are exact: decimals with a declared precision, dates, timestamps and uuids travel as strings and are cast back. Inside a struct, a map or a source's own JSON a number is a JSON number, floating point on several backends | declare precision on numeric columns (REQ-1961); inside structs and in a source's own JSON, keep money as strings and write the cast |
| Nothing is typed inside a struct or map | the whole value and the untyped operations work; a typed value from inside needs `RETURNING` or a cast the user writes | `JSON_VALUE(c, '$.amount' RETURNING numeric(18,2))` |
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
- Notes appear with the plan only. They do not ride the warning channel of an execution.
- A note is given where the backend cannot serve the operation from an index, or where pushdown
  is not known. Every pushdown cell starts as not known.

## 11. Governance of the change

V1: no migrations, no shims.

| Question | Answer |
|---|---|
| A column registered as `text` today that is natively an array, struct or map | at the next registration or reconcile it becomes `json` with its attributes. Its replica is rebuilt |
| Things declared against the text reading (regex mask, `LIKE` in a row rule or view) | fail model validation then, each named |
| `object_fields`, `_json_schema_to_gql_type` | unchanged |
| Requirement text | REQ-1965 is the requirement. REQ-980 ("remain collapsed to text") and REQ-846 need a line pointing at it |

Guards and tests the build will trip (found by content search, to read before starting):
`tests/unit/test_ir_types.py`, `test_discovery_through_driver.py`, `test_ops_domain.py` (array →
text); `tests/unit/pgwire/test_server.py`, `test_extended_protocol_portals.py`, `test_catalog.py`
(list wire types, first-row inference); `test_no_per_request_sql_derivation.py` (REQ-1877); the
engine-kinds equality test (the rule table needs the same held-equal test per engine kind);
import layering (attribute and type parsing in `provisa/core`, rule table in the engine and
transpiler layers); the length ratchet on `transpile.py` and `pgwire/server.py`.

## 12. Build order and the rule-table shape

L = unit tests run locally (DuckDB in-process, SQLite files). C = container or credential lanes
in CI.

| # | Piece | Value alone | Tests |
|---|---|---|---|
| 1 | Registration. A parser turns every native spelling into a structured native type (kind: scalar / array / struct / map; element; fields; precision, scale). One function derives from it: `data_type` (`json` for any nested kind), depth, element type, native type. The three attributes on `Column`, in storage, loader and admin API; the suggested type in the registration picker goes through the same function. PostgreSQL family via `pg_catalog` (`format_type`, `typcategory`, `typelem`) | registering a native array, struct or map never raises and never stores `text` | L |
| 2 | The datasource cast: to-JSON expression per backend, with the element pre-cast for depth ≥ 1 (decimal strings, timestamps, hex binary; NaN and infinity raise by name). Whole-value read on every wire; landing a replica by the same expression; `jsonb` on PostgreSQL. DuckDB, PostgreSQL, Trino first | JSON on every wire from native nested columns | L for DuckDB; C |
| 3 | The rule table, JSON form, for PostgreSQL, DuckDB, Trino; casts in and out from the attributes; path-depth checks; the route rule at `router.py:273` replaced by the table; plan notes in `analyze_sql`, the SQL editor and the MCP tool | JSON operations in semantic SQL, DIRECT where the source can; the plan says what will scan | L snapshot and DuckDB execution; C |
| 4 | Native form at depth 1 for PostgreSQL (element, length, contains; never unnest), DuckDB, Trino; then depth above 1 where the backend states it; M1 and M2 | source indexes serve JSON-spelled predicates | L; C; perf-bench |
| 5 | JSON form for MySQL, MariaDB, SQL Server, SQLite source; physical type fixes (Fabric, Oracle) | sources without arrays, DIRECT | L for SQLite; C |
| 6 | Landing by SQL expression instead of the per-row parse; JSON passed through to GraphQL/REST where no sub-field handling needs it parsed; the text-column parsing defect at `serialize.py:67-72` | no value converted where avoidable | L |
| 7 | Surfaces: grid rendering and empty filters for JSON cells, gRPC, MCP, Bolt as a JSON string | JSON columns on every client | L; Playwright in CI |
| 8 | Drift refusal at reconcile | a changed source type cannot give a wrong cast | L |
| 9 | Remaining backends: ClickHouse, Snowflake, BigQuery, Databricks, Oracle, Exasol, each unverified kind, each pushdown cell | breadth | C |

Rule-table shape. One table, total over its key; a missing entry is a refusal, and a unit test
enumerates it.

| Part | Content |
|---|---|
| Key | (operation, backend, form). operation ∈ element, scalar, length, contains, each, exists, construct_array, construct_object, aggregate_array, keys, to_json, from_json. backend = the dialect or engine kind the statement is routed to. form ∈ json, native |
| Entry: `emit` | a function from the operands (sqlglot expressions), the column's attributes and the path to a sqlglot expression. Not a string template |
| Entry: `refused` | set instead of `emit` where the backend cannot do it; carries the reason text for `query.json_operation_unsupported` |
| Entry: `index` | the index kind that can serve this entry on this backend, or none |
| Entry: `pushdown` | per attached-source kind: known pushed, or not known. Starts not known |
| Entry: `max_native_depth` | native form only: 1 or unbounded |
| Entry: `verified` | whether a test against the real backend pins this entry. An unverified entry of an unverified backend is a refusal |
| Per backend | `to_json(native type)`, the element pre-cast per element type, index base, the physical JSON type |
| Selection | form = native when depth ≥ 1, the address read is the source, the source kind has arrays, and the path depth is within `max_native_depth`; otherwise json. Every column reference starts as its to-JSON expression; a recognised operation over it is replaced by the native entry; anything else keeps to-JSON |
| Outputs per statement | the rewritten expression tree; the result column types; the plan notes |

Documentation, written with the piece that makes it true (doc-writer agent):

| Page | Sections | With piece |
|---|---|---|
| `docs/modeling.md` | "JSON columns": what a native array, struct or map registers as; what is typed (arrays of a primitive) and what is not; drift | 1, 2 |
| `docs/sources.md` | per source kind: native types that become JSON, the spelling of exact values, PostgreSQL multidimensional values, what is not available | 2, 5, 9 |
| new `docs/json.md` | the operations with one example each; typed extraction; unnesting, by level; the tradeoff table of section 9 with its practices; "when to use a child table"; the per-backend table of what can use an index, what is pushed down and what is refused, generated from the rule table; M1 and M2 figures | 3, 4 |
| `docs/engines.md` | per engine: JSON physical type, index kinds, pushdown to attached sources | 3, 5, 9 |
| `docs/pgwire.md`, `docs/api-reference.md`, `docs/cypher.md` | a JSON column on each wire; what BI and JDBC clients receive and how to select typed parts | 2, 7 |
| SQL editor help, `docs/admin.md` | reading the plan notes | 3 |
| `docs/glossary.md` | JSON column, array of a primitive, child table | 1 |

## 13. Settled choices, and what remains open

Settled (maintainer, 2026-10-09, in REQ-1965): JSON columns only; three hidden attributes with
depth as an integer; structs, maps, arrays of structs and mixed arrays plain JSON with nothing
typed inside; attributes from a native type only; decimals with a declared precision as strings;
SQL/JSON standard functions with PostgreSQL operators as the same operations; drift refused by
name; plan notes with the plan only; Bolt a JSON string; GraphQL the opaque JSON scalar;
PostgreSQL lands `jsonb`; NaN and infinity raise by name.

Settled after the first final commit (maintainer, 2026-10-09):

| # | Point | Ruling |
|---|---|---|
| 1 | PostgreSQL multidimensional values in a depth-1 column (section 6.2) | unnest on the PostgreSQL family is always via JSON (`to_jsonb`, then each); no native unnest there |
| 2 | `JSON_VALUE` with fewer array steps than the column's depth | a compile error naming the column and its depth (`query.json_path_not_scalar`) |
| 3 | Backends whose to-JSON or JSON functions are unverified | registered, read refused by name, until a test pins the entry |

Nothing is open.

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
| 9 | The type the registration picker suggests is the raw native spelling lower-cased, not passed through `to_ir`; the raise surfaces later, at landing or import | `provisa/api/admin/schema_query.py:2338-2343,2368-2371,2410-2415`; raises at `provisa/federation/residency.py:87`, `provisa/api/admin/import_typing.py:53`, `provisa/federation/replica_routing.py:385` | found and cited by teammate json; not re-read by me |
| 10 | The UI's IR type map shows `json`/`jsonb` as "text" | `provisa-ui/src/irTypes.ts:36-37` | found by teammate json; not re-read by me |
| 11 | Further spellings that raise in `to_ir`: PostgreSQL `USER-DEFINED`, `text[]`; Snowflake `ARRAY`/`OBJECT`/`VARIANT`; Redshift `super`; ClickHouse `Tuple`/`Map`/`Nested`; BigQuery `STRUCT<..>`; Databricks `struct`/`map`. DuckDB `STRUCT(..)`/`MAP(..)` do reach `text` (correcting row 1: not only Trino's spelling is mapped) | `provisa/core/ir_types.py:244-255` | found by teammate json |

## 15. The mail sources

Their canonical schema is not in code yet (`feat/gmail-source` holds settings and OAuth grants
only); this follows the lead's description: list columns as JSON text, child tables for joins.

The list columns are JSON at their source (an API payload), so they carry none of the three
attributes: they are plain JSON columns, readable whole and with the untyped operations, and an
element is text unless the user casts it. For string lists that is already the right type. The
child tables stay as the indexed join path, the practice section 9 recommends. To hold: the JSON
is an array of scalar strings where it can be; an empty list is `[]` and an absent field is SQL
NULL; the child table carries the element position.

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
- PostgreSQL pg_attribute: https://www.postgresql.org/docs/current/catalog-pg-attribute.html
- ClickHouse Variant: https://clickhouse.com/docs/sql-reference/data-types/variant
- DuckDB UNION: https://duckdb.org/docs/current/sql/data_types/union.html
- Trino types: https://trino.io/docs/current/language/types.html
- Snowflake structured types: https://docs.snowflake.com/en/sql-reference/data-types-structured
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
