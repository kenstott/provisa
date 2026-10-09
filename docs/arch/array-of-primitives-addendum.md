# Addendum: one array type, arrays of primitives only

Status: evaluation, 2026-10-09. Third design beside `array-types-design.md` (v1, typed and
nested) and `json-columns-design.md` (v2, JSON only, REQ-1965).

Proposal (maintainer): "rdbs don't allow arrays of arbitrary types. should we just support
arrays of primitives". Semantic SQL gets one array type, `array<T>`, a single-level array of a
primitive. Array of arrays, struct, map and array of struct stay JSON columns with a schema, as
in v2.

Evidence tags as in v2: `[code]`, `[doc]`, `[probe]`, `[mem]` (unverified).

## 1. Is it the common denominator of the array-capable backends

Yes for the type. The element is whatever that backend's scalar of the same IR type is, so the
existing scalar mapping decides each element type.

| Backend | One-level array of a primitive | Limit that remains |
|---|---|---|
| PostgreSQL, Greenplum, YugabyteDB, openGauss | `T[]` for any base type | the type does not record dimensions `[doc]`: an `int[]` column can hold a 2-D value. A declared one-level column holding one is a data fault |
| CockroachDB | `T[]`; "does not support nested arrays" `[doc]` | none for this proposal |
| DuckDB | `T[]` `[doc]` | none |
| Trino | `array(T)` `[doc]` | none |
| Databricks, Hive | `ARRAY<T>` `[doc]` | 0-based; handled in translation |
| ClickHouse | `Array(T)` `[doc]` | the array itself cannot be `Nullable` `[mem]`: NULL array and empty array are one value |
| BigQuery | `ARRAY<T>`; arrays of arrays not allowed `[doc]`, which this proposal never needs | a NULL array written to a table "is converted to an empty array"; a result array with a NULL element raises `[doc]` |
| Snowflake | structured `ARRAY(T)` in standard and Iceberg tables `[doc]` | the older `ARRAY` holds VARIANT elements, untyped: registers as JSON, or as `array<T>` only if the registrar states `T` |
| Redshift | no array column type; `SUPER` `[doc]` | JSON class, not array-capable |

Primitives: text, smallint, integer, bigint, boolean, float, double, numeric(p,s), date,
timestamp are array elements on all of them. uuid, time, interval and binary follow each
backend's scalar mapping (several have no uuid type and carry text).

So two semantic gaps survive even at one level: NULL versus empty (BigQuery, ClickHouse) and
NULL elements (BigQuery). Either the type says "NULL and empty are not distinguished, elements
are not NULL" or those two backends differ from the rest.

## 2. Where `array<T>` meets a backend with no arrays

A source without arrays has no native array column, so the DIRECT route never meets one. The
meeting points:

| Meeting point | Example |
|---|---|
| A replica landed in a self-only engine without arrays | a PostgreSQL `text[]` table replicated into a region whose engine is MySQL, SQL Server, Oracle, MariaDB, TiDB, SingleStore, Exasol, Db2, SAP HANA, Fabric, Synapse, Redshift, Firebird, MonetDB, SAP ASE, SQL Anywhere, or the generic `sqlalchemy` kind |
| An API, file or stream source whose scalar list lands in that engine's cache | OpenAPI `items: {type: string}` under a MySQL engine |
| Moving a model to such an engine | engine swap; an environment on a different engine kind |

Reading another region's replicas in place is done only by DuckDB, PostgreSQL and Trino
(`provisa/core/engine_kinds.py`, `REGION_READERS` `[code]`), all array-capable, so that path is
not a meeting point.

Of the engine kinds in `ENGINE_KINDS` `[code]`, about 13 have array columns and about 17 do not.

| Choice at the meeting point | Catalog type = read type | Provisa touches a value | Model the same on every engine |
|---|---|---|---|
| A. Stored as a JSON array, served as `json` there | holds within that deployment, but the model says `array<T>` and the catalog says `json` | no | no: the column's type depends on the engine kind |
| B. Stored as a JSON array, decoded by Provisa to a list | holds | yes: 0.49 s per 1M rows to Arrow, 0.82–1.27 s to Python lists `[probe]` | yes |
| C. Refused: a table with an `array<T>` column cannot be served by an engine without arrays; the registrar registers the column as `json` to use it there | holds | no | no: such a model does not move to those engines unchanged |

No choice keeps all three. That is the original question ("where does this translation happen,
and will provisa become a bottleneck") returning for the engines without arrays.

## 3. Does Provisa touch a value

Under A or C: no, as in v2. Under B: on that one route. pgwire needs an array encoder per
element type, which works on values the wire layer already holds; Arrow passes list batches
through.

## 4. What it buys over v2, and what it costs

| Buys | Weight |
|---|---|
| A list type on pgwire, Arrow and JDBC | real for programmatic clients (drivers return lists, pandas gets `list<T>`). How well BI tools use array columns was not checked `[mem]` |
| Native indexes and speed without pattern recognition | modest: v2's native form already sends element, length, contains and unnest to the native operation; this removes the recognition step and covers expressions v2 would leave as to-JSON |
| Exact numerics without string spelling | real for arrays of `numeric(p,s)` |
| No schema needed for the common case | real: the element type is the type |
| PostgreSQL array syntax (`= ANY`, `unnest`, `c[1]`, `array_agg`) | real for users who know it |

| Costs | Note |
|---|---|
| Two mechanisms | v2 is still needed in full for nested arrays, structs, maps and arrays of structs |
| pgwire work | an array OID, text and binary codec per element type; `pg_type` rows. The three wire defects (first-row typing at `provisa/pgwire/server.py:406-431`, the `l`/`item` Arrow child name, `to_ir` spellings) must be fixed either way |
| Every surface twice | GraphQL list and its filter, gRPC (bare `repeated` cannot hold NULL versus empty), grid filters, masks, fakes, each for arrays as well as JSON |
| Two spellings, two index bases | arrays 1-based with PostgreSQL syntax; JSON 0-based with JSON functions |
| The NULL/empty and NULL-element gaps | section 1 |
| The meeting point | section 2: one of three properties is given up on about 17 engine kinds |

## 5. One operation vocabulary for both

Yes. The internal operations are the same five: element, length, contains, each (unnest),
aggregate. The rule table is keyed by (operation, backend, operand form). v2 already has two
forms, JSON and native; the native-form rows of v2 section 4.2 are exactly the `array<T>` rules.
`array<T>` adds no rows, only a second way to reach the native rows, plus the cast between
array and JSON (the to-JSON expressions of v2 section 3). The index and pushdown facts and the
plan notes are shared. What cannot be shared is the surface spelling and the index base.

## 6. Recommendation

JSON only (v2), as REQ-1965 states it.

Reason: it is the only one of the three whose model is the same on every engine kind while
Provisa touches no value and the catalog type equals the read type. `array<primitive>` gives
that up on the engines without arrays, which are more than half of the kinds supported.

If a list on the wire is wanted for the array-capable engines, it can be added after v2 without
changing v2: a natively one-level primitive array may be registered as `array<T>` instead of
`json`, choice C at the meeting point, reusing v2's native-form rules. That is one more piece at
the end of v2's build order, not a different design. Deciding it now is not required to start.

## 7. What would change in REQ-1965 under `array<primitive>` + JSON

| Sentence in REQ-1965 | Becomes |
|---|---|
| "Semantic SQL has no array, struct or map type" | one array type: a single-level array of a primitive; no nested array, struct or map type |
| "JSON is the type registration suggests" for a native array | `array<T>` for a one-level array of a primitive; JSON for nested arrays, structs, maps, arrays of structs |
| "Every column that is natively an array, struct or map carries a JSON Schema" | only those registered as JSON |
| "The type a catalog lists for such a column is JSON on every surface" | `array<T>` as a list type on pgwire, Arrow, JDBC, GraphQL for array columns |
| "Provisa touches no value on any route" | needs the meeting-point rule: A, B or C of section 2 |
| new | what an `array<T>` column is on an engine without arrays; whether NULL and empty arrays are distinguished; whether elements may be NULL |
| "A decimal with a declared precision ... keep their exact value inside JSON" | unchanged for JSON; array elements are exact natively |
