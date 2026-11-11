# Polyfill audit (REQ-2236)

Every per-engine table and form the translator held on 2026-11-11 (origin/main 565eb6b548), what it
is, which feature family it serves, and what it becomes in the polyfill grid
(`provisa/transpiler/polyfill`).

- **pattern rule**: a PostgreSQL pattern with typed holes and one replacement per engine in that
  engine's SQL, or a refusal row. Applied by tree matching and grafting.
- **function-name rule**: the row's rule is the name of a rewrite function, which matches and
  rewrites on the tree itself (REQ-2236, THE ESCAPE HATCH IS A FUNCTION NAME). Used where the
  rewrite reads the statement (its other calls, its FROM, a constant's value) or chooses by type in
  a way a pattern cannot state.
- **not a polyfill**: routing or capability knowledge that decides *where* a statement runs, not
  *how* PostgreSQL's behaviour is supplied; it stays outside the grid.

Engines in the grid: DuckDB (`duckdb`), Trino (`trino`, with `presto` and `athena`), ClickHouse
(`clickhouse`). PostgreSQL is the reference and is native for every row.

## Summary counts

| Source | Rows / entries | Family | Becomes |
|---|---:|---|---|
| `function_macros._ARRAYS` | 13 overloads | arrays (shape, fill, positions, sample, trim) | pattern rules — **moved in this job** |
| `function_macros._NUMBERS` (+ `_integer_pairs`, `_float_pairs`, `_numeric_conversions`) | 148 | numbers, casts between integer/float/numeric | pattern rules |
| `function_macros._EXACT_ROWS` + `_EXACT_OVERLOADS` | 27 rows, 11 overloads | exact numeric aggregates and math (DuckDB UDFs) | pattern rules (replacement calls a `provisa_` UDF) |
| `function_macros._REGRESSIONS` | 9 | regr_* aggregates | pattern rules |
| `function_macros._DATES` | 33 | dates, times, intervals | pattern rules; `make_interval` and `_said_scale` rows function-name |
| `function_macros._SHAPES`, `_geometric()`, `pg_forms_geometric.ROWS` | 31 + 181 (168 geometric rows) | geometric types | pattern rules generated per type; `width_bucket` row choice function-name |
| `function_macros._TSVECTOR`, `pg_forms_textsearch.ROWS` | 4 + 45 | text search | pattern rules; `pg_forms_textsearch.lowered` function-name |
| `function_macros._XML`, `pg_xml` | 16 + its call table | XML | function-name (`pg_xml.lowered` reads the statement) |
| `function_macros._MONEY`, `pg_money` | 4 + money family | money | function-name (`pg_money` rewrites by the said type of operands) |
| `function_macros._ENUMS`, `pg_forms_enum.ROWS` | 13 + 7 | enums, sequences | pattern rules; refusals (`NOT_SUPPORTED`) become refusal rows |
| `function_macros._DIGESTS`, `_hashes()`, `pg_forms_hash` | 9 + 92 | hashes and digests | pattern rules; `_bytea_input`, numeric/array input readers function-name |
| `function_macros._text_matches()`, `_comparisons()` | 24 + per-type | LIKE / regex operators by type | pattern rules (operator rows) |
| `function_macros._OTHERS`, `_ABBREVS`, `VALUE_FUNCTIONS_OF_THE_SERVER` | 5 + 3 + 24 | server state, abbreviations | pattern rules |
| `function_macros._ROWS` (own rows, not counted above) | ~280 | strings, bytes, uuid, network, snapshots, misc | pattern rules |
| `pg_forms_system.ROWS` + `_REFUSALS` | 55 + 9 | server state, regexp_replace's Nth match | pattern rules; `_REFUSALS` become refusal rows |
| `pg_forms_json.ROWS`, `REFUSED`, `lowered` | 20 rows + operators | json / jsonb | pattern rules; operator lowering and `_READ_ARGUMENT` function-name |
| `pg_forms_type_io.ROWS` | 271 | type input/output/send | pattern rules generated per type |
| `pg_forms_types.OPERATORS`, `ARRAY_OPERATORS` | 433 + 3 | operator-named functions (int4pl, …) | pattern rules generated from the operator table |
| `pg_forms_aggregate.STATE_FUNCTIONS`, `FINAL_FUNCTIONS` | 15 + 16 | aggregate state/final functions | pattern rules |
| `pg_forms_index.SUPPORT_FUNCTIONS`, `per_engine()` | 36 + 4 | btree support functions | pattern rules |
| `function_macros.OVERLOADS` (+ module OVERLOADS) | 215 | overload choice by said type | typed holes of the pattern (one rule per overload) |
| `function_macros._DIFFERS_ON`, `REFUSED_ROWS` | 9 + 4 groups | replay-refused forms | refusal rows |
| `function_macros` special rewrites in `inline` (percentiles, hypothetical sets, num_nulls, regexp_replace from the end, substring past the edge, date_part of NULL, ntile cast, `_columns_of`) | 9 | ordered-set aggregates, strictness, edge cases | function-name rules |
| `engine_refusals._NOT_HELD` | trino 8, clickhouse 9 types | types an engine cannot hold | write-side refusal rows (per type feature) |
| `engine_refusals._READ_AS_HELD` | 5 | exceptions to `_NOT_HELD` | limitation text of the hash rows |
| `engine_refusals._ON_BOTH` | 85 | functions/overloads refused on Trino and ClickHouse | refusal rows (typed-hole patterns for the overloads) |
| `engine_refusals._ON` | trino 2, clickhouse 19 | engine-specific refusals | refusal rows |
| `engine_refusals._MOST_ARGUMENTS` | 7 | arity ceilings | refusal rows of the longer overloads |
| `engine_refusals._NOT_EMPTY`, `_plain_format`, `_plain_part` | 3 checks | ClickHouse empty pattern, to_char format, date_part part | function-name rules (they read a constant's value) |
| `pg_ranges` | ranges and multiranges family | ranges | function-name rules (parses range literals, folds constants) |
| `zoned_instants` | timestamptz in the session zone | zoned instants | function-name rule (reads the session zone) |
| `native_subtype` | subtype semantics by dialect | domains/subtypes | function-name rule |
| `pg_functions` (`_TYPE_CALLS`, `_SIZED_CALLS`, `_UDF_CALLS`, `before`) | DuckDB rewrites chosen by arguments | to_timestamp, trim, overlay, generate_series… | function-name rules (they choose by argument value/type) |
| `to_char` (`_STRFTIME`) | 1 table | to_char formats | function-name rule (parses the format constant) |
| `pg_literals`, `pg_type_literals` | literal input | write polyfills of literals | write-side function-name rules |
| `pg_held_types.HELD_AS` | 3 held types, 30 names | types held as another on the engine | write-side pattern rules (cast to held type) |
| `native_json._BACKENDS` | duckdb, postgres (+ elements per type) | REQ-1965 to-JSON of native arrays/structs | read polyfill of nested columns; becomes the typed-array family (later job) |
| `transpile.py` dialect sets (`_LIKE_ESCAPE_WRITTEN`, `_INSTANT_AS_UTC_WALL_TIME`, `_INSTANT_NAMES_UTC`, `_DUCKDB_LIST_FUNCTIONS`, `_PG_LIST_FUNCTIONS`, `_WHOLE_VALUE_REGEXP_LIKE`, `_REGEXP_FLAGS_KEPT`, `_FIRST_MATCH_WRITTEN`) | 8 | LIKE escape, instants, list functions, regex flags | pattern rules (list functions) and function-name rules (regex flags) |
| `provisa/federation/duckdb_pg_scalars.py` | 41 UDFs | scalars needing PostgreSQL's algorithm | not rules: the UDFs a DuckDB replacement calls |
| `provisa/federation/duckdb_pg_json.py`, `duckdb_pg_jsonpath.py` | 27 UDFs | json / jsonpath | not rules: UDFs a DuckDB replacement calls |
| `provisa/federation/duckdb_pg_xml.py` | 20 UDFs | xml | not rules: UDFs a DuckDB replacement calls |
| `provisa/federation/duckdb_pg_numeric.py` | exact numeric helpers | numeric | not rules: UDF implementation |
| `provisa/federation/duckdb_pg_functions.py` | 1 UDF (`provisa_size_pretty`) + registration | pg_size_pretty | not rules: UDF registration |
| `dialect_capabilities.CAPABILITIES` | 19 dialects × 12 constructs | routing: what a direct source runs | not a polyfill (routing) |
| `engine_functions` (`COMPUTED_BY`, `_TYPED`, `_NAMED`) | 3 dialects, 17 names | Cypher-lowered engine functions | not a polyfill (routing) |

Totals: `function_macros.MACROS` holds 1647 overloads with 3144 per-dialect forms (766 of them
PostgreSQL-dialect `Pg` forms sqlglot writes for every dialect, 78 calling a DuckDB `provisa_` UDF).
Outcomes today: DuckDB 1632 emulated / 15 refused; Trino 1407 emulated / 19 mapped / 221 refused;
ClickHouse 1370 emulated / 9 mapped / 268 refused. `engine_refusals` holds 85 + 21 refusals, 17
not-held types and 7 arity ceilings.

## Each source

### `provisa/transpiler/function_macros.py`

The `Macro` table (`MACROS`, keyed by name and arity) and its rule `Macro.resolved`: own function,
then forms, then operators, else refused. Forms are strings, many assembled by f-strings
(`_in_dimensions`, `_trimmed`, `_trino_out_of`, `strict`, `quoted_identifier`, `_snapshot`), parsed
by sqlglot in the target dialect (or PostgreSQL's for `Pg`), with parameters as bare column names
bound by `_written`. Overloads are chosen by the type the statement says of the first argument
(`OVERLOADS`). `inline` also holds special-case rewrites listed above.

- Parameters as bare columns become typed holes (`$a::anyarray`), so the overload choice by said
  type becomes the pattern's own typing and `OVERLOADS` goes away family by family.
- f-string assembled forms become literal rule text per engine.
- `differs_on` and `REFUSED_ROWS` become refusal rows with their SQLSTATE and message.
- `_written`'s argument handling (parenthesising, holding a volatile argument once, a lambda over
  a subquery argument) stays the translator's, applied to every grafted rule.

### `provisa/transpiler/engine_refusals.py`

Replay-refused calls on Trino and ClickHouse: `_NOT_HELD` (types with no engine type),
`_READ_AS_HELD` (exceptions), `_ON_BOTH` and `_ON` (function name → said types refused),
`_MOST_ARGUMENTS` (arity ceilings), `_NOT_EMPTY` / `_plain_format` / `_plain_part` (constant
checks). Each becomes a refusal row of the feature it refuses: a typed pattern
(`max($a::numeric)`) with `refused 0A000 '…'` on the engine. The constant checks are function-name
rules. Today the refusal message is generic (`f() has no form on dialect`); the grid row will hold
the exact text.

### `provisa/transpiler/engine_functions.py`, `provisa/transpiler/dialect_capabilities.py`

Routing: which constructs a direct source's dialect runs, and which engine-only functions a Cypher
read lowers to. They decide where a statement runs, not how a PostgreSQL feature is supplied, so they
stay out of the grid.

### `provisa/transpiler/pg_forms_*.py`

Per-family form tables in the same shape as `MACROS` rows (`(name, params, {dialects: sql})`),
generated per engine by `Engine` helpers. Each family moves as one job; generated rows become
generated rules (the rule text is still printed verbatim in the docs).

### `provisa/federation/duckdb_pg_*.py`

Python UDFs registered on DuckDB. They are not rules: a DuckDB replacement calls them by name
(`provisa_numeric_quotient($a, $b)`), and the docs print the replacement verbatim.

## Moved in this job: arrays

`array_ndims`, `array_dims`, `array_lower`, `array_upper`, `array_length`, `array_fill` (2 and 3
arguments),
`array_positions`, `array_replace`, `array_sample`, `array_shuffle`, `trim_array`,
`generate_subscripts` — 13 read rules, all pattern rules: on DuckDB, Trino and ClickHouse, except
`array_length` (DuckDB only; Trino and ClickHouse take the engine's own, as sqlglot writes it) and
`generate_subscripts` (DuckDB only; refused on Trino and ClickHouse by its grid row). Next array rows for a later job: `array_cat`, `array_remove`,
`array_position` (in `_ROWS`, matched by sqlglot's own node classes).
