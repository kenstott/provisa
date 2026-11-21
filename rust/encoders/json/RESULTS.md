# Rust JSON encoders: GraphQL, REST, JSON:API and OData (wired)

**Wired (J504w, REQ-2194, REQ-1867).** Every GraphQL, REST (JSON and CSV), JSON:API and OData
answer is written by this crate, `provisa._encoders.json`, straight from the engine's Arrow record
batches; the Python serializers it was compared against are gone from the product
(`serialize_rows`, `serialize_aggregate` and their helpers in `executor/serialize.py`; JSON:API's
`serializer.py`, `rows_to_jsonapi`, `_extract_included`, `rename_row_keys` and
`build_pagination_links`; OData's `expand.write` and `edm.edm_value`). They survive only as the
differential's frozen reference, `tests/unit/json_encoder/oracle.py`, corrected where a defect was
fixed (each correction marked `CORRECTED (n)` with its number below). There is no fallback.

The crate began as the experiment J504 (`experiments/graphql_encoder_rs` on
`feat/graphql-encoder-rust` 9e8126b9d: 3,962 differential cases byte-identical to the Python path;
2,000,000 rows: GraphQL 22.9x, REST 20.4x, JSON:API 17.4x, OData 6.7x). Its benchmark and its
account of the Python path are in that branch's `RESULTS.md`.

## Layout

- `rust/encoders/json` (this crate, `json_encoder`, an rlib), linked into `provisa._encoders`
  (`rust/encoders`) as its `json` submodule, beside `pgwire` (`rust/pgwire`). The packaging is
  the pgwire encoder's (setuptools-rust, `pyproject.toml`, the four platform wheels); `rust/build-local.sh`
  builds the module for this machine.
- Named time zones are the pgwire crate's (`pgwire_encoder::tz`: chrono-tz's IANA 2026f tables,
  then each zone's POSIX rule from 2100), as zoneinfo and PostgreSQL read them (J498). No pytz
  table is linked.
- `provisa/executor/json_encoders.py` is what the routers call: it hands over the compiled plan
  (one tuple per column), rebuilds a list view as a list (arrow-rs imports none), and answers the
  encoder's errors as the product does.

## The entry points

| module | function | answers | router |
|---|---|---|---|
| `graphql` | `encode_field(batches, columns, result_limit=None)` -> `(rows, count, joined, truncated)` | a root field's rows (embedded in `{"data": ...}` as an `orjson.Fragment`, `DocumentResponse`) | `endpoint._execute_one_field`, NL |
| `graphql` | `encode_aggregate(batches, columns, aggregate_alias)` | an aggregate field's `{aggregate, nodes}` | the same |
| `rest` | `encode(batches, columns, aggregate_alias=None)` -> `(body, truncated)` | `{"data": rows}` / `{"data": aggregate}` | `rest_table_endpoint` |
| `rest` | `encode_csv(batches, columns)` -> `(body, truncated)` | `text/csv` | the same |
| `jsonapi` | `encode(...)` -> `(body, truncated)` | the compound document: data, relationships, included, meta (total, warnings), links | `_jsonapi_table_endpoint` |
| `jsonapi` | `encode_group_by(...)`, `encode_aggregate(...)` | `?groupBy=`, `?aggregate` | the same |
| `odata` | `encode(...)`, `encode_navigation(...)` | an entity set read (expansions with their own nextLinks); `set(key)/navigation` | `odata/router._read`, `_navigate` |

`joined` is each relationship's matched rows (a to-many's elements, a to-one's objects), which the
GraphQL stats read instead of walking Python rows; `truncated` is each to-one path that matched
several rows for one parent, which every surface reports (defect 7). REST's Parquet and Arrow IPC
answers are the engine's batches themselves (`rest/generator._arrow_file`, defect 8).

Errors: `ValueRefused` -> `JsonValueRefused`, `ResponseRefused` -> `JsonResponseRefused`,
`ODataRefused` -> `QueryOptionRefused`, `EncodeError` -> `EncoderFailed` (a 500 naming its cause).

## Defects fixed (from the experiment's list), and their tests

`tests/unit/json_encoder/test_defects.py` holds each to its answer through the product's path:

1. **Dedup by exact typed value.** Rows, list elements and the to-one objects compared for
   truncation are one when they are the same typed value: a string is never a number, `1` is not
   `1.0` nor `true` (a JSON number keeps its digits), a JSON array is never the text of one, NaN
   is NaN (`pywrite::Keyer`). `test_rows_differing_in_json_type_are_not_one`,
   `test_list_elements_differing_in_type_are_not_one`; Rust: `pywrite::tests`.
2. **Named zones by tzdata** (J498), not pytz's 32-bit tables: daylight time after 2037, every
   transition before 1901, local mean time with its seconds; `UTC` and `+HH:MM` are fixed
   offsets. `test_a_named_zone_after_2037_keeps_its_daylight_time`,
   `test_local_mean_time_keeps_its_seconds`, `test_zones.py` (every zone).
3. **NaT and the nanosecond limits give the PostgreSQL answer**: a nanosecond timestamp or
   duration is the instant floored to the microsecond, as PostgreSQL holds it and as the pgwire
   encoder writes it; the int64 minimum is 1677-09-21T00:12:43.145224, not `"NaT"`, and a zoned
   value whose local time leaves int64 nanoseconds is written, not a 500.
   `test_the_int64_minimum_nanosecond_is_an_instant_not_nat`,
   `test_a_zoned_nanosecond_past_the_int64_local_time_is_written`,
   `test_nanoseconds_are_floored_to_the_microsecond`.
4. **JSON:API `?groupBy=` with a NULL key**: the group's key is its fields, each null (the Python
   path called `.items()` on None, a 500). `test_surfaces.py::test_jsonapi_group_by_null_key`.
5. **An include named twice** is the one include. `test_an_include_named_twice_is_the_one_include`.
6. **JSON:API ids are the text the answer writes** (a string as itself, `true`, a number's JSON
   digits, a JSON object's compact text, a decimal's exact digits), never Python's `str()` /
   `repr()`; a column named `id` is an attribute unless the id is that column.
   `test_ids_are_the_text_the_answer_writes`,
   `test_an_id_column_is_an_attribute_when_the_id_is_other_keys`.
7. **The to-one truncation warning reaches every surface** (resolved by the wiring): GraphQL's
   `extensions.warnings` and REST's `X-Provisa-Warnings` (the router `warn()`s each path the
   encoder reports); JSON:API's `meta.warnings` after the request's own, with the same code and
   message (`serialize.TO_ONE_TRUNCATED_MESSAGE`). `test_a_truncated_to_one_reaches_every_surface`,
   `test_the_requests_warnings_come_first_in_meta`, and through the endpoint
   `test_exact_json_values.py::test_the_truncation_warning_reaches_the_request`.
8. **REST Parquet and Arrow IPC carry the engine's types** (resolved by the wiring): the batches
   themselves, each column named by its field (`rel.field` for a relationship's), a DECIMAL a
   decimal, a timestamp a timestamp, JSON text as text. `test_rest_parquet_and_arrow_carry_the_engines_types`,
   `test_rest_parquet_of_no_batch_is_the_fields`.
9. **OData Edm.Date, Edm.TimeOfDay and Edm.Boolean are typed**: a date (or a timestamp's date, or
   engine text of either), a time of day (or a timestamp's, or text of one), a boolean (or 0/1, or
   text PostgreSQL's `boolin` reads); any other value is a 500 naming the type, never a value
   orjson writes as it is or Python truthiness (`"false"` was `true`). An infinite date or
   timestamp is `"infinity"` / `"-infinity"` as Edm.Date and Edm.DateTimeOffset, as on every JSON
   surface. `test_edm_boolean_reads_text_as_postgresql_does`,
   `test_edm_date_and_time_of_day_are_typed`, `test_odata_infinite_dates_are_infinity`;
   Rust: `odata::tests`.

Found while wiring: an aggregate whose first batch was empty read row 0 of it (`nest::aggregate`
now takes the first row of the first non-empty batch); the row-engine fetch boundary
(`ir_arrow.row_batch`) refused a Decimal with a positive exponent (`1E+3`) and now holds it at
scale 0 (`test_exact_json_values.py::TestDecimal::test_positional_never_an_exponent`).

## Differential (`tests/unit/json_encoder/`)

`test_compare.py` (GraphQL), `test_surfaces.py` (REST, REST CSV, JSON:API, OData) and
`test_zones.py` run the product's encoder path (`encoders.py`, the routers' calls) against the
reference (`python_ref.py` over `oracle.py`) on the same batches, and `check.compare` parses both
bodies and compares them structurally: key order, JSON types, every value exactly, floats within a
relative tolerance of 1e-14; a refusal by its class and what it names, any other error as
`EncoderFailed`. The case corpus (`cases.py`) is the experiment's: every Arrow type at its
extremes, NULLs and random values in five shapes, 61 JSON texts, 18 nesting plans with five result
limits, 1,500 random plans, the JSON:API and OData envelopes.

- `test_compare.py`: 1,765 passed. `test_surfaces.py`: 1,599 passed. `test_zones.py`: 598 zones
  (every zone of the pinned `tzdata`, 2026f, `Factory` included) at 60,178 instants from 1800 to
  2400, all equal.
- The reference reads zones from the pinned `tzdata` package (the release chrono-tz is built from),
  not the host's `/usr/share/zoneinfo`, which may be another release (2026e on the build machine),
  and so does the product: importing `provisa` empties zoneinfo's search path (`provisa/__init__.py`).

### Intentional differences from the old Python path

Each is a correction in `oracle.py`: the encoders' answer is the corrected one.

- Dedup and truncation by typed value (1); zones by tzdata, not pytz (2), so a `str()` or `repr()`
  of an aware value (CSV cells and JSON:API ids of non-string values in containers) names
  `zoneinfo.ZoneInfo(key=...)` or `datetime.timezone(...)` where it named a pytz zone; nanoseconds
  floored, no `NaT` (3); the NULL group key (4); one include per name (5); ids as written text and
  `id` kept as an attribute (6); truncation warnings in JSON:API's meta (7); OData's typed
  Edm.Date, Edm.TimeOfDay and Edm.Boolean and its infinities (9).
- REST Parquet and Arrow IPC (8) carry the engine's rows as they are: native types, field names,
  and no deduplication of identical rows (the JSON answer still deduplicates by its root columns).
  Contract change for those two media types.
- The to-one truncation check compares each object as the row built it, by its own fields (the
  Python path read it back from the answer, where a nested path of the same name could stand in a
  field's place).

## Tests and gates

- `cargo test --manifest-path rust/encoders/json/Cargo.toml --release` (macOS: with
  `DYLD_INSERT_LIBRARIES` naming the interpreter's library, as the test binary links no Python).
- `pytest tests/unit/json_encoder/` (the differential and the defects).
- The routers' unit tests, which stub the engine with `QueryResult`s (a row engine's result, made
  Arrow at its fetch boundary) or DuckDB's own Arrow (`test_odata_service.py`).
