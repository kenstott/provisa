# Rust GraphQL response encoder: differential test and benchmark

An experiment, isolated from the product: nothing in `provisa/` or `vendor/` calls this crate. It
applies the J483 method (the pgwire DataRow encoder, REQ-2194) to the GraphQL JSON answer
(REQ-1867).

## What is compared

**Python (the product's path, run unmodified; `python_ref.py`):** an ENGINE-routed root field's
Arrow batches → `stream_rows_from_arrow` (`column_values` per column, rows zipped) and `iter_rows`,
as `_execute_plan`'s buffered drain collects them → `serialize_rows` (through `_format_response`),
which nests the flat JOIN rows into GraphQL objects → `_execute_one_field` keeps
`data[root_field]` → `_handle_query` answers `{"data": {root_field: rows}}` as a
`TimedJSONResponse`, whose `render` is `json_response.dumps` (orjson, `_encode_unknown` for the
rest). The stage timer is left out.

**Rust (`src/`):** `graphql_encoder_rs.encode_response(batches, columns, root_field,
result_limit=None)` takes the same Arrow batches zero-copy (arrow-rs `pyarrow` feature) and the
selection plan `serialize_rows` reads, one `(field_name, nested_in, cardinality, is_agg)` per
column (`python_ref.plan_tuples(compiled.columns)`), and returns the response body. The GIL is
released while encoding.

The port follows `serialize_rows` step by step, not just its output on well-formed input:

- **Root rows are deduplicated by value**, as `_to_hashable` keys them: Python `==` on the converted
  values, so `-0.0 == 0.0`, a Decimal's int equals the same float, every float NaN is distinct (each
  is its own object), and a dict or list is keyed by `json.dumps(sort_keys=True)` (where NaN equals
  NaN, `-0.0` differs from `0.0`, and `1` differs from `1.0` and `true`). `_serialize_flat` keys by
  the non-aggregate root columns, or by the whole row when every root column is an aggregate;
  `_serialize_with_one_to_many` keys by every root column.
- **Flat and many-to-one** (`_serialize_flat`, `_build_nested_obj`): the first row for a key builds
  the object and later rows are dropped. A to-one path whose parent is null is skipped, and an
  all-null to-one is `null`. Because a result row never changes after it is built, the Rust encoder
  writes it immediately and keeps only the keys.
- **One-to-many** (`_serialize_with_one_to_many`): the paths are placed in sorted order under their
  nearest enclosing path, list elements are deduplicated per list by their values, a to-one under a
  list element merges with `update`, and an all-null to-many element leaves `[]`. The whole tree is
  kept and written at the end, as Python keeps it.
- **A converted dict or list is a live object.** A parsed JSON text, a struct or a list value is
  held as Python holds it. When a relationship shares a response key with it, the serializer merges
  into it (`current.update`), appends elements to it, or walks a path through it, and the port does
  the same (`Tree::get` turns it into a node the moment it is reached into).
- **Repeated keys** (`{col.field_name: ...}`): a name keeps its first position and takes its last
  value. A relationship over a scalar field of the same name replaces it in place.
- **Errors happen where Python raises them.** `to_pylist` fails eagerly (an out-of-range date,
  timestamp, duration or interval, or a struct with duplicate field names), even for a row that
  is later dropped. orjson fails only on what it writes (an int outside 64 bits, a lone surrogate,
  a non-empty container nested past level 254), so a too-big value in a dropped row is harmless on
  both paths (`test_poison_in_repeated_row_is_not_written`).

Value conversion (`_convert_value`, then orjson):

| Arrow type | written as |
|---|---|
| bool, int8–64, uint8–64 | JSON bool, int |
| float32, float64 | orjson's float: shortest round-trip digits, fixed for a decimal exponent in [-5, 16) with at least `.0`, else `d.ddde±x`; NaN and ±Infinity `null` |
| decimal128, decimal256 | `float(d)`, or `int(float(d))` when `str(d)` has no point and the float is whole (`fmt::decimal_value`) |
| utf8, large_utf8, utf8_view, dictionary<utf8> (enum) | a string, or, when longer than one character, opening with `{` or `[` and parsing under `json.loads`, the parsed value with every nested string converted again (`jtext.rs`, a port of the strict C scanner: NaN/Infinity, duplicate keys, the 4,300-digit int limit, surrogate pairs) |
| binary, large_binary, fixed_size_binary | `"\x…"` hex (`bytea_hex`) |
| fixed_size_binary(16) `arrow.uuid` | the hyphenated UUID |
| date32 | `isoformat`; ±(2^31−1) as the IR infinity (`9999-12-31`, `0001-01-01`) |
| timestamp s/ms/µs, naive, UTC or fixed `±HH:MM` | `temporal_text` (isoformat, fraction zeros dropped, offset); µs ±(2^63−1) as the IR infinity |
| time32 s/ms, time64 µs | `temporal_text` |
| duration s/ms/µs, interval month_day_nano | `iso8601_duration` (nanoseconds floored to µs, the timedelta range checks) |
| list, large_list, fixed_size_list, struct | `to_pylist`, each element converted as above |
| null | `null` |

## Differential test (`test_compare.py`)

**1,659 passed, 28 skipped, 0 failed. All 1,701 cases that produce a body are byte-identical**, floats
included. In 255 more, both encoders raise. Both bodies are parsed and compared structurally (key
order, JSON types, every value; floats within a relative tolerance of 1e-14), and the bytes are
tallied besides. A case where one encoder raises and the other does not fails, and so does a Rust
`NotImplementedError`. Covered:

- **72 single-type columns**, each in five shapes: the root field alone (rows deduplicated by it),
  beside a key, inside a one-to-many element (alone and beside another field), and as a many-to-one
  object. The types: bool; every int width at its extremes; float64 specials (±0, NaN, ±Infinity,
  subnormal, max, the orjson notation boundaries) and 100,000 random bit patterns; float32 with
  100,000 random bit patterns; decimal128 at nine precision/scale pairs (3,000 random values each,
  scale 0, negative scale, zero at scale 7), tiny values in scientific notation, −0.00, values past
  2^53, decimal256; 58 texts in utf8, large_utf8 and utf8_view (escapes, control characters, every
  JSON corner that parses or must not: extra data, trailing commas, leading zeros, NaN, `1e400`,
  duplicate keys, surrogate pairs, JSON inside JSON strings, a 4,401-digit int); enum;
  binary/large/fixed; uuid; date32 with infinities; timestamps in s/ms/µs × naive/UTC/+05:30/−03:00
  with infinities (2,000 random each); time32/64; durations at the timedelta limits;
  3,000 random intervals; list, large_list and fixed_size_list of ints, floats, strings, decimals,
  dates, timestamps (UTC, +05:30), uuids and nulls; nested lists; structs with time, duration,
  binary and lists; the null type.
- **15 nesting plans × 5 result limits** over an orders table: flat; many-to-one; two-level
  many-to-one; one-to-many; two-level one-to-many; one-to-many with a many-to-one inside and beside;
  aliases repeating a name; a field and a relationship sharing a name (to-one and to-many);
  `is_agg` root columns, all-aggregate roots; only nested columns; a to-one path with no parent
  path; child path before parent. Each also with zero rows and with zero batches, and one split into
  1-, 3- and 7/1/13-row batches.
- **1,500 random plans** over random tables of up to 60 rows (ints, floats with NaN and ±0, decimals
  with ±0, JSON texts that are equal under different spellings, strings, structs, lists; 15% NULL),
  up to eight columns placed at random at the root or under five paths of random cardinality, with
  names that collide, and a random result limit. 28 are skipped: see product behavior 6.
- **Errors, both raising:** a decimal past 2^64 (and 2^64−1, which rounds to it), a big
  negative-scale decimal, a JSON int past 64 bits, lone surrogates in a JSON value and key, JSON
  nesting past orjson's limit on either side of the boundary, out-of-range date, timestamp
  (including one pushed past 9999 by its offset), duration (the floor-days rule), interval and a
  struct with duplicate field names; a dropped repeated row that would have failed orjson (both
  succeed) or `to_pylist` (both raise); the `_detect_truncated_paths` crash (product behavior 5).

Run from the worktree root:

```
experiments/graphql_encoder_rs/build.sh
PYTHONPATH=.:experiments/graphql_encoder_rs/build:experiments/graphql_encoder_rs \
  ~/.provisa-agent/heavy.sh .venv/bin/python -m pytest -p no:cacheprovider -q -s \
  experiments/graphql_encoder_rs/test_compare.py
```

### Not ported (Rust raises `NotImplementedError`; Python would encode)

- Timestamps in a named time zone (`America/New_York`): pyarrow converts with pytz, and only UTC
  and fixed offsets are ported. DuckDB's Arrow `timestamptz` carries the session `TimeZone`, so this
  matters if that zone is not UTC.
- Nanosecond timestamps, times and durations (to_pylist yields pandas objects); date64.
- Map columns (`to_pylist` yields lists of tuples, which `_convert_value` leaves for orjson and
  `_encode_unknown`); a month_day_nano interval or a dictionary inside a list or struct; any other
  Arrow type.
- `serialize_aggregate` (aggregate and group-by roots with nodes), `serialize_connection`, merging
  several root fields, and the redirect, cache-hit and file-format branches of `_handle_query`.

## Benchmark (`bench.py`)

BENCHMARK

## Product behaviors found (reported, not fixed)

The port reproduces each of these; the differential test pins them.

1. **Every DECIMAL leaves as a float, or as an int made from a float.** `_convert_value` writes
   `float(d)`, so a `DECIMAL(18,2)` loses digits past about 15 to 17 significant figures
   (`1234567890123456.78` → `1234567890123456.8`), and a scale-0 decimal (and DuckDB's HUGEINT,
   which its Arrow export carries as `decimal128(38,0)`) above 2^53 is rounded
   (`9007199254740993` → `9007199254740992`). From 2^64 up, including 2^64−1 which rounds to it,
   orjson raises `Integer exceeds 64-bit range` and the whole request fails. The JSON type also
   follows `str(d)`: `Decimal("0E-7")` (zero at scale above 6) is written `0` beside `0.0` and
   `1.5` in the same column, and a negative-scale `1E+3` is `1000` while `1.23E+5` is `123000.0`.
   (REQ-1867's docstring says a Decimal reaches `_encode_unknown`; on this path it never does.)
2. **PostgreSQL ±infinity dates and timestamps are written as finite dates.** `column_values` reads
   them as the IR infinities, and `_convert_value` calls `isoformat` on them, giving `"9999-12-31"`,
   `"0001-01-01"` and `"9999-12-31T23:59:59.999999+00:00"`. pgwire writes `infinity`. These strings
   are also the texts of real values, so a root row keyed by one dedups against the other.
3. **Any text that looks like JSON is parsed, at any depth.** Every VARCHAR value, not only JSON
   columns, longer than one character, opening with `{` or `[` and parsing, is written as an
   object or array: a product name `[1,2]` becomes an array. A string inside a JSON value is parsed
   again (`{"a": "[1]"}` → `{"a": [1]}`). `NaN` and `Infinity` in JSON text become `null`. A JSON
   value with an int past 64 bits, a lone `\uD800` escape, or non-empty nesting deeper than about
   250 levels fails the whole response in orjson (500).
4. **The many-to-one truncation warning never reaches the client.** `_serialize_flat` puts it in
   `extensions.warnings`, and `_execute_one_field` keeps only `data[root_field]`. The check also
   compares the new row's column tuple with the previous object's `values()`, so a repeated root
   row whose to-one is null with two or more columns would be warned about falsely.
5. **`_detect_truncated_paths` crashes on a null parent.** A repeated root row whose to-one parent
   was null, with a nested to-one path under it (`rel` and `rel.sub`), walks into `None` and raises
   `AttributeError`, a 500 (`test_detect_truncated_crash`).
6. **List elements are keyed by `id(list)`.** When a one-to-many list hangs under a name its
   many-to-one parent re-sets on every row (a parent field and a child relationship sharing a
   response key), `update` drops the list each row, and a later row's element can match a stale
   key when CPython reuses the dropped list's address, so the element vanishes. GraphQL validation
   rejects such a selection (fields with one response key must merge), so this is latent. The
   random plans that produce it are skipped, since CPython's address reuse cannot be reproduced.
   The same collisions also merge a relationship into a JSON value of the same name (live-object
   rule above).

Not a defect, but worth knowing: root rows are deduplicated by the selected values, so a
selection with no unique column collapses equal rows (`{ orders { status } }` returns one row per
status). `tests/unit/test_serialize_properties.py` asserts this as intended.

## What wiring it in would need

- **Arrow at the call site.** `_execute_plan`'s buffered drain takes `stream.iter_rows()`, and
  DuckDB's `run_sync` reads through Arrow only when an interval, date or timestamp column is
  present (`READ_THROUGH_ARROW`); otherwise it fetches DB-API rows. The encoder needs the
  engine's Arrow stream (`run_arrow_stream`) with the inline threshold counted on batches. DIRECT
  and admin routes and the response cache hold Python rows, so they would need Arrow too, or would
  stay on the Python path by name.
- **A fragment API.** `_handle_query` merges root fields, and `_execute_one_field` hands
  `field_rows` to `note_request_rows`, `_record_per_source_stats` and the Mermaid trace. The
  encoder would return the field's array bytes plus its row count, and the endpoint would splice
  the fragments into the envelope and answer with a raw `Response` (keeping the stage timer).
- **The rest of the serializer.** `serialize_aggregate`, `serialize_connection` and every type
  listed under "Not ported", or a refusal by name for each (no fallbacks).
- **A decision on the product behaviors above.** The port is identical to them by design. Fixing
  any of them (exact decimals, `infinity`, parsing only JSON-typed columns) changes the contract
  for both paths at once and is a requirements decision.
- **Packaging.** A maturin wheel per platform in place of `build.sh`; pyo3, arrow-rs, ryu, itoa
  and hashbrown are MIT/Apache-2.0.

## Files

- `Cargo.toml`, `Cargo.lock`, `.cargo/config.toml`: the cdylib (pyo3 + arrow-rs); macOS link flags.
- `src/lib.rs`: the selection plan, the response tree, `_serialize_flat` and
  `_serialize_with_one_to_many`, the entry point.
- `src/cells.rs`: one converter per Arrow column type: the value, its key, its eager errors.
- `src/jtext.rs`: `json.loads` and the nested conversion, orjson's writer, the `_to_hashable` key.
- `src/fmt.rs`: orjson float and string text, `Decimal` conversion, dates, times, ISO 8601 durations.
- `build.sh`: `cargo build --release`, then copies the module to `build/`. Nothing is installed
  into any venv.
- `python_ref.py`: the product's path. `test_compare.py`: the differential test. `bench.py`: the
  benchmark; `bench-2m.log` and `bench-2m.json` hold its output.
