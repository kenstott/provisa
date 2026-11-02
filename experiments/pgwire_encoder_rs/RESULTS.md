# Rust pgwire DataRow encoder: differential test and benchmark

An experiment, isolated from the product: nothing in `provisa/` or `vendor/` calls this crate.

## What is compared

**Python (the product's path, run unmodified):** Arrow `RecordBatch` →
`provisa.federation.runtime_support.stream_rows_from_arrow` (`column_values` per column, `zip` into
row tuples) → `provisa.pgwire.server.ProvisaQueryResult` (column types from the engine's type names)
→ `buenavista.postgres.BuenaVistaHandler.send_data_rows` (the `BVTYPE_TO_PGTYPE` converters, DataRow
framing, 500-row chunked writes). `python_ref.py` drives it into an in-memory writer.

**Rust (`src/`):** `pgwire_encoder_rs.encode_batch(batch, bvtypes, formats)` takes the same Arrow
batch zero-copy through the Arrow C data interface (arrow-rs `pyarrow` feature), the BVType
`ProvisaQueryResult` resolved for each column, and the per-column result formats, and returns the
DataRow message bytes. `encode_stream(batches, bvtypes, write, formats)` streams a batch iterator
and hands each batch's bytes to `write`, the shape `send_data_rows` has. The GIL is released while
encoding.

## Differential test (`test_compare.py`)

**171 passed, 0 byte mismatches.** It covers 77 single-column cases, each run in text and binary
format, plus the mixed and randomized tests listed below. In every case the bytes are identical, or
both encoders raise. Covered:

- **bool**; **int2/int4/int8/oid** from every Arrow int width, with range overflow in binary
  (int64 → INTEGER, uint64 → BIGINT, negative → OID).
- **float8 text**: NaN, ±Infinity, −0, subnormals, max, the 15-digit fixed/exponent boundary, and
  300,000 random bit patterns.
- **float4 text**: 300,000 random bit patterns; a float64 column described as REAL; overflow.
- **numeric**: decimal128 at many scales (80,000 random values), decimal256, negative scale, zero
  with scale (`0E-10`), and int → numeric.
- **text**: empty, unicode, NUL, 5,000 chars, every `str.isspace()` code point, large_string,
  dictionary-encoded, and an all-NULL column.
- **bytea**: hex in text, raw in binary.
- **date**: years 1 and 9999, ±infinity sentinels, out of range.
- **timestamp / timestamptz**: s, ms and µs units; UTC, `+05:30`, `-03:00`; ±infinity; years 1 and
  9999; 200,000 random values; a naive column described as timestamptz, which raises in binary.
- **time**: s, ms and µs units.
- **interval**: month-day-nano (50,000 random), duration (s and µs), sign rules, nanosecond flooring,
  timedelta range overflow.
- **json/jsonb**: text passthrough; struct, map and list values through `json.dumps`, with
  `ensure_ascii` escapes and surrogate pairs, NaN, and float repr.
- **uuid**: hyphenated, upper-case, braces, `urn:uuid:` and bare spellings; a malformed value.
- **arrays** (int2/4/8, float4/8, numeric, bool, text): element quoting, NULL elements, empty arrays,
  large_list, 2-D arrays, and the ragged and NULL-sub-list errors.
- **mixed batches**: a 10-column batch in default, text, binary and alternating formats, and a split
  across 1-, 700- and 533-row batches.

Run from the worktree root:

```
experiments/pgwire_encoder_rs/build.sh
PYTHONPATH=.:vendor/buenavista:experiments/pgwire_encoder_rs/build:experiments/pgwire_encoder_rs \
  ~/.provisa-agent/heavy.sh .venv/bin/python -m pytest -p no:cacheprovider -q \
  experiments/pgwire_encoder_rs/test_compare.py
```

### Product behaviors the port had to reproduce

Matching byte for byte meant porting these as they are. The first and third are product defects,
reported here and not fixed (the product is out of scope for this job).

1. **Binary numeric rounds past 28 significant digits.** `_numeric_to_pg_binary` calls `abs(d)`,
   which applies the default decimal context (`prec=28`, round half to even). A `DECIMAL(38,s)`
   value with more than 28 digits is sent in binary format rounded:
   `1234567890123456789012345678.9999999999` goes out as `1234567890123456789012345679` with
   dscale 0. Text format (`str(d)`) is exact. The Rust port applies the same rounding
   (`fmt_num::context_round`); the `decimal_context_rounding*` cases pin it.
2. **float8 text ties.** `repr` (David Gay's mode 0) breaks an exact tie between two shortest
   candidates toward the even digit: `-738171652909232.25` is written `...232.2`. Rust's `{:e}`
   writes `...232.3`. The port takes the correctly rounded form at the shortest length whenever 15
   or more digits are needed, the only place such a tie can occur. This is not a product defect.
3. **Interval days are split with float division.** `_interval_parts` computes
   `int(micros / 86_400_000_000)`, a correctly rounded float quotient. Past about 100,000 days that
   quotient rounds up across a day boundary. `timedelta(days=500000, microseconds=86399999999)` is
   written `500001 days -00:00:00.000001`, where PostgreSQL writes `500000 days 23:59:59.999999`.
   The value is equal, but the text is not PostgreSQL's. The port reproduces it exactly
   (`temporal::trunc_true_div`).

### Not ported (Rust raises `NotImplementedError`; Python would encode)

- Timestamps in a named time zone (`America/New_York`). pyarrow converts these with pytz; only UTC
  and fixed `±HH:MM` offsets are ported. DuckDB's Arrow timestamptz carries the session `TimeZone`,
  so this matters if the engine's session zone is not UTC.
- Nanosecond timestamps, times and durations (to_pylist yields pandas objects), and date64.
- UUID spellings that `int(hex, 16)` accepts besides plain hex digits (whitespace, `_`, `0x`), and
  UUIDs stored as fixed_size_binary(16).
- A 2-D text array whose first sub-list is empty or NULL. Python's binary path writes `str(list)` for
  each inner list; the port refuses.
- Every other BVType: inet, cidr, money, geometric, range, tsvector, xml, the catalog types, and
  the interval/money/oid arrays.
- `RawDataRowBytes` passthrough, and the row coercers (`_SHAPE_COERCERS`, `_describe_casts`, `xml_out`,
  `_duckdb_inet`) that `ProvisaQueryResult` applies to a described statement's rows. The benchmark
  and test results carry no coercers.

## Benchmark (`bench.py`)

**Machine:** Apple M3 Pro, 12 cores, 18 GiB RAM, macOS 26.7.1. Python 3.12.0, pyarrow 24.0.0,
numpy 2.4.6. rustc 1.95.0, arrow-rs 56.2.1, pyo3 0.25.1. Release build with fat LTO,
`codegen-units=1` and `target-cpu=native`. Single-threaded, run through `heavy.sh` (the machine's
one job slot).

**Dataset:** 5,000,000 rows in 77 batches of 65,536 rows (DuckDB's Arrow stream batch size),
574.3 MB of Arrow, generated in memory with numpy from seed 483 and held once. Twelve columns:

| column | Arrow type | described as | NULLs |
|---|---|---|---|
| id | int64 | BIGINT | |
| qty | int32 | INTEGER | |
| price | float64 (normal, mean 1000) | DOUBLE | |
| ratio | float32 [0,1) | FLOAT (float4) | |
| amount | decimal128(18,2) | DECIMAL(18,2) | |
| name | utf8, word + number, some non-ASCII | VARCHAR | |
| created | timestamp[us, UTC], 2000–2030 | TIMESTAMP WITH TIME ZONE | |
| day | date32 | DATE | |
| active | bool | BOOLEAN | |
| payload | utf8 JSON text | JSON | |
| note | utf8 | VARCHAR | 30% |
| parent_id | int64 | BIGINT | 20% |

**Method:** each run is end to end, Arrow batches in and DataRow bytes out, with the output counted
and not kept. The bytes of the first two batches were checked identical before timing, and the
total row and byte counts agree. Best of 3 runs for Python and best of 5 for Rust. CPU time is
`process_time`. Free memory stayed at about 49% (the guard aborts under 25%).

| format | rows | DataRow bytes | Python wall (cpu) | Rust wall (cpu) | Python | Rust | speedup |
|---|---|---|---|---|---|---|---|
| text | 5,000,000 | 1,022.9 MB | 70.25 s (69.98 s) | 4.830 s (4.804 s) | 0.071 Mrow/s, 14.6 MB/s | 1.04 Mrow/s, 211.8 MB/s | **14.5×** |
| binary | 5,000,000 | 793.3 MB | 51.14 s (50.98 s) | 1.254 s (1.239 s) | 0.098 Mrow/s, 15.5 MB/s | 3.99 Mrow/s, 632.9 MB/s | **40.8×** |

Run-to-run spread: Python 70.2–71.6 s (text) and 51.1–52.9 s (binary); Rust 4.83–4.91 s (text)
and 1.25–1.39 s (binary). The full output is in `bench-5m.log`.

### Where the Python time goes

- **Arrow → Python rows: 24.2 s of the 5M-row run** (`stream_rows_from_arrow` plus
  `ProvisaQueryResult.rows()` alone, measured separately). That is about 34% of the text run and 47%
  of the binary run. Within it, `column_values` → `to_pylist` builds about 60M Python objects. The
  date32 and µs-timestamp columns each pay for `_with_infinities`: a cast, two compares, an `or`
  and an `any` per batch, and when the batch is free of infinities (always here) a second
  `to_pylist`.
- **The per-cell loop in `send_data_rows`.** cProfile over 458,752 rows (7 batches), with the
  profiler inflating Python time about 2×:
  - The loop body itself (`tottime`) is 2.9 s of 13.4 s: twelve indexed reads, a None check,
    `str.encode` and a `struct.pack` per cell.
  - Text format: `_float4_text` costs 3.2 s cumulative. Its search formats and parses up to nine
    candidate strings per value, so the one float4 column costs more than any other. `_float8_text`
    costs 1.3 s (`repr` → `Decimal` → `format` → `_shortest_text`). `_timestamp_text` costs 1.6 s
    (`_pg_clock`, `_pg_offset`, `utcoffset`). The rest is about 5.3M `str.encode` and
    `Struct.pack` calls.
  - Binary format: `_numeric_to_pg_binary` costs 3.0 s, by string slicing of `format(d, "f")`.
    `_timestamp_binary` costs 1.3 s (tz `replace` plus datetime subtraction per value). The int
    columns cost about 2.2M `int.to_bytes` calls.
- **What is left in Rust.** Text costs 4× binary (4.8 s against 1.25 s). Most of that is float text:
  float4 keeps the product's digit search (up to nine exact formats and parses per value, for
  byte-identity), and float8 uses core's shortest formatter. A faster float4 (shortest f32 digits,
  then one confirming parse) is possible but would need its own proof of identity against the
  double-rounding read-back the product does.

## Files

- `Cargo.toml`, `Cargo.lock`, `.cargo/config.toml`: the cdylib (pyo3 + arrow-rs); macOS link flags.
- `src/lib.rs`: kinds, Arrow access, per-type writers, arrays, DataRow framing, Python entry points.
- `src/fmt_num.rs`: float8/float4 text, float repr, `Decimal.__str__`, binary numeric.
- `src/temporal.rs`: date, timestamp, time and interval text and parts.
- `src/json.rs`: `json.dumps` of Arrow nested values.
- `build.sh`: `cargo build --release`, then copies the module to `build/`. Nothing is installed into
  any venv.
- `python_ref.py`: drives the product's Python path. `test_compare.py`: the differential test.
  `bench.py`: the benchmark. `bench-5m.log`: the benchmark's output.
