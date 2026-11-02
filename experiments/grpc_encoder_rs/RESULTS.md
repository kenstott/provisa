# Rust gRPC result encoder: differential test and benchmark

An experiment, isolated from the product: nothing in `provisa/` or `vendor/` calls this crate.

## What is compared

**Python (the product's path, run unmodified):** Arrow `RecordBatch` →
`provisa.federation.runtime_support.stream_rows_from_arrow` (`column_values` per column, `zip` into
row tuples) → per row, `_handle_query_bound`'s `_kwargs_for` (`provisa.grpc.server._proto_value` per
non-null cell) → `msg_cls(**kwargs)` → `SerializeToString`, the bytes `_streaming_rpc` hands gRPC.
Query{Type}Batch wraps the row messages in `{Type}Batch(rows=...)` of `batch_rows` each, carried
across Arrow batches. A relationship-shaped row (a many-to-one relation as a struct, a one-to-many as
a list of structs) goes through `ProvisaServicer._dict_row_to_message`, the product's
nested-relationship encoder. `_kwargs_for` and the column-to-field key (`_norm`) are closures inside
`_handle_query_bound`, so `python_ref.py` restates them line for line (without a read mask). Every
other step is the product's own function.

**Service:** the product's `generate_proto` over a two-table `SchemaInput` covering every proto type
`_PROTO_TYPE_MAP` produces (int32, int64, double, float, bool, string, bytes,
`google.protobuf.Timestamp`), with a many-to-one relation (`Orders.customers`) and a one-to-many
(`Customers.orders`). It is generated once for the wire (`authoritative`) and once for a role that is
not granted `orders.secret`, through one `FieldNumberAllocator`, and compiled with the product's
`OrgDescriptor` / `compile_descriptor_set`.

**Rust (`src/`):** `grpc_encoder_rs.Encoder(descriptor_set, message, columns, mode, batch_message,
batch_rows)` reads the service's serialized `FileDescriptorSet` with a hand-written decoder
(`descriptor.rs`). There is no build-time code generation: the service is generated at run time. It
takes the Arrow batches zero-copy through the Arrow C data interface and writes the protobuf wire
format by hand. `encode(batch)` returns the messages a batch completes, one `bytes` per message as a
gRPC handler yields them. `encode_framed(batch)` returns them in one buffer of gRPC length-prefixed
frames. `finish()` returns the last, partly filled batch message. The GIL is released while
encoding.

## Differential test (`test_compare.py`)

**113 passed: 30,879 cases and 2,234,308 messages. Every pair decodes equal field by field, and
every pair is byte-identical. In 153 cases both paths raise.** Both outputs are decoded with the same
descriptor (`FromString`) and compared field by field: the same fields present, floats and doubles
within a relative tolerance of 1e-14, everything else exact (value and Python type), repeated fields
item by item, nested messages recursively, and unknown fields (`UnknownFieldSet`) equal. Where Python
raises, Rust must raise an error other than `NotImplementedError`. Covered:

- **int32 / int64 fields** from every Arrow int width (2,000 random values plus the edges each), with
  out-of-range values raising; from bool, float (truncation; NaN, ±inf and 1e19 raise), decimal
  (truncation, negative scale, out of range), date32 and timestamp (`toordinal`, including the
  infinity sentinels).
- **double / float fields**: special values (NaN, ±inf, ±0, subnormals, max, FLT_MAX and the point
  where float rounds to inf), 150,000 random bit patterns, float32 input, bool, int64 / uint64
  (rounding past 2^53), and decimal128 at five precision/scale pairs (300,000 values) through
  `float(Decimal)`. date, timestamp and duration input raise.
- **bool fields** from bool, ints, floats (NaN is true), decimals, strings, bytes, dates, timestamps
  and times.
- **string fields**: text (empty, unicode, NUL, 5,000 chars, whitespace) as utf8, large_utf8,
  utf8_view and dictionary; `str()` of ints, bools, floats (`repr`, 120,000 random bit patterns,
  powers of ten), decimals (six precision/scale pairs, 60,000 values), dates (±infinity, years 1 and
  9999, out of range), timestamps (s/ms/µs × naive, UTC, `+00:00`, `+05:30`, `-03:00`, `+14:00`,
  `-12:45`, 20,000 random values each, plus edges and infinities), times (s, ms, µs), month-day-nano
  intervals and durations (s, ms, µs) as ISO 8601 (`iso8601_duration`, 35,000 random values,
  sign rules, nanosecond flooring, timedelta overflow).
- **Timestamp fields** (`FromDatetime`): the same timestamp matrix, naive as UTC, aware moved to UTC,
  the infinity sentinels as `datetime.max` / `min`; date and int input raise.
- **bytes fields**: binary, large_binary, fixed_size_binary raw; text, ints, floats, dates and
  timestamps through `str(v).encode()`.
- **NULL and absent fields**: all-NULL and null-typed columns (an empty message), a zero written but
  a NULL not (`optional` presence), masked values (NULL in any type, mask strings).
- **Redacted fields**: the wire's `secret` decoded with the role's service, which does not have it,
  reads equal on both sides (it is an unknown field to the role). A result column with no field
  raises only at its first non-NULL value, as `msg_cls(**kwargs)` does. Columns are matched to fields
  by the server's case- and underscore-blind key.
- **Nested and repeated messages**: Query{Type}Batch with 1, 7 and 5,000 rows per message across
  1-, 700-, 533- and 6,000-row Arrow batches. A many-to-one relation (NULL, all-NULL children (an
  empty message that is still present), unknown child keys skipped). A one-to-many relation (an empty
  list, a NULL list, NULL items, large_list, nested Timestamps and decimals). Two levels with 3,000
  random rows, and the shape mismatches `_dict_row_to_message` drops (a list into a singular
  relation, a scalar into a relation).
- **Empty results**: no batches, empty batches, an empty batch around rows, in both RPC shapes.
- **Errors inside nested values**: an out-of-range timestamp in a relation, and an out-of-range date
  in a value that is dropped, both raise (`to_pylist` converts it first).

Run from the worktree root:

```
experiments/grpc_encoder_rs/build.sh
PYTHONPATH=.:experiments/grpc_encoder_rs/build:experiments/grpc_encoder_rs \
  ~/.provisa-agent/heavy.sh .venv/bin/python -m pytest -p no:cacheprovider -q \
  experiments/grpc_encoder_rs/test_compare.py
```

### Product behaviors the port reproduces

Matching the product meant porting these as they are:

- The IR infinities exist only in a column's own values (`column_values._with_infinities`). Inside a
  struct or a list, `to_pylist` reads the sentinel as an out-of-range value and raises, so the port
  does too.
- `to_pylist` converts every column, so a value that cannot be read raises even when its column has
  no field, or its relation is dropped.
- upb writes fields in field-number order, casts an out-of-range `float` to ±inf, and keeps an empty
  sub-message set by `CopyFrom` present (`1a 00`).

### Not ported (Rust raises `NotImplementedError` where Python encodes)

- Text into int, float or Timestamp fields (`int(str)`, `float(str)`, `datetime.fromisoformat`).
- Timestamps in a named time zone, and nanosecond timestamps, times and durations.
- `str()` of bytes, and of a timedelta into a bytes field, and a native array, struct or map into a
  string field. Each sends a Python repr (see defects).
- A relation in a Query{Type} row (`msg_cls(**kwargs)` builds it from the raw dict). The product
  never selects one there.
- A read mask's JSON sub-path selection (`restrict_json`), repeated scalar fields, and the proto
  types the product never generates (sint, fixed, enum).

## Benchmark (`bench.py`)

**Machine:** Apple M3 Pro, 12 cores, 18 GiB RAM, macOS 26.7.1. Python 3.12.0, protobuf 6.33.6
(upb), pyarrow 24.0.0, numpy 2.4.6. rustc 1.95.0, arrow-rs 56.2.1, pyo3 0.25.1. Release build with
fat LTO, `codegen-units=1` and `target-cpu=native`. Single-threaded, run through `heavy.sh` (the
machine's one job slot).

**Dataset:** 2,000,000 rows in 31 batches of 65,536 rows (DuckDB's Arrow stream batch size),
generated in memory with numpy from seed 495 and held once. Thirteen columns of `Orders`: id int64
(int64), qty int32 (int32), price float64 (double), ratio float32 (float), amount decimal128(18,2)
(double), name utf8 with some non-ASCII (string), created naive µs timestamp (Timestamp), created_tz
UTC µs timestamp (Timestamp), day date32 (string), active bool, payload JSON text (string), secret
utf8 30% NULL (string), customer_id int64 20% NULL. That is 246.0 MB of Arrow. The nested shape adds
`customers`, a struct of (int64, utf8, UTC timestamp) that is 10% NULL: the many-to-one relation as
a nested message, one nesting level (313.8 MB of Arrow).

**Shapes:** `query` is Query{Type}, one message per row. `batch` is Query{Type}Batch, 5,000 rows a
message (`_GRPC_BATCH_ROWS`). `nested` is relationship rows through `_dict_row_to_message`. Rust is
timed twice: "list" returns one `bytes` per message, as a gRPC Python handler must yield them, and
"framed" returns one buffer of length-prefixed gRPC frames per Arrow batch.

**Method:** each run is end to end, Arrow batches in and serialized messages out, with the output
counted and not kept. The first two batches' messages were checked byte-identical before timing, and
the message and byte totals of every run agree. Best of 3 runs for Python, best of 5 for Rust. CPU
time is `process_time`. Free memory stayed between 42% and 47% (the guard aborts under 25%).

| shape | rows | messages | message bytes | Python wall (cpu) | Rust list wall (cpu) | Rust framed wall | Python | Rust list | speedup (list / framed) |
|---|---|---|---|---|---|---|---|---|---|
| query | 2,000,000 | 2,000,000 | 259.0 MB | 26.94 s (26.78 s) | 0.726 s (0.726 s) | 0.686 s | 0.074 Mrow/s, 9.6 MB/s | 2.76 Mrow/s, 356.7 MB/s | **37.1× / 39.3×** |
| batch | 2,000,000 | 400 | 264.3 MB | 28.98 s (28.75 s) | 0.716 s (0.714 s) | 0.703 s | 0.069 Mrow/s, 9.1 MB/s | 2.79 Mrow/s, 369.2 MB/s | **40.5× / 41.2×** |
| nested | 2,000,000 | 2,000,000 | 328.0 MB | 48.53 s (48.18 s) | 0.892 s (0.890 s) | 0.831 s | 0.041 Mrow/s, 6.8 MB/s | 2.24 Mrow/s, 367.6 MB/s | **54.4× / 58.4×** |

Run-to-run spread: Python 26.9–28.0 s, 29.0–30.2 s and 48.5–48.9 s. Rust list 0.73–0.75 s,
0.72–0.85 s and 0.89–0.94 s. The full output, with the profiles, is in `bench-2m.log`.

### Where the Python time goes

- **Arrow → Python rows: 11.0 s of the 26.9 s query run (41%)**, measured separately as
  `stream_rows_from_arrow` alone. It is 12.6 s of the batch run and 19.7 s (41%) of the nested run,
  where the struct column's `to_pylist` builds a dict per row. Within it, `_with_infinities` costs as
  much as all the other columns' `to_pylist`. For each date32 and µs-timestamp column it does a cast,
  two compares, an `or` and an `any`, and when the batch holds no infinities (always here) a second
  `to_pylist`.
- **Rows → messages.** cProfile over 196,608 rows (3 batches), with the profiler inflating Python time
  about 1.5×, for the query shape (4.14 s):
  - `_proto_value`: 1.83 s cumulative (44%) for 2.46M calls. `Timestamp.FromDatetime` accounts for
    0.65 s of it (393K calls, through `GetMessageClass`, `utctimetuple`, `calendar.timegm` and a
    pytz `fromutc` for the UTC column). The rest is the type dispatch, `isinstance` and `float()` /
    `int()` per cell.
  - `_kwargs_for`: 0.50 s on its own, the per-cell loop and dict building. `msg_cls(**kwargs)` is
    upb's C constructor and sits in its caller's time (0.34 s).
  - `SerializeToString`: 0.05 s (1%). Serialization is not where the time goes. The time is in
    building Python objects for every cell and then a message from them.
  - The batch shape is the same work plus `{Type}Batch(rows=...)`, which copies each row message
    into the batch (about 2 s more over 2M rows).
  - Nested (7.7 s): `_dict_row_to_message` takes 5.0 s cumulative. Per row it recurses into a second
    message, does `fields_by_name.get` per key (3.2M calls, 0.44 s), and builds a `getattr(self._pb2,
    ...)` class lookup and a `CopyFrom` per relation.
- **What is left in Rust.** About 0.35 µs per row across 13 columns. It does one Arrow read per cell
  through an enum dispatch, a correctly rounded string parse per decimal → double, and a
  length-prefix move per message and per Timestamp. The "list" form spends about
  0.04 s of its 0.73 s making 2M Python `bytes` objects.

## Files

- `Cargo.toml`, `Cargo.lock`, `.cargo/config.toml`: the cdylib (pyo3 + arrow-rs); macOS link flags.
- `src/descriptor.rs`: the `FileDescriptorSet` decoder (messages, fields, presence).
- `src/lib.rs`: Arrow readers (`to_pylist`'s values and errors), the `_proto_value` conversions,
  the wire writer, the per-field plans (row and node modes), and the Python `Encoder`.
- `src/pyfmt.rs`: Python `str()` of floats, decimals, dates, datetimes, times, and
  `iso8601_duration`. float repr and `Decimal.__str__` come from experiments/pgwire_encoder_rs.
- `build.sh`: `cargo build --release`, then copies the module to `build/`. Nothing is installed into
  any venv.
- `python_ref.py`: the service and the product's Python path. `test_compare.py`: the differential
  test. `bench.py`: the benchmark. `bench-2m.log`: the benchmark's output.
