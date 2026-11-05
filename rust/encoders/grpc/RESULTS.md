# Rust gRPC result encoder (provisa._encoders.grpc)

**Wired (J518, REQ-2194, REQ-1965).** Every gRPC query RPC answers through this crate:
`provisa/grpc/server.py` builds one `Encoder` per RPC (`_row_encoder`) from the organisation's
compiled service and the result's columns, hands it each of the result's Arrow record batches
(`ResultStream.arrow_batches()`, on every route: the ENGINE stream, a DIRECT stream or buffered
read, a response-cache hit, the API route), and yields the serialized `{Type}` (Query{Type}) or
`{Type}Batch` (Query{Type}Batch) messages it returns. A group's nodes (Query{Type}GroupBy,
REQ-1405) are written by `encode_nodes` from the JSON its nodes column holds. The Python value
path it replaced (`_proto_value`, `_text`, `_json_value`, `_float32`,
`ProvisaServicer._dict_row_to_message`, and the per-row `_kwargs_for` / `msg_cls(**kwargs)`) is
gone from the product; it survives only as the differential's frozen reference,
`tests/unit/grpc_encoder/oracle.py`.

The crate began as the experiment `experiments/grpc_encoder_rs` (branch
`feat/grpc-encoder-rust` at dc26b72da): 2,234,308 messages byte-identical to the Python path and
37–58x faster on 2,000,000 rows (its `RESULTS.md` and `bench-2m.log` there).

## Interface

```python
from provisa._encoders import grpc

service = grpc.Service(file_descriptor_set_bytes)        # once per compiled service
enc = grpc.Encoder(service, "provisa.v1.Orders", out_cols,
                   masks=None,                            # read mask selections, per column
                   batch_message=None, batch_rows=0)      # "provisa.v1.OrdersBatch", 5000
enc.encode(record_batch) -> list[bytes]                   # the messages this batch completes
enc.finish() -> list[bytes]                               # the last, partly filled batch message
grpc.encode_nodes(service, "provisa.v1.Orders", field_number, nodes_json) -> bytes
```

The descriptor set is read with a hand-written decoder (`descriptor.rs`): the service is generated
at run time, so nothing is generated at build time. The Arrow batches are taken zero-copy through
the Arrow C data interface and the GIL is released while encoding.

## What each value is written as

As the Python path wrote it, with the values REQ-1965 (amended 2026-11-02, J502) fixes:

- a string field: text as it is; a number, date, time or timestamp as Python's `str()` of it; an
  interval as ISO 8601; bytes as PostgreSQL's hex (`\x6162`); PostgreSQL's infinities as
  `infinity` / `-infinity`; an array, struct or map as compact JSON (a decimal inside as its exact
  text, a date or time as PostgreSQL's json writes it, bytes as hex, an interval as ISO 8601, NaN
  and the infinities refused by name; a map an object keyed by `str(key)`, an empty one `[]`);
- a bytes field: bytes as they are, anything else as the string field's text;
- a Timestamp field: the instant (naive is UTC), PostgreSQL's infinities as
  9999-12-31T23:59:59.999999Z and 0001-01-01T00:00:00Z, text through `datetime.fromisoformat`;
- an integer field: `int()` (a date or timestamp its ordinal, text parsed as Python parses it),
  out of the field's range refused;
- a float field: `float()`, a finite value past float32 refused by name ("out of range for type
  real"); a double field: `float()`, a finite value past the double range refused by name (J502b:
  "out of range for type double precision"), PostgreSQL's own infinities sent as they are;
- a read mask's sub-path selection (REQ-803) inside JSON text (`json.loads`, restrict,
  `json.dumps` as Python writes it) or inside a struct.

A named time zone's offset is the pgwire crate's (`rust/pgwire/src/tz.rs`, read in by path):
chrono-tz's tables from the IANA release the pgwire crate carries, then the zone's POSIX rule, as
zoneinfo applies it (J498).

## Differential (`tests/unit/grpc_encoder/test_differential.py`)

122 tests. Both paths encode the same Arrow batches with the product's `generate_proto` service
(every proto type the type map produces, a redacted column, a many-to-one and a one-to-many
relation); each pair of messages is decoded with one descriptor and compared field by field
(floats within a relative tolerance of 1e-14, everything else exact, unknown fields equal), and
the bytes are compared too. Where the reference raises, the encoder raises (never
`NotImplementedError`). Covered: every Arrow integer width, floats (bit patterns, specials,
float16/32), decimal128 and decimal256, text into integer, float and Timestamp fields, dates
(date32, date64, infinities), timestamps in s/ms/µs/ns across fixed offsets and named zones,
times, intervals, durations, bytes, arrays, structs and maps as JSON (nested, with every scalar
kind inside), read masks, NULLs and absent fields, a redacted field, two columns of one field,
batching across Arrow batches, sliced batches, and a group's nodes (relations, shape mismatches,
errors).

The reference is the frozen Python path, corrected where the encoder is meant to answer otherwise.
These are the intentional differences:

1. A timestamp in a named zone is read as zoneinfo reads it (J498). pyarrow's `to_pylist` took
   pytz, which has no daylight time after 2037 and rounds old offsets to the minute: New York on
   2050-07-10T00:00Z is `2050-07-09 20:00:00-04:00`, where the Python path said `19:00:00-05:00`.
2. A nanosecond timestamp keeps its nanoseconds in a Timestamp field (pandas' `Timestamp`, which
   `to_pylist` makes of one, gave `FromDatetime` its microseconds only).
3. A finite value past a float or double field's range is refused by name (J502b), where
   `float()` made it an infinity that protobuf sent. A row engine's numeric past the double range
   reaches the encoder as numeric_out's text (`ir_arrow.row_batch`); the refusal names it in
   Decimal's exponent form (`1E+400`).
4. An int64 nanosecond duration of -2^63 is the duration it is; pandas reads that one value as its
   `NaT` sentinel, which the Python path sent as the text `NaT`.

Refused where the Python path sent something, each a Python repr or a rare input: a map key that
is bytes, an interval or nested (`str()` of it is a repr); a JSON text with a lone surrogate
escape; `fromisoformat` text with a UTC offset that has a fraction of a second.

## Tests

- `rust/encoders/grpc/test.sh`: the crate's own tests (`cargo test`; the JSON reader and writer,
  `int()` / `float()` / `fromisoformat` of text).
- `tests/unit/grpc_encoder/test_differential.py`: the differential above.
- `tests/unit/test_grpc_values_exact.py`: REQ-1965's values through the servicer's own query path,
  J502b on every route (INVALID_ARGUMENT, not UNKNOWN).

## Files

- `src/lib.rs`: Arrow readers, the value conversions, the wire writer, the per-column plans, the
  group nodes, the Python entry points.
- `src/descriptor.rs`: the FileDescriptorSet decoder. `src/json.rs`: JSON as Python's json module
  reads and writes it, and a read mask's selection. `src/parse.rs`: `int()`, `float()` and
  `datetime.fromisoformat()` of text. `src/pyfmt.rs`: Python's `str()` / `isoformat()` of the
  values, float repr and `Decimal.__str__` (from the pgwire encoder, J483).
