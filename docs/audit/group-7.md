# Audit — Group 7: Result Delivery

Date: 2026-06-18
Scope: **Group 7 — Result Delivery** (REQ-047–051, REQ-137–146). Output formats and
serialization under `provisa/executor/`, large-result redirect / CTAS under
`provisa/executor/redirect.py` + `trino_write.py` wired in `provisa/api/data/endpoint.py`,
and Arrow Flight under `provisa/api/flight/`.
Method: read implementation against requirement text with file:line evidence.
Companion to the Group-2 audit ([group-2.md](group-2.md)).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 047 | Output & Delivery | To spec | Flat JOIN rows reassembled to nested GraphQL JSON; m2o→object, o2m→array, null propagation `serialize.py:439` |
| 048 | Output & Delivery | To spec | NDJSON: one JSON object per line, Decimal-aware encoder `formats/ndjson.py:31` |
| 049 | Output & Delivery | Incomplete | CSV/Parquet emitted, but only denormalized/flat; no FK-preserving normalized multi-table output `formats/tabular.py:95` |
| 050 | Output & Delivery | Incomplete | Denormalized single-table CSV/Parquet present; no single-file-vs-partitioned option `formats/tabular.py:109` |
| 051 | Output & Delivery | To spec | Arrow IPC stream buffer + native Arrow Table for Flight `formats/arrow.py:36` |
| 137 | Redirect & CTAS | To spec | `X-Provisa-Redirect-Format` / `-Threshold` / `-Redirect` headers parsed; format-without-threshold forces redirect `endpoint.py:281` |
| 138 | Redirect & CTAS | To spec | Parquet/ORC use Iceberg CTAS; Trino writes S3 directly, no data through Provisa `trino_write.py:60`, `endpoint.py:1922` |
| 139 | Redirect & CTAS | To spec | JSON/NDJSON/CSV/Arrow serialized by Provisa, uploaded via boto3 `redirect.py:231` |
| 140 | Redirect & CTAS | To spec | Probe is `LIMIT threshold+1`; redirect when probe returns >= threshold, no COUNT(*) `endpoint.py:2184`, `endpoint.py:2096` |
| 141 | Redirect & CTAS | To spec | `schedule_s3_cleanup` deletes S3 objects after TTL `asyncio.sleep` `trino_write.py:114`, `endpoint.py:1948` |
| 142 | Redirect & CTAS | To spec | `PROVISA_REDIRECT_FORMAT` env default, falls back to parquet `redirect.py:71`, `endpoint.py:2182` |
| 143 | Arrow Flight | To spec | Flight server on 8815 streams batches; query routed through `_govern_and_route` security `flight/server.py:106`, `flight/server.py:557` |
| 144 | Arrow Flight | To spec | Zaychik Flight SQL proxy connection translates to Trino JDBC; SQL substituted inline `trino_flight.py:29`, `app.py:825` |
| 145 | Arrow Flight | To spec | Trino route returns `GeneratorStream` over a lazy batch generator, full result never materialized `server.py:592`, `trino_flight.py:134` |
| 146 | Arrow Flight | Not added | When `flight_client is None`, Trino route raises `FlightServerError` telling operator to configure Zaychik; no Trino-REST materialization fallback `server.py:511`, `server.py:579` |

11 to spec, 2 incomplete (REQ-049, REQ-050 — tabular output is denormalized-only,
no normalized/FK or partitioned variant), 1 not added (REQ-146 — no Trino REST
fallback for the Flight Trino route).

## Detail

### Output & Delivery (REQ-047–051)

- **REQ-047 — nested GraphQL JSON.** `serialize_rows` splits root vs nested columns,
  detects one-to-many paths, and rebuilds nested objects/arrays with null
  propagation for absent relationships
  ([serialize.py:439](../../provisa/executor/serialize.py#L439),
  [serialize.py:289](../../provisa/executor/serialize.py#L289)).
- **REQ-048 — NDJSON.** `rows_to_ndjson` emits one JSON object per row joined by
  `\n`, Decimal coerced to int/float
  ([ndjson.py:31](../../provisa/executor/formats/ndjson.py#L31)).
- **REQ-049 — normalized tabular.** `rows_to_csv` / `rows_to_parquet` produce a
  single flat table with dot-notation column names; the docstring labels the output
  "denormalized/flat" ([tabular.py:95](../../provisa/executor/formats/tabular.py#L95),
  [tabular.py:109](../../provisa/executor/formats/tabular.py#L109)). No code splits a
  nested result into multiple relational tables with FK columns preserved — the
  "normalized tabular ... with FK relationships preserved" path is absent.
- **REQ-050 — denormalized tabular.** The fully-flattened single-table CSV/Parquet
  is implemented ([tabular.py:59](../../provisa/executor/formats/tabular.py#L59)),
  but there is no "single file or partitioned" choice — output is always one buffer
  ([tabular.py:120](../../provisa/executor/formats/tabular.py#L120)).
- **REQ-051 — Arrow.** `rows_to_arrow_ipc` writes an Arrow IPC stream buffer and
  `rows_to_arrow_table` returns a native `pa.Table` for the Flight path
  ([arrow.py:36](../../provisa/executor/formats/arrow.py#L36)).

### Large Result Redirect & CTAS (REQ-137–142)

- **REQ-137 — client-controlled redirect.** Endpoint reads
  `X-Provisa-Redirect`, `X-Provisa-Redirect-Threshold`, `X-Provisa-Redirect-Format`
  ([endpoint.py:281](../../provisa/api/data/endpoint.py#L281)); `_build_redirect_params`
  derives force/threshold/format ([endpoint.py:240](../../provisa/api/data/endpoint.py#L240)).
- **REQ-138 — CTAS for native formats.** `is_trino_native_format` gates
  Parquet/ORC; `execute_ctas_redirect` issues `CREATE TABLE ... AS SELECT` on the
  Iceberg `results` catalog so Trino writes to S3
  ([trino_write.py:60](../../provisa/executor/trino_write.py#L60)); dispatched at
  [endpoint.py:2062](../../provisa/api/data/endpoint.py#L2062).
- **REQ-139 — Provisa serialization for non-native.** `upload_and_presign`
  serializes JSON/NDJSON/CSV/Arrow and `s3.put_object`s via boto3
  ([redirect.py:231](../../provisa/executor/redirect.py#L231),
  [redirect.py:109](../../provisa/executor/redirect.py#L109)).
- **REQ-140 — threshold probe.** `probe_limit = threshold + 1`
  ([endpoint.py:2186](../../provisa/api/data/endpoint.py#L2186)); injected via
  `_inject_probe_limit`, redirect triggered when `len(result.rows) >= probe_limit`
  ([endpoint.py:2096](../../provisa/api/data/endpoint.py#L2096)) — no COUNT(*); inline
  results below threshold are not re-run.
- **REQ-141 — scheduled cleanup.** `schedule_s3_cleanup` sleeps the TTL then
  `delete_objects` under the CTAS prefix
  ([trino_write.py:114](../../provisa/executor/trino_write.py#L114)); scheduled as a
  background task ([endpoint.py:1948](../../provisa/api/data/endpoint.py#L1948)).
- **REQ-142 — configurable default format.** `PROVISA_REDIRECT_FORMAT` read in
  `RedirectConfig.from_env`, default `parquet`
  ([redirect.py:71](../../provisa/executor/redirect.py#L71)); effective format falls
  back to parquet ([endpoint.py:2182](../../provisa/api/data/endpoint.py#L2182)).

### Arrow Flight (REQ-143–146)

- **REQ-143 — Flight server + security.** `ProvisaFlightServer` binds
  `grpc://0.0.0.0:8815` ([server.py:106](../../provisa/api/flight/server.py#L106));
  GraphQL/SQL queries run through `_govern_and_route` / `_govern_and_route_compiled`
  before execution, applying the governance pipeline
  ([server.py:557](../../provisa/api/flight/server.py#L557),
  [server.py:501](../../provisa/api/flight/server.py#L501)); a rate-limit slot caps
  concurrent streams per role ([server.py:233](../../provisa/api/flight/server.py#L233)).
- **REQ-144 — Zaychik proxy.** `create_flight_connection` opens an ADBC Flight SQL
  connection to Zaychik, which fronts Trino JDBC
  ([trino_flight.py:29](../../provisa/executor/trino_flight.py#L29)); host/port from
  `ZAYCHIK_HOST`/`ZAYCHIK_PORT` ([app.py:825](../../provisa/api/app.py#L825)).
- **REQ-145 — unbounded streaming.** `execute_trino_flight_stream` returns a schema
  plus a generator yielding `RecordBatch` lazily
  ([trino_flight.py:134](../../provisa/executor/trino_flight.py#L134)); the GraphQL
  Trino route wraps it in `flight.GeneratorStream`
  ([server.py:592](../../provisa/api/flight/server.py#L592)).
- **REQ-146 — fallback to Trino REST.** Not implemented. When
  `self._state.flight_client is None`, both the SQL and GraphQL Trino routes raise
  `FlightServerError("Zaychik Flight SQL proxy is not configured...")`
  ([server.py:511](../../provisa/api/flight/server.py#L511),
  [server.py:579](../../provisa/api/flight/server.py#L579)) instead of materializing
  through Trino REST. (The Cypher path does run against `trino_conn` directly at
  [server.py:443](../../provisa/api/flight/server.py#L443), but that is not the
  Zaychik-unavailable fallback the requirement describes.)

## Named tests

All six named files exist:

- `tests/unit/test_formats.py` (12 tests) — covers REQ-047–051 serializers.
- `tests/unit/test_redirect.py` (10 tests) — covers REQ-137–142.
- `tests/unit/test_zaychik_flight_unit.py` (25 tests) — covers REQ-143–145;
  `create_flight_connection` URI/auth verified
  ([test_zaychik_flight_unit.py:160](../../tests/unit/test_zaychik_flight_unit.py#L160)).
  No test asserts a Trino-REST fallback (REQ-146), consistent with the gap.
- `tests/integration/test_output_formats.py` (16 tests).
- `tests/integration/test_blob_upload.py` (4 tests).
- `tests/integration/test_arrow_flight_integration.py` (11 tests).

Gap: no named test exercises normalized/FK tabular output (REQ-049) or
partitioned output (REQ-050) — neither feature exists to test.

## Remaining tasks

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 049 | Feature | M | Add normalized tabular output: split nested result into per-entity tables with FK columns; emit a CSV/Parquet file set, not one flat table |
| 2 | 050 | Feature | M | Add single-file-vs-partitioned option for denormalized output (e.g. partition key + multiple Parquet files) |
| 3 | 146 | Feature | S | When `flight_client is None`, materialize the Trino SQL via Trino REST (`execute_trino` on `state.trino_conn`) and return a `RecordBatchStream` instead of raising, for both SQL and GraphQL Trino routes |
| 4 | 049/050 | Test | S | Add tests asserting normalized FK output and partitioned output once #1/#2 land |
| 5 | 146 | Test | S | Add a test that drives the Flight Trino route with `flight_client=None` and asserts REST-materialized batches |
