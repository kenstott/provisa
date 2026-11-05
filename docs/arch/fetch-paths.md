# Fetch paths: how each engine and source hands a result to the encoders

Status: map and plan, 2026-11-02; pgwire wired to the Rust encoder (section 1.1). The requirement (maintainer, 2026-11-02): every
engine hands the encoder an Arrow buffer or a direct row cursor on its own result, never rows a
Python client has parsed; where a columnar endpoint exists and Provisa calls a row endpoint, the
call is redesigned; an engine or source with neither is documented here with what it costs.

Evidence tags: `[code]` read in this tree, `[probe]` run against the installed client,
`[doc]` vendor documentation, not run here.

## 1. The seam

Every read reaches `pgwire._pipeline._run_plan_terminal` (`_execute_plan`'s terminal) through one
of four calls on the bound `FederationEngine` (`provisa/federation/runtime.py`):

| Call | Used by | Returns |
|---|---|---|
| `execute_engine` (async) | ENGINE route of every buffered surface | a materialized result |
| `execute_engine_sync` | the AUTOMATIC threshold drain, pgwire, Flight SQL, gRPC | a `ResultStream` |
| `execute_engine_arrow` / `execute_engine_stream` | Flight SQL, airport, MV refresh | `pa.Table` / `(schema, batches)` |
| `execute_native` / `execute_native_stream` | DIRECT route (one source on its own driver) | a materialized result / a row stream |

The result contract (`provisa/executor/result.py`) had only row results until this change:
`QueryResult` (a list of tuples) and `StreamingQueryResult` (batches of tuples). It now has two
Arrow results, so a terminal can carry the engine's Arrow to the consumer unchanged:

- `ArrowStreamResult` — a `StreamingQueryResult` whose source is the engine's record batches.
  `arrow_batches()` yields them as produced; `batches()`/`iter_rows()`/`rows` build Python rows
  from each batch only as a row consumer pulls it.
- `ArrowResult` — a `QueryResult` holding the engine's `pa.Table`; `rows` is built on first read
  (and its `repr` does not build it).

Encoders that read `arrow_batches()` get no Python row at all; the row consumers in place today
(GraphQL, JSON:API, Bolt) keep working, converting at the point they read. An async terminal
builds the rows on its executor thread before handing the result back (`ArrowResult.with_rows()`):
the event loop does no per-row work.

### 1.1 Every encoder takes the result in the form it has

pgwire's DataRow messages are written by the Rust encoder (`provisa._encoders.pgwire`, crate
`rust/pgwire`) and by nothing else, and so are gRPC's messages (`provisa._encoders.grpc`) and the
JSON surfaces' documents (`provisa._encoders.json`). Each encoder takes a result in the form it
has, and nothing converts a result into another form only to encode it:

- an Arrow result (`ArrowResult`, `ArrowStreamResult`; `executor.result.is_arrow`) hands the
  encoder the engine's record batches (`arrow_batches()`);
- a row result (`QueryResult`, `StreamingQueryResult`: a DIRECT driver's rows, a response-cache
  entry held as rows, an API source, an admin or catalog answer) hands the encoder its batches of
  rows (`batches()`), and the encoder reads each cell as it is (pgwire `encode_rows_stream`,
  gRPC `Encoder.encode_rows`, JSON `RowBatches`). A row result has no `arrow_batches()`;
- a passthrough's result (`DataRowResult`, `DataRowStreamResult`) hands on `datarow_batches()`,
  forwarded unchanged (`forward_raw`).

Each crate reads a row result's cell into the value model its Arrow reader already produces
(pgwire `PyVal`, gRPC `V`, JSON `P`: the value `column_values` gives of an Arrow cell), so one set
of writers answers both forms. A value of a type no rule reads (a range, a point, an address, a
time with a zone) is written as the text `ir_arrow.text_of` gives of it, chosen by the engine's
type name for the column. Each encoder's differential holds the rows form to the Arrow form's
bytes for every value both can hold (`tests/unit/*_encoder/`), and each `test_rows.py` holds a
driver's own values (UUIDs, addresses, memoryviews, relativedeltas, PostgreSQL's infinities, pytz
datetimes) to what the encoder answered of them when they were built into Arrow first. Where the
Arrow form could not hold a value as it is, the rows form now writes the value itself: dicts of
differing keys are JSON objects (Arrow held their repr's text), an int past 128 bits is a JSON
number, and a timestamp keeps its own offset rather than an instant in UTC.

What wraps a stream keeps its kind (`executor.result.rewrapped`): the live-read permit guard and
the response-cache tee hand Arrow on as Arrow (the tee keeps an Arrow IPC entry, which a hit
replays as an `ArrowResult`), and a passthrough's DataRow messages as DataRow messages. A SQL
cursor buffers the batches it pulled and hands out slices of them; query_to_xml and its kin read
Python values from the batches (they write an XML document, not DataRow messages).

DuckDB's lossless export carries `HUGEINT`, `UHUGEINT`, `VARINT`, `BIT` and `TIMETZ` as its own
bytes (Arrow opaque types); the encoder reads them from those bytes as `duckdb_arrow.arrow_rows`
does, so a DuckDB result reaches the wire with no Python row at all.

### 1.2 gRPC

Each Query{Type} / Query{Type}Batch RPC hands its result (ENGINE stream, DIRECT stream or
buffered read, response-cache hit, API route) to one `Encoder` built from the organisation's
compiled service, in the form it has (1.1): `encode` per Arrow record batch, `encode_rows` per
batch of rows (a DIRECT stream's fetched rows go to it as they are fetched). A group's nodes
(Query{Type}GroupBy) are written by the encoder from the JSON its nodes column holds.
`rust/encoders/grpc/RESULTS.md` records what each value is written as and the differential.

## 2. Map

"Python rows built at" is where a Python object per value is first created for a result.

### 2.1 ENGINE terminals (the bound federation engine)

| Engine | Read call today | What it returns | Python rows built at | Columnar alternative |
|---|---|---|---|---|
| DuckDB (`duckdb_runtime`) | **this change:** `run`/`run_sync` read `to_arrow_table`/`to_arrow_reader` on a lossless-export cursor | `ArrowResult` / `ArrowStreamResult` | the consumer, via `duckdb_arrow.arrow_rows` | — (done) `[code]` |
| DuckDB, before | `run_sync`: `stream_from_dbapi` (`fetchmany`) unless an INTERVAL/DATE/TIMESTAMP column; `run`: `fetch_rows` | row stream / `QueryResult` | DuckDB's C++ row fetch | its Arrow reader `[code]` |
| Trino (`TrinoBackend.execute`/`execute_sync`) | `executor.trino_flight.execute_trino_flight`/`_sync`: ADBC Flight SQL through the Zaychik proxy | `ArrowResult` / `ArrowStreamResult` | `trino_flight.trino_rows` at the consumer | done (option A, §4) `[code]` |
| Postgres engine (`pg_runtime`) | `run_sync`: psycopg2 named (server-side) cursor, `fetchmany`; `run`: same, drained | row stream / `QueryResult` | psycopg2 text-protocol decode | ADBC postgresql (already pooled for `run_arrow*`, COPY BINARY under the hood) `[code]` |
| SQLAlchemy engines (`sqlalchemy_runtime`) | `run_sync`: `stream_results` / DB-API `fetchmany`; `run`: `fetchall` | row stream / `QueryResult` | the dialect's DB-API driver | per dialect, §3 |
| Snowflake (`snowflake_runtime`) | `run_sync`: `fetch_arrow_batches` → `stream_rows_from_arrow` | row stream | `stream_rows_from_arrow` (`column_values` / `to_pylist`) at the fetch | return `ArrowStreamResult` instead (§3) `[code]` |
| Databricks (`databricks_runtime`) | `run_sync`: `fetchmany_arrow` (Cloud Fetch) → `stream_rows_from_arrow` | row stream | `stream_rows_from_arrow` | same `[code]` |
| BigQuery (`bigquery_runtime`) | `run_sync`: `to_arrow_iterable` (Storage Read API) → `stream_rows_from_arrow` | row stream | `stream_rows_from_arrow` | same `[code]` |
| ClickHouse (`clickhouse_runtime`) | `run_sync`: `query_arrow_stream` → `stream_rows_from_arrow` | row stream | `stream_rows_from_arrow`; the chDB backend also `to_pylist`s per column | same `[code]` |
| Fabric / Synapse (`mssql_warehouse_runtime`) | `run_sync`: pyodbc `fetchmany` → packed into Arrow → `stream_rows_from_arrow` | row stream | pyodbc, then again from Arrow (rows → Arrow → rows) | `arrow-odbc` (§3) `[code]` |

Every `run` (async) of the Arrow warehouses goes through `run_async_materialized`, which drains
the row stream into a `QueryResult` on the executor thread: the same `to_pylist` cost, all of it.

### 2.2 DIRECT terminals (`executor/direct.py`, `executor/drivers/`)

| Source | Driver | Python rows built at | Columnar alternative |
|---|---|---|---|
| PostgreSQL, pgwire servers | psycopg 3 pool (`drivers/postgresql.py`), server-side cursor for streams | psycopg 3 binary/text loaders | none taken: stays a row source, its rows encoded as they are (ADBC postgresql not taken, see section 3, item 4); for pgwire, the raw DataRow passthrough is the direct cursor `[code]` |
| DuckDB / MotherDuck | **this change:** `drivers/duckdb_driver.py` reads `to_arrow_table` (an `ArrowResult`) and streams from `to_arrow_reader`; before, `fetchall` / `fetchmany` | `duckdb_arrow.arrow_rows`, in the driver (`execute`) or per fetch (`open_stream`) | — (done) `[code]` |
| Snowflake | `drivers/snowflake.py`: `fetchall` / `fetchmany` | the connector's row conversion | `fetch_arrow_batches` (the runtime already uses it) or ADBC snowflake (installed, 1.11.0) `[code]` |
| Databricks | `drivers/databricks.py` via `pooled.py`: `fetchall` / `fetchmany` | the connector | `fetchmany_arrow` / `fetchall_arrow` (the runtime already uses it) `[code]` |
| BigQuery | `drivers/bigquery.py`: `_JobCursor.fetchmany` over the job's row iterator (REST `tabledata.list`, JSON) | google-cloud-bigquery JSON decode | `RowIterator.to_arrow_iterable(bqstorage_client=...)`, the Storage Read API (installed 2.39.0) `[code]` `[probe]` |
| ClickHouse | `drivers/clickhouse.py`: `execute` reads `client.query` rows, `open_stream` `query_row_block_stream` | clickhouse-connect's row decode | `query_arrow_stream` (the driver's own `execute_arrow` already uses it) `[code]` |
| SQL Server, Fabric/Synapse | pyodbc (`drivers/sqlserver.py`, `mssql_warehouse.py`, `cursor_stream.py`) | pyodbc | `arrow-odbc` (not installed) `[doc]` |
| Oracle | oracledb (`drivers/oracle.py`, `cursor_stream.py`) | oracledb | `Connection.fetch_df_batches` (oracledb 4.0.1: present), Arrow PyCapsule data frames `[probe]` |
| MySQL / MariaDB / SingleStore | PyMySQL unbuffered cursor | PyMySQL | none native; `arrow-odbc` over Connector/ODBC is the only no-Python-rows path `[doc]` |
| Athena | PyAthena default cursor, `fetchall` | PyAthena | `pyathena.arrow.cursor.ArrowCursor` (present in 3.39.0), optionally with UNLOAD to Parquet `[probe]` |
| Exasol | pyexasol, `fetchmany` | pyexasol | `export_to_parquet` / `export_to_polars` (HTTP transport; present in 2.4.0) `[probe]` |
| HiveServer2 | impyla, `fetchall`/`fetchmany` | impyla | `HiveServer2Cursor.fetchcolumnar` (HS2 columnar TRowSet; present in 0.24.0) — columns, still Python lists `[probe]` |
| Trino / Presto as a source | SQLAlchemy `trino` dialect | the trino client (JSON pages) | the same as the Trino engine, §4 |
| Other SQLAlchemy dialects | `drivers/sqlalchemy_driver.py` | the dialect's DB-API | per dialect |
| Neo4j | neo4j Bolt driver, records | the driver | none |
| Qdrant, code sources | their own clients / Python functions | the client / the function | none: the data is Python objects by nature |

### 2.3 The admin route (`source_id == "provisa-admin"`)

`_run_plan_terminal` reads `meta.*` through `state.model_db` (`core/database.py`): a synchronous
SQLAlchemy engine on psycopg behind an asyncpg-shaped API; `fetch_with_columns` /
`fetch_described` return records, made into tuples in the terminal. Python rows are built by
psycopg. Columnar alternative: ADBC postgresql, or psycopg `COPY (...) TO STDOUT (FORMAT BINARY)`
decoded straight into Arrow buffers. These results are small catalog reads; it is the lowest
priority row path.

## 3. Plan for each remaining engine

Ordered by what it costs today and how small the change is.

1. **Snowflake, Databricks, BigQuery, ClickHouse runtimes** — already read Arrow; only
   `stream_rows_from_arrow` turns it into rows at the fetch. Change: `run_sync` returns
   `ArrowStreamResult(schema, batches, types, to_rows=<column_values rows>)`, and `run` returns an
   `ArrowResult` over the table (`run_arrow`) instead of `run_async_materialized`. One shared
   helper in `runtime_support`. Values a row consumer sees are unchanged (same `column_values`).
   Proven only on the warehouse lane.
2. **DIRECT Snowflake / Databricks / ClickHouse** — the same Arrow calls the runtimes use
   (`fetch_arrow_batches`, `fetchmany_arrow`, `query_arrow_stream`) behind `open_stream` and
   `execute`. Warehouse lane.
3. **BigQuery DIRECT** — replace the REST row iterator with
   `job.result().to_arrow_iterable(bqstorage_client=BigQueryReadClient(...))`. The Storage Read
   API is billed per byte read; a small result read through it costs more than through
   `tabledata.list`. Warehouse lane.
4. **Postgres DIRECT driver: decided, stays psycopg rows** (maintainer, 2026-11-03). Every encoder now
   takes a row result's rows as they are, so the Arrow build in Python that made DIRECT Postgres slow is gone; what remains is psycopg's own per-value decode. ADBC
   postgresql (1.11.0, probed against PostgreSQL 17, J567) was not taken because it cannot give
   the values the driver serves today: PostgreSQL's `infinity` / `-infinity` dates and timestamps
   raise inside ADBC ("date value out of range"), so there is no value to encode; uuid, inet,
   cidr, macaddr, timetz, point and the other geometric types, bit, tsvector, the reg* types and
   every range arrive as PostgreSQL's binary wire bytes (`arrow.opaque` on binary storage), which
   an encoder would write as bytes; money arrives as int64 cents; numeric arrives as text with
   NaN lowercased to `nan`; oid arrives as int32, which overflows past 2^31. Taking ADBC would
   need readers for those opaque types in each encoder crate and a way past the infinity failure.
   The rest of this item (the Postgres engine) is unchanged:
   **Postgres engine (`pg_runtime`)** — `run_sync`/`run` on the ADBC postgresql connection the runtime already pools for `run_arrow*`
   (`cursor.fetch_record_batch()`), returning `ArrowStreamResult`/`ArrowResult`. Open point: the
   rows a row consumer is given must equal psycopg's (interval, infinity, numeric, json, arrays,
   the column type oids pgwire reports) — the same parity test this change wrote for DuckDB
   (`tests/unit/test_duckdb_arrow_rows.py`), against a Postgres container.
5. **Fabric / Synapse / SQL Server** — `arrow-odbc` (`read_arrow_batches_from_odbc`) on the same
   ODBC connection string pyodbc uses; removes the rows → Arrow → rows round trip in
   `mssql_warehouse_runtime`. New dependency (MIT; BSL-compatible). Warehouse lane for Fabric.
6. **Oracle** — `connection.fetch_df_batches(sql, size=...)`, wrapped with `pa.table(df)`
   (PyCapsule, no copy).
7. **Athena** — `ArrowCursor` (results file read into Arrow, or UNLOAD to Parquet for large reads).
8. **Exasol** — `export_to_parquet`/`export_to_polars` through the HTTP transport (needs the
   transport reachable from Provisa).
9. **HiveServer2** — `fetchcolumnar` gives columns, not Arrow; Arrow is built from them in Python
   (one list per column, no row tuples). Partial.
10. **SQLAlchemy runtime** — follows its dialect: Postgres → 4, MSSQL → 5, Oracle → 6, MySQL →
    documented below.
11. **Admin route** — ADBC postgresql or `COPY ... BINARY` into Arrow; low priority (small reads).

### Documented exceptions (no columnar endpoint)

| Source | Read path | Rows through Python | Cost |
|---|---|---|---|
| MySQL / MariaDB / SingleStore | PyMySQL unbuffered cursor | yes, one Python tuple per row | PyMySQL is pure Python: decode of every value in Python, typically the slowest driver here. `arrow-odbc` over Connector/ODBC avoids it at the cost of an ODBC driver in the image. |
| Neo4j | Bolt records | yes | per-record Python objects; graph results are rarely wide or long |
| Qdrant | its Python client | yes | points arrive as Python objects |
| Code sources | Python functions | yes | the function produces Python values by definition |

## 4. Trino: the engine read through Flight (option A)

`execute`/`execute_sync` read through the Zaychik Flight SQL proxy only, as `execute_arrow`/
`execute_stream` already did. The proxy at the commit the image builds
(`Raiffeisen-DGTL/zaychik-trino-proxy@17fce97`) answered differently from the trino client in six
ways; `zaychik/trino-statement-protocol.patch` and `executor/trino_flight.py` close each `[code]`:

1. **Precision**: the proxy ran statements through Trino JDBC and mapped TIMESTAMP/TIME to
   milliseconds. A Flight statement now runs through the Trino client protocol
   (`StatementClient`, the protocol the Python client speaks) and TIME/TIMESTAMP are microsecond
   Arrow types; a value with more than six fractional digits is rounded half-to-even, as the
   Python client rounds it. A date is read from the protocol's text (proleptic Gregorian), not
   through `java.sql.Date`.
2. **Types JDBC could not map** (`JAVA_OBJECT`/`OTHER`: map, row, uuid, json, ipaddress, the
   intervals; also arrays and time with time zone): Utf8 holding the protocol's text (scalars) or
   JSON (structural), `trino.encoding` saying which. `trino_rows` decodes them with the Trino
   client's own value mappers (`trino.mapper`), so a nested value is the Python value the client
   gave (`NamedRowTuple`, `UUID`, `relativedelta`, ...). A REAL is read as the shortest decimal of
   the float, as the protocol writes it.
3. **Session**: every Flight statement is a Trino session of its own. Its `source` (the org,
   REQ-056), catalog, schema and session properties (the FTE hints, the caller's hints,
   `query_max_execution_time`) travel in the `x-provisa-trino-context` call header and reach
   Trino as the client protocol sends them. With no header the proxy keeps the JDBC driver's
   source `trino-jdbc` (the `global.flight` resource group).
4. **Type names**: every field carries `trino.type` (the coordinator's own type string, what
   the trino client's `description` gave) and `trino.type_signature`; `column_types` come from it.
5. **Errors**: a failed statement's Flight error carries Trino's error name, type, code, message
   and query id as trailers (`x-trino-error-*`); `trino_flight.engine_error` builds the same
   `FederationError` (and `SystemCatalogUnavailable`) the trino client's `TrinoQueryError` did, so
   the coordinator-loss retry and the memory-error refusal work by name. A proxy or coordinator
   that cannot be reached is `EngineUnreachable` (a `ConnectionError`: retried inside the budget,
   classified `connection`).
6. **Parameters**: bound as the trino client binds them, `EXECUTE IMMEDIATE '<sql>' USING
   <literals>`, with the client's own literal rendering (`trino_flight.render_param`).

The statement runs at GetFlightInfo until Trino names its columns, so the schema the FlightInfo
announces is the stream's, and a statement Trino rejects is answered there. A failure while the
rows of a streamed read are being read raises the engine's error at the consumer; it is not
retried (the materialized `execute` retries the whole read).

An org whose terminal is a coordinator other than the proxy's configured one (an external engine,
REQ-1412; an isolated host, REQ-1043) names it in the context header (`server`). The proxy dials it
only if the deployment declared it at start (`TF_TRINO_ALLOWED_SERVERS`: `scheme://host:port`
entries, a host may hold `{org_id}`; compose fills it from `PROVISA_ZAYCHIK_ALLOWED_SERVERS`, and
`docker-compose.isolated-engine.yml` adds the isolated host template). Any other address is refused
by name (`CoordinatorRefused`) with nothing dialed: a request never chooses where the proxy
connects. On a provisioning deployment each shard's proxy is a sidecar in front of that shard's own
coordinator (REQ-1518) and no statement names one.

## 5. Measured: DuckDB, 2,000,000 rows

`select i, i::varchar, i * 0.5 from range(2000000)` (and the same plus a DATE column), median of
5, this machine, through `~/.provisa-agent/heavy.sh`. Before is `origin/main` (1a218e2236).

| Read | Before | After |
|---|---|---|
| `run_sync`, terminal gets the result (encoder face, `arrow_batches()`) | n/a (rows only) | 0.06 s |
| `run_sync`, drained as Python rows | 1.17 s | 0.45 s |
| `run_sync` with a DATE column, as Python rows | 2.56 s | 1.44 s |
| `run` + `.rows` (rows built on the executor thread) | 1.23 s | 0.52 s |
| `run` with a DATE column + `.rows` | 2.76 s | 1.55 s |

Row consumers got faster too: `arrow_rows` reads text, decimals and null-free numbers through
numpy (the same Python values, five to ten times faster than `to_pylist`).
