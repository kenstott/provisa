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
has (REQ-2195), and nothing converts a result into another form only to encode it:

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
| Trino (`TrinoBackend.execute`/`execute_sync`) | `executor.trino.execute_trino`: trino Python client, `fetchall` | `QueryResult` | the trino client, decoding Trino's JSON pages | Zaychik Flight SQL (`execute_arrow`/`execute_stream` already use it) — **blocked, see §4** `[code]` |
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
| PostgreSQL, pgwire servers | psycopg 3 pool (`drivers/postgresql.py`), server-side cursor for streams | psycopg 3 binary/text loaders | ADBC postgresql; for pgwire, the raw DataRow passthrough is the direct cursor `[code]` |
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
4. **Postgres engine (`pg_runtime`) and the Postgres DIRECT driver** — `run_sync`/`run` and
   `open_stream` on the ADBC postgresql connection the runtime already pools for `run_arrow*`
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

## 4. Trino: why the engine read is not yet switched to Flight

`execute_arrow`/`execute_stream` already read through the Zaychik Flight SQL proxy
(`executor/trino_flight.py`). Switching `execute`/`execute_sync` to it as well, with no row path
left, changes answers today. From the proxy's source at the commit the image builds
(`zaychik/Dockerfile`, `Raiffeisen-DGTL/zaychik-trino-proxy@17fce97`) `[code]`:

1. **Precision**: TIMESTAMP and TIME are mapped to `Timestamp(MILLISECOND)` /
   `Time(MILLISECOND)` (`FlightSQLProducerHelper.getArrowTypeFromJdbcType`). Trino's
   `timestamp(6)` loses its microseconds.
2. **Types that fail**: a column whose JDBC type is outside the mapped list (BOOLEAN, the
   integers, DECIMAL, REAL/DOUBLE, the character types, DATE, TIME, TIMESTAMP, the binary types,
   ARRAY, NULL, STRUCT) raises `Unmapped JDBC type`. The Trino JDBC driver reports `map`, `row`,
   `uuid`, `json`, `ipaddress` and the interval types as `JAVA_OBJECT`/`OTHER` `[doc]`, not run
   here: such a query would fail where the trino client answers it.
3. **Session properties**: every statement opens a fresh JDBC connection
   (`TrinoConnectionFactory.getConnection`), so `SET SESSION` cannot carry over; the
   `query_max_execution_time` every read sets, and the FTE `retry_policy` hints, would be lost.
4. **Type names**: the Arrow schema carries no Trino type name; `column_types` (pgwire
   RowDescription, MV output introspection) would come from Arrow types (every integer a
   BIGINT).
5. **Errors**: failures come back as `CallStatus.INTERNAL` with a message; Trino's error name is
   gone, so `FederationError` classification, the coordinator-loss retry and `refusal_of` stop
   working.
6. **Parameters**: `_substitute_params` inlines only strings, None and `str(value)`; a date,
   timestamp, decimal or bytes parameter is rendered wrongly.

Options (decision for the maintainer):

- **A (recommended)**: extend the Zaychik patch set the image already applies: microsecond
  time/timestamp units; map `JAVA_OBJECT`/`OTHER` to Utf8 (JSON text) and put Trino's type name in
  each field's metadata; take session properties from a Flight call header into the JDBC
  `sessionProperties` connection property; return Trino's error name/code in the Flight error
  detail. Then `execute`/`execute_sync` read Flight only, `column_types` from field metadata,
  `FederationError` from the error detail, and parameters bound by a typed literal renderer.
- **B**: keep the trino client for `execute`/`execute_sync` and record Trino in the exceptions
  table. The requirement allows this only where no columnar endpoint exists, so this needs the
  requirement amended.
- **C**: Trino's spooling protocol (Trino 466+) moves results through object storage, but its
  encodings in Trino 481 are JSON (`json`, `json+zstd`, `json+lz4`) `[doc]`; it saves transfer,
  not the Python decode. Not a fix on its own.

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
