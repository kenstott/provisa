# Copyright (c) 2026 Kenneth Stott
# Canary: 5eaf445b-ca47-4509-bd81-026c44187b2b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Fully automated perf-demo benchmark runner. Runs queries.QUERIES against whichever Provisa
transports are reachable (pgwire/SQL, Bolt/Cypher, HTTP/Cypher-JSON, HTTP/GraphQL-JSON, Arrow
Flight (gRPC/Arrow), native gRPC/Protobuf), and writes a tagged JSON report plus a printed
summary table.

Transport vs. encoding: this matrix has two orthogonal axes, not N flat "transports" —
WIRE TRANSPORT (pgwire / Bolt / HTTP / gRPC) x ENCODING carried on it (SQL rows / Cypher-JSON /
GraphQL-JSON / Arrow / Protobuf). ``http`` and ``graphql`` are the SAME wire transport (both hit
this server's HTTP port), just different encodings on it (Cypher-JSON vs GraphQL-JSON) — see
HttpTransport/GraphqlTransport's docstrings. ``flight`` and ``grpc`` are likewise both gRPC
transport, differing only in encoding (Arrow vs Protobuf) — see FlightTransport/GrpcTransport's
docstrings. Each is still implemented as its own Transport subclass (one class per instance in
this matrix, transport+encoding together), matching every existing transport's pattern; the
distinction matters for how results should be read and for scoping the future 2-VM/network
benchmark (REQ-1859/1860), not for how this file is structured.

This script only ever talks to a Provisa instance that is ALREADY running — it never starts,
stops, or restarts Provisa itself (see orchestrate.sh, which is the piece meant to be run
directly; this script is what it invokes once per engine).

Two metrics anchor the report, deliberately different axes:
  - bytes/sec: the ONE number comparable across every query in the matrix regardless of shape
    (a single-row lookup and an 80M-row scan land on the same scale). Computed identically for
    every transport: json.dumps(rows, default=str) of the returned row data — a logical payload
    size, not a raw wire-protocol byte count, so the metric measures query cost, not encoding
    density. This is a deliberate methodology choice: Arrow's wire format is denser than pgwire's,
    but that's an encoding-efficiency fact, not something this benchmark is trying to isolate.
  - QPS + p50/p95/p99 latency: only meaningful comparing the SAME query across engines/transports,
    never across different queries in the matrix.

Every transport's run_sql/run_cypher returns (row_count, byte_count, overhead_s): overhead_s is
time that method itself spent computing byte_count (json.dumps, plus Arrow->Python conversion for
Flight) — real CPU cost, but the benchmark harness's own, not Provisa's/the network's. Every
latency sample subtracts it out. Without this, large_scan's ~8000 batches spent a material
fraction of "measured latency" JSON-serializing rows for the bytes/sec metric alone — confirmed
live on the GCP perf-bench run before this was split out.

Usage:
  python run_benchmark.py --engine duckdb --pgwire-host localhost --pgwire-port 5439 \\
      --bolt-host localhost --bolt-port 17687 --http-base-url http://localhost:8001 \\
      --flight-host localhost --flight-port 8815 --output-dir results
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from queries import QUERIES, Query

TRANSPORTS = ("sql", "cypher", "http", "flight", "graphql", "grpc")


def _payload_bytes(rows: list[dict]) -> int:
    """Uniform, transport-independent measure of the data a query returned. Call per BATCH and
    accumulate — never on a full large result: building one Python list of every row AND a full
    JSON string of it simultaneously OOM-killed this harness on an 80M-row scan (31GB RSS, kernel
    oom-killer, confirmed live via dmesg on the GCP perf-bench run)."""
    return len(json.dumps(rows, default=str).encode("utf-8"))


# Sampling for _ByteEstimator: only json.dumps-measure 1 in every _SAMPLE_STRIDE batches, capped
# at _MAX_SAMPLED_BATCHES total. Row width in a generated synthetic table is uniform (same schema,
# similar value ranges throughout), so extrapolating total bytes from a sampled average bytes/row
# is statistically sound — and for large_scan's ~8000 batches, measuring (and dict-converting)
# every single one was real wall-clock time spent on the harness's OWN bookkeeping for its biggest
# cost center. Spread across the whole result (not just the first N batches) so the estimate isn't
# biased by any position-correlated row-size variation.
_SAMPLE_STRIDE = 8
_MAX_SAMPLED_BATCHES = 50


class _ByteEstimator:
    """Accumulates row_count exactly and byte_count as a sampled estimate across streamed
    batches — exact for small results (every batch gets sampled), extrapolated for large ones.
    ``overhead_s`` is real time this class spent on its own bookkeeping (row->dict conversion +
    json.dumps for sampled batches only), excluded from the caller's timed latency the same way
    as every other transport's overhead_s."""

    __slots__ = ("row_count", "overhead_s", "_sampled_bytes", "_sampled_rows", "_batch_idx")

    def __init__(self) -> None:
        self.row_count = 0
        self.overhead_s = 0.0
        self._sampled_bytes = 0
        self._sampled_rows = 0
        self._batch_idx = 0

    def add(self, n: int, raw_batch, to_dicts) -> None:
        """``n`` is this batch's row count (explicit, not len(raw_batch) — an Arrow RecordBatch
        uses .num_rows, not len()). ``raw_batch`` is whatever the transport buffers per-batch
        (asyncpg/neo4j Records, or an Arrow RecordBatch); ``to_dicts(raw_batch) -> list[dict]``
        converts it, called ONLY when this batch is actually sampled — a skipped batch pays no
        conversion cost at all, not just no json.dumps cost."""
        self.row_count += n
        take_sample = (
            self._batch_idx % _SAMPLE_STRIDE == 0
            and self._sampled_rows < _MAX_SAMPLED_BATCHES * _FETCH_BATCH_SIZE
        )
        if take_sample:
            t0 = time.perf_counter()
            self._sampled_bytes += _payload_bytes(to_dicts(raw_batch))
            self._sampled_rows += n
            self.overhead_s += time.perf_counter() - t0
        self._batch_idx += 1

    @property
    def byte_count(self) -> int:
        if self._sampled_rows == 0:
            return 0
        return round(self._sampled_bytes / self._sampled_rows * self.row_count)


# Bounded batch size for streaming a large result through row_count/byte_count accumulation
# instead of materializing the whole thing — see _payload_bytes's docstring for why this matters.
_FETCH_BATCH_SIZE = 10_000


@dataclass
class Sample:
    elapsed_s: float
    row_count: int
    payload_bytes: int
    error: str | None = None


@dataclass
class QueryResult:
    query_id: str
    category: str
    transport: str
    samples: list[Sample] = field(default_factory=list)
    concurrency: int | None = None  # set for concurrency-ramp samples

    @property
    def ok_samples(self) -> list[Sample]:
        return [s for s in self.samples if s.error is None]

    def summary(self) -> dict:
        ok = self.ok_samples
        errors = len(self.samples) - len(ok)
        if not ok:
            return {
                "query_id": self.query_id,
                "category": self.category,
                "transport": self.transport,
                "concurrency": self.concurrency,
                "errors": errors,
                "status": "all_failed",
            }
        latencies_ms = sorted(s.elapsed_s * 1000 for s in ok)
        total_bytes = sum(s.payload_bytes for s in ok)
        total_rows = sum(s.row_count for s in ok)
        wall_s = (
            sum(s.elapsed_s for s in ok)
            if self.concurrency is None
            else max(s.elapsed_s for s in ok)
        )
        # Concurrency runs overlap in wall-clock time; sequential runs sum their own elapsed time
        # as a proxy for "time spent doing this query" (each ran back-to-back, single-threaded).
        return {
            "query_id": self.query_id,
            "category": self.category,
            "transport": self.transport,
            "concurrency": self.concurrency,
            "n": len(ok),
            "errors": errors,
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "bytes_per_sec": total_bytes / wall_s if wall_s > 0 else None,
            "qps": len(ok) / wall_s if wall_s > 0 else None,
            "latency_ms_p50": statistics.median(latencies_ms),
            "latency_ms_p95": latencies_ms[int(len(latencies_ms) * 0.95) - 1]
            if len(latencies_ms) > 1
            else latencies_ms[0],
            "latency_ms_p99": latencies_ms[int(len(latencies_ms) * 0.99) - 1]
            if len(latencies_ms) > 1
            else latencies_ms[0],
            "latency_ms_max": latencies_ms[-1],
        }


class Transport:
    name = "base"

    def available(self) -> bool:
        raise NotImplementedError

    def run_sql(self, *_args, **_kwargs) -> tuple[int, int, float]:
        """Returns (row_count, byte_count, overhead_s) — never the materialized rows themselves
        (a large result must stream through _FETCH_BATCH_SIZE-sized batches; see _payload_bytes).
        ``overhead_s`` is time this METHOD spent doing client-side bookkeeping (computing
        _payload_bytes) that the caller's wall-clock latency measurement must subtract out — it is
        real CPU cost, but it's the benchmark harness's own cost, not Provisa's/the network's, and
        including it inflated every reported latency (confirmed: for large_scan's ~8000 batches
        this was a significant fraction of the "measured" time)."""
        raise NotImplementedError

    def run_cypher(self, *_args, **_kwargs) -> tuple[int, int, float]:
        raise NotImplementedError

    def run_graphql(self, *_args, **_kwargs) -> tuple[int, int, float]:
        """Same (row_count, byte_count, overhead_s) contract as run_sql/run_cypher — GraphQL is
        just another query-text encoding, not a different result contract."""
        raise NotImplementedError

    def run_grpc(self, *_args, **_kwargs) -> tuple[int, int, float]:
        """Same contract, but the first arg is a query SPEC dict (queries.py's Query.grpc), not
        query text — the native gRPC surface has no query language, only per-table typed RPCs
        (Query{Type}/Query{Type}GroupBy). See GrpcTransport's docstring."""
        raise NotImplementedError

    def close(self) -> None:
        pass


PGWIRE_POOL_SIZE = 50  # real concurrent connections, independent of saturation's 4000 workers —
# see run_saturation's docstring: a pool caps actual backend connections at a realistic number
# (matching how a real client behaves) so saturation tests query throughput, not connection-slot
# exhaustion. A single shared psycopg2 connection (the original, buggy version of this class) is
# not thread-safe for concurrent cursor use at all — confirmed by 100% failures under load.


class PgwireTransport(Transport):
    """Raw SQL over Provisa's Postgres wire protocol server, via a connection pool (see
    PGWIRE_POOL_SIZE) so concurrent saturation load doesn't corrupt a single shared connection.

    Uses asyncpg, not psycopg2: psycopg2's default (unnamed) cursor is not a genuine streaming
    cursor — cur.execute() pulls the WHOLE result into libpq's internal buffer immediately, and
    fetchmany() only paginates that already-fully-buffered data client-side. That OOM-killed this
    harness on large_scan's 80M-row order_items (31GB RSS, confirmed via dmesg on the GCP
    perf-bench run) even after batching the row_count/byte_count accumulation — the buffering
    happened before the batching loop ever ran. asyncpg's cursor() genuinely streams: it uses the
    wire protocol's native Bind/Execute(limit)/PortalSuspend mechanism (not textual SQL DECLARE
    CURSOR/FETCH, which Provisa's pgwire compiler does not implement), the exact same mechanism
    Provisa's own DIRECT postgres driver already relies on for real streaming
    (provisa/executor/drivers/postgresql.py's _PgDirectStream) — so this works against Provisa's
    pgwire server as-is, no server-side feature gap to fill.

    asyncpg is async-native; this class runs its own event loop on a dedicated background thread
    so run_sql's callers (the sequential matrix runner and the saturation ramp's worker threads)
    keep the exact same synchronous call signature — asyncio.run_coroutine_threadsafe bridges each
    call onto that one loop, which is what actually lets asyncpg's connections be used safely from
    multiple caller threads without each needing its own event loop."""

    name = "sql"

    def __init__(self, host: str, port: int):
        self.host, self.port = host, port
        self._pool = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        try:
            import asyncpg

            self._asyncpg = asyncpg
            self._loop = asyncio.new_event_loop()
            self._loop_thread = threading.Thread(target=self._loop.run_forever, daemon=True)
            self._loop_thread.start()

            async def _connect() -> asyncpg.Pool:
                return await asyncpg.create_pool(
                    # pgwire username IS the role (verified live: "admin" is not a real role and
                    # fails "No schema for role 'admin'" — org_admin matches fragment.yaml's
                    # visible_to lists and config's default_assignments).
                    host=host,
                    port=port,
                    user="org_admin",
                    password="ignored",
                    database="provisa",
                    min_size=1,
                    max_size=PGWIRE_POOL_SIZE,
                    # Provisa's pgwire has no server-side prepared-statement cache to reuse across
                    # connections the way real Postgres does — mirrors this project's own
                    # PgBouncer-transaction-mode guidance (asyncpg's usual advice for poolers).
                    statement_cache_size=0,
                    timeout=10,
                )

            self._pool = asyncio.run_coroutine_threadsafe(_connect(), self._loop).result(timeout=30)

            async def _probe() -> None:
                assert self._pool is not None
                async with self._pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")

            asyncio.run_coroutine_threadsafe(_probe(), self._loop).result(timeout=30)
        except Exception as exc:  # noqa: BLE001 - availability probe
            self._error = str(exc)

    def available(self) -> bool:
        return self._pool is not None

    async def _async_run_sql(self, sql: str, params: dict) -> tuple[int, int, float]:
        # Literal substitution, not $1/$2 placeholders + args: confirmed live (isolated repro
        # against Provisa's pgwire) that conn.cursor(query, *args, ...) sends a real PREPARE, and
        # Provisa's pgwire describe_statement (vendor/buenavista/buenavista/postgres.py) raises an
        # unhandled exception for any parameter OID it doesn't recognize in its fixed TYPE_OIDS
        # table — which is exactly OID 0 (UNKNOWNOID, the standard wire-protocol convention for "a
        # type the client hasn't pinned down yet, infer it"), what asyncpg sends when it hasn't
        # been given explicit parameter types. The result is a malformed/empty ErrorResponse that
        # leaves the connection in a state where cleanup (transaction rollback on __aexit__) never
        # gets a reply — this IS the actual cause of point_lookup hanging indefinitely, not a
        # memory issue. Filed as a real Provisa pgwire protocol-compliance bug (unspecified
        # parameter types should be legal, not fatal); worked around here rather than fixed live
        # mid-benchmark. FlightTransport already does the same literal-embedding for the same
        # reason (see its own run_sql) — safe here since queries.py's params are fixed, trusted
        # benchmark literals, never external/adversarial input.
        pg_sql = sql
        for k, v in params.items():
            literal = f"'{v}'" if isinstance(v, str) else str(v)
            pg_sql = pg_sql.replace(f":{k}", literal)
        assert self._pool is not None
        # Raw asyncpg Records, NOT dicts: a real SQL client never serializes to JSON — asyncpg
        # hands back typed Python values straight off the binary wire protocol. dict()-converting
        # a batch exists only to feed _payload_bytes's json.dumps (the harness's own bytes/sec
        # measurement); _ByteEstimator only pays that cost for sampled batches, never all of them.
        estimator = _ByteEstimator()
        async with self._pool.acquire() as conn, conn.transaction():
            # A server-side cursor requires an open transaction (the read is read-only, so this
            # never needs a real commit decision — the `async with` context closes it either way).
            #
            # cursor.fetch(n), NOT `async for record in conn.cursor(...)`: the async-for form
            # yields one Record per __anext__ call, so an 80M-row scan pays 80M coroutine
            # dispatches — confirmed live on perf-bench (large_scan via sql ran 50+ minutes,
            # client CPU pegged at ~39% the whole time with the server otherwise idle).
            # cursor.fetch(n) pulls a whole batch per call, cutting that to row_count/batch_size
            # dispatches.
            cur = await conn.cursor(pg_sql)
            while True:
                batch = await cur.fetch(_FETCH_BATCH_SIZE)
                if not batch:
                    break
                estimator.add(len(batch), batch, lambda b: [dict(r) for r in b])
        return estimator.row_count, estimator.byte_count, estimator.overhead_s

    def run_sql(self, sql: str, params: dict) -> tuple[int, int, float]:
        assert self._loop is not None
        return asyncio.run_coroutine_threadsafe(
            self._async_run_sql(sql, params), self._loop
        ).result()

    def close(self) -> None:
        if self._pool is not None and self._loop is not None:
            asyncio.run_coroutine_threadsafe(self._pool.close(), self._loop).result(timeout=10)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5)


class BoltTransport(Transport):
    """Cypher over Provisa's Bolt server, translated to SQL and run on the federated engine."""

    name = "cypher"

    def __init__(self, host: str, port: int):
        self._driver = None
        try:
            from neo4j import GraphDatabase

            self._driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=None)
            self._driver.verify_connectivity()
        except Exception as exc:  # noqa: BLE001 - availability probe
            self._error = str(exc)

    def available(self) -> bool:
        return self._driver is not None

    def run_cypher(self, cypher: str, params: dict) -> tuple[int, int, float]:
        assert self._driver is not None
        # Raw neo4j Records, not dicts — same reasoning as PgwireTransport: dict() conversion
        # exists only to feed _payload_bytes; _ByteEstimator only pays it for sampled batches.
        batch: list = []
        estimator = _ByteEstimator()
        with self._driver.session() as session:
            result = session.run(cypher, params)  # type: ignore[arg-type]
            for r in result:
                batch.append(r)
                if len(batch) >= _FETCH_BATCH_SIZE:
                    estimator.add(len(batch), batch, lambda b: [dict(r) for r in b])
                    batch = []
        if batch:
            estimator.add(len(batch), batch, lambda b: [dict(r) for r in b])
        return estimator.row_count, estimator.byte_count, estimator.overhead_s

    def close(self) -> None:
        if self._driver:
            self._driver.close()


class HttpTransport(Transport):
    """Cypher-over-HTTP via POST /data/cypher (provisa/api/rest/cypher_router.py:419) — same
    parse->translate->govern->execute pipeline as Bolt, reached over a plain request/response
    call instead of a long-lived session. There is no raw-SQL-over-HTTP endpoint in Provisa
    (verified: no /data/sql or equivalent route exists) — GraphQL/JSON:API/REST are schema-driven,
    not free-SQL, so Cypher is the one HTTP surface that takes ad hoc query text like pgwire/Bolt
    do. This transport therefore only runs the matrix's `cypher` queries, not `sql` ones.

    No auth: `--demo perf` runs with auth.provider: none (config/provisa-install*.yaml, checked
    in), under which every request is auto-identified anonymous/org_admin with no token at all —
    verified in provisa/auth/middleware.py:417-441 ("there is no auth to validate against"). A
    benchmark against a SECURED Provisa instance would need a bearer token added here; this
    demo's whole point is a clean, predictable reset each time, including that posture, so no
    login step belongs in this tool.
    """

    name = "http"

    def __init__(self, base_url: str):
        self._client = None
        try:
            import httpx

            self._client = httpx.Client(base_url=base_url, timeout=120)
        except Exception as exc:  # noqa: BLE001 - availability probe
            self._error = str(exc)

    def available(self) -> bool:
        return self._client is not None

    def run_cypher(self, cypher: str, params: dict) -> tuple[int, int, float]:
        # The full response is already buffered by httpx before this code runs (no streaming-JSON
        # option here), so batching wouldn't reduce PEAK memory the way it does for the other
        # transports' server-side cursors — but this transport is never used for a large-result
        # query (HttpTransport only runs `cypher` queries, none of which are large_result), so the
        # once-materialized `dict_rows` list is bounded and this is not the same risk.
        assert self._client is not None
        resp = self._client.post("/data/cypher", json={"query": cypher, "params": params})
        resp.raise_for_status()
        body = resp.json()
        columns, rows = body["columns"], body["rows"]
        # The array-of-arrays -> array-of-dicts conversion exists only so _payload_bytes has a
        # uniform row shape to json.dumps across every transport — real client code would just
        # read the array response directly against a known column order. Excluded from the timed
        # region along with _payload_bytes itself, same as every other transport.
        t0 = time.perf_counter()
        dict_rows = [dict(zip(columns, r)) if isinstance(r, list) else r for r in rows]
        byte_count = _payload_bytes(dict_rows)
        overhead_s = time.perf_counter() - t0
        return len(dict_rows), byte_count, overhead_s

    def close(self) -> None:
        if self._client:
            self._client.close()


class GraphqlTransport(Transport):
    """GraphQL-JSON encoding over the SAME HTTP transport HttpTransport already uses (POST
    /data/graphql, provisa/api/data/endpoint.py:208 graphql_endpoint — the http host:port,
    not a separate service). Distinguished from HttpTransport by encoding only: HttpTransport
    speaks Cypher-over-HTTP (REST/array-of-arrays JSON), this speaks Provisa's auto-generated
    GraphQL schema (nested object JSON, field names repeated per row — the "more intense
    serialization" this benchmark exists to compare).

    Query text/shape verified against the real schema generator, not guessed:
      - Root query field names camelCase the table name (default
        state.global_gql_naming_convention == "apollo_graphql", provisa/api/app.py:258-259;
        provisa/compiler/naming.py's generate_name/_apply_table_convention). `orders` -> `orders`,
        `order_events` -> `orderEvents`, `order_docs` -> `orderDocs`. Column names camelCase the
        same way (provisa/compiler/schema_inputs.py's _gql_col_name/apply_gql_name):
        `order_id` -> `orderId`, `customer_id` -> `customerId`.
      - `where` arg filters are typed per column: `{field: {eq: v}}` / `{gte: v, lte: v}` etc.
        (provisa/compiler/type_map.py's _filter_fields/_ordered_filter_fields/FILTER_TYPE_MAP;
        args wired in via schema_inputs.py's _build_db_field_args — args are `where`, `order_by`,
        `limit`, `offset`, `sample`, `distinct_on`, never a bare `filter`).
      - Aggregation is a `{field}GroupBy(by: [...])` root field returning
        `{ groupKey aggregate { count sum { col } ... } nodes { ... } }`
        (schema_gen.py's _build_group_by_query_field, gated by fragment.yaml's
        enable_group_by; aggregate_gen.py's build_agg_fields_type for the count/sum/avg/
        stddev/variance/min/max shape — sum/avg/etc. sub-fields are the RAW column name, e.g.
        `sum { amount }`, not camelCased, since aggregate_gen.py keys them by physical
        col_name directly).
      - No relationship is registered between orders/order_events/order_docs in fragment.yaml
        (only the Neo4j bench_* edge tables have any), so federated_join/large_federated_join
        cannot be one nested GraphQL selection. They run as multiple ALIASED root fields in one
        request instead — endpoint.py's `_handle_query` docstring: "Multiple root fields are
        executed independently and merged" — still one HTTP round trip per iteration, matching
        the SQL join's one round trip, just without a server-side JOIN.
      - large_scan/large_federated_join have no GraphQL query text: a GraphQL response over
        threshold gets redirected to an S3 file manifest instead of inline JSON
        (X-Provisa-Redirect-Threshold, endpoint.py:223-231) — a fundamentally different
        response shape from every other transport's inline-JSON large_scan run, so it would not
        be measuring the same thing. Skipped rather than silently comparing apples to oranges.

    No auth, same reasoning as HttpTransport (`--demo perf` runs with auth.provider: none).
    """

    name = "graphql"

    def __init__(self, base_url: str):
        self._client = None
        try:
            import httpx

            self._client = httpx.Client(base_url=base_url, timeout=120)
        except Exception as exc:  # noqa: BLE001 - availability probe
            self._error = str(exc)

    def available(self) -> bool:
        return self._client is not None

    def run_graphql(self, query: str, params: dict) -> tuple[int, int, float]:
        # Same non-streaming caveat as HttpTransport: httpx already buffers the full response
        # before this code runs, but GraphqlTransport never runs large_scan/large_federated_join
        # (see class docstring), so the once-materialized row list is bounded — small enough that
        # sampling (_ByteEstimator) isn't needed here, unlike the streaming transports.
        assert self._client is not None
        resp = self._client.post("/data/graphql", json={"query": query, "variables": params})
        resp.raise_for_status()
        body = resp.json()
        if body.get("errors"):
            raise RuntimeError(f"GraphQL errors: {body['errors']}")
        data = body["data"]
        # Flatten every top-level (possibly aliased) root field's rows into one list — same
        # uniform-shape-for-_payload_bytes reasoning as HttpTransport's array-of-arrays -> dict
        # conversion, excluded from the timed region along with _payload_bytes itself.
        t0 = time.perf_counter()
        rows: list = []
        for value in data.values():
            rows.extend(value if isinstance(value, list) else [value])
        byte_count = _payload_bytes(rows)
        overhead_s = time.perf_counter() - t0
        return len(rows), byte_count, overhead_s

    def close(self) -> None:
        if self._client:
            self._client.close()


class FlightTransport(Transport):
    """Arrow Flight/gRPC — the columnar path backing the Python ADBC client and JDBC driver.
    Ticket schema verified against provisa/api/flight/server.py:481-530 (_do_get_inner): a JSON
    ticket with a `query` key (SQL or GraphQL, auto-detected) and an optional `token` key. No
    token is sent here — see HttpTransport's docstring for why `--demo perf` needs none; the
    server's own `_authenticate(None)` path handles an absent credential the same way.
    """

    name = "flight"

    def __init__(self, host: str, port: int):
        self._client = None
        try:
            import pyarrow.flight as fl

            self._fl = fl
            self._client = fl.FlightClient(f"grpc://{host}:{port}")
        except Exception as exc:  # noqa: BLE001 - availability probe
            self._error = str(exc)

    def available(self) -> bool:
        return self._client is not None

    def run_sql(self, sql: str, params: dict) -> tuple[int, int, float]:
        # reader.read_all() materializes the whole result as one Arrow Table, then .to_pylist()
        # doubles that into a Python list — the exact pattern that OOM-killed this harness on
        # large_scan's 80M-row order_items (31GB RSS, confirmed via dmesg on the GCP perf-bench
        # run). Iterate the stream's own chunks instead — never buffer more than one batch.
        pg_sql = sql
        for k, v in params.items():
            literal = f"'{v}'" if isinstance(v, str) else str(v)
            pg_sql = pg_sql.replace(f":{k}", literal)
        assert self._client is not None
        ticket = self._fl.Ticket(json.dumps({"query": pg_sql}).encode())
        reader = self._client.do_get(ticket)
        estimator = _ByteEstimator()
        for chunk in reader:
            batch = chunk.data
            if batch is None or batch.num_rows == 0:
                continue
            # Arrow-to-Python conversion (to_pylist) is done ONLY so _payload_bytes can measure a
            # sampled batch's logical size — a real client that stays in Arrow never pays this,
            # and _ByteEstimator only calls it for batches it actually samples.
            estimator.add(batch.num_rows, batch, lambda b: b.to_pylist())
        return estimator.row_count, estimator.byte_count, estimator.overhead_s

    def close(self) -> None:
        if self._client:
            self._client.close()


class GrpcTransport(Transport):
    """Provisa's OTHER gRPC-based data-access surface — provisa/grpc/server.py's dynamically
    generated ``ProvisaService`` (package ``provisa.v1``), for querying registered tables
    directly via typed per-table RPCs. gRPC transport + Protobuf encoding, distinct from
    FlightTransport's gRPC transport + Arrow encoding (Arrow Flight/SQL streaming is its own,
    already-covered surface; this is the native `Query{Type}`/`Query{Type}GroupBy` RPC surface,
    verified against provisa/grpc/proto_gen.py + provisa/grpc/server.py + provisa/grpc/
    query_ir.py — not MCP, which is a tool-calling protocol, not a raw query surface).

    NO PRE-COMPILED .proto: the server compiles a per-role .proto at boot into a tempdir
    (provisa/grpc/schema_gen.py's compile_proto) with no stable published copy a client can
    build against ahead of time. The intended discovery path is gRPC server reflection —
    provisa/grpc/reflection.py registers grpc_reflection.v1alpha specifically so external
    clients can do this — so this transport resolves message descriptors at RUN TIME via
    ServerReflectionInfo (file_containing_symbol / file_by_filename) into a protobuf
    DescriptorPool, then builds message classes with google.protobuf.message_factory, instead
    of shipping a static _pb2.py.

    CONFIRMED GAP (read, not guessed — grep for "request.filter" or ".filter" under
    provisa/grpc/*.py returns zero hits): `{Type}Request.filter` and `{Type}GroupByRequest.filter`
    are defined in the generated .proto but NEVER read server-side.
    query_ir.grpc_table_to_semantic_sql only ever forwards `limit`; query_ir.
    grpc_table_to_group_by_graphql_text only ever forwards `by`/`funcs`/`include`/`include_nodes`.
    So Query{Type}/Query{Type}GroupBy always return an UNFILTERED table/group-by no matter what
    the client sets. This transport therefore only ever gets called for the matrix's two
    filter-free queries (large_scan's own SQL has no WHERE either; single_source_aggregation's
    own SQL has no WHERE, only GROUP BY) — every query needing a WHERE (point_lookup,
    federated_join, large_federated_join, the cypher_* queries) has `grpc=None` in queries.py
    and is never dispatched here, rather than silently returning the wrong rows under the same
    query_id.
    """

    name = "grpc"

    def __init__(self, host: str, port: int):
        self._channel = None
        self._pool = None
        self._loaded_files: set[str] = set()
        self._message_class_cache: dict[str, type] = {}
        try:
            import grpc as _grpc
            from google.protobuf import descriptor_pool as _descriptor_pool

            self._grpc = _grpc
            self._pool = _descriptor_pool.Default()
            self._channel = _grpc.insecure_channel(f"{host}:{port}")
            _grpc.channel_ready_future(self._channel).result(timeout=10)
        except Exception as exc:  # noqa: BLE001 - availability probe
            self._error = str(exc)

    def available(self) -> bool:
        return self._channel is not None

    def _resolve_message_class(self, full_name: str) -> type:
        """Resolve a `provisa.v1.<Message>` class via gRPC server reflection, recursively
        pulling in dependency .proto files (google/protobuf/field_mask.proto etc.) the same way
        any reflection-based client (grpcurl, polyglot) must — the server has no other way to
        publish message shapes for a per-role, dynamically compiled schema."""
        if full_name in self._message_class_cache:
            return self._message_class_cache[full_name]
        from google.protobuf import descriptor_pb2, message_factory
        from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

        stub = reflection_pb2_grpc.ServerReflectionStub(self._channel)

        def _fetch(request) -> list:
            responses = list(stub.ServerReflectionInfo(iter([request])))
            return list(responses[0].file_descriptor_response.file_descriptor_proto)

        def _load(name: str, by_symbol: bool) -> None:
            request = (
                reflection_pb2.ServerReflectionRequest(file_containing_symbol=name)
                if by_symbol
                else reflection_pb2.ServerReflectionRequest(file_by_filename=name)
            )
            for fdp_bytes in _fetch(request):
                fdp = descriptor_pb2.FileDescriptorProto()
                fdp.ParseFromString(fdp_bytes)
                if fdp.name in self._loaded_files:
                    continue
                for dep in fdp.dependency:
                    if dep in self._loaded_files:
                        continue
                    try:
                        self._pool.FindFileByName(dep)
                        self._loaded_files.add(dep)
                    except KeyError:
                        _load(dep, by_symbol=False)
                self._pool.Add(fdp)
                self._loaded_files.add(fdp.name)

        _load(full_name, by_symbol=True)
        descriptor = self._pool.FindMessageTypeByName(full_name)
        cls = message_factory.GetMessageClass(descriptor)
        self._message_class_cache[full_name] = cls
        return cls

    def run_grpc(self, spec: dict, params: dict) -> tuple[int, int, float]:
        assert self._channel is not None
        type_name = spec["type_name"]
        metadata = [("x-provisa-role", "org_admin")]  # matches every other transport's role
        batch: list = []
        estimator = _ByteEstimator()

        if spec["mode"] == "scan":
            request_cls = self._resolve_message_class(f"provisa.v1.{type_name}Request")
            response_cls = self._resolve_message_class(f"provisa.v1.{type_name}")
            rpc_name = f"Query{type_name}"
            # TEMPORARY (2026-09-26): matches queries.py's large_scan LIMIT 2000000 shrink — see
            # that comment. Restore limit=0 (unbounded) together with large_scan's own SQL text.
            request = request_cls(limit=2_000_000)
        elif spec["mode"] == "group_by":
            request_cls = self._resolve_message_class(f"provisa.v1.{type_name}GroupByRequest")
            response_cls = self._resolve_message_class(f"provisa.v1.{type_name}GroupByRow")
            rpc_name = f"Query{type_name}GroupBy"
            request = request_cls(by=spec["by"])
        else:
            raise ValueError(f"unknown grpc query mode {spec['mode']!r}")

        stream = self._channel.unary_stream(
            f"/provisa.v1.ProvisaService/{rpc_name}",
            request_serializer=request_cls.SerializeToString,
            response_deserializer=response_cls.FromString,
        )
        from google.protobuf.json_format import MessageToDict

        for msg in stream(request, metadata=metadata):
            batch.append(msg)
            if len(batch) >= _FETCH_BATCH_SIZE:
                # Proto message -> dict conversion (MessageToDict) exists only so _payload_bytes
                # can json.dumps a uniform row shape — a real client stays in typed proto
                # messages and never pays this, so it's excluded from the timed region exactly
                # like every other transport's own client-side bookkeeping, and _ByteEstimator
                # only pays it for sampled batches.
                estimator.add(len(batch), batch, lambda b: [MessageToDict(m) for m in b])
                batch = []
        if batch:
            estimator.add(len(batch), batch, lambda b: [MessageToDict(m) for m in b])
        return estimator.row_count, estimator.byte_count, estimator.overhead_s

    def close(self) -> None:
        if self._channel:
            self._channel.close()


_CALL_METHOD: dict[str, str] = {
    "sql": "run_sql",
    "cypher": "run_cypher",
    "graphql": "run_graphql",
    "grpc": "run_grpc",
}


def _run_sequential(transport: Transport, q: Query, method: str, text: str) -> QueryResult:
    result = QueryResult(query_id=q.id, category=q.category, transport=transport.name)
    call = getattr(transport, _CALL_METHOD[method])
    for i in range(q.iterations):
        t0 = time.perf_counter()
        try:
            n, byte_count, overhead_s = call(text, q.params)
            # overhead_s (time spent computing _payload_bytes, and for Flight, Arrow->Python
            # conversion) is the benchmark harness's own client-side bookkeeping cost, not
            # Provisa's/the network's — excluded so reported latency reflects query cost, not
            # measurement cost. See Transport.run_sql's docstring.
            elapsed = (time.perf_counter() - t0) - overhead_s
            result.samples.append(Sample(elapsed, n, byte_count))
            print(
                f"    iter {i + 1}/{q.iterations}: {elapsed * 1000:.0f}ms, {n} rows, "
                f"{byte_count / 1e6:.1f}MB",
                file=sys.stderr,
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001 - recorded as a failed sample, not fatal
            elapsed = time.perf_counter() - t0
            result.samples.append(Sample(elapsed, 0, 0, error=str(exc)))
            print(
                f"    iter {i + 1}/{q.iterations}: FAILED after {elapsed * 1000:.0f}ms: {exc}",
                file=sys.stderr,
                flush=True,
            )
    return result


@dataclass
class SaturationPoint:
    target_rate: float
    submitted: int
    completed: int
    errors: int
    latency_ms_p50: float
    latency_ms_p99: float
    bytes_per_sec: float


# duration_s, target arrivals/sec — climbs well past any expected safe rate so the actual
# breaking point shows up in the data, matching artillery-concurrency-ramp.yml's shape.
SATURATION_PHASES: list[tuple[float, float]] = [
    (10, 1),
    (30, 50),
    (30, 200),
    (30, 500),
    (30, 1000),
    (30, 2000),
]


def _saturation_mix(method: str) -> list[tuple[float, str, dict]]:
    """70% point_lookup (small) / 30% single_source_aggregation (heavy) — 'varying size' load,
    matching artillery-concurrency-ramp.yml's two weighted scenarios."""
    by_id = {q.id: q for q in QUERIES}
    small, heavy = by_id["point_lookup"], by_id["single_source_aggregation"]
    # method names line up 1:1 with Query's own field names (sql/cypher/graphql/grpc).
    small_text = getattr(small, method)
    heavy_text = getattr(heavy, method)
    if not small_text or not heavy_text:
        return []
    return [(0.7, small_text, small.params), (0.3, heavy_text, heavy.params)]


def run_saturation(transport: Transport, method: str) -> list[SaturationPoint]:
    """How many queries of varying size can this transport handle before it falls over.

    Open-model: submissions fire on a fixed schedule independent of completions, so a slow
    response never delays the next submission — avoids "coordinated omission" (see
    artillery-concurrency-ramp.yml's header for the full explanation; this is the same fix,
    applied to the transports Artillery can't reach). Approximated with a large thread pool
    (4000 workers, chosen to stay well above peak target rate x typical latency so submission
    queuing itself never becomes the bottleneck) rather than a true async event-loop scheduler —
    an honest methodology limitation, not a true open-loop implementation.

    For the `sql` transport, actual concurrent DB connections are capped at PGWIRE_POOL_SIZE
    (50), independent of the 4000 submission workers — a real client behaves the same way (a
    bounded pool, not one connection per request), so this tests query throughput rather than
    connection-slot exhaustion. Once in-flight sql load exceeds the pool size, psycopg2's
    ThreadedConnectionPool.getconn() raises immediately rather than queuing (fail-fast at the
    pool limit) — that shows up as errors in the report once load passes ~50 concurrent sql
    requests, which is a real, distinct "falling over" signal, not a bug in this harness.
    """
    mix = _saturation_mix(method)
    if not mix:
        return []
    call = getattr(transport, _CALL_METHOD[method])
    weights = [w for w, _, _ in mix]
    choices = [(t, p) for _, t, p in mix]
    points: list[SaturationPoint] = []
    pool = ThreadPoolExecutor(max_workers=4000)
    try:
        for duration_s, rate in SATURATION_PHASES:
            phase_samples: list[Sample] = []
            lock = threading.Lock()

            def _one(text: str, params: dict) -> None:
                t0 = time.perf_counter()
                try:
                    n, byte_count, overhead_s = call(text, params)
                    s = Sample((time.perf_counter() - t0) - overhead_s, n, byte_count)
                except Exception as exc:  # noqa: BLE001 - recorded as a failed sample, not fatal
                    s = Sample(time.perf_counter() - t0, 0, 0, error=str(exc))
                with lock:
                    phase_samples.append(s)

            interval = 1.0 / rate if rate > 0 else 1.0
            end = time.monotonic() + duration_s
            submitted = 0
            next_fire = time.monotonic()
            while time.monotonic() < end:
                text, params = random.choices(choices, weights=weights, k=1)[0]
                pool.submit(_one, text, params)
                submitted += 1
                next_fire += interval
                sleep_for = next_fire - time.monotonic()
                if sleep_for > 0:
                    time.sleep(sleep_for)
            time.sleep(min(5.0, interval * 50))  # let in-flight requests drain before measuring
            with lock:
                samples = list(phase_samples)
            ok = [s for s in samples if s.error is None]
            errors = len(samples) - len(ok)
            if ok:
                lat = sorted(s.elapsed_s * 1000 for s in ok)
                total_bytes = sum(s.payload_bytes for s in ok)
                points.append(
                    SaturationPoint(
                        rate,
                        submitted,
                        len(ok),
                        errors,
                        statistics.median(lat),
                        lat[int(len(lat) * 0.99) - 1] if len(lat) > 1 else lat[0],
                        total_bytes / duration_s,
                    )
                )
            else:
                points.append(SaturationPoint(rate, submitted, 0, errors, 0.0, 0.0, 0.0))
            print(
                f"    saturation[{transport.name}] rate={rate}/s: {len(ok)}/{submitted} ok, {errors} errors",
                file=sys.stderr,
            )
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return points


def run_all(
    transports: dict[str, Transport],
    query_ids: set[str] | None = None,
    on_result: Callable[[list[QueryResult]], object] | None = None,
) -> list[QueryResult]:
    # concurrency_ramp is NOT in QUERIES — it runs separately via artillery-concurrency-ramp.yml
    # (open-model arrival-rate load; this module only does closed-model sequential runs — see
    # that file's header for why the distinction matters for that one category).
    # `on_result`, called after every single query×transport result (not just at the end): a
    # multi-hour run has no business losing everything already collected to a crash anywhere
    # downstream (confirmed live: a hung transport's close() took out a 13-hour run's entire
    # report, which until then only got built and written after run_all AND the saturation ramps
    # AND every transport's close() all succeeded) — see main()'s checkpoint writer.
    results: list[QueryResult] = []
    for q in QUERIES:
        if query_ids is not None and q.id not in query_ids:
            continue
        for method, text in (
            ("sql", q.sql),
            ("cypher", q.cypher),
            ("graphql", q.graphql),
            ("grpc", q.grpc),
        ):
            if not text:
                continue
            candidates = [
                t
                for t in transports.values()
                if t.available()
                and (
                    (
                        method == "sql" and t.name in ("sql", "flight")
                    )  # pgwire + Flight take raw SQL
                    or (
                        method == "cypher" and t.name in ("cypher", "http")
                    )  # Bolt + /data/cypher take Cypher
                    or (
                        method == "graphql" and t.name == "graphql"
                    )  # /data/graphql, GraphQL-JSON encoding
                    or (
                        method == "grpc" and t.name == "grpc"
                    )  # native ProvisaService, Protobuf encoding
                )
            ]
            for t in candidates:
                print(
                    f"  {q.id} [{q.category}] via {t.name} ({method})...",
                    file=sys.stderr,
                    flush=True,
                )
                results.append(_run_sequential(t, q, method, text))
                if on_result is not None:
                    on_result(results)
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True, choices=["duckdb", "pg", "trino"])
    parser.add_argument("--pgwire-host", default="localhost")
    parser.add_argument("--pgwire-port", type=int, default=5439)
    parser.add_argument("--bolt-host", default="localhost")
    # 17687, not the standard Neo4j 7687 — verified live and hardcoded as the advertised routing
    # address in provisa/bolt/session.py.
    parser.add_argument("--bolt-port", type=int, default=17687)
    parser.add_argument("--http-base-url", default="http://localhost:8001")
    parser.add_argument("--flight-host", default="localhost")
    parser.add_argument("--flight-port", type=int, default=8815)
    # GraphQL is the SAME host:port as --http-base-url (POST /data/graphql, same FastAPI app as
    # POST /data/cypher) — no separate flag; see GraphqlTransport's docstring.
    # 50051 matches app_startup.py's GRPC_PORT/server_cfg["grpc_port"] default.
    parser.add_argument("--grpc-host", default="localhost")
    parser.add_argument("--grpc-port", type=int, default=50051)
    parser.add_argument("--output-dir", default="results")
    parser.add_argument(
        "--skip-saturation",
        action="store_true",
        help="Skip the per-transport saturation ramp (~8 min per engine run otherwise)",
    )
    parser.add_argument(
        "--query-id",
        action="append",
        dest="query_ids",
        default=None,
        help="Run only this query id (repeatable). Default: the full matrix. For a supplemental "
        "re-run of one query added after another engine's round already completed, so all "
        "engines get comparable coverage without re-running the whole matrix.",
    )
    parser.add_argument(
        "--start-at",
        default=None,
        help="Skip every query BEFORE this query id in queries.QUERIES's order (inclusive of the "
        "named id) — resumes a matrix run from a known-good point instead of re-running "
        "everything already-passing legs before a fix under test. Mutually exclusive with "
        "--query-id; unlike --query-id this only bounds the START, every later query still runs.",
    )
    args = parser.parse_args()
    if args.start_at and args.query_ids:
        parser.error("--start-at and --query-id are mutually exclusive")
    query_ids = set(args.query_ids) if args.query_ids else None
    if args.start_at:
        all_ids = [q.id for q in QUERIES]
        if args.start_at not in all_ids:
            parser.error(f"--start-at {args.start_at!r} is not a known query id: {all_ids}")
        query_ids = set(all_ids[all_ids.index(args.start_at) :])

    transports: dict[str, Transport] = {
        "sql": PgwireTransport(args.pgwire_host, args.pgwire_port),
        "cypher": BoltTransport(args.bolt_host, args.bolt_port),
        "http": HttpTransport(args.http_base_url),
        "flight": FlightTransport(args.flight_host, args.flight_port),
        "graphql": GraphqlTransport(args.http_base_url),
        "grpc": GrpcTransport(args.grpc_host, args.grpc_port),
    }
    for name, t in transports.items():
        status = (
            "reachable" if t.available() else f"UNAVAILABLE ({getattr(t, '_error', 'unknown')})"
        )
        print(f"[{name}] {status}", file=sys.stderr)

    # out_path is fixed up front (named for the run's START, not its end) and (re)written after
    # every single result and after every saturation ramp — not just once at the very end. A
    # multi-hour run has no business losing everything already collected to a crash anywhere
    # downstream of the actual measurements (confirmed live: a hung transport's close() took out
    # a 13-hour run's entire report, which until this change only got built and written after
    # run_all AND every saturation ramp AND every transport's close() all succeeded in one go).
    out_dir = Path(args.output_dir) / args.engine
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    saturation: dict[str, list[dict]] = {}

    def _checkpoint(results_so_far: list[QueryResult]) -> list[dict]:
        summaries = [r.summary() for r in results_so_far]
        report = {
            "engine": args.engine,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "results": summaries,
            "saturation": saturation,
        }
        tmp_path = out_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(report, indent=2, default=str))
        tmp_path.replace(out_path)  # atomic on POSIX — out_path is never left half-written
        return summaries

    print(f"Running benchmark matrix, engine={args.engine}...", file=sys.stderr)
    results = run_all(transports, query_ids, on_result=_checkpoint)

    if not args.skip_saturation:
        # sql (pgwire) + cypher (Bolt) + flight + grpc here; "http" AND "graphql" are excluded —
        # both are encodings on the one HTTP transport artillery-concurrency-ramp.yml already
        # saturates more rigorously (open-model arrival-rate load), so running either here would
        # just produce a second, conflicting saturation number for the same underlying surface.
        # "grpc" is included for structural symmetry with sql/cypher/flight (its own distinct
        # wire transport, not HTTP) — but as of this change it will report an empty ramp: the 70/30
        # mix needs point_lookup, and point_lookup.grpc is None (native gRPC's Query{Type}
        # RPC ignores its filter field server-side — see GrpcTransport's docstring), so
        # _saturation_mix returns [] for method="grpc" until that server-side gap is fixed.
        print("Running per-transport saturation ramps...", file=sys.stderr)
        for name, method in (
            ("sql", "sql"),
            ("cypher", "cypher"),
            ("flight", "sql"),
            ("grpc", "grpc"),
        ):
            t = transports[name]
            if not t.available():
                continue
            points = run_saturation(t, method)
            saturation[name] = [asdict(p) for p in points]
            _checkpoint(results)

    for t in transports.values():
        try:
            t.close()
        except Exception as exc:  # noqa: BLE001 - best-effort cleanup, never masks the report
            print(f"Warning: {t.name}.close() failed: {exc}", file=sys.stderr)

    summaries = _checkpoint(results)
    _print_table(summaries)
    print(f"\nWrote {out_path}")
    return 0


def _print_table(summaries: list[dict]) -> None:
    header = f"{'query_id':<24} {'cat':<26} {'transport':<8} {'conc':>5} {'n':>5} {'QPS':>10} {'bytes/s':>14} {'p50ms':>8} {'p99ms':>8}"
    print("\n" + header)
    print("-" * len(header))
    for s in summaries:
        if s.get("status") == "all_failed":
            print(
                f"{s['query_id']:<24} {s['category']:<26} {s['transport']:<8} {'':>5} {'ALL FAILED':>10}"
            )
            continue
        print(
            f"{s['query_id']:<24} {s['category']:<26} {s['transport']:<8} "
            f"{str(s['concurrency'] or ''):>5} {s['n']:>5} "
            f"{s['qps']:>10.1f} {s['bytes_per_sec']:>14,.0f} "
            f"{s['latency_ms_p50']:>8.1f} {s['latency_ms_p99']:>8.1f}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
