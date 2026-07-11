# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f9a83-2d47-4e15-9a08-7b3e0d6c1f52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse federation runtime — ties the connectors and execution together (REQ-909, REQ-912).

ClickHouse is one SQL engine delivered two ways, and this runtime supports both behind a single
backend seam (REQ-912):

- SERVER (HTTP)   — a running ClickHouse server over HTTP via clickhouse-connect (local or remote).
- SERVER (native) — a running server over the native TCP protocol via clickhouse-driver; lower
                    framing overhead on large transfers, needs the native port (9000/9440) open.
- EMBEDDED        — chdb, the ClickHouse engine linked into this process (no server, like DuckDB).
                    A path makes it persistent; without one the session is in-memory and per-process.

All three expose the identical SQL surface and the identical integration engines (PostgreSQL/MySQL/
S3/URL/File/MongoDB), so the connector/DDL/capability layer is shared — only ``command``/``query``
differ. Backend is chosen by the engine URL scheme (``clickhouse://`` → HTTP, ``clickhouse+native://``
→ native TCP, ``chdb://`` → embedded); ``from_url`` is the one selection point.

Each registered source is exposed at its PHYSICAL ``schema.table`` name (what
rewrite_semantic_to_physical emits). ClickHouse is a flat ``database.table`` namespace, so the
two-level physical name is modelled as a database named after the source's schema holding a view
named after the table:

- Relational sources (postgresql/mysql) mount as a DATABASE engine that auto-exposes every remote
  table; the physical view selects from ``local_schema.table``.
- File sources (csv/parquet) and MongoDB mount as a per-table TABLE engine; the physical view
  selects from that engine-backed table. File engines infer their columns; MongoDB needs a column
  list supplied from registry metadata.

execute() runs governed physical SQL through transpile("clickhouse"). This is the engine primitive
a live EngineRuntime dispatch calls; routing/HTTP wiring is separate — mirrors DuckDBFederationRuntime.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, cast
from urllib.parse import urlparse

from provisa.executor.result import QueryResult

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa
from provisa.federation.engine import build_clickhouse_engine
from provisa.federation.runtime_support import columns_from_describe, run_async
from provisa.transpiler.transpile import transpile


class _CHBackend(Protocol):
    """The minimal execute seam shared by the server and embedded ClickHouse backends."""

    def command(self, sql: str) -> None:
        """Run a statement for its effect (DDL/INSERT); no result is returned."""
        ...

    def query(self, sql: str) -> tuple[list[tuple], list[str]]:
        """Run a query, returning ``(rows, column_names)``."""
        ...

    def query_arrow(self, sql: str) -> pa.Table:
        """Run a query, returning a materialized Arrow table (REQ-986)."""
        ...

    def query_arrow_stream(self, sql: str) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        """Run a query, returning ``(schema, lazy RecordBatch iterator)`` (REQ-986)."""
        ...

    def close(self) -> None: ...


class _ServerBackend:
    """A running ClickHouse server reached over HTTP (clickhouse-connect)."""

    def __init__(self, *, host: str, port: int, username: str, password: str) -> None:
        import clickhouse_connect

        self._client = clickhouse_connect.get_client(
            host=host, port=port, username=username, password=password
        )

    def command(self, sql: str) -> None:
        self._client.command(sql)

    def query(self, sql: str) -> tuple[list[tuple], list[str]]:
        res = self._client.query(sql)
        return [tuple(r) for r in res.result_rows], list(res.column_names)

    def query_arrow(self, sql: str) -> pa.Table:
        # clickhouse-connect requests FORMAT Arrow and returns a native pyarrow Table — no row
        # materialization (REQ-986). use_strings maps ClickHouse String to Arrow utf8, not binary.
        return self._client.query_arrow(sql, use_strings=True)

    def query_arrow_stream(self, sql: str) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        # FORMAT ArrowStream: the server streams IPC blocks and clickhouse-connect wraps them in a
        # StreamContext over a pyarrow RecordBatchStreamReader. The context must stay open for the
        # lifetime of the iterator, so the generator owns enter/exit (REQ-986).
        import pyarrow as pa

        stream_ctx = self._client.query_arrow_stream(sql, use_strings=True)
        reader: Any = stream_ctx.gen  # the pyarrow RecordBatchStreamReader the context wraps
        schema = reader.schema  # available before consumption; GeneratorStream needs it up front
        stream_ctx.__enter__()

        def _batches() -> Iterator[pa.RecordBatch]:
            try:
                for chunk in stream_ctx:
                    # clickhouse-connect yields one Arrow object per IPC block — normalize either a
                    # Table or a RecordBatch to the record batches the Flight stream expects.
                    if isinstance(chunk, pa.Table):
                        yield from chunk.to_batches()
                    else:
                        yield chunk
            finally:
                stream_ctx.__exit__(None, None, None)

        return schema, _batches()

    def close(self) -> None:
        self._client.close()


class _NativeBackend:
    """A running ClickHouse server reached over the native TCP protocol (clickhouse-driver).

    Lower framing overhead than HTTP on large transfers; needs the native port (9000/9440) open.
    """

    def __init__(self, *, host: str, port: int, username: str, password: str) -> None:
        from clickhouse_driver import Client

        self._client = Client(host=host, port=port, user=username, password=password)

    def command(self, sql: str) -> None:
        self._client.execute(sql)

    def query(self, sql: str) -> tuple[list[tuple], list[str]]:
        # with_column_types=True → (rows, [(name, type), ...]); the driver stub types execute() as a
        # union (row-count int for DDL), so narrow it explicitly.
        rows, cols = cast(
            "tuple[list[tuple], list[tuple[str, str]]]",
            self._client.execute(sql, with_column_types=True),
        )
        return [tuple(r) for r in rows], [c[0] for c in cols]

    def query_arrow(self, sql: str) -> pa.Table:
        raise NotImplementedError(
            "the ClickHouse native TCP transport (clickhouse-driver) has no Arrow format; "
            "use clickhouse:// (HTTP) or chdb:// for the Arrow Flight ENGINE path (REQ-986)"
        )

    def query_arrow_stream(self, sql: str) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        raise NotImplementedError(
            "the ClickHouse native TCP transport (clickhouse-driver) has no Arrow format; "
            "use clickhouse:// (HTTP) or chdb:// for the Arrow Flight ENGINE path (REQ-986)"
        )

    def close(self) -> None:
        self._client.disconnect()


class _EmbeddedBackend:
    """chdb — the ClickHouse engine in-process. ``path`` persists the store; None is in-memory."""

    def __init__(self, *, path: str | None = None) -> None:
        from chdb import session

        # chdb reads results in a named format; ArrowStream round-trips through pyarrow losslessly.
        self._session = session.Session(path) if path else session.Session()

    def command(self, sql: str) -> None:
        self._session.query(sql)

    def query(self, sql: str) -> tuple[list[tuple], list[str]]:
        import io

        import pyarrow as pa

        raw = self._session.query(sql, "ArrowStream").bytes()
        if not raw:
            return [], []
        table = pa.ipc.open_stream(io.BytesIO(raw)).read_all()
        cols = table.column_names
        rows = [tuple(d[c] for c in cols) for d in table.to_pylist()]
        return rows, list(cols)

    def query_arrow(self, sql: str) -> pa.Table:
        import io

        import pyarrow as pa

        raw = self._session.query(sql, "ArrowStream").bytes()
        if not raw:
            return pa.table({})
        return pa.ipc.open_stream(io.BytesIO(raw)).read_all()

    def query_arrow_stream(self, sql: str) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        # chdb hands back the whole ArrowStream buffer at once, so this is pseudo-streaming: the
        # result is already materialized in-process. We still expose it as a batch iterator so the
        # Flight ARROW_STREAM transport is uniform across backends (REQ-986).
        import io

        import pyarrow as pa

        raw = self._session.query(sql, "ArrowStream").bytes()
        if not raw:
            empty = pa.table({})
            return empty.schema, iter(())
        reader = pa.ipc.open_stream(io.BytesIO(raw))
        schema = reader.schema

        def _batches() -> Iterator[pa.RecordBatch]:
            yield from reader

        return schema, _batches()

    def close(self) -> None:
        self._session.close()


class ClickHouseFederationRuntime:  # REQ-825, REQ-840, REQ-909, REQ-912
    def __init__(self, backend: _CHBackend) -> None:
        self._backend = backend
        self._engine = build_clickhouse_engine()
        self._staging = "_provisa_attach"  # database holding engine-backed tables before the view
        self._backend.command(f'CREATE DATABASE IF NOT EXISTS "{self._staging}"')

    # -- backend selection (REQ-912) -------------------------------------------

    @classmethod
    def from_url(cls, url: str) -> ClickHouseFederationRuntime:
        """Build a runtime from an engine URL, selecting the backend by scheme:

        - ``chdb://`` / ``chdb:///path``            → embedded chdb (no server; path persists it)
        - ``clickhouse://user:pass@host:port``      → a ClickHouse server over HTTP (default 8123)
        - ``clickhouse+native://user:pass@host:port`` → a server over native TCP (default 9000)
        """
        u = urlparse(url)
        base, _, variant = u.scheme.partition("+")
        if base == "chdb":
            # chdb:///var/lib/x → persistent at /var/lib/x; chdb:// → in-memory.
            return cls(_EmbeddedBackend(path=u.path or None))
        if base == "clickhouse":
            if variant == "native":
                return cls(
                    _NativeBackend(
                        host=u.hostname or "localhost",
                        port=u.port or 9000,
                        username=u.username or "default",
                        password=u.password or "",
                    )
                )
            if variant in ("", "http", "https"):
                return cls(
                    _ServerBackend(
                        host=u.hostname or "localhost",
                        port=u.port or 8123,
                        username=u.username or "default",
                        password=u.password or "",
                    )
                )
        raise ValueError(
            f"unknown ClickHouse engine URL scheme {u.scheme!r}; "
            "use clickhouse:// (HTTP), clickhouse+native:// (TCP), or chdb:// (embedded)"
        )

    @classmethod
    def server(
        cls,
        *,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
    ) -> ClickHouseFederationRuntime:
        """A runtime against a running ClickHouse server (clickhouse-connect)."""
        return cls(_ServerBackend(host=host, port=port, username=username, password=password))

    @classmethod
    def native(
        cls,
        *,
        host: str = "localhost",
        port: int = 9000,
        username: str = "default",
        password: str = "",
    ) -> ClickHouseFederationRuntime:
        """A runtime against a running ClickHouse server over native TCP (clickhouse-driver)."""
        return cls(_NativeBackend(host=host, port=port, username=username, password=password))

    @classmethod
    def embedded(cls, *, path: str | None = None) -> ClickHouseFederationRuntime:
        """A runtime against embedded chdb — in-process, no server. ``path`` persists the store."""
        return cls(_EmbeddedBackend(path=path))

    # -- source exposure -------------------------------------------------------

    def attach_source(self, source: Any, columns: list[tuple[str, str]] | None = None) -> None:
        """Expose an ATTACH source at its physical ``schema.table`` via the engine's connector.

        ``columns`` (name, clickhouse_type) is required only for engines that cannot infer their
        schema (MongoDB); file/relational engines ignore it.
        """
        entry = self._engine.resolve(source)  # picks the (clickhouse, source_type) connector
        details = entry.details
        self._backend.command(f'CREATE DATABASE IF NOT EXISTS "{source.schema_name}"')
        phys = f'"{source.schema_name}"."{source.table_name}"'
        if "attach_ddl" in details:  # relational DATABASE engine (postgresql/mysql)
            for stmt in details["attach_ddl"]:
                self._backend.command(stmt)
            remote = f'"{details["local_schema"]}"."{source.table_name}"'
            self._backend.command(f"CREATE VIEW IF NOT EXISTS {phys} AS SELECT * FROM {remote}")
            return
        # per-table TABLE engine (file S3/URL/File, or MongoDB)
        clause = details["engine_clause"].replace("{table}", source.table_name)
        staged = f'"{self._staging}"."{source.schema_name}__{source.table_name}"'
        if details.get("requires_columns"):
            if not columns:
                raise ValueError(
                    f"source {source.id!r} ({source.type.value}) needs a column list to attach; "
                    "MongoDB's ClickHouse engine cannot infer its schema"
                )
            col_ddl = ", ".join(f'"{n}" {t}' for n, t in columns)
            self._backend.command(
                f"CREATE TABLE IF NOT EXISTS {staged} ({col_ddl}) ENGINE = {clause}"
            )
        else:  # file engine — ClickHouse infers the columns
            self._backend.command(f"CREATE TABLE IF NOT EXISTS {staged} ENGINE = {clause}")
        self._backend.command(f"CREATE VIEW IF NOT EXISTS {phys} AS SELECT * FROM {staged}")

    # -- metadata --------------------------------------------------------------

    def introspect_columns(self, source: Any) -> dict[str, str]:
        """Column types as the ClickHouse engine reports them for a registered source — the engine's
        metadata view (attach the source, DESCRIBE the physical relation). Returns
        ``{column_name: clickhouse_type_name}``. This is the ClickHouse implementation of the
        engine-introspection seam (REQ-825/840); callers reach it via EngineRuntime."""
        self.attach_source(source)
        phys = f'"{source.schema_name}"."{source.table_name}"'
        rows, _ = self._backend.query(f"DESCRIBE TABLE {phys}")
        # DESCRIBE columns: name, type, default_type, default_expression, ...
        return columns_from_describe(rows)

    # -- execution -------------------------------------------------------------

    async def execute(self, physical_or_governed_sql: str) -> QueryResult:
        """Execute physical SQL (post-governance) on the engine (transpiled to ClickHouse)."""
        return await self.run(transpile(physical_or_governed_sql, "clickhouse"))

    # -- NativeEngineBackend runtime protocol ----------------------------------

    def run_sync(self, sql: str, params: list | None = None) -> QueryResult:
        """Execute SQL ALREADY in the ClickHouse dialect (transpiled by the backend seam)."""
        rows, cols = self._backend.query(sql)
        return QueryResult(rows=rows, column_names=cols)

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        return await run_async(self.run_sync, sql, params)

    def run_arrow(self, sql: str) -> pa.Table:
        """Execute ClickHouse-dialect SQL and return a native Arrow table — the ENGINE ARROW terminal
        the Flight server calls (REQ-986). Mirrors run_sync but keeps results columnar end-to-end."""
        return self._backend.query_arrow(sql)

    def run_arrow_stream(self, sql: str) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        """Execute ClickHouse-dialect SQL and return ``(schema, lazy RecordBatch iterator)`` — the
        ENGINE ARROW_STREAM terminal for the Flight server (REQ-986)."""
        return self._backend.query_arrow_stream(sql)

    @property
    def connection(self):
        """The ClickHouse backend (command/query). Used by the cache terminal, which is not yet wired
        for ClickHouse — see ensure_materialize_attached."""
        return self._backend

    def ensure_materialize_attached(self) -> str:
        """ClickHouse reaches an external materialization store via CREATE DATABASE ENGINE=PostgreSQL,
        and the API-result cache write expects a cursor-style terminal — not yet wired for ClickHouse.
        Federated execution (run) works; the API-cache terminal is explicit follow-up, not a fallback."""
        raise NotImplementedError(
            "clickhouse materialization-store cache terminal is not wired (execution works)"
        )

    def close(self) -> None:
        self._backend.close()
