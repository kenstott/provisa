# Copyright (c) 2026 Kenneth Stott
# Canary: 2a9f5c73-8e1d-4b62-a70f-6c3e9d1a4b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-engine backend: the engine-specific implementation of the runtime seam (REQ-825, REQ-840).

``EngineRuntime`` (runtime.py) is engine-agnostic — it never branches on the engine name. Each
``FederationEngine`` instance carries a ``backend`` that implements the concrete terminal behavior
(execute, dialect, lifecycle, source registration, introspection, error mapping). The Trino
implementation (``TrinoBackend``) is the only backend that references Trino; native in-process
engines (duckdb/pg/clickhouse/sqlalchemy) use the default ``EngineBackend``. This is what keeps
every Trino reference inside the Trino engine's own instance and out of the generic seam.
"""

# complexity-gate: allow-ble=2 reason="introspection augmentation + cluster-health probe are best-effort: any driver/network failure keeps declared types / reports unhealthy, never crashes the seam"

from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from provisa.executor.result import QueryResult
    from provisa.federation.engine import FederationEngine

_log = logging.getLogger(__name__)


class EngineBackend:
    """Default backend for native in-process engines (duckdb/pg/clickhouse/sqlalchemy).

    A native engine has no external cluster, dynamic catalog, or watchdog, so the lifecycle hooks
    are no-ops. Introspection uses the engine's own native runtime (DuckDB/ClickHouse). The live
    ENGINE-terminal execution binding for native engines is separate feature work — ``execute``
    raises until an engine wires it, rather than silently falling back to another engine.
    """

    def __init__(self, engine: FederationEngine) -> None:
        self.engine = engine

    @property
    def dialect(self) -> str:
        """The physical SQL dialect the engine speaks (transpile target). Named by native_store
        for native engines (duckdb → ``duckdb``); falls back to the engine name."""
        return self.engine.native_store or self.engine.name

    def transpile_physical(self, pg_sql: str) -> str:
        """Transpile governed PostgreSQL-dialect SQL to the engine's physical dialect. Native
        engines use the plain SQLGlot transpile to their dialect; the Trino backend overrides with
        its dialect-specific rewrite pipeline. Callers reach this through the seam so they never
        hardcode a specific engine's dialect."""
        from provisa.transpiler.transpile import transpile

        return transpile(pg_sql, self.dialect)

    # -- lifecycle -------------------------------------------------------------

    def is_connected(self, state: Any) -> bool:
        """Native engines run in-process — always connected once built."""
        return True

    def provision(self, state: Any, ops_views: list, retention_hours: int | None) -> None:
        """No external terminal to connect; telemetry lands in the dedicated ops store."""

    async def provision_infra(self, state: Any) -> None:
        """No Arrow-Flight proxy / object store / results schema for a native engine."""

    async def watchdog(self, state: Any) -> None:
        """No external process to watch."""

    async def reload_catalog(
        self, state: Any, catalog: str, ops_views: list, retention_hours: int | None
    ) -> dict:
        return {
            "success": False,
            "errors": [f"engine {self.engine.name!r} has no reloadable catalog"],
        }

    def classify_error(self, exc: Exception) -> str | None:
        return None

    # -- federation lifecycle (REQ-825) ----------------------------------------
    # The boot sequence drives every engine through the same lifecycle:
    #   write_config → configure_session → provision → provision_infra → (serve) → close
    # plus the ongoing watchdog. Each phase is engine-agnostic at the call site; an engine slots its
    # own behavior into a phase, and a phase it doesn't need is a no-op (never a name-branch caller).

    def write_config(self, state: Any, config_path: str) -> None:
        """Lifecycle: render the engine's cluster config from platform config (Trino jvm.config /
        config.properties). In-process engines have no external cluster to configure — no-op."""

    def configure_session(self, state: Any, server_cfg: dict) -> None:
        """Lifecycle: set engine session hints (e.g. Trino fault-tolerant execution) on ``state``.
        Native engines have no per-session cluster tuning — no-op."""

    def polling_provider(
        self, state: Any, catalog: str, schema: str, table: str, watermark_column: str
    ):
        """A change-data polling provider for the engine, or ``None`` when the engine offers no
        catalog-polling transport (native engines poll their source directly instead)."""
        return None

    def close(self, state: Any) -> None:
        """Lifecycle: tear down the engine terminal. Native engines close with the process — no-op."""

    def register_kafka_catalog(self, state: Any, kafka_source: dict) -> None:
        """Register a Kafka source as an engine catalog. Native engines reach Kafka through their
        own connector (or not at all) — no-op."""

    def reseed_ops(self, state: Any, ops_views: list, retention_hours: int | None) -> None:
        """Idempotently re-seed the OTel ops store (self-heal if boot seeding raced). No-op for a
        native engine, whose telemetry lives in the dedicated ops store."""

    def cluster_diagnostics(self, state: Any) -> tuple[bool, int, int]:
        """Engine health for the admin system-health view: ``(connected, worker_count,
        active_workers)``. A native in-process engine has no worker cluster."""
        return (self.is_connected(state), 0, 0)

    def ctas_redirect(self, state: Any, physical_sql: str, output_format: str) -> dict:
        """Execute a query as CTAS-to-object-store and return the redirect manifest. A native
        engine has no CTAS-to-S3 redirect path."""
        raise NotImplementedError(
            f"engine {self.engine.name!r} does not implement CTAS-to-object-store redirect"
        )

    # -- source lifecycle ------------------------------------------------------

    def register_source(self, state: Any, source: Any, resolved_password: str) -> None:
        """Native engines attach lazily at query time — nothing to provision here."""

    def drop_source(self, state: Any, source_id: str) -> None:
        """No dynamic catalog to drop."""

    def analyze(self, state: Any, source: Any, tables: list) -> None:
        """Native engines gather statistics implicitly or not at all."""

    # -- connections -----------------------------------------------------------

    @contextmanager
    def isolated_sync(self, state: Any):
        """Native engines share the bound in-process connection."""
        yield state.engine_conn

    # -- introspection ---------------------------------------------------------

    def introspect_by_catalog(
        self, state: Any, catalog: str, schema: str, table: str
    ) -> dict[str, str]:
        """Native engines have no live physical-catalog information_schema to read at compile time."""
        return {}

    def introspect_columns(
        self, state: Any, source: Any, schema_name: str, table_name: str
    ) -> dict[str, str]:
        """Column types as the native engine reports them (DuckDB DESCRIBE / ClickHouse DESCRIBE).
        Returns ``{column_name: type_name}``; ``{}`` when the engine cannot introspect live."""
        if self.engine.native_store == "duckdb":
            from types import SimpleNamespace

            from provisa.core.secrets import resolve_secrets
            from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

            def _rs(v: Any) -> Any:  # resolve ${env:..}/${secret:..} in connection strings
                return resolve_secrets(v) if isinstance(v, str) else v

            merged = SimpleNamespace(
                id=source.id,
                type=source.type,
                host=_rs(getattr(source, "host", None)),
                port=getattr(source, "port", None),
                database=_rs(getattr(source, "database", None)),
                username=_rs(getattr(source, "username", None)),
                password=_rs(getattr(source, "password", None)),
                path=_rs(getattr(source, "path", None)),
                schema_name=schema_name,
                table_name=table_name,
            )
            import duckdb

            runtime = DuckDBFederationRuntime()
            try:
                return runtime.introspect_columns(merged)
            except duckdb.Error:
                # Engine can't reach the source right now (e.g. offline extension install,
                # source down): keep declared types. Introspection only augments — logged.
                _log.warning(
                    "duckdb introspection of %s.%s failed; keeping declared types",
                    schema_name,
                    table_name,
                    exc_info=True,
                )
                return {}
            finally:
                runtime.close()
        if self.engine.native_store == "clickhouse":  # REQ-909 / REQ-912
            from types import SimpleNamespace

            from provisa.core.secrets import resolve_secrets

            # The ClickHouse ENGINE backend (server via clickhouse://, or embedded chdb via chdb://)
            # comes from the configured engine URL ($PROVISA_ENGINE_URL or the persisted config);
            # without it the engine cannot introspect live — keep declared types (seam contract).
            from provisa.federation.engine import configured_engine_url

            dsn = configured_engine_url()
            if not dsn:
                return {}

            def _rs(v: Any) -> Any:  # resolve ${env:..}/${secret:..} in connection strings
                return resolve_secrets(v) if isinstance(v, str) else v

            merged = SimpleNamespace(
                id=source.id,
                type=source.type,
                host=_rs(getattr(source, "host", None)),
                port=getattr(source, "port", None),
                database=_rs(getattr(source, "database", None)),
                username=_rs(getattr(source, "username", None)),
                password=_rs(getattr(source, "password", None)),
                path=_rs(getattr(source, "path", None)),
                federation_hints=getattr(source, "federation_hints", {}),
                schema_name=schema_name,
                table_name=table_name,
            )
            from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

            runtime = ClickHouseFederationRuntime.from_url(dsn)
            try:
                return runtime.introspect_columns(merged)
            except Exception:
                # Engine can't reach the source right now (server down, private bucket, Mongo needs
                # a column list): keep declared types. Introspection only augments — logged.
                _log.warning(
                    "clickhouse introspection of %s.%s failed; keeping declared types",
                    schema_name,
                    table_name,
                    exc_info=True,
                )
                return {}
            finally:
                runtime.close()
        return {}

    # -- execution -------------------------------------------------------------

    async def execute(
        self,
        state: Any,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
        fresh: bool = False,
        conn_kwargs: dict | None = None,
        span_attrs: dict[str, str] | None = None,
        extra_table_attrs: list[dict[str, str]] | None = None,
    ) -> QueryResult:
        raise NotImplementedError(
            f"live ENGINE-terminal execution for engine {self.engine.name!r} is not wired "
            "(native-runtime execution binding is separate feature work)"
        )

    def execute_sync(self, state: Any, sql: str, params: list | None = None) -> QueryResult:
        raise NotImplementedError(
            f"live ENGINE-terminal execution for engine {self.engine.name!r} is not wired"
        )

    # -- engine-specific transports (Arrow) ------------------------------------

    def execute_arrow(self, state: Any, sql: str, params: list | None = None):
        raise NotImplementedError(
            f"engine {self.engine.name!r} does not implement an Arrow transport"
        )

    def execute_stream(self, state: Any, sql: str, params: list | None = None):
        raise NotImplementedError(
            f"engine {self.engine.name!r} does not implement an Arrow stream transport"
        )


class TrinoBackend(EngineBackend):
    """The Trino engine's backend — the ONE backend that references Trino. Delegates to the Trino
    implementation modules (trino_lifecycle / core.catalog / compiler.introspect / executor.trino)."""

    @property
    def dialect(self) -> str:
        return "trino"

    def transpile_physical(self, pg_sql: str) -> str:
        from provisa.transpiler.transpile import transpile_to_trino

        return transpile_to_trino(pg_sql)

    # -- lifecycle -------------------------------------------------------------

    def is_connected(self, state: Any) -> bool:
        return state.engine_conn is not None

    def provision(self, state: Any, ops_views: list, retention_hours: int | None) -> None:
        from provisa.federation import trino_lifecycle

        trino_lifecycle.provision(state, ops_views, retention_hours)

    async def provision_infra(self, state: Any) -> None:
        from provisa.federation import trino_lifecycle

        await trino_lifecycle.connect_infra(state)

    async def watchdog(self, state: Any) -> None:
        from provisa.federation import trino_lifecycle

        await trino_lifecycle.watchdog(state)

    async def reload_catalog(
        self, state: Any, catalog: str, ops_views: list, retention_hours: int | None
    ) -> dict:
        from provisa.federation import trino_lifecycle

        return await trino_lifecycle.reload_catalog(state, catalog, ops_views, retention_hours)

    def classify_error(self, exc: Exception) -> str | None:
        from provisa.federation import trino_lifecycle

        return trino_lifecycle.classify_error(exc)

    def write_config(self, state: Any, config_path: str) -> None:
        from provisa.federation import trino_lifecycle

        trino_lifecycle.write_config(config_path)

    def configure_session(self, state: Any, server_cfg: dict) -> None:
        from provisa.federation import trino_lifecycle

        trino_lifecycle.configure_session(state, server_cfg)

    def polling_provider(
        self, state: Any, catalog: str, schema: str, table: str, watermark_column: str
    ):
        from provisa.federation import trino_lifecycle

        return trino_lifecycle.polling_provider(state, catalog, schema, table, watermark_column)

    def close(self, state: Any) -> None:
        if getattr(state, "flight_client", None) is not None:
            state.flight_client.close()
        if state.engine_conn is not None:
            state.engine_conn.close()

    def register_kafka_catalog(self, state: Any, kafka_source: dict) -> None:
        from provisa.federation import trino_lifecycle

        trino_lifecycle.register_kafka_catalog(state, kafka_source)

    def reseed_ops(self, state: Any, ops_views: list, retention_hours: int | None) -> None:
        if state.engine_conn is None:
            return
        from provisa.observability.ops_trino import seed_ops_trino

        seed_ops_trino(state.engine_conn, ops_views, retention_hours)

    def cluster_diagnostics(self, state: Any) -> tuple[bool, int, int]:
        conn = state.engine_conn
        if conn is None:
            return (False, 0, 0)
        worker_count = 0
        active_workers = 0
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            connected = True
            cursor.execute("SELECT state, count(*) FROM system.runtime.nodes GROUP BY state")
            for row in cursor.fetchall():
                node_state, cnt = row[0], int(row[1])
                worker_count += cnt
                if node_state == "active":
                    active_workers = cnt
        except Exception:
            return (False, 0, 0)
        return (connected, worker_count, active_workers)

    def ctas_redirect(self, state: Any, physical_sql: str, output_format: str) -> dict:
        from provisa.executor.trino_write import execute_ctas_redirect

        return execute_ctas_redirect(state.engine_conn, physical_sql, output_format)

    # -- source lifecycle ------------------------------------------------------

    def register_source(self, state: Any, source: Any, resolved_password: str) -> None:
        if state.engine_conn is not None:
            from provisa.core import catalog

            catalog.create_catalog(state.engine_conn, source, resolved_password)

    def drop_source(self, state: Any, source_id: str) -> None:
        if state.engine_conn is not None:
            from provisa.core import catalog

            catalog.drop_catalog(state.engine_conn, source_id)

    def analyze(self, state: Any, source: Any, tables: list) -> None:
        if state.engine_conn is not None:
            from provisa.core import catalog

            catalog.analyze_source_tables(state.engine_conn, source, tables)

    # -- connections -----------------------------------------------------------

    @contextmanager
    def isolated_sync(self, state: Any):
        """A fresh, thread-isolated Trino dbapi connection, closed on exit."""
        from provisa.federation import trino_lifecycle

        conn = trino_lifecycle.connect(state.engine_conn_kwargs)
        try:
            yield conn
        finally:
            conn.close()

    # -- introspection ---------------------------------------------------------

    def introspect_by_catalog(
        self, state: Any, catalog: str, schema: str, table: str
    ) -> dict[str, str]:
        if state.engine_conn is None:
            return {}
        from provisa.compiler.introspect import introspect_column_types

        return introspect_column_types(state.engine_conn, catalog, schema, table)

    def introspect_columns(
        self, state: Any, source: Any, schema_name: str, table_name: str
    ) -> dict[str, str]:
        conn = state.engine_conn
        if conn is None:
            return {}
        from provisa.compiler.introspect import introspect_column_types
        from provisa.compiler.naming import source_to_catalog

        return introspect_column_types(conn, source_to_catalog(source.id), schema_name, table_name)

    # -- execution -------------------------------------------------------------

    async def execute(
        self,
        state: Any,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
        fresh: bool = False,
        conn_kwargs: dict | None = None,
        span_attrs: dict[str, str] | None = None,
        extra_table_attrs: list[dict[str, str]] | None = None,
    ) -> QueryResult:
        from provisa.executor.trino import execute_trino

        if fresh and conn_kwargs is None:
            conn_kwargs = state.engine_conn_kwargs
        conn = state.engine_conn
        if conn is None and conn_kwargs is None:
            raise RuntimeError(f"engine {self.engine.name!r} connection not available")
        _conn = cast("Any", conn)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: execute_trino(
                _conn,
                sql,
                params=params,
                session_hints=session_hints,
                conn_kwargs=conn_kwargs,
                span_attrs=span_attrs,
                extra_table_attrs=extra_table_attrs,
            ),
        )

    def execute_sync(self, state: Any, sql: str, params: list | None = None) -> QueryResult:
        from provisa.executor.trino import execute_trino

        conn = state.engine_conn
        if conn is None:
            raise RuntimeError(f"engine {self.engine.name!r} connection not available")
        return execute_trino(cast("Any", conn), sql, params=params)

    # -- engine-specific transports (Arrow via Zaychik Flight SQL proxy) --------

    def _flight_transport(self, state: Any) -> Any:
        client = state.flight_client
        if client is None:
            raise RuntimeError(
                f"engine {self.engine.name!r} Arrow Flight transport is not configured "
                "(set ZAYCHIK_HOST/ZAYCHIK_PORT and ensure the proxy is running)"
            )
        return client

    def execute_arrow(self, state: Any, sql: str, params: list | None = None):
        from provisa.executor.trino_flight import execute_trino_flight_arrow

        return execute_trino_flight_arrow(self._flight_transport(state), sql, params)

    def execute_stream(self, state: Any, sql: str, params: list | None = None):
        from provisa.executor.trino_flight import execute_trino_flight_stream

        return execute_trino_flight_stream(self._flight_transport(state), sql, params)
