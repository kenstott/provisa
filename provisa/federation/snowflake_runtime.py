# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SnowflakeFederationRuntime — Snowflake promoted from source-only to a first-class engine (REQ-988).

A self-only MPP warehouse: every source LANDs into Snowflake (no in-place attach — ``attach_source``
is a no-op), and governed physical SQL runs against it via snowflake-connector-python. Snowflake has
native Arrow support (``fetch_arrow_all`` / ``fetch_arrow_batches``), so the read transport is Arrow
end-to-end — ``run_arrow``/``run_arrow_stream`` deliver ``pyarrow`` without Python row materialization,
surfaced through the Provisa Arrow Flight server. Conforms to the NativeEngineBackend runtime protocol.

The snowflake-connector driver is imported lazily (inside ``__init__``) so this module — and the engine
registry that references it — imports even where the driver is not installed (REQ-988: the engine is
selectable/declarable; a live connection requires the driver + a Snowflake account).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

import pyarrow as pa

from provisa.executor.result import QueryResult
from provisa.federation.runtime_support import result_from_dbapi, run_async


class SnowflakeFederationRuntime:  # REQ-825, REQ-840, REQ-988
    def __init__(self, *, url: str) -> None:
        # snowflake://<user>:<pass>@<account>/<database>/<schema>?warehouse=<WH>&role=<ROLE>
        u = urlparse(url)
        if not u.hostname or not u.username:
            raise ValueError(
                "snowflake engine URL requires account host and user "
                "(snowflake://user:pass@account/db/schema?warehouse=WH)"
            )
        q = parse_qs(u.query)
        path_parts = [p for p in (u.path or "").split("/") if p]
        self._database = path_parts[0] if path_parts else None
        self._schema = path_parts[1] if len(path_parts) > 1 else None
        import snowflake.connector as sf

        self._engine: Any = None
        self._conn = sf.connect(
            account=u.hostname,
            user=u.username,
            password=u.password or "",
            database=self._database,
            schema=self._schema,
            warehouse=q.get("warehouse", [None])[0],
            role=q.get("role", [None])[0],
        )

    def _engine_for(self) -> Any:
        if self._engine is None:
            from provisa.federation.engine import build_snowflake_engine

            self._engine = build_snowflake_engine()
        return self._engine

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        """(database, schema, table) — the governed physical name. The compiler pins the catalog to a
        per-source database (source id, hyphen→underscore); schema/table are the source's."""
        from provisa.core.catalog import _to_catalog_name

        return _to_catalog_name(source.id), source.schema_name, source.table_name

    # -- source exposure -------------------------------------------------------

    def attach_source(self, source: Any) -> None:
        """Object/lake sources on cloud storage attach as a ZERO-COPY external table over an external
        stage (an ``ATTACH_R`` SCAN — REQ-988): INSTALL the stage (with the source's credentials),
        create the external table at the compiler's physical name, and VALIDATE (read a row) so bad
        credentials / an unreachable object fail loud at attach time. Every other source LANDs, so
        attach is a no-op for it. NOTE: not live-verified (no Snowflake account available)."""
        from provisa.federation.connector_base import Mechanism

        entry = self._engine_for().resolve(source)
        if entry.mechanism not in (Mechanism.ATTACH_R, Mechanism.ATTACH_RW):
            return None
        from provisa.federation.snowflake_connectors import stage_and_external_table_ddl

        database, schema, table = self._phys_parts(source)
        stage = f"provisa_stg_{table}"
        cur = self._conn.cursor()
        try:
            for stmt in stage_and_external_table_ddl(database, schema, table, stage, entry.details):
                cur.execute(stmt)
        finally:
            cur.close()
        return None

    # -- materialization store -------------------------------------------------

    def ensure_materialize_attached(self) -> str:
        """The store IS the warehouse; landed/cache tables live in its database directly."""
        return self._database or ""

    @property
    def connection(self):
        return self._conn

    # -- execution -------------------------------------------------------------

    def run_sync(self, sql: str, params: list | None = None) -> QueryResult:
        """Execute SQL already in the Snowflake dialect (transpiled by the backend seam)."""
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params or None)
            return result_from_dbapi(cur)
        finally:
            cur.close()

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        return await run_async(self.run_sync, sql, params)

    # -- Arrow transport (REQ-988) ---------------------------------------------

    def run_arrow(self, sql: str, params: list | None = None) -> Any:
        """Execute Snowflake-dialect SQL and return a ``pyarrow.Table`` — Snowflake exports Arrow
        natively (``fetch_arrow_all``), so no Python rows are materialized for the Flight transport.
        An empty result set yields an empty table rather than ``None``."""
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params or None)
            table = cur.fetch_arrow_all()
            if table is None:  # snowflake returns None for a zero-row result
                names = [d[0] for d in (cur.description or [])]
                return pa.table({name: [] for name in names})
            return table
        finally:
            cur.close()

    def run_arrow_stream(self, sql: str, params: list | None = None) -> tuple[Any, Any]:
        """Execute Snowflake-dialect SQL and return ``(schema, batch_generator)`` for lazy
        record-batch streaming through the Flight server's GeneratorStream (REQ-988)."""
        table = self.run_arrow(sql, params)

        def _batches():
            yield from table.to_batches()

        return table.schema, _batches()

    def close(self) -> None:
        self._conn.close()
