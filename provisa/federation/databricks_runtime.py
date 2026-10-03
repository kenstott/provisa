# Copyright (c) 2026 Kenneth Stott
# Canary: d46468e0-fb6b-44f1-a1a2-4a9d115ec162
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DatabricksFederationRuntime — the Databricks SQL warehouse as a first-class engine (REQ-987).

A self-only MPP warehouse: every source LANDs into Databricks (no in-place attach — ``attach_source``
is a no-op), and governed physical SQL runs against the warehouse over the databricks-sql-connector.
Databricks produces Arrow natively (Cloud Fetch ``EXTERNAL_LINKS``), so the read transport is Arrow
end-to-end — ``run_arrow``/``run_arrow_stream`` deliver ``pyarrow`` without Python row materialization,
surfaced through the Provisa Arrow Flight server. Conforms to the NativeEngineBackend runtime protocol.
"""

# Requirements: REQ-987, REQ-1657

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from provisa.core import request_deadline
from provisa.federation.land_guard import LandGuard
from provisa.executor.result import QueryResult, ResultStream
from provisa.federation.runtime_support import (
    close_cursor,
    open_cursor,
    run_async_materialized,
    stream_rows_from_arrow,
)

_ARROW_CHUNK_ROWS = 65_536  # rows per lazy Cloud Fetch chunk (REQ-1216)


class DatabricksFederationRuntime:  # REQ-825, REQ-840, REQ-987
    def __init__(self, *, url: str) -> None:
        # databricks://token:<ACCESS_TOKEN>@<host>?http_path=<PATH>&catalog=<CAT>&schema=<SCH>
        u = urlparse(url)
        q = parse_qs(u.query)
        http_path = q.get("http_path", [""])[0]
        if not u.hostname or not http_path:
            raise ValueError(
                "databricks engine URL requires host and ?http_path= "
                "(databricks://token:TOKEN@host?http_path=/sql/1.0/warehouses/…)"
            )
        self._catalog = q.get("catalog", ["main"])[0]
        self._host = (
            u.hostname
        )  # for the Unity Catalog REST API (credential/external-location install)
        # urlparse does not percent-decode; a DSN encodes reserved chars in the token,
        # so decode it back before use.
        self._token = unquote(u.password or u.username or "")
        self._engine: Any = None
        from databricks import sql as dbsql

        from provisa.federation.databricks_tls import databricks_tls_kwargs

        def _open() -> Any:
            return dbsql.connect(
                server_hostname=u.hostname,
                http_path=http_path,
                access_token=self._token,
                **databricks_tls_kwargs(),
            )

        # A replica build writes on a connection of its own (REQ-1915), opened by the same call.
        self._open = _open
        self._conn = _open()
        # One store connection, so one write on it at a time (two lands interleaving on it was a
        # confirmed regression). A lock serializes it, and each write runs on the thread that asked
        # for it — a read-triggered land stays on its request's thread (REQ-1882), where a
        # one-worker pool took it off.
        self._land_guard = LandGuard("Databricks store connection")

    @property
    def dialect(self) -> str:
        return "databricks"

    def _engine_for(self) -> Any:
        """The Databricks engine — resolves a source's connector (mechanism + attach details)."""
        if self._engine is None:
            from provisa.federation.engine import build_databricks_engine

            self._engine = build_databricks_engine()
        return self._engine

    # -- source exposure -------------------------------------------------------

    def attach_source(self, source: Any) -> None:
        """Object/lake sources on cloud storage attach as a ZERO-COPY Databricks external table (an
        ``ATTACH_R`` SCAN — REQ-987): install + validate the Unity Catalog credential/external location
        for the bucket, then create an external table at the compiler's physical name. Every other
        source is replicated (``land_table``), so attach is a no-op for it — never a copy."""
        from provisa.federation.connector_base import LIVE_IN_PLACE

        entry = self._engine_for().resolve(source)
        if (
            entry.mechanism not in LIVE_IN_PLACE
        ):  # attach only what the engine reads in place (REQ-951)
            return None
        from provisa.federation.databricks_uc import ensure_external_link

        d = entry.details
        # Install + VALIDATE the storage credential/external location before any DDL (creds are tested
        # in Databricks — a bad credential or unreachable path raises here, not a silent bad table).
        ensure_external_link(
            self._host, self._token, location=d["location"], credential=d["credential"]
        )
        catalog, schema, table = self._phys_parts(source)
        from provisa.federation.replica_guard import refuse_live_in_write_surface

        refuse_live_in_write_surface(schema, table)  # REQ-1912
        cur = open_cursor(self._conn)
        try:
            cur.execute(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{table}` "
                f"USING {d['format']} LOCATION '{d['location']}'"
            )
        finally:
            close_cursor(cur)
        return None

    def detach_source(self, source: Any) -> None:
        """Remove the live external table of ``source``'s table, when the engine read it in place.
        Called when the table's reads move to its replica (REQ-1912). Dropping an external table
        removes its catalog entry only; the files at its location are the source's and stay."""
        from provisa.federation.connector_base import LIVE_IN_PLACE

        if self._engine_for().resolve(source).mechanism not in LIVE_IN_PLACE:
            return
        catalog, schema, table = self._phys_parts(source)
        cur = open_cursor(self._conn)
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table}`")
        finally:
            close_cursor(cur)

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        """The (catalog, schema, table) the compiler emits for a source the engine reads in place —
        catalog = the source id with hyphens normalized (``core.catalog._to_catalog_name``), a
        Unity Catalog of the source's own. A replica lives in the warehouse catalog's replicas
        schema instead (REQ-1912)."""
        from provisa.core.catalog import _to_catalog_name

        return _to_catalog_name(source.id), source.schema_name, source.table_name

    # -- materialization store -------------------------------------------------

    def _stage_from_env(self) -> Any:
        """The object stage for the bulk COPY-INTO ingest (REQ-990), or None when unconfigured.

        Presence of ``PROVISA_DATABRICKS_STAGE_URL`` turns the bulk path on (a large batch then lands
        via COPY INTO); its absence is a capability gate → the INSERT path (REQ-990 permits INSERT
        when the target lacks bulk). If the stage URL is set but its R2 credentials are missing, that
        is a misconfiguration — raise, never a silent fallback."""
        import os

        root = os.environ.get("PROVISA_DATABRICKS_STAGE_URL")
        if not root:
            return None
        missing = [
            k
            for k in (
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_ENDPOINT_OVERRIDE",
                "CLOUDFLARE_ACCOUNT_ID",
            )
            if not os.environ.get(k)
        ]
        if missing:
            raise RuntimeError(
                f"PROVISA_DATABRICKS_STAGE_URL is set but staging config is incomplete: {missing}"
            )
        from provisa.federation.databricks_store import DatabricksStage

        return DatabricksStage(
            root_url=root.rstrip("/") + "/",
            endpoint_url=os.environ["AWS_ENDPOINT_OVERRIDE"],
            credential={
                "access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
                "secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
                "account_id": os.environ["CLOUDFLARE_ACCOUNT_ID"],
            },
            uc_host=self._host,
            uc_token=self._token,
        )

    def ensure_materialize_attached(self) -> str:
        """The store IS the warehouse; landed/cache tables live in its catalog directly."""
        return self._catalog

    def mv_store_schema(self, org_id: str) -> str:
        """MVs materialize into an org-scoped cache schema inside the warehouse catalog — a dedicated
        namespace (distinct from the per-source landing schemas), created on demand at refresh.

        REQ-1623: scoped to the environment being served, so one environment's refresh does not
        overwrite the rows another environment reads."""
        from provisa.core.environments import active_org_schema

        return active_org_schema(org_id, "_mv_cache")

    async def land_table(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
        match_floor: float = 0.0,
        shape: str | None = None,
    ) -> str:
        """Land ``rows`` into the Delta table ``schema.table`` of the warehouse catalog (REQ-987,
        REQ-990, REQ-1730). Columnar bulk write via ``land_databricks_native`` — a large batch takes
        the bulk COPY INTO from a staged Parquet object when a stage is configured, else the
        multi-row INSERT. For a replica, ``schema`` is the replicas schema (REQ-1912).

        REPLACE/APPEND only: CDC is not a shape ``land_databricks_native`` implements, and is
        refused rather than mishandled."""

        from provisa.core.change_signal import CDC, select_landing_shape
        from provisa.federation.databricks_store import land_databricks_native

        del match_floor
        landing_shape = shape or select_landing_shape(change_signal, watermark_column)
        if landing_shape == CDC:
            raise NotImplementedError(
                "Databricks native landing has no CDC shape; use replace or append"
            )
        catalog = self._catalog
        stage = self._stage_from_env()
        cur = open_cursor(self._conn)
        try:
            await self._land_guard.run(
                lambda: land_databricks_native(
                    cur,
                    catalog=catalog,
                    schema=schema,
                    table=table,
                    columns=columns,
                    rows=rows,
                    change_signal=change_signal,
                    watermark_column=watermark_column,
                    stage=stage,
                    pk_columns=pk_columns,
                ),
            )
        finally:
            close_cursor(cur)
        return f"{catalog}.{schema}.{table}"

    async def reconcile_replica(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str] | None = None,
    ) -> str:
        """Eager reconcile (boot/registration): converge the replica ``schema.table`` of the
        warehouse catalog WITHOUT copying data (DDL only), so the store is complete at startup and
        survives restart. ``schema`` is the replicas schema (REQ-1912)."""

        from provisa.federation.databricks_store import reconcile_databricks_native

        catalog = self._catalog
        cur = open_cursor(self._conn)
        try:
            return await self._land_guard.run(
                lambda: reconcile_databricks_native(
                    cur,
                    catalog=catalog,
                    schema=schema,
                    table=table,
                    columns=columns,
                    pk_columns=pk_columns,
                ),
            )
        finally:
            close_cursor(cur)

    async def reconcile_landed_metadata(self, plan: Any) -> int:
        """Apply the replicated model's keys, descriptions and tags (REQ-1657): informational
        PRIMARY/FOREIGN KEY constraints, COMMENTs and ``provisa_governance:*`` tags on each replica,
        at the address the plan carries for it."""

        from provisa.federation.databricks_store import reconcile_metadata_native
        from provisa.federation.landed_keys import plan_targets

        targets = plan_targets(plan)

        def _run() -> int:
            cur = open_cursor(self._conn)
            try:
                return reconcile_metadata_native(
                    cur, targets=targets, edges=plan.edges, known_tags=plan.known_tags
                )
            finally:
                close_cursor(cur)

        return await self._land_guard.run(_run)

    @property
    def connection(self):
        return self._conn

    # -- execution -------------------------------------------------------------

    def run_sync(self, sql: str, params: list | None = None) -> ResultStream:
        """Execute SQL already in the Databricks dialect (transpiled by the backend seam) and STREAM it.

        Built on the lazy ``fetchmany_arrow`` terminal (``run_arrow_stream``) so the pgwire ENGINE route
        stays memory-bounded — no full ``QueryResult`` materialization (REQ-1217, Defect 3)."""
        schema, batches = self.run_arrow_stream(sql, params)
        return stream_rows_from_arrow(schema, batches)

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        return await run_async_materialized(self.run_sync, sql, params)

    # -- Arrow transport (REQ-987) ---------------------------------------------

    def run_arrow(self, sql: str, params: list | None = None) -> Any:
        """Execute Databricks-dialect SQL and return a ``pyarrow.Table`` — Databricks delivers Arrow
        natively via Cloud Fetch, so no Python rows are materialized for the Flight transport."""
        cur = open_cursor(self._conn)
        try:
            with request_deadline.cancel_on_deadline(cur.cancel):
                cur.execute(sql, params or None)
                return cur.fetchall_arrow()
        finally:
            close_cursor(cur)

    def run_arrow_stream(self, sql: str, params: list | None = None) -> tuple[Any, Any]:
        """Execute Databricks-dialect SQL and return ``(schema, batch_generator)`` for lazy
        record-batch streaming through the Flight server's GeneratorStream (REQ-987, REQ-1216, REQ-1217).

        Genuinely lazy: ``fetchmany_arrow`` pulls Cloud Fetch chunks from the server on demand, so the
        full result never materializes — peak memory is bounded by one chunk. The cursor closes when the
        generator drains or the consumer stops early. A zero-row result yields an empty-schema stream."""
        cur = open_cursor(self._conn)
        try:
            with request_deadline.cancel_on_deadline(cur.cancel):
                cur.execute(sql, params or None)
                first = cur.fetchmany_arrow(_ARROW_CHUNK_ROWS)
        except BaseException:
            # A statement the deadline cut short, or one that failed: no stream will own the
            # cursor, so it is closed here (REQ-1905).
            close_cursor(cur)
            raise
        if first.num_rows == 0:  # exhausted immediately — carries the column schema, no rows
            schema = first.schema
            close_cursor(cur)
            return schema, iter(())
        schema = first.schema

        def _batches():
            try:
                yield from first.to_batches()
                while True:
                    with request_deadline.cancel_on_deadline(cur.cancel):
                        tbl = cur.fetchmany_arrow(_ARROW_CHUNK_ROWS)
                    if tbl.num_rows == 0:
                        break
                    yield from tbl.to_batches()
            finally:
                close_cursor(cur)

        return schema, _batches()

    def close(self) -> None:
        self._conn.close()
