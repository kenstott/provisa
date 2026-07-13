# Copyright (c) 2026 Kenneth Stott
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

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

from provisa.executor.result import QueryResult
from provisa.federation.runtime_support import result_from_dbapi, run_async


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
        self._token = u.password or u.username or ""
        self._engine: Any = None
        from databricks import sql as dbsql

        self._conn = dbsql.connect(
            server_hostname=u.hostname,
            http_path=http_path,
            access_token=self._token,
        )

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
        source LANDs (handled by ``materialize_source``), so attach is a no-op for it — never a copy."""
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
        cur = self._conn.cursor()
        try:
            cur.execute(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{table}` "
                f"USING {d['format']} LOCATION '{d['location']}'"
            )
        finally:
            cur.close()
        return None

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        """The (catalog, schema, table) the compiler emits for a source — catalog = the source id with
        hyphens normalized (``core.catalog._to_catalog_name``), so a self-only Databricks engine lands
        each source into its own Unity Catalog and the governed query resolves natively (no view)."""
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

    async def materialize_source(
        self,
        source: Any,
        columns: list[tuple[str, str]],
        rows: list[dict],
        *,
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
    ) -> None:
        """LAND a source into the warehouse at its compiler-physical name (REQ-987, REQ-990). Self-only:
        the landed Delta table IS the physical relation the governed query reads — no separate mat table
        or view. Columnar bulk write via ``land_databricks_native`` — a large batch takes the bulk COPY
        INTO from a staged Parquet object when a stage is configured, else the multi-row INSERT."""
        del pk_columns  # Delta MERGE/CDC identity is a future path; batch land needs no PK
        import asyncio

        from provisa.federation.databricks_store import land_databricks_native

        catalog, schema, table = self._phys_parts(source)
        stage = self._stage_from_env()
        cur = self._conn.cursor()
        try:
            await asyncio.to_thread(
                land_databricks_native,
                cur,
                catalog=catalog,
                schema=schema,
                table=table,
                columns=columns,
                rows=rows,
                change_signal=change_signal,
                watermark_column=watermark_column,
                stage=stage,
            )
        finally:
            cur.close()

    async def attach_landed_source(
        self, source: Any, columns: list[tuple[str, str]], *, pk_columns: list[str] | None = None
    ) -> None:
        """Eager reconcile (boot/registration): converge the landing table at the physical name
        WITHOUT landing data (DDL only), so the catalog is complete at startup and survives restart."""
        del pk_columns
        import asyncio

        from provisa.federation.databricks_store import reconcile_databricks_native

        catalog, schema, table = self._phys_parts(source)
        cur = self._conn.cursor()
        try:
            await asyncio.to_thread(
                reconcile_databricks_native,
                cur,
                catalog=catalog,
                schema=schema,
                table=table,
                columns=columns,
            )
        finally:
            cur.close()

    @property
    def connection(self):
        return self._conn

    # -- execution -------------------------------------------------------------

    def run_sync(self, sql: str, params: list | None = None) -> QueryResult:
        """Execute SQL already in the Databricks dialect (transpiled by the backend seam)."""
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params or None)
            return result_from_dbapi(cur)
        finally:
            cur.close()

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        return await run_async(self.run_sync, sql, params)

    # -- Arrow transport (REQ-987) ---------------------------------------------

    def run_arrow(self, sql: str, params: list | None = None) -> Any:
        """Execute Databricks-dialect SQL and return a ``pyarrow.Table`` — Databricks delivers Arrow
        natively via Cloud Fetch, so no Python rows are materialized for the Flight transport."""
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params or None)
            return cur.fetchall_arrow()
        finally:
            cur.close()

    def run_arrow_stream(self, sql: str, params: list | None = None) -> tuple[Any, Any]:
        """Execute Databricks-dialect SQL and return ``(schema, batch_generator)`` for lazy
        record-batch streaming through the Flight server's GeneratorStream (REQ-987)."""
        table = self.run_arrow(sql, params)

        def _batches():
            yield from table.to_batches()

        return table.schema, _batches()

    def close(self) -> None:
        self._conn.close()
