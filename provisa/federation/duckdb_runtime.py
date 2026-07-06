# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7b2a19-6d3c-4f81-9b25-8a1e5c9d2f47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB federation runtime — ties the connectors, materialize store, and execution together.

One in-process DuckDB connection acts as the single-node federation engine. Each registered source
is exposed at its PHYSICAL ``schema.table`` name (what rewrite_semantic_to_physical emits) so
the query executes unchanged:

- ATTACH sources (postgres/sqlite/csv/parquet) are referenced in place via the (duckdb, source_type)
  connector's DDL, then wrapped in a physical-named view.
- NON-attachable sources (openapi/graphql_remote) are LANDED into the Postgres materialization store
  (land_rows_into_pg), which DuckDB ATTACHes, then wrapped in a physical-named view.

execute() runs governed semantic SQL through rewrite_semantic_to_physical -> transpile("duckdb").
This is the engine primitive a live EngineRuntime dispatch would call; routing/HTTP wiring is separate.
"""

from __future__ import annotations

import asyncio
from typing import Any

import duckdb

from provisa.executor.result import QueryResult
from provisa.federation.engine import build_duckdb_engine
from provisa.federation.materialize_exec import land_rows_into_pg
from provisa.transpiler.transpile import transpile


class DuckDBFederationRuntime:  # REQ-825, REQ-840, REQ-844
    def __init__(self, *, materialize_dsn: str | None = None) -> None:
        from provisa.federation.engine import configured_materialize_url

        self._con = duckdb.connect()
        self._engine = build_duckdb_engine()
        # Default the LAND store from config ($PROVISA_MATERIALIZE_URL / materialize_store_url).
        self._materialize_dsn = materialize_dsn or configured_materialize_url()
        self._sqlite_loaded = False
        self._pg_ext_loaded = False  # postgres DuckDB extension INSTALL/LOAD (source ATTACH)
        self._pg_attached = False  # matpg materialization store ATTACH (distinct)

    # -- source exposure -------------------------------------------------------

    def attach_source(self, source: Any) -> None:
        """Expose an ATTACH source at its physical ``schema.table`` via the engine's connector."""
        entry = self._engine.resolve(source)  # picks the (duckdb, source_type) connector
        details = entry.details
        # The physical view lives in a DuckDB schema named after the source's schema —
        # create it first (DuckDB's default schema is "main"; anything else must exist).
        self._con.execute(f'CREATE SCHEMA IF NOT EXISTS "{source.schema_name}"')
        phys = f'"{source.schema_name}"."{source.table_name}"'
        if "view_ddl" in details:  # csv / parquet scanner
            scan = details["view_ddl"].split(" AS ", 1)[1]
            self._con.execute(f"CREATE VIEW {phys} AS {scan}")
        else:  # ATTACH postgres / sqlite, then view the remote table
            if source.type.value == "sqlite" and not self._sqlite_loaded:
                self._con.execute("INSTALL sqlite")
                self._con.execute("LOAD sqlite")
                self._sqlite_loaded = True
            elif source.type.value == "postgresql" and not self._pg_ext_loaded:
                self._con.execute("INSTALL postgres")
                self._con.execute("LOAD postgres")
                self._pg_ext_loaded = True
            self._con.execute(details["attach"])
            # Reference the attached table as alias.schema.table (postgres/sqlite expose the
            # remote schema); quote each part so hyphenated ids and mixed case survive.
            remote = f'"{source.id}"."{source.schema_name}"."{source.table_name}"'
            self._con.execute(f"CREATE VIEW {phys} AS SELECT * FROM {remote}")

    async def materialize_source(
        self, source: Any, columns: list[tuple[str, str]], rows: list[dict]
    ) -> None:
        """LAND a non-attachable source into the PG store, ATTACH it, expose at physical name."""
        if self._materialize_dsn is None:
            raise RuntimeError("no materialize store configured for a non-attachable source")
        import asyncpg

        pg = await asyncpg.connect(dsn=self._materialize_dsn)
        try:
            await land_rows_into_pg(pg, schema="mat", table=source.id, columns=columns, rows=rows)
        finally:
            await pg.close()
        if not self._pg_attached:
            self._con.execute("INSTALL postgres")
            self._con.execute("LOAD postgres")
            self._con.execute(f"ATTACH '{self._dsn_kv()}' AS matpg (TYPE postgres)")
            self._pg_attached = True
        phys = f'"{source.schema_name}"."{source.table_name}"'
        self._con.execute(f"CREATE VIEW {phys} AS SELECT * FROM matpg.mat.{source.id}")

    def _dsn_kv(self) -> str:
        # postgresql://user:pw@host:port/db  ->  host=.. port=.. dbname=.. user=.. password=..
        from urllib.parse import urlparse

        u = urlparse(self._materialize_dsn or "")
        return (
            f"host={u.hostname} port={u.port or 5432} dbname={u.path.lstrip('/')} "
            f"user={u.username} password={u.password}"
        )

    # -- metadata --------------------------------------------------------------

    def introspect_columns(self, source: Any) -> dict[str, str]:
        """Column types as the DuckDB engine reports them for a registered source —
        the engine's metadata view (attach the source, DESCRIBE the physical relation).
        Returns {column_name: duckdb_type_name}. This is the DuckDB implementation of
        the engine-introspection seam (REQ-825/840); callers reach it via EngineRuntime."""
        self.attach_source(source)
        phys = f'"{source.schema_name}"."{source.table_name}"'
        res = self._con.execute(f"DESCRIBE {phys}")
        # DESCRIBE rows: (column_name, column_type, null, key, default, extra)
        return {row[0]: str(row[1]).lower() for row in res.fetchall()}

    # -- execution -------------------------------------------------------------

    async def execute(self, physical_or_governed_sql: str) -> QueryResult:
        """Execute physical SQL (post-governance) on the engine (transpiled to DuckDB)."""
        duck_sql = transpile(physical_or_governed_sql, "duckdb")
        loop = asyncio.get_event_loop()

        def _run() -> QueryResult:
            res = self._con.execute(duck_sql)
            cols = [d[0] for d in res.description] if res.description else []
            return QueryResult(rows=res.fetchall(), column_names=cols)

        return await loop.run_in_executor(None, _run)

    def close(self) -> None:
        self._con.close()
