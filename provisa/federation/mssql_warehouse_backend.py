# Copyright (c) 2026 Kenneth Stott
# Canary: eb7fa93e-db0a-4a4e-891c-45208d474f20
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FabricBackend / SynapseBackend — the Microsoft Fabric Warehouse / Azure Synapse engine terminals.

Both drive the shared ``MssqlWarehouseRuntime`` (T-SQL over TDS/ODBC, Azure AD auth); they differ only
in which env supplies the server + database. Dialect is T-SQL (transpile target)."""

from __future__ import annotations

import os
from typing import Any

from provisa.federation.native_backend import NativeEngineBackend


class _MssqlWarehouseBackend(NativeEngineBackend):
    _server_env = ""
    _database_env = ""
    _engine_name = ""

    @property
    def dialect(self) -> str:
        return "tsql"

    def landing_target(
        self,
        *,
        store_schema: str,
        source_id: str,
        source_type: Any,
        schema_name: str,
        table_name: str,
    ) -> tuple[str, str]:
        """EVERY MATERIALIZED source's replica lands at its REGISTERED address — same reasoning and
        same fix as ``BigQueryBackend``/``PgBackend``/``SqlAlchemyBackend`` (REQ-1730): there is no
        DuckDB-style mangled-name+separate-view indirection to redirect a MATERIALIZE_ONLY source
        through here. ``MssqlWarehouseRuntime.attach_landed_source``/``land_table`` (REQ-1633's own
        gap — Fabric/Synapse had neither before this) address the landed table directly at
        ``self._database``.``schema_name``.``table_name`` — the catalog is the FIXED warehouse
        database (module doc: "T-SQL is database.schema.table, a fixed warehouse database +
        per-source schema"), never per-source, so unlike Databricks/ClickHouse this needs no
        catalog-folding through ``schema_name`` at all. The base ``EngineBackend`` default (a
        mangled name under the ``store_schema`` this function receives) would send
        ``land_table``'s row LOAD to a schema nothing ever DDL-reconciles."""
        del store_schema, source_id, source_type  # never chooses a different table
        return schema_name, table_name

    def _new_runtime(self) -> Any:
        from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime

        return MssqlWarehouseRuntime(
            server=os.environ.get(self._server_env, ""),
            database=os.environ.get(self._database_env, ""),
            engine_name=self._engine_name,
        )


class FabricBackend(_MssqlWarehouseBackend):
    _server_env = "FABRIC_SQL_SERVER"
    _database_env = "FABRIC_DATABASE"
    _engine_name = "fabric"


class SynapseBackend(_MssqlWarehouseBackend):
    _server_env = "SYNAPSE_SQL_SERVER"
    _database_env = "SYNAPSE_DATABASE"
    _engine_name = "synapse"
