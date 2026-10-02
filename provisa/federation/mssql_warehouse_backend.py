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

from provisa.federation.engine import UnreachableSource
from provisa.federation.mssql_warehouse_runtime import driver_error
from provisa.federation.native_backend import NativeEngineBackend


class _MssqlWarehouseBackend(NativeEngineBackend):
    # The driver error ORed into the base "this table is not queryable" set, as PgBackend/
    # DuckDBBackend do (native_backend._attach_errors). Without it one registered source whose
    # attach fails on the warehouse (e.g. a config source registered under schema `public`, which
    # T-SQL cannot create: `public` is a fixed database role, error 2714) aborted _attach_registered
    # and failed every query on the engine, including sources that attach fine.
    _attach_errors = (driver_error(), KeyError, UnreachableSource)
    _server_env = ""
    _database_env = ""
    _engine_name = ""

    @property
    def dialect(self) -> str:
        return "tsql"

    def _new_runtime(self) -> Any:
        from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime

        return MssqlWarehouseRuntime(
            server=os.environ.get(self._server_env, ""),
            database=os.environ.get(self._database_env, ""),
            engine_name=self._engine_name,
        )

    def replica_target(self, state: Any, *, address: Any, args: Any, engine: Any) -> Any:
        """A replica in this warehouse's own database: bulk inserts into a build table on a
        connection of the build's own, then the replica's rows replaced from it in one
        transaction (REQ-1915)."""
        del engine
        from provisa.federation.replica_target_warehouse import MssqlWarehouseStoreTarget

        runtime = self._runtime_for(state)
        schema, table = runtime._store_parts(address.schema, address.table)
        return MssqlWarehouseStoreTarget(
            runtime._connect,
            schema=schema,
            table=table,
            columns=args.columns,
            transactional=self.engine.transactional,
        )


class FabricBackend(_MssqlWarehouseBackend):
    _server_env = "FABRIC_SQL_SERVER"
    _database_env = "FABRIC_DATABASE"
    _engine_name = "fabric"


class SynapseBackend(_MssqlWarehouseBackend):
    _server_env = "SYNAPSE_SQL_SERVER"
    _database_env = "SYNAPSE_DATABASE"
    _engine_name = "synapse"
