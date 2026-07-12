# Copyright (c) 2026 Kenneth Stott
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
