# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1e8d3c-2f7b-4c95-b0a4-7e3d9c1f5b82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Fabric/Synapse attach failure makes that ONE table unqueryable — it never aborts the attach
pass for every other registered table (native_backend._attach_errors contract)."""

# Requirements: REQ-841

import pyodbc

from provisa.federation.engine import UnreachableSource
from provisa.federation.mssql_warehouse_backend import FabricBackend, SynapseBackend
from provisa.federation.native_backend import NativeEngineBackend


def test_warehouse_backends_treat_a_driver_error_as_this_table_not_queryable():
    for backend in (FabricBackend, SynapseBackend):
        assert issubclass(backend, NativeEngineBackend)
        errors = backend._attach_errors
        assert issubclass(pyodbc.ProgrammingError, errors)  # e.g. 2714 CREATE SCHEMA [public]
        assert UnreachableSource in errors and KeyError in errors  # the base contract is kept


def test_a_registered_public_schema_has_one_valid_tsql_physical_name():
    """`public` is a T-SQL fixed database role, so it cannot be a schema (CREATE SCHEMA → 2714). The
    runtime's DDL and the SQL sent to the warehouse must map it identically."""
    from types import SimpleNamespace

    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime
    from provisa.transpiler.transpile import transpile, tsql_physical_schema

    assert tsql_physical_schema("public") == "provisa_public"
    assert tsql_physical_schema("PUBLIC") == "provisa_PUBLIC"
    assert tsql_physical_schema("sales") == "sales"

    rt = MssqlWarehouseRuntime.__new__(MssqlWarehouseRuntime)
    rt._database = "wh"
    parts = rt._phys_parts(SimpleNamespace(schema_name="public", table_name="orders"))
    assert parts == ("wh", "provisa_public", "orders")

    sql = transpile('SELECT "id" FROM "wh"."public"."orders" JOIN "wh"."sales"."x" ON 1=1', "tsql")
    assert "[wh].[provisa_public].[orders]" in sql
    assert "[wh].[sales].[x]" in sql
    assert transpile("SELECT 1", "tsql") == "SELECT 1"
