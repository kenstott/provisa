# Copyright (c) 2026 Kenneth Stott
# Canary: 2f8c9a63-5b48-4e75-9e12-3c7a0d4f9d33
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Direct write path source-of-truth: driver ∩ sqlglot dialect (REQ-229)."""

from __future__ import annotations

from provisa.executor.writable import (
    WritePath,
    engine_writable_source_types,
    is_writable,
    is_writable_on,
    is_writable_via_engine,
    resolve_write_path,
    sqlglot_write_dialect,
    writable_source_types,
)


def test_sqlglot_dialect_resolves_for_mapped_rdbs():
    assert sqlglot_write_dialect("postgresql") == "postgres"
    assert sqlglot_write_dialect("sqlserver") == "tsql"
    assert sqlglot_write_dialect("singlestore") == "singlestore"  # sqlglot knows it


def test_sqlglot_dialect_none_for_unmapped_or_nonrelational():
    assert sqlglot_write_dialect("iceberg") is None  # no SOURCE_TO_DIALECT entry
    assert sqlglot_write_dialect("redis") is None
    assert sqlglot_write_dialect("csv") is None


def test_writable_requires_both_gates():
    # postgresql: native driver (asyncpg, always installed) + sqlglot dialect → writable.
    assert is_writable("postgresql") is True
    # a type with a sqlglot dialect but no direct driver is NOT writable.
    assert sqlglot_write_dialect("redshift") == "redshift"
    assert is_writable("redshift") is False  # no direct driver registered
    # a type with neither gate.
    assert is_writable("iceberg") is False


def test_writable_set_is_subset_of_dialect_mapped_and_driver_backed():
    from provisa.executor.drivers.registry import has_driver

    for t in writable_source_types():
        assert has_driver(t), f"{t} in writable set but has no driver"
        assert sqlglot_write_dialect(t) is not None, f"{t} in writable set but no sqlglot dialect"
    # postgresql is always present (asyncpg is a core dependency).
    assert "postgresql" in writable_source_types()


def test_sqlalchemy_fallback_makes_sqlite_writable():
    # sqlite has no native async driver, but the SQLAlchemy fallback covers it (sqlalchemy + stdlib
    # sqlite3 are always available) and sqlglot has a sqlite dialect → writable via the fallback.
    from provisa.executor.drivers.registry import create_driver, has_driver
    from provisa.executor.drivers.sqlalchemy_driver import SQLAlchemyDriver

    assert has_driver("sqlite") is True
    assert isinstance(create_driver("sqlite"), SQLAlchemyDriver)
    assert sqlglot_write_dialect("sqlite") == "sqlite"
    assert is_writable("sqlite") is True
    assert "sqlite" in writable_source_types()


# ---- engine-routed writes + preference order (REQ-826/842) ------------------


def _engine_with(*connectors):
    from provisa.federation.engine import FederationEngine

    return FederationEngine("test", list(connectors))


def test_is_writable_via_engine_reflects_connector_write_flag():
    from provisa.federation.engine import build_pg_engine

    eng = build_pg_engine()
    assert is_writable_via_engine("postgresql", eng) is True  # postgres_fdw write=True
    assert is_writable_via_engine("csv", eng) is False  # file scanner, read-only
    assert is_writable_via_engine("mongodb", eng) is False  # no connector at all


def test_engine_writable_source_types_lists_only_write_connectors():
    from provisa.federation.engine import build_pg_engine

    got = engine_writable_source_types(build_pg_engine())
    assert got == {"postgresql", "sqlite", "mysql"}  # write=True connectors
    assert "csv" not in got and "parquet" not in got and "json" not in got


def test_resolve_write_path_prefers_native_then_sqlalchemy_then_engine():
    from provisa.federation.connector import WarehouseNativeConnector

    # postgresql: native asyncpg is always present → NATIVE wins even though an engine could too.
    eng = _engine_with(WarehouseNativeConnector("test", "postgresql"))
    assert resolve_write_path("postgresql", eng) is WritePath.NATIVE

    # sqlite: no native driver, SQLAlchemy fallback present → SQLALCHEMY (not engine).
    eng = _engine_with(WarehouseNativeConnector("test", "sqlite"))
    assert resolve_write_path("sqlite", eng) is WritePath.SQLALCHEMY

    # a source with neither a native driver nor a fallback, only a write-capable connector → ENGINE.
    eng = _engine_with(WarehouseNativeConnector("test", "cassandra"))  # write=True, no driver/dialect
    assert resolve_write_path("cassandra", eng) is WritePath.ENGINE

    # no path at all → None.
    assert resolve_write_path("cassandra", None) is None


def test_is_writable_on_true_when_any_path_exists():
    from provisa.federation.connector import WarehouseNativeConnector

    eng = _engine_with(WarehouseNativeConnector("test", "cassandra"))
    assert is_writable_on("cassandra", eng) is True  # engine-only
    assert is_writable_on("cassandra", None) is False  # no direct path, no engine
    assert is_writable_on("postgresql", None) is True  # native direct
