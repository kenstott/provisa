# Copyright (c) 2026 Kenneth Stott
# Canary: 5a8c1e70-4b39-4d62-9f01-7e2b0c94d358
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-893: PostgreSQL as a pluggable FederationEngine (DriverClass.PARTIAL) via postgres_fdw.

A Postgres engine attaches foreign tables from other Postgres servers in place (ATTACH, no
data movement): FDW types are provisioned at install via CREATE EXTENSION, and per-source
connections are created at runtime via CREATE SERVER + USER MAPPING + IMPORT FOREIGN SCHEMA —
mirroring Trino's bundled-connector / runtime-catalog split. Pure unit tier: the engine driver
class + the PostgresFdwConnector attach DDL / capability / probe (fake fetch, no live Postgres).
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism, PostgresFdwConnector
from provisa.federation.engine import DriverClass, build_pg_engine


def _src(sid: str, **kw) -> Source:
    fields = {"host": "h", "port": 5432, "database": "db", "username": "u", "password": "p", **kw}
    return Source(id=sid, type=SourceType.postgresql, **fields)


class _FakeFetch:
    """Async fetch for _probe_pg_extension: reports installed / available extensions."""

    def __init__(self, *, installed: set[str] | None = None, available: set[str] | None = None):
        self._installed = set(installed or ())
        self._available = set(available or ())

    async def __call__(self, sql: str):
        for ext in self._installed:
            if "pg_extension" in sql and f"'{ext}'" in sql:
                return [{"one": 1}]
        for ext in self._available:
            if "pg_available_extensions" in sql and f"'{ext}'" in sql:
                return [{"one": 1}]
        return []


# ---- engine driver class (REQ-893) ------------------------------------------


def test_pg_engine_is_a_partial_single_node_federator():
    engine = build_pg_engine()
    assert engine.driver_class() is DriverClass.PARTIAL
    assert engine.native_store == "postgres"
    assert engine.mpp is False  # single-node (cross-server joins materialize locally)


def test_pg_engine_reaches_postgres_sources_via_a_connector():
    engine = build_pg_engine()
    assert engine.reachable("postgresql") is True


# ---- connector identity / mechanism (REQ-893) -------------------------------


def test_postgres_fdw_connector_identity():
    c = PostgresFdwConnector()
    assert c.engine == "postgres"
    assert c.source_type == "postgresql"
    assert c.key == "postgres_fdw"
    assert c.mechanism is Mechanism.ATTACH  # references in place, no data movement


def test_postgres_fdw_capability_full_pushdown_and_writable():
    cap = PostgresFdwConnector().capability()
    assert cap.predicate_pushdown is True
    assert cap.join_pushdown is True
    assert cap.aggregate_pushdown is True
    assert cap.write is True


# ---- runtime attach DDL: CREATE SERVER + USER MAPPING + IMPORT (REQ-893) -----


def test_attach_ddl_provisions_extension_then_server_mapping_and_import():
    # The remote schema override rides on federation_hints (Source has no `schema` field —
    # ``source.schema`` is pydantic's BaseModel.schema method, so getattr never sees a real value).
    details = PostgresFdwConnector().details(
        _src(
            "orders",
            host="remote",
            port=5433,
            database="orders_db",
            federation_hints={"schema": "sales"},
        )
    )
    ddl = details["attach_ddl"]
    assert ddl[0] == "CREATE EXTENSION IF NOT EXISTS postgres_fdw"
    assert any(
        "CREATE SERVER IF NOT EXISTS fdw_orders" in s
        and "host 'remote'" in s
        and "port '5433'" in s
        and "dbname 'orders_db'" in s
        for s in ddl
    )
    assert any("CREATE USER MAPPING" in s and "user 'u'" in s and "password 'p'" in s for s in ddl)
    assert any(
        "IMPORT FOREIGN SCHEMA sales FROM SERVER fdw_orders INTO fdw_orders" in s for s in ddl
    )
    assert details["local_schema"] == "fdw_orders"


def test_attach_ddl_defaults_remote_schema_to_public_when_unset():
    # No federation_hints schema override -> the documented default, NOT a leaked method repr.
    details = PostgresFdwConnector().details(_src("plain"))
    imports = [s for s in details["attach_ddl"] if s.startswith("IMPORT FOREIGN SCHEMA")]
    assert imports == ["IMPORT FOREIGN SCHEMA public FROM SERVER fdw_plain INTO fdw_plain"]
    assert "bound method" not in imports[0]  # regression guard for the schema-vs-.schema() bug


# ---- install-time provisioning probe (REQ-904) ------------------------------


@pytest.mark.asyncio
async def test_probe_available_when_extension_installed():
    r = await PostgresFdwConnector().probe(_FakeFetch(installed={"postgres_fdw"}))
    assert r.available is True


@pytest.mark.asyncio
async def test_probe_available_when_installable_since_engine_auto_creates_on_attach():
    # postgres_fdw is auto-created on attach, so an installable (not-yet-created) extension is enough.
    r = await PostgresFdwConnector().probe(_FakeFetch(available={"postgres_fdw"}))
    assert r.available is True


@pytest.mark.asyncio
async def test_probe_unavailable_with_remediation_when_extension_absent():
    r = await PostgresFdwConnector().probe(_FakeFetch())
    assert r.available is False
    assert r.remediation and "postgres_fdw" in r.remediation


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
