# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8c0a17-6d29-4b45-9e71-2c5b0e4a9d18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-907: SqliteFdwConnector / MysqlFdwConnector — attach DDL, capability,
runtime_deps packaging surface, and functional probe.

Pure logic — the async ``probe(fetch)`` is driven by a fake fetch callable that
returns canned pg_extension / pg_available_extensions rows; no live Postgres.
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_base import DriverProvider, RuntimeDep
from provisa.federation.connector_duckdb import MysqlFdwConnector, SqliteFdwConnector


def _src(sid: str, type_: SourceType, **kw) -> Source:
    fields = {"host": "h", "port": 3306, "database": "db", "username": "u", "password": "p", **kw}
    return Source(id=sid, type=type_, **fields)


class _FakeFetch:
    """Async ``fetch(sql)`` returning [{...}] for whitelisted probe SQL, else []."""

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


# ---- identity / packaging (REQ-907) -----------------------------------------


def test_sqlite_fdw_identity_and_runtime_deps():
    c = SqliteFdwConnector()
    assert c.engine == "postgres"
    assert c.source_type == "sqlite"
    assert c.key == "sqlite_fdw"
    assert c.mechanism is Mechanism.ATTACH_RW
    assert c.runtime_deps == (RuntimeDep("libsqlite3", DriverProvider.SYSTEM),)
    assert c.operator_deps == ()  # system-provided — never BYO (REQ-948)


def test_mysql_fdw_identity_and_runtime_deps():
    c = MysqlFdwConnector()
    assert c.engine == "postgres"
    assert c.source_type == "mysql"
    assert c.key == "mysql_fdw"
    assert c.mechanism is Mechanism.ATTACH_RW
    assert c.runtime_deps == (
        RuntimeDep("libmysqlclient / mariadb-connector-c", DriverProvider.BUNDLED),
    )
    assert c.operator_deps == ()  # bundled — Provisa ships it, not BYO (REQ-948)


# ---- capability (REQ-907) ----------------------------------------------------


def test_sqlite_fdw_capability_predicate_and_write():
    cap = SqliteFdwConnector().capability()
    assert cap.predicate_pushdown is True
    assert cap.write is True


def test_mysql_fdw_capability_predicate_join_write():
    cap = MysqlFdwConnector().capability()
    assert cap.predicate_pushdown is True
    assert cap.join_pushdown is True
    assert cap.write is True


# ---- attach DDL (REQ-907) ----------------------------------------------------


def test_sqlite_fdw_attach_ddl_binds_file_path():
    details = SqliteFdwConnector().details(_src("inq", SourceType.sqlite, path="/data/inq.sqlite"))
    ddl = details["attach_ddl"]
    assert "CREATE EXTENSION IF NOT EXISTS sqlite_fdw" in ddl[0]
    assert any("FOREIGN DATA WRAPPER sqlite_fdw" in s for s in ddl)
    assert any("database '/data/inq.sqlite'" in s for s in ddl)
    assert any("IMPORT FOREIGN SCHEMA public FROM SERVER fdw_inq" in s for s in ddl)
    assert details["local_schema"] == "fdw_inq"


def test_mysql_fdw_attach_ddl_creates_server_user_mapping_and_import():
    details = MysqlFdwConnector().details(
        _src("inv", SourceType.mysql, host="mysqlhost", port=3306, database="inventory")
    )
    ddl = details["attach_ddl"]
    assert "CREATE EXTENSION IF NOT EXISTS mysql_fdw" in ddl[0]
    assert any("FOREIGN DATA WRAPPER mysql_fdw" in s and "host 'mysqlhost'" in s for s in ddl)
    assert any("CREATE USER MAPPING" in s and "username 'u'" in s for s in ddl)
    assert any("IMPORT FOREIGN SCHEMA inventory FROM SERVER fdw_inv" in s for s in ddl)


# ---- functional probe (REQ-904 / REQ-907) ------------------------------------


@pytest.mark.asyncio
async def test_sqlite_fdw_probe_available_when_installed():
    r = await SqliteFdwConnector().probe(_FakeFetch(installed={"sqlite_fdw"}))
    assert r.available is True


@pytest.mark.asyncio
async def test_sqlite_fdw_probe_available_when_only_installable_auto_created():
    # auto_create=True -> installable is enough (engine CREATE EXTENSIONs on attach).
    r = await SqliteFdwConnector().probe(_FakeFetch(available={"sqlite_fdw"}))
    assert r.available is True


@pytest.mark.asyncio
async def test_mysql_fdw_probe_unavailable_when_absent_with_remediation():
    r = await MysqlFdwConnector().probe(_FakeFetch())
    assert r.available is False
    assert r.remediation and "mysql_fdw" in r.remediation


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
