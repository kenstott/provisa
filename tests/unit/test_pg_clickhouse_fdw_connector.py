# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8a1f4d-6e29-4b57-9a83-2d6f0e7c9b41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1870: PgClickHouseFdwConnector — live ATTACH reach for a clickhouse source via ClickHouse's
own official pg_clickhouse extension (clickhouse_fdw), alongside the existing DIRECT reach.

Live-verified this session against a real ~60M-row table: IMPORT FOREIGN SCHEMA auto-typed the
full schema, count(*) matched exactly, and EXPLAIN VERBOSE confirmed predicate+aggregate pushdown.
This test suite is pure logic — no live ClickHouse/Postgres — driven by a fake fetch callable.
"""

from __future__ import annotations

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import PgClickHouseFdwConnector


def _src(sid: str, **kw) -> Source:
    return Source(id=sid, type=SourceType.clickhouse, **kw)


class _FakeFetch:
    def __init__(self, *, installed: bool, available: bool = False):
        self._installed = installed
        self._available = available

    async def __call__(self, sql: str):
        if "pg_extension" in sql:
            return [{"one": 1}] if self._installed else []
        if "pg_available_extensions" in sql:
            return [{"one": 1}] if self._available else []
        return []


def test_connector_identity_and_reach_modes():
    c = PgClickHouseFdwConnector()
    assert c.engine == "postgres"
    assert c.source_type == "clickhouse"
    assert c.key == "pg_clickhouse"
    assert c.mechanism is Mechanism.ATTACH_RW
    assert c.reach_modes == frozenset({Mechanism.ATTACH_RW, Mechanism.DIRECT})
    assert c.reads_in_place is True


def test_capability_reports_predicate_and_aggregate_pushdown_not_join():
    cap = PgClickHouseFdwConnector().capability()
    assert cap.predicate_pushdown is True
    assert cap.aggregate_pushdown is True
    assert cap.join_pushdown is False


def test_details_emit_extension_server_mapping_and_import_schema():
    details = PgClickHouseFdwConnector().details(
        _src("orders_ch", host="clickhouse", database="analytics", username="default", password="p")
    )
    ddl = details["attach_ddl"]
    assert ddl[0] == "CREATE EXTENSION IF NOT EXISTS pg_clickhouse"
    assert any(
        'CREATE SERVER IF NOT EXISTS "ch_orders_ch"' in s
        and "driver 'binary'" in s
        and "host 'clickhouse'" in s
        and "dbname 'analytics'" in s
        for s in ddl
    )
    assert any("user 'default'" in s and "password 'p'" in s for s in ddl)
    assert any(
        'IMPORT FOREIGN SCHEMA "analytics" FROM SERVER "ch_orders_ch" INTO "ch_orders_ch"' in s
        for s in ddl
    )
    assert details["local_schema"] == "ch_orders_ch"


def test_details_defaults_database_to_default_when_unset():
    details = PgClickHouseFdwConnector().details(_src("bare", host="clickhouse"))
    assert any("dbname 'default'" in s for s in details["attach_ddl"])
    assert any("user 'default'" in s for s in details["attach_ddl"])


async def test_probe_available_when_extension_installed():
    r = await PgClickHouseFdwConnector().probe(_FakeFetch(installed=True))
    assert r.available is True


async def test_probe_unavailable_when_extension_absent():
    r = await PgClickHouseFdwConnector().probe(_FakeFetch(installed=False, available=False))
    assert r.available is False
    assert r.remediation is not None
