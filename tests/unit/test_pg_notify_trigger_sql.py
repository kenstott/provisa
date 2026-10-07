# Copyright (c) 2026 Kenneth Stott
# Canary: 345f3402-7cd2-42e1-8dbc-b752e6c38623
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1515: the notify trigger's SQL carries the payload guard that keeps a write alive.

Whether PostgreSQL actually accepts the NOTIFY is proved by
``tests/integration/test_pg_notify_payload_limit.py`` against a real server. What is provable
without one is that the DDL Provisa installs contains the guard at all — a trigger emitted
without it announces every row unconditionally, and the first oversized row aborts the writer's
transaction.
"""

from __future__ import annotations

from provisa.core.database import Capabilities
from provisa.subscriptions.pg_provider import CHANNEL_PREFIX
from provisa.subscriptions.pg_triggers import MAX_NOTIFY_BYTES, _trigger_sql


def test_payload_limit_stays_below_the_postgres_maximum() -> None:
    # 8000 is the point at which NOTIFY raises, so the budget has to sit under it.
    assert MAX_NOTIFY_BYTES < 8000


def test_trigger_measures_the_payload_before_announcing_it() -> None:
    sql = _trigger_sql("public", "query_audit_log")

    assert f"octet_length(payload) > {MAX_NOTIFY_BYTES}" in sql
    assert f"budget := {MAX_NOTIFY_BYTES};" in sql


def test_oversized_row_is_truncated_rather_than_dropped() -> None:
    sql = _trigger_sql("public", "query_audit_log")

    # The subscriber still learns the row changed and reads its leading columns.
    assert "'truncated', true" in sql
    assert "'row_text', left(rowjson, budget)" in sql
    assert "lower(TG_OP)" in sql


def test_budget_halves_until_the_envelope_fits_and_the_loop_ends() -> None:
    sql = _trigger_sql("public", "query_audit_log")

    # A character encodes to as many as four bytes, so one pass is not enough — but a budget
    # that never reaches zero would spin inside the writer's transaction.
    assert "budget := budget / 2;" in sql
    assert f"EXIT WHEN octet_length(payload) <= {MAX_NOTIFY_BYTES} OR budget < 1;" in sql


def test_notify_targets_the_table_channel_the_provider_listens_on() -> None:
    sql = _trigger_sql("app", "orders")

    assert f"pg_notify('{CHANNEL_PREFIX}orders'" in sql
    assert "provisa_notify_app_orders" in sql
    assert "ON app.orders" in sql


def test_a_view_is_served_by_polling_without_attempting_a_trigger(caplog) -> None:
    # REQ-258: a view cannot carry a row trigger, so its subscription uses watermark polling; that
    # is decided up front from the catalog, never by a failed CREATE TRIGGER and a warning (the
    # control-plane meta views -- roles_domain_access and the rest -- hit this at startup).
    import asyncio
    import logging

    from provisa.subscriptions.pg_triggers import ensure_pg_notify_triggers

    class _FakeConn:
        capabilities = Capabilities.for_dialect("postgresql")

        def __init__(self, base: set[tuple[str, str]]) -> None:
            self._base = base
            self.executed: list[str] = []

        async def fetch(self, sql, wanted):  # noqa: ANN001
            assert "relkind" in sql  # the up-front base-table decision
            return [w for w in wanted if (w["schema"], w["name"]) in self._base]

        async def execute(self, sql):  # noqa: ANN001
            self.executed.append(sql)
            return "OK"

    conn = _FakeConn(base={("org_default", "orders")})
    tables = [
        {"source_id": "cp", "schema_name": "org_default", "table_name": "orders"},  # base table
        {
            "source_id": "cp",
            "schema_name": "org_default",
            "table_name": "roles_domain_access",
        },  # view
        {"source_id": "api", "schema_name": "api", "table_name": "list_pets"},  # non-pg: ignored
    ]
    source_types = {"cp": "postgresql", "api": "openapi"}
    with caplog.at_level(logging.WARNING, logger="provisa.subscriptions.pg_triggers"):
        installed = asyncio.run(ensure_pg_notify_triggers(conn, tables, source_types))

    assert installed == {"orders"}  # only the base table got a trigger
    assert len(conn.executed) == 1 and "org_default.orders" in conn.executed[0]
    # The view triggered no CREATE and no "failed ... fall back to polling" warning.
    assert not [r for r in caplog.records if "roles_domain_access" in r.getMessage()]


def test_a_non_postgres_control_plane_installs_no_triggers_and_does_not_query_pg_catalog() -> None:
    # REQ-258: LISTEN/NOTIFY is PostgreSQL-only. On a SQLite demo/dev control plane the walk must
    # install nothing and must NOT run the PG-only pg_class probe (it would fail the whole walk).
    import asyncio

    from provisa.subscriptions.pg_triggers import ensure_pg_notify_triggers

    class _SqliteConn:
        capabilities = Capabilities.for_dialect("sqlite")

        async def fetch(self, *_a, **_k):  # noqa: ANN002, ANN003
            raise AssertionError("pg_class probe must not run on a non-postgres control plane")

        async def execute(self, *_a, **_k):  # noqa: ANN002, ANN003
            raise AssertionError("no trigger DDL on a non-postgres control plane")

    tables = [{"source_id": "cp", "schema_name": "org_default", "table_name": "orders"}]
    installed = asyncio.run(ensure_pg_notify_triggers(_SqliteConn(), tables, {"cp": "postgresql"}))
    assert installed == set()
