# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2a8c41-9e7b-4f13-b6a0-c3e8f1d72b95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: the entitlement store is published in the ops system schema, beside ops.requests:
ops.entitlements (the source rows) and ops.entitlement_expiries (one row per expiry rule).

Each is an ops log over its own table, registered in domain ops like the other ops tables, keyed
by the table's id, and subscribable on its schema's own channel. Rows carry no valid-to column: the
view derives it as entitlements.history does (the next row's valid-from for the same key, as
recorded), so the view and the history reader agree row for row.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import sqlglot
from sqlalchemy import insert, text

from provisa.api._catalog_descriptions import COLUMN_DESCRIPTIONS, TABLE_DESCRIPTIONS
from provisa.api._meta_views import (
    _OPS_LOG_BASE_TABLE,
    _OPS_LOG_TABLE_ALIAS,
    _OPS_LOG_TABLE_VIEWS,
)
from provisa.core.schema_org import entitlement_expiries, entitlements, user_directory
from provisa.entitlements import history
from provisa.subscriptions.pg_provider import schema_channel

SCHEMA_SQL = (Path(__file__).resolve().parents[2] / "provisa/core/schema.sql").read_text()

TABLES = ("entitlements", "entitlement_expiries")

#: What every row of both tables shows of its valid period (REQ-2219, valid time, 2026-11-19).
PERIOD = {"valid_from", "valid_to", "recorded_at"}

#: The stored columns each view restates: every column of its table.
STORED = {
    name: {c.name for c in table.columns}
    for name, table in zip(TABLES, (entitlements, entitlement_expiries), strict=True)
}

T1 = datetime(2026, 1, 1, tzinfo=UTC)
T2 = datetime(2026, 2, 1, tzinfo=UTC)
T3 = datetime(2026, 3, 1, tzinfo=UTC)


def _columns(ddl: str) -> list[str]:
    select = sqlglot.parse_one(ddl, read="postgres").expression
    return [e.alias_or_name for e in select.expressions]


# --------------------------------------------------------------------------- #
# Registry                                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("table", TABLES)
def test_each_is_an_ops_log_over_its_own_table(table):
    view = f"{table}_ops"
    assert _OPS_LOG_TABLE_ALIAS[table] == view
    assert _OPS_LOG_BASE_TABLE[table] == table
    tree = sqlglot.parse_one(_OPS_LOG_TABLE_VIEWS[table], read="postgres")
    assert tree.this.name == view
    assert table in {t.name for t in tree.find_all(sqlglot.exp.Table)}


@pytest.mark.parametrize("table", TABLES)
def test_the_view_shows_every_stored_column_and_the_valid_period(table):
    cols = _columns(_OPS_LOG_TABLE_VIEWS[table])
    assert len(cols) == len(set(cols))
    assert STORED[table] | PERIOD <= set(cols)
    # The table has no valid-to column; the view derives it.
    assert "valid_to" not in STORED[table]


@pytest.mark.parametrize("table", TABLES)
def test_every_column_is_described(table):
    assert TABLE_DESCRIPTIONS[table]
    assert set(_columns(_OPS_LOG_TABLE_VIEWS[table])) == set(COLUMN_DESCRIPTIONS[table])


def _seeding_conn() -> AsyncMock:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _transaction():
        yield

    conn = AsyncMock()
    conn.transaction = _transaction
    return conn


def test_the_seed_registers_both_in_domain_ops_keyed_by_the_tables_id():
    from provisa.api.startup_seed import _seed_ops_domain

    views = {f"{t}_ops": _columns(_OPS_LOG_TABLE_VIEWS[t]) for t in TABLES}
    keys_read: list[str] = []

    async def reflect_columns(name, schema=None):
        if name in TABLES:  # the base table: its key
            keys_read.append(name)
            return [{"column_name": "id", "data_type": "bigint", "is_primary_key": True}]
        cols = views.get(name, ["id"])  # another ops view: only its key matters here
        return [{"column_name": c, "data_type": "text", "is_primary_key": False} for c in cols]

    conn = _seeding_conn()
    ids: dict[str, int] = {}

    async def upsert_returning(_table, payload, **_kw):
        return ids.setdefault(payload["table_name"], len(ids) + 1)

    conn.upsert_returning = AsyncMock(side_effect=upsert_returning)
    conn.reflect_columns = AsyncMock(side_effect=reflect_columns)

    asyncio.run(_seed_ops_domain(conn, org_id="default"))

    registered = {
        call.args[1]["table_name"]: call.args[1] for call in conn.upsert_returning.await_args_list
    }
    columns = [call.args[1] for call in conn.upsert.await_args_list]
    for table in TABLES:
        assert registered[table]["domain_id"] == "ops"
        assert registered[table]["source_id"] == "provisa-admin"
        assert table in keys_read
        mine = [c for c in columns if c["table_id"] == ids[table]]
        assert [c["column_name"] for c in mine] == views[f"{table}_ops"]
        assert PERIOD <= {c["column_name"] for c in mine}
        # Governed like the other ops tables: org_admin reads them by the ops grant.
        assert all(c["domain_id"] == "ops" and c["visible_to"] == ["org_admin"] for c in mine)
        assert {c["column_name"] for c in mine if c["is_primary_key"]} == {"id"}


# --------------------------------------------------------------------------- #
# Subscription                                                                 #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("table", TABLES)
def test_a_trigger_reports_each_table_on_its_schemas_channel(table):
    assert re.search(
        rf"CREATE TRIGGER provisa_sub_{table}\s+AFTER INSERT OR UPDATE OR DELETE ON {table}\s+"
        r"FOR EACH ROW EXECUTE FUNCTION provisa_notify_entitlement_rows\(\)",
        SCHEMA_SQL,
    )
    assert "'provisa_' || TG_TABLE_NAME || '_' || substr(md5(TG_TABLE_SCHEMA), 1, 12)" in SCHEMA_SQL
    schema = "org_acme_env_feature_x"
    want = f"provisa_{table}_" + hashlib.md5(schema.encode()).hexdigest()[:12]
    assert schema_channel(table, schema) == want
    assert len(want) < 64  # PostgreSQL's identifier limit


@pytest.mark.parametrize("table", TABLES)
def test_subscribe_listens_on_the_schema_channel(table):
    from provisa.api.data.subscribe import _channel

    ops = SimpleNamespace(source_id="provisa-admin", schema_name="org_acme")
    assert _channel(table, ops) == schema_channel(table, "org_acme")


# --------------------------------------------------------------------------- #
# On the control plane's PostgreSQL                                            #
# --------------------------------------------------------------------------- #


@pytest.fixture
async def conn(control_plane):
    from provisa.api.startup_seed import _adapt_view_ddl

    db = control_plane.org()
    async with db.acquire() as c:
        for table in TABLES:
            await c.execute(_adapt_view_ddl(_OPS_LOG_TABLE_VIEWS[table]))
        yield c


async def _put(conn, identifiers, *, valid_from, recorded_at, removed=False, capability=None):
    await conn.execute_core(
        insert(entitlements).values(
            source="provisa",
            principal="alice",
            role_id="viewer",
            kind="tables",
            capability=capability,
            identifiers=identifiers,
            removed=removed,
            set_by="ops",
            reason="r",
            valid_from=valid_from,
            recorded_at=recorded_at,
        )
    )


async def _put_rule(conn, ends_at, *, valid_from, recorded_at, removed=False):
    await conn.execute_core(
        insert(entitlement_expiries).values(
            rule_key="k1",
            principal="alice",
            role_id="viewer",
            ends_at=ends_at,
            ends={"read": ["a.b"]},
            list_kind="tables",
            removed=removed,
            set_by="ops",
            reason="r",
            valid_from=valid_from,
            recorded_at=recorded_at,
        )
    )


async def _view(conn, view: str) -> dict[int, tuple]:
    rows = await conn.execute_core(
        text(f"SELECT id, valid_from, valid_to, recorded_at FROM {view}")
    )
    return {r[0]: (r[1], r[2], r[3]) for r in rows.fetchall()}


def _by_history(periods) -> dict[int, tuple]:
    return {p.row.id: (p.valid_from, p.valid_to, p.row.recorded_at) for p in periods}


@pytest.mark.asyncio
async def test_entitlements_read_as_history_reads_them_now(conn):
    # A set, a change, a correction recorded later with an earlier valid time, a removal; and a
    # second key (capability read) that no other key's rows close.
    await _put(conn, ["a.b"], valid_from=T1, recorded_at=T1)
    await _put(conn, ["c.d"], valid_from=T3, recorded_at=T3)
    await _put(conn, ["e.f"], valid_from=T2, recorded_at=T3)
    await _put(conn, ["x.y"], valid_from=T1, recorded_at=T1, capability="read")
    await _put(conn, [], valid_from=T3, recorded_at=T3, capability="read", removed=True)

    shown = await _view(conn, "entitlements_ops")
    assert shown == _by_history(await history.entitlement_periods(conn))
    assert sorted((v[0], v[1]) for v in shown.values()) == [
        (T1, T2),
        (T1, T3),
        (T2, T3),
        (T3, None),
        (T3, None),
    ]


@pytest.mark.asyncio
async def test_expiry_rules_read_as_history_reads_them_now(conn):
    await _put_rule(conn, T3, valid_from=T1, recorded_at=T1)
    await _put_rule(conn, T3, valid_from=T2, recorded_at=T2, removed=True)

    shown = await _view(conn, "entitlement_expiries_ops")
    assert shown == _by_history(await history.expiry_periods(conn))
    assert sorted((v[0], v[1]) for v in shown.values()) == [(T1, T2), (T2, None)]


@pytest.mark.asyncio
async def test_a_row_recorded_after_now_is_not_shown(conn):
    later = datetime(2999, 1, 1, tzinfo=UTC)
    await _put(conn, ["a.b"], valid_from=T1, recorded_at=T1)
    await _put(conn, ["c.d"], valid_from=T2, recorded_at=later)
    shown = await _view(conn, "entitlements_ops")
    assert [(v[0], v[1]) for v in shown.values()] == [(T1, None)]


@pytest.mark.asyncio
async def test_names_are_read_from_the_directory(conn):
    await conn.execute_core(
        insert(user_directory).values(user_id="alice", display_name="Alice", email="a@x.io")
    )
    await _put(conn, ["a.b"], valid_from=T1, recorded_at=T1)
    await _put_rule(conn, T3, valid_from=T1, recorded_at=T1)
    for view in ("entitlements_ops", "entitlement_expiries_ops"):
        rows = await conn.execute_core(text(f"SELECT principal_name, set_by_name FROM {view}"))
        assert [tuple(r) for r in rows.fetchall()] == [("Alice", None)]


@pytest.mark.parametrize("table", TABLES)
def test_a_change_is_notified_on_the_schemas_channel(control_plane, table):
    import psycopg
    from psycopg import sql

    from provisa.core.environments import org_schema

    schema = org_schema("default")
    url = re.sub(r"^postgresql\+\w+://", "postgresql://", control_plane.url)
    channel = schema_channel(table, schema)
    with (
        psycopg.connect(url, autocommit=True) as listener,
        psycopg.connect(url, autocommit=True) as writer,
    ):
        listener.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
        writer.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        if table == "entitlements":
            row = writer.execute(
                "INSERT INTO entitlements (source, principal, role_id, kind) "
                "VALUES ('provisa', 'alice', 'viewer', 'tables') RETURNING id"
            ).fetchone()
        else:
            row = writer.execute(
                "INSERT INTO entitlement_expiries (rule_key, principal, role_id, ends_at) "
                "VALUES ('k1', 'alice', 'viewer', now()) RETURNING id"
            ).fetchone()
        assert row is not None
        new_id = row[0]
        got = list(listener.notifies(timeout=10, stop_after=1))
    assert [(n.channel, n.payload) for n in got] == [
        (channel, f'{{"op" : "insert", "row" : {{"id" : {new_id}}}}}')
    ]
