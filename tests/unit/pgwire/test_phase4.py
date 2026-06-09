# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-defa-123456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 4 pgwire catalog coverage tests.

Covers untested catalog tables and rewrite functions:
- information_schema.views (empty stub)
- pg_catalog.pg_description (empty stub)
- pg_catalog.pg_index (empty stub)
- pg_catalog.pg_proc (empty stub)
- pg_catalog.pg_auth_members (empty stub)
- pg_catalog.pg_tables (populated from context)
- pg_catalog.pg_stat_user_tables (empty stub)
- pg_catalog.pg_statio_user_tables (same internal table, empty)
- pg_table_is_visible() rewrite → TRUE
- pg_has_role() rewrite → TRUE
- current_setting() function
- answer() for newly-covered tables via wire-rewrite path
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from unittest.mock import MagicMock, patch

import asyncpg
import pytest
import pytest_asyncio

from provisa.pgwire.catalog import _build_catalog_db, answer, classify


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_server(port: int):
    from provisa.pgwire.server import ProvisaConnection, ProvisaServer

    conn = ProvisaConnection()
    server = ProvisaServer(("127.0.0.1", port), conn)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.1)
    return server


def _stub_auth_provider(valid_user: str, valid_password: str):
    provider = MagicMock()

    def _login(username, password):
        if username == valid_user and password == valid_password:
            return username
        raise ValueError("Invalid credentials")

    provider.login.side_effect = _login
    return provider


def _make_col(name, dtype, nullable):
    c = MagicMock()
    c.column_name = name
    c.data_type = dtype
    c.is_nullable = nullable
    return c


def _make_ctx_with_tables():
    from provisa.compiler.sql_gen import TableMeta

    dogs_tm = MagicMock(spec=TableMeta)
    dogs_tm.table_name = "dogs"
    dogs_tm.field_name = "public__dogs"
    dogs_tm.display_name = "dogs"
    dogs_tm.schema_name = "public"
    dogs_tm.catalog_name = "provisa"
    dogs_tm.domain_id = "public"
    dogs_tm.table_id = 1
    dogs_tm.type_name = "Dog"

    ctx = MagicMock()
    ctx.tables = {"dog": dogs_tm}
    ctx.pk_columns = {1: ["id"]}
    ctx.joins = {}
    return ctx


def _make_state(ctx=None):
    state = MagicMock()
    if ctx is None:
        mc = MagicMock()
        mc.tables = {}
        state.contexts = {"alice": mc}
    else:
        state.contexts = {"alice": ctx}
    state.schema_build_cache = {
        "column_types": {
            1: [
                _make_col("id", "integer", False),
                _make_col("name", "varchar", True),
            ]
        }
    }
    state.auth_config = {"provider": "simple"}
    state.auth_middleware_active = True
    return state


# ── information_schema.views ─────────────────────────────────────────────────


def test_is_views_table_exists():
    db = _build_catalog_db("alice", _make_state())
    rows = db.execute("SELECT * FROM _is_views").fetchall()
    db.close()
    assert rows == []


def test_is_views_schema_has_expected_columns():
    db = _build_catalog_db("alice", _make_state())
    cur = db.execute("SELECT * FROM _is_views")
    col_names = [d[0] for d in cur.description]
    db.close()
    assert "table_name" in col_names
    assert "view_definition" in col_names
    assert "is_updatable" in col_names


# ── pg_catalog.pg_description ────────────────────────────────────────────────


def test_pg_description_table_exists():
    db = _build_catalog_db("alice", _make_state())
    rows = db.execute("SELECT * FROM _pg_description").fetchall()
    db.close()
    assert rows == []


def test_pg_description_schema_has_expected_columns():
    db = _build_catalog_db("alice", _make_state())
    cur = db.execute("SELECT * FROM _pg_description")
    col_names = [d[0] for d in cur.description]
    db.close()
    assert "objoid" in col_names
    assert "classoid" in col_names
    assert "objsubid" in col_names
    assert "description" in col_names


# ── pg_catalog.pg_index ──────────────────────────────────────────────────────


def test_pg_index_table_exists():
    db = _build_catalog_db("alice", _make_state())
    rows = db.execute("SELECT * FROM _pg_index").fetchall()
    db.close()
    assert rows == []


def test_pg_index_schema_has_expected_columns():
    db = _build_catalog_db("alice", _make_state())
    cur = db.execute("SELECT * FROM _pg_index")
    col_names = [d[0] for d in cur.description]
    db.close()
    assert "indexrelid" in col_names
    assert "indrelid" in col_names
    assert "indisprimary" in col_names
    assert "indisunique" in col_names


# ── pg_catalog.pg_proc ───────────────────────────────────────────────────────


def test_pg_proc_table_exists():
    db = _build_catalog_db("alice", _make_state())
    rows = db.execute("SELECT * FROM _pg_proc").fetchall()
    db.close()
    assert rows == []


def test_pg_proc_schema_has_expected_columns():
    db = _build_catalog_db("alice", _make_state())
    cur = db.execute("SELECT * FROM _pg_proc")
    col_names = [d[0] for d in cur.description]
    db.close()
    assert "proname" in col_names
    assert "pronamespace" in col_names
    assert "prokind" in col_names
    assert "prorettype" in col_names


# ── pg_catalog.pg_auth_members ───────────────────────────────────────────────


def test_pg_auth_members_table_exists():
    db = _build_catalog_db("alice", _make_state())
    rows = db.execute("SELECT * FROM _pg_auth_members").fetchall()
    db.close()
    assert rows == []


def test_pg_auth_members_schema_has_expected_columns():
    db = _build_catalog_db("alice", _make_state())
    cur = db.execute("SELECT * FROM _pg_auth_members")
    col_names = [d[0] for d in cur.description]
    db.close()
    assert "roleid" in col_names
    assert "member" in col_names
    assert "grantor" in col_names
    assert "admin_option" in col_names


# ── pg_catalog.pg_tables ─────────────────────────────────────────────────────


def test_pg_tables_empty_without_ctx():
    db = _build_catalog_db("alice", _make_state())
    rows = db.execute("SELECT * FROM _pg_tables").fetchall()
    db.close()
    assert rows == []


def test_pg_tables_populated_with_ctx():
    ctx = _make_ctx_with_tables()
    db = _build_catalog_db("alice", _make_state(ctx))
    rows = db.execute("SELECT schemaname, tablename FROM _pg_tables").fetchall()
    db.close()
    table_names = [r[1] for r in rows]
    assert "dogs" in table_names


def test_pg_tables_schema_has_expected_columns():
    db = _build_catalog_db("alice", _make_state())
    cur = db.execute("SELECT * FROM _pg_tables")
    col_names = [d[0] for d in cur.description]
    db.close()
    assert "schemaname" in col_names
    assert "tablename" in col_names
    assert "tableowner" in col_names
    assert "hasindexes" in col_names


# ── pg_catalog.pg_stat_user_tables ──────────────────────────────────────────


def test_pg_stat_user_tables_exists():
    db = _build_catalog_db("alice", _make_state())
    rows = db.execute("SELECT * FROM _pg_stat_user_tables").fetchall()
    db.close()
    assert rows == []


def test_pg_stat_user_tables_schema_has_expected_columns():
    db = _build_catalog_db("alice", _make_state())
    cur = db.execute("SELECT * FROM _pg_stat_user_tables")
    col_names = [d[0] for d in cur.description]
    db.close()
    assert "relid" in col_names
    assert "schemaname" in col_names
    assert "relname" in col_names
    assert "seq_scan" in col_names
    assert "n_live_tup" in col_names


# ── pg_table_is_visible() executes TRUE against real pg_class rows ───────────


def test_pg_table_is_visible_executes_true():
    ctx = _make_ctx_with_tables()
    state = _make_state(ctx)
    result = answer(
        "SELECT pg_table_is_visible(c.oid) FROM pg_catalog.pg_class c",
        "alice",
        state,
    )
    # dogs table → one pg_class row → one result row containing True
    assert len(result.rows) >= 1
    assert all(r[0] is True for r in result.rows)


# ── pg_has_role() executes TRUE against real pg_roles row ───────────────────


def test_pg_has_role_executes_true():
    state = _make_state()
    result = answer(
        "SELECT pg_has_role(r.rolname, 'public', 'USAGE') FROM pg_catalog.pg_roles r",
        "alice",
        state,
    )
    # pg_roles always has the current role → one row containing True
    assert len(result.rows) == 1
    assert result.rows[0][0] is True


# ── pg_get_constraintdef() executes NULL against real constraint rows ────────


def test_pg_get_constraintdef_executes_null():
    ctx = _make_ctx_with_tables()  # dogs table has a PK constraint
    state = _make_state(ctx)
    result = answer(
        "SELECT pg_get_constraintdef(c.oid, true) FROM pg_catalog.pg_constraint c",
        "alice",
        state,
    )
    # At least the PK on dogs → one constraint row, value must be NULL
    assert len(result.rows) >= 1
    assert all(r[0] is None for r in result.rows)


# ── pg_get_indexdef() executes NULL (pg_index is empty) ─────────────────────


def test_pg_get_indexdef_executes_null():
    ctx = _make_ctx_with_tables()
    state = _make_state(ctx)
    result = answer(
        "SELECT pg_get_indexdef(i.indexrelid, 0, false) FROM pg_catalog.pg_index i",
        "alice",
        state,
    )
    # pg_index is empty → no rows, not an error
    assert result.rows == []


# ── current_setting() ───────────────────────────────────────────────────────


def test_classify_current_setting_intercepted():
    assert classify("SELECT current_setting('server_version')") == "INTERCEPT"


def test_answer_current_setting_server_version():
    state = _make_state()
    result = answer("SELECT current_setting('server_version')", "alice", state)
    assert len(result.rows) == 1
    assert "provisa" in str(result.rows[0][0]).lower()


def test_answer_current_setting_timezone():
    state = _make_state()
    result = answer("SELECT current_setting('timezone')", "alice", state)
    assert len(result.rows) == 1
    assert result.rows[0][0] == "UTC"


def test_answer_current_setting_unknown_key_returns_empty_string():
    state = _make_state()
    result = answer("SELECT current_setting('nonexistent_key')", "alice", state)
    assert len(result.rows) == 1
    assert result.rows[0][0] == ""


# ── answer() path for newly-covered tables ──────────────────────────────────


def test_answer_information_schema_views():
    state = _make_state()
    result = answer("SELECT * FROM information_schema.views", "alice", state)
    assert result.rows == []
    assert "table_name" in result.column_names


def test_answer_pg_description():
    state = _make_state()
    result = answer("SELECT * FROM pg_catalog.pg_description", "alice", state)
    assert result.rows == []
    assert "description" in result.column_names


def test_answer_pg_index():
    state = _make_state()
    result = answer("SELECT * FROM pg_catalog.pg_index", "alice", state)
    assert result.rows == []
    assert "indexrelid" in result.column_names


def test_answer_pg_proc():
    state = _make_state()
    result = answer("SELECT * FROM pg_catalog.pg_proc", "alice", state)
    assert result.rows == []
    assert "proname" in result.column_names


def test_answer_pg_auth_members():
    state = _make_state()
    result = answer("SELECT * FROM pg_catalog.pg_auth_members", "alice", state)
    assert result.rows == []
    assert "roleid" in result.column_names


def test_answer_pg_tables_populated():
    ctx = _make_ctx_with_tables()
    state = _make_state(ctx)
    result = answer("SELECT tablename FROM pg_catalog.pg_tables", "alice", state)
    table_names = [r[0] for r in result.rows]
    assert "dogs" in table_names


def test_answer_pg_stat_user_tables():
    state = _make_state()
    result = answer("SELECT * FROM pg_catalog.pg_stat_user_tables", "alice", state)
    assert result.rows == []


def test_answer_pg_statio_user_tables():
    state = _make_state()
    result = answer("SELECT * FROM pg_catalog.pg_statio_user_tables", "alice", state)
    assert result.rows == []


# ── wire-level: new tables queryable via asyncpg ────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def pgwire_server_p4():
    import provisa.pgwire.server as _srv

    loop = asyncio.get_running_loop()
    with _srv._loop_lock:
        _srv._loop = loop
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()
    with _srv._loop_lock:
        _srv._loop = None


@pytest.fixture(scope="module")
def mock_state_p4():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.contexts = {"alice": ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": "simple"}
    state.auth_middleware_active = True
    return state


@pytest.fixture(scope="module")
def mock_state_p4_with_tables():
    ctx = _make_ctx_with_tables()
    state = MagicMock()
    state.contexts = {"alice": ctx}
    state.schema_build_cache = {
        "column_types": {
            1: [
                _make_col("id", "integer", False),
                _make_col("name", "varchar", True),
            ]
        }
    }
    state.auth_config = {"provider": "simple"}
    state.auth_middleware_active = True
    return state


@pytest.mark.asyncio
async def test_wire_pg_description_queryable(pgwire_server_p4, mock_state_p4):
    port = pgwire_server_p4
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p4),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        rows = await conn.fetch("SELECT * FROM pg_catalog.pg_description")
        await conn.close()
    assert rows == []


@pytest.mark.asyncio
async def test_wire_pg_index_queryable(pgwire_server_p4, mock_state_p4):
    port = pgwire_server_p4
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p4),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        rows = await conn.fetch("SELECT * FROM pg_catalog.pg_index")
        await conn.close()
    assert rows == []


@pytest.mark.asyncio
async def test_wire_pg_proc_queryable(pgwire_server_p4, mock_state_p4):
    port = pgwire_server_p4
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p4),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        rows = await conn.fetch("SELECT * FROM pg_catalog.pg_proc")
        await conn.close()
    assert rows == []


@pytest.mark.asyncio
async def test_wire_information_schema_views_queryable(pgwire_server_p4, mock_state_p4):
    port = pgwire_server_p4
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p4),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        rows = await conn.fetch("SELECT * FROM information_schema.views")
        await conn.close()
    assert rows == []


@pytest.mark.asyncio
async def test_wire_pg_tables_empty_without_context(pgwire_server_p4, mock_state_p4):
    port = pgwire_server_p4
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p4),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        rows = await conn.fetch("SELECT * FROM pg_catalog.pg_tables")
        await conn.close()
    assert rows == []


@pytest.mark.asyncio
async def test_wire_pg_tables_contains_table_from_context(
    pgwire_server_p4, mock_state_p4_with_tables
):
    port = pgwire_server_p4
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p4_with_tables),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        rows = await conn.fetch("SELECT tablename FROM pg_catalog.pg_tables")
        await conn.close()
    table_names = [r["tablename"] for r in rows]
    assert "dogs" in table_names


@pytest.mark.asyncio
async def test_wire_pg_stat_user_tables_queryable(pgwire_server_p4, mock_state_p4):
    port = pgwire_server_p4
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p4),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        rows = await conn.fetch("SELECT * FROM pg_catalog.pg_stat_user_tables")
        await conn.close()
    assert rows == []
