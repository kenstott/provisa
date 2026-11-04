# Copyright (c) 2026 Kenneth Stott
# Canary: 9d41c7a2-5e08-4b3f-8a16-c2f7e09b4d53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The server state the client tools read is answered as Provisa's own (REQ-2167, amended
2026-11-04): pg_stat_activity lists the caller's session alone, pg_stat_database the caller's
database with its count of the caller's queries, pg_stat_user_tables Provisa's row estimates,
pg_tablespace PostgreSQL's two tablespaces, and pg_relation_is_publishable is false. No answer
holds another session's state."""

# Requirements: REQ-2167

from __future__ import annotations

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from provisa.pgwire.catalog import CatalogSession, answer
from provisa.transpiler.metadata_oids import table_oid
from tests.unit.pgwire.result_rows import rows_of

_ROLE = "analyst"


def _table(table_id: int, name: str) -> SimpleNamespace:
    return SimpleNamespace(
        table_id=table_id,
        field_name=name,
        catalog_name="pg",
        schema_name="public",
        table_name=name,
        domain_id="",
        source_id="",
        type_name="",
        source_type="",
        original_table_name="",
        display_name="",
        column_presets={},
    )


def _state(*tables: SimpleNamespace):
    ctx = MagicMock()
    ctx.tables = {t.table_name: t for t in tables}
    ctx.joins = {}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {_ROLE: ctx}
    state.schema_build_cache = {
        "column_types": {
            t.table_id: [
                SimpleNamespace(
                    column_name="id",
                    data_type="integer",
                    is_nullable=False,
                    subtype=None,
                    native_type=None,
                )
            ]
            for t in tables
        }
    }
    return state


def _rows(sql: str, state=None, session=None) -> list[tuple]:
    return [tuple(r) for r in answer(sql, _ROLE, state or _state(), session=session).rows]


def _server_session(state, monkeypatch):
    from provisa.pgwire.server import ProvisaSession

    monkeypatch.setattr("provisa.api.app.state", state)
    session = ProvisaSession()
    session.role_id = _ROLE
    return session


#: A statement of the caller's own, answered by the catalog.
_ANY = "SELECT count(*) FROM pg_catalog.pg_namespace"


def _served(session, sql: str) -> list[tuple]:
    return [tuple(r) for r in rows_of(session.execute_sql(sql))]


# --- pg_stat_activity ---------------------------------------------------------------------------


def test_pg_stat_activity_names_the_callers_address_and_port():
    session = CatalogSession(pid=11, client_end=("10.1.2.3", 54321))
    rows = _rows(
        "SELECT host(client_addr), client_port FROM pg_catalog.pg_stat_activity", session=session
    )
    assert rows == [("10.1.2.3", 54321)]


def test_pg_stat_activity_holds_what_provisa_knows_and_null_for_the_rest():
    started = datetime.datetime(2026, 11, 4, 9, 30, tzinfo=datetime.timezone.utc)
    session = CatalogSession(pid=12, application_name="DBeaver", backend_start=started)
    sql = (
        "SELECT backend_start, query_start IS NOT NULL, state, leader_pid, client_hostname, "
        "wait_event_type, wait_event, backend_xid, query_id FROM pg_catalog.pg_stat_activity"
    )
    assert _rows(sql, session=session) == [
        (started, True, "active", None, None, None, None, None, None)
    ]


def test_another_sessions_activity_never_appears(monkeypatch):
    state = _state()
    first = _server_session(state, monkeypatch)
    second = _server_session(state, monkeypatch)
    first.process_id, second.process_id = 101, 102  # as a server numbers its connections
    _served(first, _ANY)
    listed = _served(second, "SELECT pid FROM pg_catalog.pg_stat_activity")
    assert listed == [(102,)]


# --- pg_stat_database ---------------------------------------------------------------------------


def test_pg_stat_database_has_one_row_for_the_callers_database():
    session = CatalogSession(pid=21, queries=5)
    rows = _rows(
        "SELECT datid, datname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit "
        "FROM pg_catalog.pg_stat_database",
        session=session,
    )
    assert rows == [(16384, "provisa", 1, 5, 0, 0, 0)]


def test_pg_stat_database_lists_no_database_where_no_session_is_known():
    assert _rows("SELECT datname FROM pg_catalog.pg_stat_database") == []


def test_pg_stat_database_joins_to_pg_database():
    rows = _rows(
        "SELECT d.datname FROM pg_catalog.pg_stat_database s "
        "JOIN pg_catalog.pg_database d ON d.oid = s.datid",
        session=CatalogSession(pid=22, queries=1),
    )
    assert rows == [("provisa",)]


def test_xact_commit_counts_the_callers_queries_alone(monkeypatch):
    state = _state()
    caller = _server_session(state, monkeypatch)
    other = _server_session(state, monkeypatch)
    for _ in range(4):
        _served(other, _ANY)
    _served(caller, _ANY)
    _served(caller, _ANY)
    sql = "SELECT xact_commit, xact_rollback FROM pg_catalog.pg_stat_database"
    # The statement reading the count is the caller's third.
    assert _served(caller, sql) == [(3, 0)]
    assert _served(other, sql) == [(5, 0)]


# --- pg_stat_user_tables ------------------------------------------------------------------------


def test_n_live_tup_is_provisas_row_estimate(monkeypatch):
    monkeypatch.setattr(
        "provisa.pgwire.catalog_facts.row_counts",
        lambda role_id, state: {table_oid(1): 1234.0},
    )
    state = _state(_table(1, "pets"), _table(2, "owners"))
    rows = _rows(
        "SELECT relid, schemaname, relname, n_live_tup FROM pg_catalog.pg_stat_user_tables",
        state,
        CatalogSession(pid=31),
    )
    assert rows == [(table_oid(1), "public", "pets", 1234)]


def test_a_table_with_no_estimate_has_no_row(monkeypatch):
    monkeypatch.setattr("provisa.pgwire.catalog_facts.row_counts", lambda role_id, state: {})
    state = _state(_table(1, "pets"))
    assert _rows("SELECT relname FROM pg_catalog.pg_stat_user_tables", state) == []


def test_an_estimate_for_a_table_the_role_does_not_list_has_no_row(monkeypatch):
    monkeypatch.setattr(
        "provisa.pgwire.catalog_facts.row_counts",
        lambda role_id, state: {table_oid(1): 10.0, table_oid(9): 99.0},
    )
    state = _state(_table(1, "pets"))
    rows = _rows("SELECT relname, n_live_tup FROM pg_catalog.pg_stat_user_tables", state)
    assert rows == [("pets", 10)]


# --- functions and pg_tablespace ----------------------------------------------------------------


def test_pg_stat_get_numscans_of_an_index_is_zero():
    assert _rows(
        "SELECT pg_catalog.pg_stat_get_numscans('pg_catalog.pg_class_oid_index'::regclass) AS v"
    ) == [(0,)]


def test_pg_tablespace_lists_pg_default_and_pg_global():
    rows = _rows(
        "SELECT oid, spcname, pg_catalog.pg_tablespace_location(oid) "
        "FROM pg_catalog.pg_tablespace ORDER BY oid"
    )
    assert rows == [(1663, "pg_default", ""), (1664, "pg_global", "")]


@pytest.mark.parametrize("relation", ["'pg_catalog.pg_class'::regclass", None])
def test_pg_relation_is_publishable_is_false(relation):
    state = _state(_table(1, "pets"))
    argument = relation or f"{table_oid(1)}::oid"
    assert _rows(f"SELECT pg_catalog.pg_relation_is_publishable({argument}) AS v", state) == [
        (False,)
    ]


@pytest.mark.parametrize("function", ["pg_relation_size", "pg_total_relation_size"])
def test_relation_sizes_stay_null(function):
    state = _state(_table(1, "pets"))
    assert _rows(f"SELECT pg_catalog.{function}({table_oid(1)}::oid) AS v", state) == [(None,)]
