# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e5a17-6c42-4d09-a1f3-7e92d0c5b684
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The values PostgreSQL 17 shows in the catalog where the served catalog once showed NULL or a
bare number (REQ-2167): a regclass compared with a name, an array type's element, a key
constraint's action codes, a role's masked password, and the connected session in
pg_stat_activity."""

# Requirements: REQ-2167

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from provisa.pgwire.catalog import CatalogSession, answer
from provisa.pgwire.catalog_constraints import (
    _build_pk_constraint_rows,
    _build_unique_constraint_rows,
)

_ROLE = "testrole"


def _state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {_ROLE: ctx}
    state.schema_build_cache = {"column_types": {}}
    return state


def _rows(sql: str, state=None, session=None) -> list[tuple]:
    return [tuple(r) for r in answer(sql, _ROLE, state or _state(), session=session).rows]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'pg_class'::regclass = 'pg_class' AS v",
        "SELECT 'pg_catalog.pg_class'::regclass = 'pg_class' AS v",
        "SELECT 'pg_class' = 'pg_class'::regclass AS v",
        "SELECT 'pg_class'::regclass <> 'pg_type' AS v",
        "SELECT 'pg_catalog.pg_class'::regclass = 'pg_catalog.pg_class' AS v",
    ],
)
def test_a_regclass_compared_with_a_name_compares_the_relations(sql):
    assert _rows(sql) == [(True,)]


def test_a_regclass_differs_from_another_relations_name():
    assert _rows("SELECT 'pg_class'::regclass = 'pg_type' AS v") == [(False,)]


def test_a_regclass_compared_with_a_name_naming_no_relation_is_an_error():
    with pytest.raises(Exception, match='relation "nope" does not exist'):
        _rows("SELECT 'pg_class'::regclass = 'nope' AS v")


def test_a_regtype_compared_with_a_name_compares_the_types():
    assert _rows("SELECT 'int4'::regtype = 'integer' AS v") == [(True,)]


def test_pg_type_array_types_hold_their_element_type():
    rows = dict(
        _rows(
            "SELECT typname, typelem FROM pg_catalog.pg_type "
            "WHERE typname IN ('int4', '_text', 'int2vector', 'text')"
        )
    )
    assert rows == {"int4": 0, "_text": 25, "int2vector": 21, "text": 0}


def test_pg_type_names_every_array_types_element_as_pg_type_does():
    assert _rows(
        "SELECT count(*) FROM pg_catalog.pg_type a "
        "WHERE a.typcategory = 'A' AND a.typelem <> 0 "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type e WHERE e.oid = a.typelem)"
    ) == [(0,)]


class _Idx:
    database = "provisa"
    table_id_to_oid = {1: 100}
    toid_to_table = {100: ("provisa", "public", "users")}
    ns_map = {"public": 2200}
    col_attnum = {(100, "id"): 1, (100, "email"): 2}


def _ctx():
    tm = SimpleNamespace(table_id=1, display_name="users")
    return SimpleNamespace(
        tables={"users": tm},
        pk_columns={1: ["id"]},
        unique_constraints={1: [("users_email_key", ["email"])]},
    )


def test_primary_and_unique_key_rows_have_a_space_for_the_action_codes():
    pk_rows, _ = _build_pk_constraint_rows(_ctx(), _Idx(), 20000)
    uq_rows, _ = _build_unique_constraint_rows(_ctx(), _Idx(), 30000)
    for row in (*pk_rows, *uq_rows):
        assert row[12:15] == (" ", " ", " ")  # confupdtype, confdeltype, confmatchtype
    assert len(pk_rows) == 1 and len(uq_rows) == 1


def test_a_roles_password_is_masked_as_postgresql_masks_it():
    state = _state()
    state.roles = {"analyst": {"id": "analyst"}}
    passwords = _rows("SELECT rolname, rolpassword FROM pg_catalog.pg_roles", state)
    assert passwords
    assert all(password == "********" for _, password in passwords)


def test_pg_stat_activity_lists_the_current_session():
    session = CatalogSession(pid=4242, application_name="psql")
    sql = (
        "SELECT pid, usename, datname, application_name, state, query, backend_type "
        "FROM pg_catalog.pg_stat_activity"
    )
    assert _rows(sql, session=session) == [
        (4242, _ROLE, "provisa", "psql", "active", sql, "client backend")
    ]


def test_pg_stat_activity_joins_the_session_to_its_role_and_database():
    session = CatalogSession(pid=7, application_name="")
    rows = _rows(
        "SELECT a.pid FROM pg_catalog.pg_stat_activity a "
        "JOIN pg_catalog.pg_database d ON d.oid = a.datid WHERE a.pid = 7",
        session=session,
    )
    assert rows == [(7,)]


def test_pg_stat_activity_is_empty_where_no_session_is_known():
    assert _rows("SELECT pid FROM pg_catalog.pg_stat_activity") == []


def test_one_sessions_activity_does_not_reach_the_next_statement():
    _rows("SELECT pid FROM pg_catalog.pg_stat_activity", session=CatalogSession(pid=1))
    assert _rows("SELECT pid FROM pg_catalog.pg_stat_activity") == []


def test_a_session_lists_itself_in_pg_stat_activity(monkeypatch):
    from provisa.pgwire.server import ProvisaSession

    monkeypatch.setattr("provisa.api.app.state", _state())
    session = ProvisaSession()
    session.role_id = _ROLE
    session.application_name = "psql"
    result = session.execute_sql(
        "SELECT pid, usename, application_name, state, backend_start IS NOT NULL "
        "FROM pg_catalog.pg_stat_activity"
    )
    assert [tuple(r) for r in result.rows()] == [
        (session.process_id, _ROLE, "psql", "active", True)
    ]
