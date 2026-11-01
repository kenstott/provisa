# Copyright (c) 2026 Kenneth Stott
# Canary: 3277a386-676d-4fe6-ba85-bbec1ad0492a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""has_table_privilege answers from the session role's governed grants, and pg_get_expr renders the
default a role's INSERT grant gives a column (REQ-2167, PGWIRE'S CATALOG FUNCTIONS)."""

# Requirements: REQ-2167

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from provisa.pgwire.catalog import answer, classify

from .test_catalog import _ColMeta, _TableMeta


def _grant(operation: str, presets: list[dict] | None = None, role: str = "analyst") -> dict:
    return {"role": role, "operation": operation, "columns": [], "presets": presets or []}


_PRESETS = [
    {"column": "status", "source": "literal", "value": "it's new"},
    {"column": "owner", "source": "session", "name": "app.user"},
    {"column": "created", "source": "now"},
    {"column": "legs", "source": "literal", "value": "4"},
]


def _state(capabilities: list[str], other_role_grant: bool = False) -> Any:
    dogs = _TableMeta(1, "dogs", "provisa", "public", "dogs")
    cats = _TableMeta(2, "cats", "provisa", "public", "cats")
    col_types = {
        1: [
            _ColMeta("id", "integer", False),
            _ColMeta("status", "varchar", True),
            _ColMeta("owner", "varchar", True),
            _ColMeta("created", "timestamp", True),
            _ColMeta("legs", "integer", True),
        ],
        2: [_ColMeta("id", "integer", False)],
    }
    dog_grants = [_grant("insert", _PRESETS), _grant("update")]
    if other_role_grant:
        dog_grants.append(_grant("delete", role="auditor"))
    raw_tables = [
        {
            "id": 1,
            "columns": [],
            "write_ops": ["insert", "update", "delete"],
            "write_grants": dog_grants,
        },
        # A source that takes writes, a role holding no grant on it.
        {"id": 2, "columns": [], "write_ops": ["insert", "update", "delete"], "write_grants": []},
    ]
    ctx = MagicMock()
    ctx.tables = {"dogs": dogs, "cats": cats}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {"analyst": ctx}
    state.roles = {"analyst": {"id": "analyst", "capabilities": capabilities}}
    state.schema_build_cache = {"column_types": col_types, "tables": raw_tables}
    return state


def _rows(sql: str, state: Any) -> list[tuple]:
    assert classify(sql) == "INTERCEPT", sql
    return answer(sql, "analyst", state).rows


def _dogs_oid(state: Any) -> int:
    ((oid,),) = _rows("SELECT oid FROM pg_class WHERE relname = 'dogs'", state)
    return oid


@pytest.fixture
def writer():
    return _state(["query_development", "write"], other_role_grant=True)


def test_select_is_held_on_every_table_the_role_is_served(writer):
    assert _rows(
        "SELECT has_table_privilege('dogs', 'SELECT'), has_table_privilege('cats', 'select')",
        writer,
    ) == [(True, True)]


def test_a_write_privilege_is_held_where_the_role_has_the_grant(writer):
    assert _rows(
        "SELECT has_table_privilege('dogs', 'INSERT'), has_table_privilege('dogs', 'UPDATE'), "
        "has_table_privilege('dogs', 'DELETE')",
        writer,
    ) == [(True, True, False)]


def test_a_table_with_no_grant_for_the_role_takes_no_write(writer):
    assert _rows(
        "SELECT has_table_privilege('cats', 'INSERT'), has_table_privilege('cats', 'UPDATE'), "
        "has_table_privilege('cats', 'DELETE')",
        writer,
    ) == [(False, False, False)]


def test_a_role_without_the_write_right_holds_no_write_privilege():
    state = _state(["query_development"])
    assert _rows(
        "SELECT has_table_privilege('dogs', 'SELECT'), has_table_privilege('dogs', 'INSERT')",
        state,
    ) == [(True, False)]


def test_a_privilege_list_is_held_when_any_item_is(writer):
    assert _rows(
        "SELECT has_table_privilege('dogs', 'DELETE, UPDATE'), "
        "has_table_privilege('dogs', 'DELETE, TRUNCATE')",
        writer,
    ) == [(True, False)]


def test_with_grant_option_is_never_held(writer):
    assert _rows("SELECT has_table_privilege('dogs', 'SELECT WITH GRANT OPTION')", writer) == [
        (False,)
    ]


def test_a_table_is_named_by_oid_or_by_schema_qualified_name(writer):
    oid = _dogs_oid(writer)
    assert _rows(
        f"SELECT has_table_privilege({oid}, 'UPDATE'), "
        "has_table_privilege('public.dogs', 'UPDATE'), "
        "has_table_privilege('\"dogs\"', 'UPDATE'), has_table_privilege('DOGS', 'UPDATE')",
        writer,
    ) == [(True, True, True, True)]


def test_a_table_that_does_not_exist_answers_null(writer):
    assert _rows("SELECT has_table_privilege('wolves', 'SELECT')", writer) == [(None,)]


def test_the_user_argument_is_the_session_role(writer):
    assert _rows(
        "SELECT has_table_privilege('analyst', 'dogs', 'INSERT'), "
        "has_table_privilege('analyst', 'cats', 'INSERT')",
        writer,
    ) == [(True, False)]


def test_another_roles_grants_are_not_held_by_this_catalog(writer):
    # The auditor's delete grant on dogs is not the session role's, and the session catalog
    # holds nothing to answer for the auditor with.
    assert _rows("SELECT has_table_privilege('auditor', 'dogs', 'DELETE')", writer) == [(None,)]


def test_a_client_filters_the_tables_it_lists_by_privilege(writer):
    # psql and DBeaver list only the tables the user may touch.
    assert _rows(
        "SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relkind = 'r' "
        "AND pg_catalog.has_table_privilege(c.oid, 'INSERT') ORDER BY 1",
        writer,
    ) == [("dogs",)]


def test_pg_get_expr_renders_the_default_an_insert_grant_gives(writer):
    rows = _rows(
        "SELECT a.attname, pg_catalog.pg_get_expr(d.adbin, d.adrelid) "
        "FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_attrdef d "
        "ON a.attrelid = d.adrelid AND a.attnum = d.adnum ORDER BY a.attname",
        writer,
    )
    assert rows == [
        ("created", "now()"),
        ("legs", "4"),
        ("owner", "current_setting('app.user')"),
        ("status", "'it''s new'::character varying"),
    ]


def test_a_column_with_no_default_has_no_attrdef_and_a_null_expression(writer):
    # psql \d: the default column is a left join to pg_attrdef, NULL where the column has none.
    rows = _rows(
        "SELECT a.attname, a.atthasdef, pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) "
        "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d "
        "ON a.attrelid = d.adrelid AND a.attnum = d.adnum "
        "WHERE a.attrelid = (SELECT oid FROM pg_class WHERE relname = 'dogs') "
        "AND a.attnum > 0 ORDER BY a.attnum",
        writer,
    )
    assert rows == [
        ("id", False, None),
        ("status", True, "'it''s new'::character varying"),
        ("owner", True, "current_setting('app.user')"),
        ("created", True, "now()"),
        ("legs", True, "4"),
        ("_name_", False, None),
        ("_domain_", False, None),
    ]


def test_information_schema_shows_the_same_default(writer):
    assert _rows(
        "SELECT column_name, column_default FROM information_schema.columns "
        "WHERE table_name = 'dogs' AND column_default IS NOT NULL ORDER BY column_name",
        writer,
    ) == [
        ("created", "now()"),
        ("legs", "4"),
        ("owner", "current_setting('app.user')"),
        ("status", "'it''s new'::character varying"),
    ]


def test_a_role_without_the_write_right_sees_no_default():
    state = _state(["query_development"])
    assert _rows("SELECT d.adbin FROM pg_catalog.pg_attrdef d", state) == []
