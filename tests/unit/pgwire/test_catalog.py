# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-ef01-456789012345
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for pgwire catalog proxy (Phase 1)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock


from provisa.pgwire.catalog import answer, classify


@dataclass
class _TableMeta:
    table_id: int
    field_name: str
    catalog_name: str
    schema_name: str
    table_name: str
    domain_id: str = ""
    source_id: str = ""
    type_name: str = ""
    source_type: str = ""
    original_table_name: str = ""
    display_name: str = ""
    column_presets: dict = field(default_factory=dict)


@dataclass
class _ColMeta:
    column_name: str
    data_type: str
    is_nullable: bool


def _make_state(tables: dict, col_types: dict) -> Any:
    ctx = MagicMock()
    ctx.tables = tables
    state = MagicMock()
    state.contexts = {"testrole": ctx}
    state.schema_build_cache = {"column_types": col_types}
    return state


class TestClassify:
    def test_set_intercepted(self):
        assert classify("SET search_path TO public") == "INTERCEPT"

    def test_set_case_insensitive(self):
        assert classify("set client_encoding = 'UTF8'") == "INTERCEPT"

    def test_show_intercepted(self):
        assert classify("SHOW server_version") == "INTERCEPT"

    def test_begin_intercepted(self):
        assert classify("BEGIN") == "INTERCEPT"

    def test_commit_intercepted(self):
        assert classify("COMMIT") == "INTERCEPT"

    def test_rollback_intercepted(self):
        assert classify("ROLLBACK") == "INTERCEPT"

    def test_information_schema_intercepted(self):
        assert classify("SELECT * FROM information_schema.tables") == "INTERCEPT"

    def test_pg_catalog_intercepted(self):
        assert classify("SELECT * FROM pg_catalog.pg_namespace") == "INTERCEPT"

    def test_plain_select_passthrough(self):
        assert classify("SELECT 1") == "PASS_THROUGH"

    def test_user_table_passthrough(self):
        assert classify("SELECT id FROM dogs") == "PASS_THROUGH"

    def test_current_setting_intercepted(self):
        assert classify("SELECT current_setting('server_version')") == "INTERCEPT"


class TestAnswerSetTxn:
    def _empty_state(self):
        return _make_state({}, {})

    def test_set_returns_empty_result(self):
        result = answer("SET search_path TO public", "testrole", self._empty_state())
        assert result.rows == []
        assert result.column_names == []

    def test_begin_returns_empty_result(self):
        result = answer("BEGIN", "testrole", self._empty_state())
        assert result.rows == []
        assert result.column_names == []

    def test_commit_returns_empty_result(self):
        result = answer("COMMIT", "testrole", self._empty_state())
        assert result.rows == []

    def test_rollback_returns_empty_result(self):
        result = answer("ROLLBACK", "testrole", self._empty_state())
        assert result.rows == []


class TestAnswerShow:
    def _empty_state(self):
        return _make_state({}, {})

    def test_show_server_version(self):
        result = answer("SHOW server_version", "testrole", self._empty_state())
        assert result.column_names == ["server_version"]
        assert len(result.rows) == 1
        assert "provisa" in result.rows[0][0]

    def test_show_all(self):
        result = answer("SHOW ALL", "testrole", self._empty_state())
        assert result.column_names == ["name", "setting"]
        assert len(result.rows) > 0


class TestAnswerInformationSchema:
    def _state_with_table(self):
        tm = _TableMeta(
            table_id=1,
            field_name="dogs",
            catalog_name="provisa",
            schema_name="public",
            table_name="dogs",
        )
        col_types = {
            1: [
                _ColMeta("id", "integer", False),
                _ColMeta("name", "varchar", True),
                _ColMeta("breed", "varchar", True),
            ]
        }
        return _make_state({"dogs": tm}, col_types)

    def test_is_tables_returns_row(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM information_schema.tables", "testrole", state)
        names = [r[2] for r in result.rows]
        assert "dogs" in names

    def test_is_columns_returns_columns(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM information_schema.columns", "testrole", state)
        col_names = [r[3] for r in result.rows]
        assert "id" in col_names
        assert "name" in col_names

    def test_is_schemata_returns_schema(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM information_schema.schemata", "testrole", state)
        schemas = [r[1] for r in result.rows]
        assert "public" in schemas

    def test_is_columns_where_table(self):
        state = self._state_with_table()
        result = answer(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'dogs'",
            "testrole",
            state,
        )
        col_names = [r[0] for r in result.rows]
        assert "id" in col_names


class TestAnswerPgCatalog:
    def _state_with_table(self):
        tm = _TableMeta(
            table_id=2,
            field_name="cats",
            catalog_name="provisa",
            schema_name="public",
            table_name="cats",
        )
        col_types = {2: [_ColMeta("id", "bigint", False)]}
        return _make_state({"cats": tm}, col_types)

    def test_pg_namespace(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_namespace", "testrole", state)
        ns_names = [r[1] for r in result.rows]
        assert "pg_catalog" in ns_names
        assert "public" in ns_names

    def test_pg_class(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_class", "testrole", state)
        rel_names = [r[1] for r in result.rows]
        assert "cats" in rel_names

    def test_pg_attribute(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_attribute", "testrole", state)
        att_names = [r[1] for r in result.rows]
        assert "id" in att_names

    def test_pg_type(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_type", "testrole", state)
        assert len(result.rows) > 0
        type_names = [r[1] for r in result.rows]
        assert "int4" in type_names

    def test_pg_database(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_database", "testrole", state)
        db_names = [r[1] for r in result.rows]
        assert "provisa" in db_names

    def test_pg_settings(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_settings", "testrole", state)
        assert len(result.rows) > 0

    def test_pg_roles_contains_role(self):
        state = self._state_with_table()
        result = answer("SELECT * FROM pg_catalog.pg_roles", "testrole", state)
        rolnames = [r[1] for r in result.rows]
        assert "testrole" in rolnames

    def test_join_pg_class_pg_namespace(self):
        state = self._state_with_table()
        result = answer(
            """SELECT c.relname, n.nspname
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public'""",
            "testrole",
            state,
        )
        rel_names = [r[0] for r in result.rows]
        assert "cats" in rel_names

    def test_pg_get_expr_rewritten_to_null(self):
        state = self._state_with_table()
        result = answer(
            "SELECT pg_get_expr(adbin, adrelid) FROM pg_catalog.pg_attrdef",
            "testrole",
            state,
        )
        assert result.rows == [] or all(r[0] is None for r in result.rows)
