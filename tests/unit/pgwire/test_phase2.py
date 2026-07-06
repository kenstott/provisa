# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 2 pgwire tests: param binding, scalar intercepts, txn fixes, pg_am."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock


from provisa.pgwire.server import _substitute_params
from provisa.pgwire.catalog import answer, classify


# ---------------------------------------------------------------------------
# Shared state helpers (mirrors test_catalog.py)
# ---------------------------------------------------------------------------


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


def _empty_state():
    return _make_state({}, {})


# ---------------------------------------------------------------------------
# TestParamSubstitution
# ---------------------------------------------------------------------------


class TestParamSubstitution:
    def test_no_params_unchanged(self):
        assert _substitute_params("SELECT 1", None) == "SELECT 1"

    def test_single_str_param(self):
        assert _substitute_params("SELECT $1", ["hello"]) == "SELECT 'hello'"

    def test_int_param(self):
        assert _substitute_params("SELECT $1", [42]) == "SELECT 42"

    def test_none_param(self):
        assert _substitute_params("SELECT $1", [None]) == "SELECT NULL"

    def test_bool_param(self):
        assert _substitute_params("SELECT $1", [True]) == "SELECT TRUE"

    def test_multi_params(self):
        assert _substitute_params("WHERE a=$1 AND b=$2", ["x", 5]) == "WHERE a='x' AND b=5"

    def test_large_index_first(self):
        params = ["a"] * 10
        result = _substitute_params("$1 $10", params)
        assert "$10" not in result
        assert "$1" not in result
        assert result == "'a' 'a'"

    def test_sql_injection_escaped(self):
        assert _substitute_params("SELECT $1", ["O'Brien"]) == "SELECT 'O''Brien'"


# ---------------------------------------------------------------------------
# TestCatalogScalars
# ---------------------------------------------------------------------------


class TestCatalogScalars:
    def test_current_user(self):
        result = answer("SELECT current_user", "alice", _empty_state())
        assert result.rows == [("alice",)]

    def test_current_database(self):
        result = answer("SELECT current_database()", "alice", _empty_state())
        assert result.rows == [("provisa",)]

    def test_version(self):
        result = answer("SELECT version()", "alice", _empty_state())
        assert "PostgreSQL" in result.rows[0][0]

    def test_classify_current_user(self):
        assert classify("SELECT current_user") == "INTERCEPT"

    def test_classify_current_database(self):
        assert classify("SELECT current_database()") == "INTERCEPT"


# ---------------------------------------------------------------------------
# TestCatalogTxnFix
# ---------------------------------------------------------------------------


class TestCatalogTxnFix:
    def test_start_transaction_intercepted(self):
        assert classify("START TRANSACTION") == "INTERCEPT"

    def test_savepoint_intercepted(self):
        assert classify("SAVEPOINT sp1") == "INTERCEPT"

    def test_release_intercepted(self):
        assert classify("RELEASE SAVEPOINT sp1") == "INTERCEPT"

    def test_show_transaction_isolation(self):
        result = answer("SHOW TRANSACTION ISOLATION LEVEL", "u", _empty_state())
        assert result.rows == [("read committed",)]


# ---------------------------------------------------------------------------
# TestPgAm
# ---------------------------------------------------------------------------


class TestPgAm:
    def test_pg_am_returns_rows(self):
        result = answer("SELECT * FROM pg_catalog.pg_am", "u", _empty_state())
        amnames = [r[1] for r in result.rows]
        assert "btree" in amnames


# ---------------------------------------------------------------------------
# TestWireParamBinding (integration, no real server)
# ---------------------------------------------------------------------------


class TestWireParamBinding:
    def test_parameterized_select(self, monkeypatch):
        """After substitution $1 is replaced before reaching the pipeline."""
        captured = {}

        async def _mock_pipeline(sql, role_id):
            captured["sql"] = sql
            from provisa.executor.result import QueryResult

            return QueryResult(rows=[(99,)], column_names=["val"])

        monkeypatch.setattr("provisa.pgwire._pipeline.execute_pgwire_sql", _mock_pipeline)

        from provisa.pgwire.server import ProvisaSession
        import asyncio

        loop = asyncio.new_event_loop()
        import provisa.pgwire.server as srv_mod

        with srv_mod._loop_lock:
            srv_mod._loop = loop

        t = __import__("threading").Thread(target=loop.run_forever, daemon=True)
        t.start()

        try:
            sess = ProvisaSession()
            sess.role_id = "testuser"
            _result = sess.execute_sql("SELECT $1::int", [99])
            assert "$1" not in captured["sql"]
            assert "99" in captured["sql"]
        finally:
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=2)
            with srv_mod._loop_lock:
                srv_mod._loop = None
