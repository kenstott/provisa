# Copyright (c) 2026 Kenneth Stott
# Canary: f6a7b8c9-d0e1-2345-f012-567890123456
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 1 pgwire server unit tests.

Tests the ProvisaQueryResult adapter and the ProvisaSession catalog path
without starting a real TCP server or touching the full app stack.
"""

from __future__ import annotations

import datetime
import decimal

import pytest

from provisa.executor.result import QueryResult as EngineResult
from provisa.pgwire.server import ProvisaQueryResult, _infer_bvtype, _tag_from_sql
from buenavista.core import BVType


class TestTagFromSql:
    def test_set(self):
        assert _tag_from_sql("SET search_path TO public") == "SET"

    def test_begin(self):
        assert _tag_from_sql("BEGIN") == "BEGIN"

    def test_commit(self):
        assert _tag_from_sql("COMMIT") == "COMMIT"

    def test_rollback(self):
        assert _tag_from_sql("ROLLBACK") == "ROLLBACK"

    def test_start_transaction(self):
        assert _tag_from_sql("START TRANSACTION") == "START"

    def test_select_returns_empty(self):
        assert _tag_from_sql("SELECT 1") == ""

    def test_empty_returns_empty(self):
        assert _tag_from_sql("") == ""


class TestInferBvtype:
    def test_int(self):
        rows = [(42,)]
        assert _infer_bvtype(rows, 0) == BVType.BIGINT

    def test_bool_before_int(self):
        rows = [(True,)]
        assert _infer_bvtype(rows, 0) == BVType.BOOL

    def test_float(self):
        rows = [(3.14,)]
        assert _infer_bvtype(rows, 0) == BVType.FLOAT

    def test_decimal(self):
        rows = [(decimal.Decimal("1.5"),)]
        assert _infer_bvtype(rows, 0) == BVType.DECIMAL

    def test_datetime(self):
        rows = [(datetime.datetime(2024, 1, 1),)]
        assert _infer_bvtype(rows, 0) == BVType.TIMESTAMP

    def test_date(self):
        rows = [(datetime.date(2024, 1, 1),)]
        assert _infer_bvtype(rows, 0) == BVType.DATE

    def test_time(self):
        rows = [(datetime.time(12, 0),)]
        assert _infer_bvtype(rows, 0) == BVType.TIME

    def test_dict_json(self):
        rows = [({"key": "val"},)]
        assert _infer_bvtype(rows, 0) == BVType.JSON

    def test_list_integer_array(self):
        rows = [([1, 2, 3],)]
        assert _infer_bvtype(rows, 0) == BVType.INTEGERARRAY

    def test_list_string_array(self):
        rows = [(["a", "b"],)]
        assert _infer_bvtype(rows, 0) == BVType.STRINGARRAY

    def test_list_empty_json(self):
        rows = [([],)]
        assert _infer_bvtype(rows, 0) == BVType.JSON

    def test_str(self):
        rows = [("hello",)]
        assert _infer_bvtype(rows, 0) == BVType.TEXT

    def test_none_falls_through_to_text(self):
        rows = [(None,)]
        assert _infer_bvtype(rows, 0) == BVType.TEXT

    def test_none_then_int(self):
        rows = [(None,), (5,)]
        assert _infer_bvtype(rows, 0) == BVType.BIGINT


class TestProvisaQueryResult:
    def _make(self, rows, cols, sql="SELECT 1"):
        tr = EngineResult(rows=rows, column_names=cols)
        return ProvisaQueryResult(tr, sql)

    def test_has_results_with_columns(self):
        qr = self._make([], ["id", "name"])
        assert qr.has_results() is True

    def test_has_results_false_when_no_columns(self):
        qr = self._make([], [], sql="SET x=1")
        assert qr.has_results() is False

    def test_column_count(self):
        qr = self._make([], ["a", "b", "c"])
        assert qr.column_count() == 3

    def test_column_name(self):
        qr = self._make([(1,)], ["id"])
        name, _ = qr.column(0)
        assert name == "id"

    def test_column_type_inferred(self):
        qr = self._make([(1,)], ["id"])
        _, bvtype = qr.column(0)
        assert bvtype == BVType.BIGINT

    def test_rows_iterator(self):
        qr = self._make([(1, "a"), (2, "b")], ["id", "name"])
        rows = list(qr.rows())
        assert rows == [(1, "a"), (2, "b")]

    def test_status_set(self):
        qr = self._make([], [], sql="SET x=1")
        assert qr.status() == "SET"

    def test_status_begin(self):
        qr = self._make([], [], sql="BEGIN")
        assert qr.status() == "BEGIN"

    def test_status_ok_fallback(self):
        qr = self._make([], [], sql="SELECT 1")
        assert qr.status() == "OK"


class TestProvisaSessionCatalog:
    """Test that ProvisaSession routes catalog queries to catalog.answer."""

    def _make_session(self, role_id="testrole"):
        from provisa.pgwire.server import ProvisaSession

        sess = ProvisaSession()
        sess.role_id = role_id
        return sess

    def _patch_state(self, monkeypatch):
        from unittest.mock import MagicMock

        state = MagicMock()
        ctx = MagicMock()
        ctx.tables = {}
        state.contexts = {"testrole": ctx}
        state.schema_build_cache = {"column_types": {}}
        monkeypatch.setattr("provisa.api.app.state", state)
        return state

    def test_set_returns_no_columns(self, monkeypatch):
        self._patch_state(monkeypatch)
        sess = self._make_session()
        result = sess.execute_sql("SET search_path TO public")
        assert result.has_results() is False

    def test_show_server_version(self, monkeypatch):
        self._patch_state(monkeypatch)
        sess = self._make_session()
        result = sess.execute_sql("SHOW server_version")
        assert result.has_results() is True
        rows = list(result.rows())
        assert len(rows) == 1
        assert "provisa" in rows[0][0]

    def test_pg_namespace(self, monkeypatch):
        self._patch_state(monkeypatch)
        sess = self._make_session()
        result = sess.execute_sql("SELECT nspname FROM pg_catalog.pg_namespace")
        assert result.has_results() is True
        ns_names = [r[0] for r in result.rows()]
        assert "public" in ns_names

    def test_no_role_raises(self, monkeypatch):
        self._patch_state(monkeypatch)
        from provisa.pgwire.server import ProvisaSession

        sess = ProvisaSession()
        with pytest.raises(RuntimeError, match="Not authenticated"):
            sess.execute_sql("SELECT * FROM dogs")
