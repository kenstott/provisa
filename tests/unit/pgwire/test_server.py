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
from provisa.executor.result import StreamingQueryResult
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


class TestProvisaQueryResultStreaming:
    """REQ-028: the ENGINE route wraps a lazy StreamingQueryResult and drains it on demand."""

    def _stream(self, batches, cols, column_types=None):
        return StreamingQueryResult(iter(batches), column_names=cols, column_types=column_types)

    def test_rows_flatten_across_batches(self):
        stream = self._stream([[(1, "a"), (2, "b")], [(3, "c")]], ["id", "name"])
        qr = ProvisaQueryResult(stream, "SELECT 1")
        assert list(qr.rows()) == [(1, "a"), (2, "b"), (3, "c")]

    def test_type_inference_buffers_only_first_batch(self):
        # No column_types → ONE batch is buffered to infer types up front, not the whole result.
        stream = self._stream([[(1,)], [(2,)], [(3,)]], ["id"])
        qr = ProvisaQueryResult(stream, "SELECT 1")
        # Head buffered → first batch's row counted; the tail is untouched until rows() runs.
        assert stream.stats.row_count == 1
        assert not stream.stats.done
        _, bvtype = qr.column(0)
        assert bvtype == BVType.BIGINT
        assert list(qr.rows()) == [(1,), (2,), (3,)]
        assert stream.stats.row_count == 3 and stream.stats.done

    def test_declared_types_skip_buffering(self):
        # column_types present with no None → no head buffered; the stream stays fully lazy.
        stream = self._stream([[(1,)], [(2,)]], ["id"], column_types=["BIGINT"])
        qr = ProvisaQueryResult(stream, "SELECT 1")
        assert stream.stats.row_count == 0  # nothing pulled at construction
        _, bvtype = qr.column(0)
        assert bvtype == BVType.BIGINT
        assert list(qr.rows()) == [(1,), (2,)]


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


class TestProvisaSessionEngineStreaming:
    """REQ-028: an ENGINE plan drains the engine's SYNC streaming terminal on the worker thread."""

    def test_engine_route_streams_via_sync_terminal(self, monkeypatch):
        import asyncio
        import threading
        from unittest.mock import MagicMock

        from provisa.pgwire import _pipeline
        from provisa.pgwire._pipeline import _Plan, _mint_stamp
        from provisa.transpiler.router import Route
        import provisa.pgwire.server as srv_mod
        from provisa.pgwire.server import ProvisaSession

        captured = {}

        # A governed ENGINE plan (validly stamped so require_governed_plan passes).
        plan = _Plan(
            route=Route.ENGINE,
            sql="select 1",
            source_id="s",
            dialect="postgres",
            exec_params=[7],
            physical_sql="SELECT 1",
            session_hints={"retry_policy": "NONE"},
            stamp=_mint_stamp(),
        )

        async def _govern(sql, role_id):
            captured["governed"] = sql
            return plan

        monkeypatch.setattr(_pipeline, "govern_pgwire_plan", _govern)

        def _execute_engine_sync(physical_sql, params, *, session_hints=None):
            captured["physical_sql"] = physical_sql
            captured["params"] = params
            captured["session_hints"] = session_hints
            return StreamingQueryResult(
                iter([[(1,)], [(2,)]]), column_names=["n"], column_types=["BIGINT"]
            )

        state = MagicMock()
        state.federation_engine.execute_engine_sync.side_effect = _execute_engine_sync
        monkeypatch.setattr("provisa.api.app.state", state)

        loop = asyncio.new_event_loop()
        with srv_mod._loop_lock:
            srv_mod._loop = loop
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()
        try:
            sess = ProvisaSession()
            sess.role_id = "alice"
            qr = sess.execute_sql("select n from t")
            # session_hints (FTE retry_policy) reach the sync terminal — not silently dropped.
            assert captured["session_hints"] == {"retry_policy": "NONE"}
            assert captured["params"] == [7]
            assert captured["physical_sql"] == "SELECT 1"
            assert list(qr.rows()) == [(1,), (2,)]
        finally:
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=2)
            with srv_mod._loop_lock:
                srv_mod._loop = None

    def test_engine_route_rejects_ungoverned_plan(self, monkeypatch):
        import asyncio
        import threading
        from unittest.mock import MagicMock

        from provisa.pgwire import _pipeline
        from provisa.pgwire._pipeline import _Plan
        from provisa.transpiler.router import Route
        import provisa.pgwire.server as srv_mod
        from provisa.pgwire.server import ProvisaSession

        # No stamp → the single-chokepoint guard must refuse before the engine runs.
        plan = _Plan(
            route=Route.ENGINE,
            sql="select 1",
            source_id="s",
            dialect="postgres",
            physical_sql="SELECT 1",
            stamp=None,
        )

        async def _govern(sql, role_id):
            return plan

        monkeypatch.setattr(_pipeline, "govern_pgwire_plan", _govern)
        state = MagicMock()
        monkeypatch.setattr("provisa.api.app.state", state)

        loop = asyncio.new_event_loop()
        with srv_mod._loop_lock:
            srv_mod._loop = loop
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()
        try:
            sess = ProvisaSession()
            sess.role_id = "alice"
            with pytest.raises(PermissionError):
                sess.execute_sql("select n from t")
            state.federation_engine.execute_engine_sync.assert_not_called()
        finally:
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=2)
            with srv_mod._loop_lock:
                srv_mod._loop = None
