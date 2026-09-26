# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9d2c47-6a1b-4e58-9d02-7c4a1e8b3f65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1858: DECLARE CURSOR / FETCH / MOVE / CLOSE over pgwire.

Exercises the SQL parsing, the forward/backward/absolute cursor state machine, and the
handler dispatch end-to-end at the ``_process_query_stmts`` boundary (no real socket).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from provisa.executor.result import QueryResult as EngineResult
from provisa.pgwire.server import (
    ProvisaQueryResult,
    ProvisaSession,
    _CursorNotScrollableError,
    _CursorState,
    _cursor_move,
    _DECLARE_CURSOR_RE,
    _normalize_cursor_name,
    _parse_cursor_nav,
)


class TestDeclareCursorRegex:
    def test_basic(self):
        m = _DECLARE_CURSOR_RE.match("DECLARE c1 CURSOR FOR SELECT * FROM t")
        assert m is not None
        assert m.group("name") == "c1"
        assert m.group("sql") == "SELECT * FROM t"

    def test_quoted_name_and_scroll(self):
        m = _DECLARE_CURSOR_RE.match(
            'DECLARE "MyCur" SCROLL CURSOR FOR SELECT a,b FROM t WHERE a > 1;'
        )
        assert m is not None
        assert m.group("name") == '"MyCur"'
        assert m.group("sql") == "SELECT a,b FROM t WHERE a > 1"
        assert m.group("scroll") == "SCROLL"

    def test_no_scroll_captured(self):
        m = _DECLARE_CURSOR_RE.match("DECLARE c3 NO SCROLL CURSOR FOR SELECT 1")
        assert m is not None
        assert m.group("scroll") == "NO SCROLL"

    def test_scroll_absent_by_default(self):
        # REQ-1858 amendment: no SCROLL/NO SCROLL keyword -> group is None, and the handler
        # treats that as NO SCROLL (PostgreSQL's own default).
        m = _DECLARE_CURSOR_RE.match("DECLARE c1 CURSOR FOR SELECT * FROM t")
        assert m is not None
        assert m.group("scroll") is None

    def test_with_hold(self):
        m = _DECLARE_CURSOR_RE.match("DECLARE c2 CURSOR WITH HOLD FOR SELECT 1")
        assert m is not None
        assert m.group("sql") == "SELECT 1"

    def test_plain_declare_variable_does_not_match(self):
        # PL/pgSQL variable DECLARE, not a cursor — must fall through untouched.
        assert _DECLARE_CURSOR_RE.match("DECLARE x INT") is None


class TestNormalizeCursorName:
    def test_unquoted_folds_lower(self):
        assert _normalize_cursor_name("MyCursor") == "mycursor"

    def test_quoted_keeps_case(self):
        assert _normalize_cursor_name('"MyCursor"') == "MyCursor"


class TestParseCursorNav:
    @pytest.mark.parametrize(
        "stmt,expected",
        [
            ("FETCH c1", ("forward", "c1", 1)),
            ("FETCH 5 FROM c1", ("forward", "c1", 5)),
            ("FETCH FROM c1", ("forward", "c1", 1)),
            ("FETCH ALL FROM c1", ("forward", "c1", None)),
            ("FETCH FORWARD ALL FROM c1", ("forward", "c1", None)),
            ("FETCH BACKWARD 3 IN c1", ("backward", "c1", 3)),
            ("FETCH ABSOLUTE 10 FROM c1", ("absolute", "c1", 10)),
            ("FETCH RELATIVE -3 FROM c1", ("relative", "c1", -3)),
            ("FETCH -3 FROM c1", ("backward", "c1", 3)),
            ("FETCH LAST FROM c1", ("last", "c1", None)),
            ("FETCH FIRST FROM c1", ("first", "c1", None)),
        ],
    )
    def test_fetch(self, stmt, expected):
        assert _parse_cursor_nav(stmt, "FETCH") == expected

    def test_move(self):
        assert _parse_cursor_nav("MOVE 2 IN c1", "MOVE") == ("forward", "c1", 2)

    def test_unsupported_spec_raises(self):
        with pytest.raises(ValueError):
            _parse_cursor_nav("FETCH SIDEWAYS FROM c1", "FETCH")


class TestCursorMove:
    def _cs(self, n=10, scrollable=True):
        # REQ-1858 amendment: PRIOR/BACKWARD/ABSOLUTE/LAST/negative-RELATIVE only work on a
        # SCROLL cursor now, so this suite (which exercises that raw machinery directly)
        # defaults to scrollable=True; the NO-SCROLL-specific behavior gets its own tests below.
        rows = [[i] for i in range(1, n + 1)]
        return _CursorState(
            name="c", query_result=MagicMock(), source=iter(rows), scrollable=scrollable
        )

    def test_forward_then_forward(self):
        cs = self._cs()
        assert _cursor_move(cs, "forward", 3) == [[1], [2], [3]]
        assert _cursor_move(cs, "forward", 2) == [[4], [5]]
        assert cs.pos == 5

    def test_forward_past_end_clips(self):
        cs = self._cs(3)
        assert _cursor_move(cs, "forward", 10) == [[1], [2], [3]]
        assert cs.pos == 3
        assert cs.source_exhausted is True

    def test_backward_after_forward(self):
        cs = self._cs()
        _cursor_move(cs, "forward", 5)
        assert _cursor_move(cs, "backward", 4) == [[5], [4], [3], [2]]
        assert cs.pos == 1

    def test_backward_at_start_returns_empty(self):
        cs = self._cs()
        assert _cursor_move(cs, "backward", 3) == []
        assert cs.pos == 0

    def test_forward_all(self):
        cs = self._cs()
        _cursor_move(cs, "forward", 1)
        rest = _cursor_move(cs, "forward", None)
        assert rest == [[i] for i in range(2, 11)]
        assert cs.pos == 10

    def test_absolute(self):
        cs = self._cs()
        assert _cursor_move(cs, "absolute", 5) == [[5]]
        assert cs.pos == 5

    def test_first_and_last(self):
        cs = self._cs()
        assert _cursor_move(cs, "last", None) == [[10]]
        cs2 = self._cs()
        assert _cursor_move(cs2, "first", None) == [[1]]

    def test_relative_negative(self):
        cs = self._cs()
        _cursor_move(cs, "forward", 5)
        assert _cursor_move(cs, "relative", -2) == [[5], [4]]
        assert cs.pos == 3


class TestNoScrollCursor:
    """REQ-1858 amendment 2026-09-25: a plain (NO SCROLL) cursor must reject backward/absolute
    navigation instead of silently buffering the whole result, and a forward-only scan through it
    must stay memory-bounded rather than accumulating every row ever fetched."""

    def _cs(self, n, scrollable=False):
        rows = [[i] for i in range(1, n + 1)]
        return _CursorState(
            name="c", query_result=MagicMock(), source=iter(rows), scrollable=scrollable
        )

    def test_default_is_not_scrollable(self):
        cs = _CursorState(name="c", query_result=MagicMock(), source=iter([]))
        assert cs.scrollable is False

    @pytest.mark.parametrize(
        "direction,count",
        [
            ("backward", 1),
            ("absolute", 3),
            ("first", None),
            ("last", None),
            ("relative", -1),
        ],
    )
    def test_backward_style_navigation_raises_on_no_scroll(self, direction, count):
        cs = self._cs(10)
        _cursor_move(cs, "forward", 5)  # give it somewhere to go backward from
        with pytest.raises(_CursorNotScrollableError):
            _cursor_move(cs, direction, count)

    def test_forward_and_relative_positive_still_allowed_on_no_scroll(self):
        cs = self._cs(10)
        assert _cursor_move(cs, "forward", 3) == [[1], [2], [3]]
        assert _cursor_move(cs, "relative", 2) == [[4], [5]]

    def test_forward_scan_is_memory_bounded_on_no_scroll(self):
        # The bug this guards against: FETCH FORWARD through a large (e.g. 80M-row) result used
        # to accumulate every row ever pulled in cs.buffer, forever. A NO SCROLL cursor must trim
        # consumed rows as it advances instead.
        total_rows = 200_000
        batch = 1_000
        cs = self._cs(total_rows)

        fetched_count = 0
        max_buffer_len = 0
        while True:
            out = _cursor_move(cs, "forward", batch)
            max_buffer_len = max(max_buffer_len, len(cs.buffer))
            fetched_count += len(out)
            if not out or cs.source_exhausted and len(out) < batch:
                break

        assert fetched_count == total_rows
        # Buffer must never hold more than roughly one batch's worth of rows — nowhere close to
        # the full 200k-row result.
        assert max_buffer_len <= batch
        assert len(cs.buffer) == 0
        assert cs.pos == 0

    def test_scroll_cursor_keeps_full_buffer(self):
        # Contrast case: an explicitly-SCROLL cursor is allowed to retain everything, since
        # backward/absolute navigation genuinely needs it.
        cs = self._cs(50, scrollable=True)
        for _ in range(5):
            _cursor_move(cs, "forward", 10)
        assert len(cs.buffer) == 50


class TestHandlerDispatch:
    """DECLARE -> FETCH -> FETCH -> CLOSE through ProvisaHandler's SQL-text dispatch."""

    def _handler(self):
        from provisa.pgwire.server import ProvisaHandler

        handler = object.__new__(ProvisaHandler)
        handler.wfile = MagicMock()
        handler.wfile.write = MagicMock()
        handler.wfile.flush = MagicMock()
        handler.send_command_complete = MagicMock()
        handler.send_row_description = MagicMock()
        handler.send_data_rows = MagicMock()
        handler._send_pg_error = MagicMock()
        return handler

    def _ctx(self, rows, cols):
        ctx = MagicMock()
        ctx.session = ProvisaSession()
        ctx.mark_error = MagicMock()

        def execute_sql(sql, params=None):
            engine_result = EngineResult(rows=rows, column_names=cols)
            return ProvisaQueryResult(engine_result, sql)

        ctx.execute_sql.side_effect = execute_sql
        return ctx

    def test_declare_then_fetch_then_close(self):
        handler = self._handler()
        rows = [[1, "a"], [2, "b"], [3, "c"]]
        ctx = self._ctx(rows, ["id", "name"])

        handler._handle_declare_cursor(ctx, "DECLARE c1 CURSOR FOR SELECT id, name FROM t")
        assert "c1" in ctx.session.cursors
        handler.send_command_complete.assert_called_with("DECLARE CURSOR\x00")

        handler._handle_fetch_move(ctx, "FETCH 2 FROM c1")
        handler.send_row_description.assert_called_once()
        fetched_result = handler.send_data_rows.call_args[0][0]
        assert list(fetched_result.rows()) == [[1, "a"], [2, "b"]]
        handler.send_command_complete.assert_called_with("FETCH 2\x00")

        handler._handle_fetch_move(ctx, "FETCH 2 FROM c1")
        fetched_result = handler.send_data_rows.call_args[0][0]
        assert list(fetched_result.rows()) == [[3, "c"]]
        handler.send_command_complete.assert_called_with("FETCH 1\x00")

        handler._handle_close_cursor(ctx, "CLOSE c1")
        assert "c1" not in ctx.session.cursors
        handler.send_command_complete.assert_called_with("CLOSE CURSOR\x00")

    def test_fetch_unknown_cursor_sends_invalid_cursor_name(self):
        handler = self._handler()
        ctx = self._ctx([], [])

        handler._handle_fetch_move(ctx, "FETCH 1 FROM nosuch")
        handler._send_pg_error.assert_called_once()
        args = handler._send_pg_error.call_args[0]
        assert args[0] == "ERROR"
        assert args[1] == "34000"
        ctx.mark_error.assert_called_once()

    def test_move_reports_row_count_without_sending_rows(self):
        handler = self._handler()
        rows = [[i] for i in range(1, 6)]
        ctx = self._ctx(rows, ["n"])

        handler._handle_declare_cursor(ctx, "DECLARE c1 CURSOR FOR SELECT n FROM t")
        handler._handle_fetch_move(ctx, "MOVE 3 FROM c1")

        handler.send_row_description.assert_not_called()
        handler.send_data_rows.assert_not_called()
        handler.send_command_complete.assert_called_with("MOVE 3\x00")

        handler._handle_fetch_move(ctx, "FETCH 1 FROM c1")
        fetched_result = handler.send_data_rows.call_args[0][0]
        assert list(fetched_result.rows()) == [[4]]

    def test_close_all(self):
        handler = self._handler()
        ctx = self._ctx([[1]], ["n"])
        handler._handle_declare_cursor(ctx, "DECLARE c1 CURSOR FOR SELECT n FROM t")
        handler._handle_declare_cursor(ctx, "DECLARE c2 CURSOR FOR SELECT n FROM t")
        assert set(ctx.session.cursors) == {"c1", "c2"}

        handler._handle_close_cursor(ctx, "CLOSE ALL")
        assert ctx.session.cursors == {}

    def test_redeclare_replaces_existing_cursor(self):
        handler = self._handler()
        ctx = self._ctx([[1], [2]], ["n"])
        handler._handle_declare_cursor(ctx, "DECLARE c1 CURSOR FOR SELECT n FROM t")
        first_state = ctx.session.cursors["c1"]

        handler._handle_declare_cursor(ctx, "DECLARE c1 CURSOR FOR SELECT n FROM t")
        assert ctx.session.cursors["c1"] is not first_state

    def test_session_close_releases_open_cursors(self):
        handler = self._handler()
        ctx = self._ctx([[1]], ["n"])
        handler._handle_declare_cursor(ctx, "DECLARE c1 CURSOR FOR SELECT n FROM t")
        cs = ctx.session.cursors["c1"]
        closer = MagicMock()
        cs.query_result.close = closer

        ctx.session.close()

        closer.assert_called_once()
        assert ctx.session.cursors == {}

    def test_declare_defaults_no_scroll_and_rejects_fetch_backward(self):
        handler = self._handler()
        rows = [[i] for i in range(1, 6)]
        ctx = self._ctx(rows, ["n"])

        handler._handle_declare_cursor(ctx, "DECLARE c1 CURSOR FOR SELECT n FROM t")
        assert ctx.session.cursors["c1"].scrollable is False
        handler._handle_fetch_move(ctx, "FETCH 2 FROM c1")

        handler._handle_fetch_move(ctx, "FETCH BACKWARD 1 FROM c1")
        handler._send_pg_error.assert_called_once()
        args = handler._send_pg_error.call_args[0]
        assert args[0] == "ERROR"
        assert args[1] == "55000"
        ctx.mark_error.assert_called_once()

    def test_declare_scroll_allows_fetch_backward(self):
        handler = self._handler()
        rows = [[i] for i in range(1, 6)]
        ctx = self._ctx(rows, ["n"])

        handler._handle_declare_cursor(ctx, "DECLARE c1 SCROLL CURSOR FOR SELECT n FROM t")
        assert ctx.session.cursors["c1"].scrollable is True
        handler._handle_fetch_move(ctx, "FETCH 3 FROM c1")

        handler._handle_fetch_move(ctx, "FETCH BACKWARD 1 FROM c1")
        handler._send_pg_error.assert_not_called()
        fetched_result = handler.send_data_rows.call_args[0][0]
        assert list(fetched_result.rows()) == [[3]]
