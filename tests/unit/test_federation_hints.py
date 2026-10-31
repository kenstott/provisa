# Copyright (c) 2026 Kenneth Stott
# Canary: e3a7f2b1-5c9d-4e8a-b6f0-1d2c3a4b5e6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for federation hint extraction (Phase AL)."""

from __future__ import annotations

import logging
from unittest.mock import patch

from provisa.compiler.hints import extract_hints


class TestExtractHints:
    def test_no_hints(self):
        sql = "SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert cleaned == sql
        assert props == {}

    def test_broadcast_hint(self):
        sql = "/*+ BROADCAST(orders) */ SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert "BROADCAST" not in cleaned
        assert props["join_distribution_type"] == "BROADCAST"

    def test_no_reorder_hint(self):
        sql = "/*+ NO_REORDER */ SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert "NO_REORDER" not in cleaned
        assert props["join_reordering_strategy"] == "NONE"

    def test_broadcast_size_hint(self):
        sql = "/*+ BROADCAST_SIZE(orders, 512MB) */ SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert props["join_max_broadcast_table_size"] == "512MB"

    def test_multiple_hints(self):
        sql = "/*+ BROADCAST(t1) NO_REORDER */ SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
        cleaned, props = extract_hints(sql)
        assert props["join_distribution_type"] == "BROADCAST"
        assert props["join_reordering_strategy"] == "NONE"
        assert "/*+" not in cleaned

    def test_hint_removed_from_sql(self):
        sql = "/*+ BROADCAST(orders) */ SELECT id, amount FROM orders WHERE amount > 100"
        cleaned, _ = extract_hints(sql)
        assert cleaned == "SELECT id, amount FROM orders WHERE amount > 100"

    def test_broadcast_size_without_table_arg_ignored(self):
        """BROADCAST_SIZE requires at least 2 args; with 1 arg it should not set a prop."""
        sql = "/*+ BROADCAST_SIZE(orders) */ SELECT id FROM orders"
        _, props = extract_hints(sql)
        assert "join_max_broadcast_table_size" not in props

    def test_unknown_hint_ignored(self):
        sql = "/*+ UNKNOWN_HINT(foo) */ SELECT id FROM orders"
        cleaned, props = extract_hints(sql)
        assert props == {}
        assert "UNKNOWN_HINT" not in cleaned

    def test_hint_case_insensitive(self):
        sql = "/*+ broadcast(orders) no_reorder */ SELECT id FROM orders"
        _, props = extract_hints(sql)
        assert props["join_distribution_type"] == "BROADCAST"
        assert props["join_reordering_strategy"] == "NONE"

    def test_sql_without_hint_unchanged_content(self):
        sql = "SELECT /* regular comment */ id FROM orders"
        cleaned, props = extract_hints(sql)
        assert "regular comment" in cleaned
        assert props == {}


class TestExecuteTrinoSessionHints:
    """Verify that session_hints are injected as SET SESSION before the main query."""

    def test_session_hints_trigger_set_session(self):
        from unittest.mock import MagicMock
        from provisa.executor.trino import execute_trino

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id", "bigint", None, None, None, None, None)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        execute_trino(
            mock_conn,
            "SELECT id FROM orders",
            session_hints={"join_distribution_type": "BROADCAST"},
        )

        # cursor().execute should have been called for SET SESSION + main query
        calls = [c.args[0] for c in mock_cursor.execute.call_args_list]
        set_calls = [c for c in calls if c.startswith("SET SESSION")]
        assert len(set_calls) >= 1
        assert any("join_distribution_type" in c and "BROADCAST" in c for c in set_calls)

    def test_no_session_hints_no_set_session(self):
        from unittest.mock import MagicMock
        from provisa.executor.trino import execute_trino

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id", "bigint", None, None, None, None, None)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        execute_trino(mock_conn, "SELECT id FROM orders")

        calls = [c.args[0] for c in mock_cursor.execute.call_args_list]
        set_calls = [c for c in calls if c.startswith("SET SESSION")]
        # Only the default timeout hint is injected; no user-supplied hints
        assert all("query_max_execution_time" in c for c in set_calls)


# --- REQ-281: source federation hint translation ---

from provisa.compiler.directives import translate_federation_hints  # noqa: E402


class TestTranslateFederationHints:
    def test_join_broadcast(self):
        out = translate_federation_hints({"join": "broadcast"})
        assert out == {"join_distribution_type": "BROADCAST"}

    def test_join_partitioned_case_insensitive(self):
        out = translate_federation_hints({"join": "Partitioned"})
        assert out == {"join_distribution_type": "PARTITIONED"}

    def test_reorder_none(self):
        assert translate_federation_hints({"reorder": "none"}) == {
            "join_reordering_strategy": "NONE"
        }
        assert translate_federation_hints({"reorder": "false"}) == {
            "join_reordering_strategy": "NONE"
        }

    def test_reorder_auto(self):
        assert translate_federation_hints({"reorder": "auto"}) == {
            "join_reordering_strategy": "AUTOMATIC"
        }

    def test_broadcast_size(self):
        assert translate_federation_hints({"broadcast_size": "1GB"}) == {
            "join_max_broadcast_table_size": "1GB"
        }

    def test_combined(self):
        out = translate_federation_hints(
            {"join": "broadcast", "reorder": "none", "broadcast_size": "2GB"}
        )
        assert out == {
            "join_distribution_type": "BROADCAST",
            "join_reordering_strategy": "NONE",
            "join_max_broadcast_table_size": "2GB",
        }

    def test_unknown_key_is_not_passed_through(self):
        # a raw Trino key stays on the source; only the vocabulary reaches the session
        out = translate_federation_hints({"query_max_memory": "8GB", "join": "broadcast"})
        assert out == {"join_distribution_type": "BROADCAST"}

    def test_unknown_join_value_dropped(self):
        # an invalid friendly value does not emit a bogus session prop
        assert translate_federation_hints({"join": "nonsense"}) == {}


# ---------------------------------------------------------------------------
# REQ-281: only the @provisa vocabulary reaches an engine's session
# ---------------------------------------------------------------------------


class _TrinoCursor:
    """Answers as Trino does: SET SESSION of a name it does not define fails the statement."""

    def __init__(self, defined: frozenset[str], issued: list[str]) -> None:
        self._defined = defined
        self._issued = issued
        self._rows: list = []
        self.description = [("name", "varchar")]

    def cancel(self) -> None:
        pass

    def execute(self, sql: str, params=None) -> None:
        self._issued.append(sql)
        self._rows = [[1]]
        if sql.startswith("SET SESSION"):
            name = sql.split()[2]
            if name not in self._defined:
                raise RuntimeError(
                    "FederationError(type=USER_ERROR, name=INVALID_SESSION_PROPERTY, "
                    f"message=\"Session property '{name}' does not exist\")"
                )

    def fetchall(self) -> list:
        return self._rows


class _Trino:
    """The state a Trino engine's plan is built on, over a coordinator defining ``defined``."""

    DEFINED = frozenset({"join_distribution_type", "query_max_execution_time", "query_max_memory"})

    def __init__(self, source_federation_hints: dict[str, dict[str, str]]) -> None:
        self.issued: list[str] = []
        self.source_federation_hints = source_federation_hints
        self.source_types = {sid: "mongodb" for sid in source_federation_hints}
        self.engine_conn = self
        self.engine_conn_kwargs: dict = {}
        self.engine_session_hints: dict = {}

    def cursor(self) -> _TrinoCursor:
        return _TrinoCursor(self.DEFINED, self.issued)

    def run(self, *sources: str) -> list[str]:
        """Build the plan's hints for a statement over ``sources``, then execute it."""
        from provisa.executor.trino import execute_trino
        from provisa.pgwire._pipeline import _engine_session_hints

        with patch("provisa.executor.trino._alive", return_value=True):
            hints = _engine_session_hints(self, sources, None, "SELECT 1", writes=False)
            self.issued.clear()
            execute_trino(self, "SELECT 1", session_hints=hints)
        return [sql for sql in self.issued if sql.startswith("SET SESSION")]


class TestOnlyTheVocabularyReachesTheSession:
    def test_a_connector_setting_does_not_fail_the_statement(self):
        # Bench P2: bench-mongodb's direct_connection, HTTP 500 INVALID_SESSION_PROPERTY.
        sent = _Trino({"bench-mongodb": {"direct_connection": "true"}}).run("bench-mongodb")
        assert not [sql for sql in sent if "direct_connection" in sql]

    def test_a_raw_session_property_on_a_source_is_not_sent(self):
        engine = _Trino({"s": {"query_max_memory": "8GB", "direct_connection": "true"}})
        assert not [sql for sql in engine.run("s") if "query_max_memory" in sql]

    def test_the_provisa_vocabulary_is_still_sent(self):
        sent = _Trino({"s": {"join": "broadcast"}}).run("s")
        assert "SET SESSION join_distribution_type = 'BROADCAST'" in sent

    def test_a_setting_holding_a_secret_never_reaches_a_statement_or_a_log(self, caplog):
        engine = _Trino({"s": {"client_secret": "hunter2"}})
        with caplog.at_level(logging.DEBUG):
            engine.run("s")
        assert not [sql for sql in engine.issued if "hunter2" in sql]
        assert "hunter2" not in caplog.text

    def test_the_hints_cost_the_engine_no_round_trip(self):
        # Keeping a raw key out takes no list of the engine's properties, so a plan asks for none.
        from provisa.pgwire._pipeline import _engine_session_hints

        engine = _Trino({"s": {"join": "broadcast", "direct_connection": "true"}})
        assert _engine_session_hints(engine, ("s",), None, "SELECT 1", writes=False) == {
            "join_distribution_type": "BROADCAST"
        }
        assert engine.issued == []


def test_a_source_with_only_connector_settings_sends_no_session_hints():
    from provisa.pgwire._pipeline import _engine_session_hints

    engine = _Trino({"s": {"direct_connection": "true"}})
    assert _engine_session_hints(engine, ("s",), None, "SELECT 1", writes=False) is None
