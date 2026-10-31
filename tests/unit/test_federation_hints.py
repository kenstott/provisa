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

from unittest.mock import MagicMock, patch

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

    def test_unknown_key_passthrough(self):
        # raw Trino keys still work (transitional)
        out = translate_federation_hints({"query_max_memory": "8GB"})
        assert out == {"query_max_memory": "8GB"}

    def test_unknown_join_value_dropped(self):
        # an invalid friendly value does not emit a bogus session prop
        assert translate_federation_hints({"join": "nonsense"}) == {}


# ---------------------------------------------------------------------------
# REQ-281: a federation hint is sent to Trino as a session property only if it is one
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
        if sql == "SHOW SESSION":
            self._rows = [[name, "", "", "varchar", ""] for name in sorted(self._defined)]
        elif sql.startswith("SET SESSION"):
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
        from provisa.federation.backend import TrinoBackend

        self.issued: list[str] = []
        self.source_federation_hints = source_federation_hints
        self.source_types = {sid: "mongodb" for sid in source_federation_hints}
        self.engine_conn = self
        self.engine_conn_kwargs: dict = {}
        self.engine_session_hints: dict = {}
        self._backend = TrinoBackend(MagicMock())

    def cursor(self) -> _TrinoCursor:
        return _TrinoCursor(self.DEFINED, self.issued)

    @property
    def federation_engine(self) -> "_Trino":
        return self

    async def session_properties(self) -> frozenset[str] | None:
        return await self._backend.session_properties(self)

    async def run(self, *sources: str) -> list[str]:
        """Build the plan's hints for a statement over ``sources``, then execute it."""
        from provisa.executor.trino import execute_trino
        from provisa.pgwire._pipeline import _defined_session_props, _engine_session_hints

        with patch("provisa.executor.trino._alive", return_value=True):
            hints = _engine_session_hints(
                self,
                sources,
                None,
                "SELECT 1",
                writes=False,
                defined=await _defined_session_props(self, sources),
            )
            self.issued.clear()
            execute_trino(self, "SELECT 1", session_hints=hints)
        return [sql for sql in self.issued if sql.startswith("SET SESSION")]


class TestSessionPropertiesOnly:
    async def test_a_connector_setting_does_not_fail_the_statement(self):
        # Bench P2: bench-mongodb's direct_connection, HTTP 500 INVALID_SESSION_PROPERTY.
        sent = await _Trino({"bench-mongodb": {"direct_connection": "true"}}).run("bench-mongodb")
        assert not [sql for sql in sent if "direct_connection" in sql]

    async def test_a_raw_session_property_on_a_source_is_still_sent(self):
        engine = _Trino({"s": {"query_max_memory": "8GB", "direct_connection": "true"}})
        assert "SET SESSION query_max_memory = '8GB'" in await engine.run("s")

    async def test_the_provisa_vocabulary_is_still_sent(self):
        sent = await _Trino({"s": {"join": "broadcast"}}).run("s")
        assert "SET SESSION join_distribution_type = 'BROADCAST'" in sent

    async def test_a_setting_holding_a_secret_never_reaches_a_statement(self):
        engine = _Trino({"s": {"client_secret": "hunter2"}})
        await engine.run("s")
        assert not [sql for sql in engine.issued if "hunter2" in sql]

    async def test_the_vocabulary_alone_costs_no_round_trip(self):
        from provisa.pgwire._pipeline import _defined_session_props

        engine = _Trino({"s": {"join": "broadcast", "broadcast_size": "1GB"}})
        assert await _defined_session_props(engine, ("s",)) is None
        assert engine.issued == []


async def test_an_engine_with_no_session_properties_is_not_asked_to_list_them():
    from provisa.federation.backend import EngineBackend

    assert await EngineBackend(MagicMock()).session_properties(MagicMock()) is None
