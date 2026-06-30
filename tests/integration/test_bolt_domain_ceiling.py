# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for REQ-808: Bolt domain-access ceiling (end-to-end session layer).

Uses real BoltSession with real PackStream/framing and real domain_policy logic.
_execute_cypher is mocked so no DB/docker is required for the ceiling/db-routing
assertions; the integration boundary is the session state machine + domain filter
applied before any query reaches execution.

Tests that require a live stack (postgres/trino) are marked with the stack
requirement comment and will fail gracefully when the stack is not up.
"""

from __future__ import annotations

import io
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# Nodes spec: (type_name, domain_id); domain_id=None means no domain.
_NodeSpec = tuple[str, str | None]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeWriter:
    """Concrete writer that satisfies BoltWriter structurally and exposes written bytes."""

    def __init__(self) -> None:
        self._buf = io.BytesIO()

    def write(self, data: bytes) -> None:
        self._buf.write(data)

    async def drain(self) -> None:
        pass

    def get_extra_info(self, key: str, default: object = None) -> object:
        _ = key
        return default

    def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass

    def getvalue(self) -> bytes:
        return self._buf.getvalue()


def _make_session(roles: list[str], bolt_version: tuple[int, int] = (5, 4)):
    from provisa.bolt.session import BoltSession

    writer = _FakeWriter()
    session = BoltSession(writer, bolt_version)  # type: ignore[arg-type]
    session.roles = list(roles)
    session.role_id = roles[0] if roles else None
    return session, writer


# ---------------------------------------------------------------------------
# 1. Unknown db → DatabaseNotFound (no fallback, no execution)
# ---------------------------------------------------------------------------


class TestDatabaseNotFoundIntegration:
    async def test_run_with_unknown_db_sends_database_not_found(self):
        """REQ-808: RUN targeting an unknown db emits DatabaseNotFound code."""
        session, writer = _make_session(["analyst"])
        await session.handle_run(
            [
                "MATCH (n) RETURN n",
                {},
                {"db": "provisa_nonexistent"},
            ]
        )
        assert b"DatabaseNotFound" in writer.getvalue()

    async def test_run_with_unknown_db_does_not_call_execute(self):
        """REQ-808: execution is never reached for unknown db."""
        session, _ = _make_session(["analyst"])
        with patch("provisa.bolt.session._execute_cypher") as mock_exec:
            await session.handle_run(
                [
                    "MATCH (n) RETURN n",
                    {},
                    {"db": "provisa_ops_nonexistent"},
                ]
            )
        assert mock_exec.call_count == 0

    async def test_run_with_unauthorized_role_sends_database_not_found(self):
        """REQ-808: switching to a role the user does not hold → DatabaseNotFound."""
        session, writer = _make_session(["analyst"])
        await session.handle_run(
            [
                "MATCH (n) RETURN n",
                {},
                {"db": "provisa_admin"},
            ]
        )
        assert b"DatabaseNotFound" in writer.getvalue()

    async def test_run_with_unauthorized_ops_role_sends_database_not_found(self):
        """REQ-808: ops view for a role the user doesn't hold → DatabaseNotFound."""
        session, writer = _make_session(["analyst"])
        await session.handle_run(
            [
                "MATCH (n) RETURN n",
                {},
                {"db": "provisa_ops_admin"},
            ]
        )
        assert b"DatabaseNotFound" in writer.getvalue()

    async def test_begin_then_run_unknown_db_sends_database_not_found(self):
        """REQ-808: explicit-tx path (BEGIN + RUN) also enforces the ceiling."""
        from provisa.bolt.session import State

        session, writer = _make_session(["analyst"])
        session.state = State.READY
        session.handle_begin([{"db": "provisa_bad"}])
        await session.handle_run(["MATCH (n) RETURN n", {}, {}])
        assert b"DatabaseNotFound" in writer.getvalue()


# ---------------------------------------------------------------------------
# 2. Known db → execution proceeds (real resolve path, mocked execution)
# ---------------------------------------------------------------------------


class TestKnownDbProceedsToExecution:
    async def test_run_with_known_role_db_calls_execute(self):
        """REQ-808: known db resolves to a role; execution is reached."""
        session, _ = _make_session(["analyst"])
        with patch(
            "provisa.bolt.session._execute_cypher",
            new=AsyncMock(return_value=(["n"], [])),
        ) as mock_exec:
            await session.handle_run(
                [
                    "MATCH (n) RETURN n",
                    {},
                    {"db": "provisa_analyst"},
                ]
            )
        mock_exec.assert_called_once()
        _, kwargs = mock_exec.call_args
        assert kwargs.get("include_ops") is False

    async def test_run_with_ops_db_calls_execute_with_include_ops_true(self):
        """REQ-808: ops view passes include_ops=True to execution."""
        session, _ = _make_session(["analyst"])
        with patch(
            "provisa.bolt.session._execute_cypher",
            new=AsyncMock(return_value=(["n"], [])),
        ) as mock_exec:
            await session.handle_run(
                [
                    "MATCH (n) RETURN n",
                    {},
                    {"db": "provisa_ops_analyst"},
                ]
            )
        mock_exec.assert_called_once()
        _, kwargs = mock_exec.call_args
        assert kwargs.get("include_ops") is True

    async def test_run_with_default_db_uses_business_view(self):
        """REQ-808: empty/system db resolves to first role, business view."""
        session, _ = _make_session(["analyst"])
        with patch(
            "provisa.bolt.session._execute_cypher",
            new=AsyncMock(return_value=(["n"], [])),
        ) as mock_exec:
            await session.handle_run(
                [
                    "MATCH (n) RETURN n",
                    {},
                    {},
                ]
            )
        mock_exec.assert_called_once()
        args, kwargs = mock_exec.call_args
        assert args[2] == "analyst"
        assert kwargs.get("include_ops") is False


# ---------------------------------------------------------------------------
# 3. _bolt_label_map: domain_access ceiling + ops/business view filtering
# ---------------------------------------------------------------------------


def _make_label_map_from_spec(nodes_spec: list[_NodeSpec]):
    from provisa.cypher.label_map import CypherLabelMap, NodeMapping

    nodes: dict[str, NodeMapping] = {}
    for type_name, domain_id in nodes_spec:
        table_label = type_name.split("_")[-1].capitalize()
        nm = NodeMapping(
            label=table_label,
            type_name=type_name,
            domain_label=None,
            domain_id=domain_id,
            table_label=table_label,
            table_id=0,
            source_id="s1",
            id_column="id",
            pk_columns=[],
            catalog_name="cat",
            schema_name="public",
            table_name=table_label.lower(),
            properties={"id": "id"},
        )
        nodes[type_name] = nm

    nodes_by_table: dict[str, list[str]] = {}
    for tn, nm in nodes.items():
        nodes_by_table.setdefault(nm.table_label, []).append(tn)

    return CypherLabelMap(
        nodes=nodes,
        relationships={},
        domains={},
        nodes_by_table=nodes_by_table,
        aliases={},
    )


class TestBoltLabelMapIntegration:
    def _make_mock_app_state(
        self,
        role_domain_access: list[str],
        nodes_spec: list[_NodeSpec],
    ):
        label_map = _make_label_map_from_spec(nodes_spec)

        mock_ctx = MagicMock()
        mock_ctx.tables = {}
        mock_ctx.joins = {}
        mock_ctx.aggregate_columns = {}
        mock_ctx.pk_columns = {}
        mock_ctx.physical_to_sql = {}
        mock_ctx.gql_governed_object_cols = {}
        mock_ctx.native_filter_columns = {}

        mock_state = MagicMock()
        mock_state.roles = {"analyst": {"domain_access": role_domain_access}}
        mock_state.schema_build_cache = {}
        mock_state.source_catalogs = {}

        return mock_ctx, mock_state, label_map

    def test_business_view_drops_ops_domain_nodes(self):
        """REQ-808: _bolt_label_map with include_ops=False excludes ops-domain nodes."""
        from provisa.bolt.session import _bolt_label_map
        from provisa.core import domain_policy

        nodes_spec: list[_NodeSpec] = [
            ("biz_orders", "sales"),
            ("ops_jobs", "ops"),
            ("meta_cfg", "meta"),
            ("sys_tables", ""),
        ]
        mock_ctx, mock_state, _ = self._make_mock_app_state(["sales"], nodes_spec)

        with patch(
            "provisa.cypher.label_map.CypherLabelMap.from_schema",
            return_value=_make_label_map_from_spec(nodes_spec),
        ):
            label_map = _bolt_label_map(
                mock_ctx, "analyst", include_ops=False, app_state=mock_state
            )

        sys_ids = set(domain_policy.system_domain_ids())
        visible_type_names = set(label_map.nodes.keys())

        assert "biz_orders" in visible_type_names
        for tn, domain_id in nodes_spec:
            if (domain_id or "") in sys_ids:
                assert tn not in visible_type_names, (
                    f"{tn} (domain_id={domain_id!r}) should be excluded from business view"
                )

    def test_ops_view_keeps_all_nodes(self):
        """REQ-808: _bolt_label_map with include_ops=True keeps all nodes."""
        from provisa.bolt.session import _bolt_label_map

        nodes_spec: list[_NodeSpec] = [
            ("biz_orders", "sales"),
            ("ops_jobs", "ops"),
            ("meta_cfg", "meta"),
        ]
        mock_ctx, mock_state, _ = self._make_mock_app_state(["sales", "ops", "meta"], nodes_spec)

        with patch(
            "provisa.cypher.label_map.CypherLabelMap.from_schema",
            return_value=_make_label_map_from_spec(nodes_spec),
        ):
            label_map = _bolt_label_map(mock_ctx, "analyst", include_ops=True, app_state=mock_state)

        visible_type_names = set(label_map.nodes.keys())
        assert "biz_orders" in visible_type_names
        assert "ops_jobs" in visible_type_names
        assert "meta_cfg" in visible_type_names


# ---------------------------------------------------------------------------
# 4. SHOW DATABASES respects role set — only user's dbs listed
# ---------------------------------------------------------------------------


class TestShowDatabasesRespectsDomainCeiling:
    async def test_show_databases_lists_only_user_roles(self):
        """REQ-808: SHOW DATABASES never leaks databases outside the user's role set."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["analyst"])
        names = {r[0] for r in rows}

        assert "provisa_analyst" in names
        assert "provisa_ops_analyst" in names
        assert "provisa_admin" not in names
        assert "provisa_ops_admin" not in names

    async def test_show_databases_multi_role_lists_all_user_roles(self):
        """REQ-808: multi-role user sees all their roles, not others."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["analyst", "viewer"])
        names = {r[0] for r in rows}

        assert "provisa_analyst" in names
        assert "provisa_ops_analyst" in names
        assert "provisa_viewer" in names
        assert "provisa_ops_viewer" in names
        assert "provisa_admin" not in names
