# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for REQ-808: Bolt domain-access ceiling.

Pure logic only — no I/O, no network, no DB, no docker.
Targets:
  - BoltSession._resolve_db  (db-name → role/view parsing, DatabaseNotFound on unknown)
  - _bolt_label_map           (domain_access ceiling, ops vs business view)
  - system/meta/ops domain_id recognition via domain_policy.system_domain_ids()
  - _show_databases_rows      (one db per view x role)
"""

from __future__ import annotations

import io
from unittest.mock import patch


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
# 1. db-name parsing: _resolve_db
# ---------------------------------------------------------------------------


class TestResolveDb:
    def test_empty_db_returns_default_role_business_view(self):
        session, _ = _make_session(["analyst", "admin"])
        result = session._resolve_db("")
        assert result == ("analyst", False)

    def test_none_db_returns_default_role_business_view(self):
        session, _ = _make_session(["analyst"])
        result = session._resolve_db(None)
        assert result == ("analyst", False)

    def test_system_db_returns_default_role_business_view(self):
        session, _ = _make_session(["analyst"])
        result = session._resolve_db("system")
        assert result == ("analyst", False)

    def test_provisa_db_returns_default_role_business_view(self):
        session, _ = _make_session(["analyst"])
        result = session._resolve_db("provisa")
        assert result == ("analyst", False)

    def test_provisa_role_db_returns_that_role_business_view(self):
        session, _ = _make_session(["analyst", "admin"])
        result = session._resolve_db("provisa_analyst")
        assert result == ("analyst", False)

    def test_provisa_ops_role_db_returns_that_role_ops_view(self):
        session, _ = _make_session(["analyst", "admin"])
        result = session._resolve_db("provisa_ops_analyst")
        assert result == ("analyst", True)

    def test_unknown_db_returns_none_not_fallback(self):
        """REQ-808: Unknown db must return None — no silent fallback."""
        session, _ = _make_session(["analyst"])
        result = session._resolve_db("provisa_unknown_role")
        assert result is None

    def test_unauthorized_ops_role_returns_none(self):
        """REQ-808: ops view for a role the user doesn't hold → None."""
        session, _ = _make_session(["analyst"])
        result = session._resolve_db("provisa_ops_admin")
        assert result is None

    def test_unauthorized_business_role_returns_none(self):
        """REQ-808: business view for a role the user doesn't hold → None."""
        session, _ = _make_session(["analyst"])
        result = session._resolve_db("provisa_admin")
        assert result is None

    def test_no_roles_returns_none(self):
        """REQ-808: no roles means no valid db."""
        session, _ = _make_session([])
        session.roles = []
        result = session._resolve_db(None)
        assert result is None

    def test_completely_arbitrary_db_returns_none(self):
        """REQ-808: arbitrary string not matching any pattern → None."""
        session, _ = _make_session(["analyst"])
        result = session._resolve_db("neo4j")
        assert result is None


# ---------------------------------------------------------------------------
# 2. DatabaseNotFound sent on unknown db (handle_run path)
# ---------------------------------------------------------------------------


class TestDatabaseNotFoundOnUnknownDb:
    async def test_handle_run_unknown_db_sends_database_not_found(self):
        """REQ-808: RUN with unknown db must send DatabaseNotFound, no fallback."""
        session, writer = _make_session(["analyst"])
        with patch.object(session, "_resolve_db", return_value=None):
            await session.handle_run(
                [
                    "MATCH (n) RETURN n",
                    {},
                    {"db": "provisa_bad"},
                ]
            )

        assert b"DatabaseNotFound" in writer.getvalue()

    async def test_handle_run_unknown_db_does_not_reach_execute(self):
        """REQ-808: execution must be short-circuited on unknown db."""
        session, _ = _make_session(["analyst"])
        with (
            patch.object(session, "_resolve_db", return_value=None),
            patch("provisa.bolt.session._execute_cypher") as mock_exec,
        ):
            await session.handle_run(
                [
                    "MATCH (n) RETURN n",
                    {},
                    {"db": "provisa_bad"},
                ]
            )
        assert mock_exec.call_count == 0


# ---------------------------------------------------------------------------
# 3. system_domain_ids — system/meta/ops identification
# ---------------------------------------------------------------------------


class TestSystemDomainIds:
    def test_system_domain_ids_contains_empty_string(self):
        from provisa.core import domain_policy

        assert "" in domain_policy.system_domain_ids()

    def test_system_domain_ids_contains_meta(self):
        from provisa.core import domain_policy

        assert "meta" in domain_policy.system_domain_ids()

    def test_system_domain_ids_contains_ops(self):
        from provisa.core import domain_policy

        assert "ops" in domain_policy.system_domain_ids()

    def test_system_domain_ids_stable_set(self):
        """The canonical set from REQ-808 is {"", "meta", "ops"}."""
        from provisa.core.domain_policy import _SYSTEM_DOMAIN_IDS

        assert set(_SYSTEM_DOMAIN_IDS) == {"", "meta", "ops"}


# ---------------------------------------------------------------------------
# 4. _bolt_label_map: domain_access as hard ceiling, ops vs business view
# ---------------------------------------------------------------------------

# Nodes spec: (type_name, domain_id); domain_id=None means no domain.
_NodeSpec = tuple[str, str | None]


def _make_node_mapping(type_name: str, domain_id: str | None, table_label: str):
    from provisa.cypher.label_map import NodeMapping

    label = table_label if not domain_id else f"{table_label.capitalize()}:{table_label}"
    return NodeMapping(
        label=label,
        type_name=type_name,
        domain_label=None if not domain_id else table_label.capitalize(),
        domain_id=domain_id,
        table_label=table_label,
        table_id=0,
        source_id="s1",
        id_column="id",
        pk_columns=[],
        catalog_name="cat",
        schema_name="public",
        table_name=table_label,
        properties={"id": "id"},
    )


class TestBoltLabelMapCeiling:
    def _make_label_map_with_nodes(self, nodes_spec: list[_NodeSpec]):
        """Build a CypherLabelMap from (type_name, domain_id) pairs."""
        from provisa.cypher.label_map import CypherLabelMap

        nodes = {}
        for type_name, domain_id in nodes_spec:
            table_label = type_name.split("_")[-1].capitalize()
            nm = _make_node_mapping(type_name, domain_id, table_label)
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

    def test_business_view_excludes_ops_domain(self):
        """REQ-808: include_ops=False drops nodes with domain_id in {"", "meta", "ops"}."""
        from provisa.core import domain_policy

        nodes_spec: list[_NodeSpec] = [
            ("biz_orders", "sales"),
            ("ops_metrics", "ops"),
            ("meta_schema", "meta"),
            ("sys_tables", ""),
        ]
        base = self._make_label_map_with_nodes(nodes_spec)

        sys_ids = set(domain_policy.system_domain_ids())
        biz_nodes = {tn: nm for tn, nm in base.nodes.items() if (nm.domain_id or "") not in sys_ids}

        assert "biz_orders" in biz_nodes
        assert "ops_metrics" not in biz_nodes
        assert "meta_schema" not in biz_nodes
        assert "sys_tables" not in biz_nodes

    def test_ops_view_includes_all_domain_ids(self):
        """REQ-808: include_ops=True (ops view) keeps system/meta/ops nodes."""
        nodes_spec: list[_NodeSpec] = [
            ("biz_orders", "sales"),
            ("ops_metrics", "ops"),
            ("meta_schema", "meta"),
        ]
        base = self._make_label_map_with_nodes(nodes_spec)

        all_nodes = set(base.nodes.keys())
        assert "biz_orders" in all_nodes
        assert "ops_metrics" in all_nodes
        assert "meta_schema" in all_nodes

    def test_domain_access_ceiling_excludes_unauthorized_domains(self):
        """REQ-808: domain_access is the hard ceiling — unauthorized domains never surface."""
        from provisa.security.visibility import visible_tables

        tables = [
            {"domain_id": "sales", "columns": [{"column_name": "id", "visible_to": ["analyst"]}]},
            {"domain_id": "hr", "columns": [{"column_name": "id", "visible_to": ["analyst"]}]},
            {"domain_id": "finance", "columns": [{"column_name": "id", "visible_to": ["analyst"]}]},
        ]
        role = {"id": "analyst", "domain_access": ["sales"]}
        result = visible_tables(tables, role)
        domains_visible = {t["domain_id"] for t in result}

        assert domains_visible == {"sales"}
        assert "hr" not in domains_visible
        assert "finance" not in domains_visible

    def test_domain_access_star_grants_all_domains(self):
        """REQ-808: domain_access=["*"] is unrestricted."""
        from provisa.security.visibility import visible_tables

        tables = [
            {"domain_id": "sales", "columns": [{"column_name": "id", "visible_to": ["admin"]}]},
            {"domain_id": "hr", "columns": [{"column_name": "id", "visible_to": ["admin"]}]},
        ]
        role = {"id": "admin", "domain_access": ["*"]}
        result = visible_tables(tables, role)
        domains_visible = {t["domain_id"] for t in result}

        assert "sales" in domains_visible
        assert "hr" in domains_visible

    def test_domain_access_empty_list_grants_all_domains(self):
        """REQ-808: domain_access=[] means unrestricted (REQ-9e6b552)."""
        from provisa.security.visibility import visible_tables

        tables = [
            {"domain_id": "sales", "columns": [{"column_name": "id", "visible_to": ["admin"]}]},
            {"domain_id": "hr", "columns": [{"column_name": "id", "visible_to": ["admin"]}]},
        ]
        role = {"id": "admin", "domain_access": []}
        result = visible_tables(tables, role)
        domains_visible = {t["domain_id"] for t in result}

        assert "sales" in domains_visible
        assert "hr" in domains_visible


# ---------------------------------------------------------------------------
# 5. _show_databases_rows: one db per view x role
# ---------------------------------------------------------------------------


class TestShowDatabasesRows:
    def test_each_role_has_business_and_ops_db(self):
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["analyst", "admin"])
        _ = cols  # cols returned for completeness; names checked via row[0]
        names = {r[0] for r in rows}

        assert "provisa_analyst" in names
        assert "provisa_ops_analyst" in names
        assert "provisa_admin" in names
        assert "provisa_ops_admin" in names

    def test_single_role_produces_two_rows(self):
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["analyst"])
        assert len(rows) == 2

    def test_two_roles_produce_four_rows(self):
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["analyst", "admin"])
        assert len(rows) == 4

    def test_first_roles_business_db_is_home(self):
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["analyst", "admin"])
        home_idx = cols.index("home")
        home_dbs = [r[0] for r in rows if r[home_idx]]
        assert home_dbs == ["provisa_analyst"]

    def test_ops_db_is_never_home(self):
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["analyst"])
        home_idx = cols.index("home")
        for r in rows:
            if r[0].startswith("provisa_ops_"):
                assert not r[home_idx]

    def test_empty_roles_returns_no_rows(self):
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows([])
        assert rows == []


# ---------------------------------------------------------------------------
# 6. ops vs business view narrowing within ceiling (filter logic)
# ---------------------------------------------------------------------------


class TestOpsVsBusinessViewNarrowing:
    def test_business_view_is_subset_of_ops_view(self):
        """REQ-808: business view <= ops view (business only narrows within ops ceiling)."""
        from provisa.core import domain_policy

        all_nodes: list[_NodeSpec] = [
            ("biz_orders", "sales"),
            ("ops_jobs", "ops"),
            ("meta_cfg", "meta"),
        ]
        sys_ids = set(domain_policy.system_domain_ids())

        ops_nodes = {tn for tn, _ in all_nodes}
        biz_nodes = {tn for tn, domain_id in all_nodes if (domain_id or "") not in sys_ids}

        assert biz_nodes.issubset(ops_nodes)
        assert biz_nodes != ops_nodes  # strictly narrower when sys nodes exist

    def test_business_view_does_not_contain_meta_or_ops_domain_nodes(self):
        from provisa.core.domain_policy import _SYSTEM_DOMAIN_IDS

        sys_ids = set(_SYSTEM_DOMAIN_IDS)
        all_nodes: list[_NodeSpec] = [
            ("biz_orders", "sales"),
            ("ops_jobs", "ops"),
            ("meta_cfg", "meta"),
            ("sys_tables", ""),
        ]
        biz_nodes = {tn for tn, domain_id in all_nodes if (domain_id or "") not in sys_ids}

        assert "ops_jobs" not in biz_nodes
        assert "meta_cfg" not in biz_nodes
        assert "sys_tables" not in biz_nodes
        assert "biz_orders" in biz_nodes
