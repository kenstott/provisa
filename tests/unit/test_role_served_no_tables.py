# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9b6a14-7c25-4d08-9f61-b2a4c8e05d73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role served no table is not refused as such (REQ-2154, amended).

Its grants serve it no column of any table, so its data surface is empty: its GraphQL schema and
proto are built, with no table in them. A statement naming a table is refused as for any role not
served that table, by name; a statement that needs no table -- a graph session's CREATE and MATCH
of its own ephemeral nodes -- runs as for any role. (The openCypher TCK runs as such a role.)
"""

# Requirements: REQ-2154, REQ-2176

from __future__ import annotations

import pytest

from provisa.api.role_surface import role_surface

_ROLE = "loose"


@pytest.fixture
def empty_role(monkeypatch):
    """The real build of two roles over one table: ``analyst`` is granted its column, ``loose``
    reaches its domain and is granted nothing."""
    import provisa.api.app as appmod
    from provisa.api import app_loaders
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    monkeypatch.setattr(app_loaders, "warm_role_surfaces", lambda _state: None)
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(state, "source_catalogs", {"pg": "pg"}, raising=False)
    roles = [
        {
            "id": r,
            "description": "Test role.",
            "capabilities": ["usage"],
            "domain_access": ["sales"],
        }
        for r in ("analyst", _ROLE)
    ]
    app_loaders._build_and_register_schemas(
        roles=roles,
        tables=[
            {
                "id": 1,
                "source_id": "pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [{"column_name": "id", "visible_to": ["analyst"]}],
                "write_ops": [],
                "write_returns_rows": True,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        col_types_converted={
            1: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
        },
        naming_rules=[],
        domains=[{"id": "sales"}],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],
    )
    return state


def test_its_surface_is_built_empty_and_its_graphql_schema_is_valid(empty_role, caplog) -> None:
    from graphql import validate_schema

    with caplog.at_level("ERROR", logger="provisa.api.role_surface"):
        surface = role_surface(empty_role, _ROLE)
    assert not [r for r in caplog.records if r.levelname == "ERROR"]
    assert validate_schema(surface.schema) == []
    # No table's field: only the catalog's resolve field every role's Query has (REQ-1365).
    assert set(surface.schema.query_type.fields) == {"resolve"}
    assert surface.schema.mutation_type is None and surface.schema.subscription_type is None
    assert surface.schema.get_type("Orders") is None
    assert surface.table_path_map == {}
    assert surface.context.tables == {}
    # Its proto: no table's message or RPC; the service keeps its command and resolve RPCs.
    assert "message Orders" not in surface.proto and "QueryOrders" not in surface.proto
    assert "rpc CallCommand(" in surface.proto
    # The role served the table is served it as before.
    assert "orders" in role_surface(empty_role, "analyst").schema.query_type.fields


def test_its_proto_compiles(empty_role, tmp_path) -> None:
    import grpc_tools
    from grpc_tools import protoc

    (tmp_path / "role.proto").write_text(role_surface(empty_role, _ROLE).proto)
    includes = f"-I{grpc_tools.__path__[0]}/_proto"
    args = ["protoc", f"-I{tmp_path}", includes, f"--python_out={tmp_path}", "role.proto"]
    assert protoc.main(args) == 0


def test_a_graphql_query_naming_a_table_is_refused_by_name(empty_role) -> None:
    from graphql import parse, validate

    errors = validate(role_surface(empty_role, _ROLE).schema, parse("{ orders { id } }"))
    assert [e.message for e in errors] == ["Cannot query field 'orders' on type 'Query'."]


def test_a_cypher_match_naming_a_table_is_refused_by_name(empty_role) -> None:
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.label_map import CypherLabelMap
    from provisa.gql.mistakes import CypherMistake
    from provisa.gql.translator import cypher_to_sql

    label_map = CypherLabelMap.from_schema(
        role_surface(empty_role, _ROLE).context, domain_access=["sales"]
    )
    with pytest.raises(CypherMistake) as refused:
        cypher_to_sql(parse_cypher("MATCH (o:Orders) RETURN o.id", label_map), label_map, {})
    assert refused.value.code == "cypher.no_labels"
    assert ":Orders" in str(refused.value)


def test_its_graph_session_creates_and_matches_ephemeral_nodes(empty_role, monkeypatch) -> None:
    from provisa.gql.label_map import CypherLabelMap
    from tests.unit.ephemeral_harness import Session

    graph = Session(monkeypatch, {})
    graph.model = CypherLabelMap.from_schema(
        role_surface(empty_role, _ROLE).context, domain_access=["sales"]
    )
    assert not graph.model.nodes  # the role's model: no table's label
    graph.run("CREATE (:Person {name: 'Ada'}), (:Person {name: 'Alan'})")
    rows = graph.run("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
    assert rows == [{"name": "Ada"}, {"name": "Alan"}]
    graph.run("MERGE (:Person {name: 'Ada'})")
    assert graph.run("MATCH (p:Person) RETURN count(p) AS n") == [{"n": 2}]


def test_a_sql_statement_naming_a_table_is_refused_by_name_as_for_any_role(empty_role) -> None:
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_validator import validate_sql
    from provisa.compiler.stage2 import build_governance_context

    def refusal(role_id: str, sql: str) -> list[tuple[str, str]]:
        role = empty_role.roles[role_id]
        ctx = role_surface(empty_role, role_id).context
        gov = build_governance_context(role_id, RLSContext.empty(), {}, ctx, [], role)
        return [(v.code, v.message) for v in validate_sql(sql, ctx, gov, role, [])]

    assert refusal(_ROLE, "SELECT id FROM orders") == [
        ("V006", "Relation 'orders' is not a registered table")
    ]
    # As the role served the table is answered for a table it is not served.
    assert refusal("analyst", "SELECT id FROM refunds") == [
        ("V006", "Relation 'refunds' is not a registered table")
    ]
    # A statement that needs no table runs as for any role.
    assert refusal(_ROLE, "SELECT 1 AS one") == []
