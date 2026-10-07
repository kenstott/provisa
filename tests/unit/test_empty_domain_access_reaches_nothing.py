# Copyright (c) 2026 Kenneth Stott
# Canary: c65a5c2c-46c9-49df-870a-51345f7cab86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role reaches the domains it lists, on every surface.

``"*"`` is the only way to say all; an EMPTY ``domain_access`` is no domains. One test per reader
of a role's domain list, each showing the three cases side by side: ``[]`` reaches nothing,
``["*"]`` reaches all, a listed domain reaches only itself. Single-domain mode
(``naming.use_domains: false``) is the one exemption and is decided in one function, so each
reader is also shown honouring it.

A column's ``visible_to=[]`` is a different list with the opposite reading (visible to every
role) and is unchanged; the tables here use it so that domain scope is the only thing deciding.

A missing role is an error, never a default: every entry point that takes the acting role raises
when it is not there.
"""

from __future__ import annotations

from typing import Any

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import _build_visible_tables, generate_schema
from provisa.compiler.schema_types import SchemaInput
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext, build_governance_context
from provisa.core import domain_policy
from provisa.security.rights import (
    META_DOMAIN_ID,
    UnknownRoleError,
    compute_meta_row_scope,
    effective_domain_access_role,
    require_role,
)
from provisa.security.visibility import visible_tables

SALES, FINANCE, META, OPS = 1, 2, 3, 4
DOMAIN_OF = {SALES: "sales", FINANCE: "finance", META: META_DOMAIN_ID, OPS: "ops"}
NAME_OF = {SALES: "orders", FINANCE: "ledger", META: "registered_tables_meta", OPS: "queries"}

# The root field each table gets (GraphQL Query field and proto Query message alike).
ROOT_OF = {SALES: "orders", FINANCE: "ledger", META: "registeredTablesMeta", OPS: "queries"}

CASES = [
    pytest.param([], set(), id="empty-reaches-nothing"),
    pytest.param(["*"], {"sales", "finance", META_DOMAIN_ID, "ops"}, id="wildcard-reaches-all"),
    pytest.param(["sales"], {"sales"}, id="listed-reaches-only-itself"),
]


def _role(domain_access: list[str], *capabilities: str) -> dict:
    return {"id": "r", "capabilities": list(capabilities), "domain_access": domain_access}


def _tables() -> list[dict]:
    """One table per domain. ``visible_to=[]`` (every role) on the data columns; ``ops`` is a
    lockdown domain, so its column names the role explicitly."""
    return [
        {
            "id": tid,
            "domain_id": DOMAIN_OF[tid],
            "source_id": "pg",
            "schema_name": "public",
            "table_name": NAME_OF[tid],
            "columns": [
                {
                    "column_name": "id",
                    "visible_to": ["r"] if tid == OPS else [],
                    "native_filter_type": None,
                }
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
        }
        for tid in (SALES, FINANCE, META, OPS)
    ]


def _schema_input(role: dict) -> SchemaInput:
    return SchemaInput(
        tables=_tables(),
        relationships=[],
        column_types={
            tid: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for tid in DOMAIN_OF
        },
        naming_rules=[],
        role=role,
        domains=[{"id": d} for d in DOMAIN_OF.values()],
    )


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    for tid, domain in DOMAIN_OF.items():
        ctx.tables[NAME_OF[tid]] = TableMeta(
            table_id=tid,
            field_name=NAME_OF[tid],
            type_name=NAME_OF[tid].title(),
            source_id="pg",
            catalog_name="pg",
            schema_name="public",
            table_name=NAME_OF[tid],
            domain_id=domain,
        )
    return ctx


@pytest.fixture
def multi_domain(monkeypatch):
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)


@pytest.fixture
def single_domain(monkeypatch):
    monkeypatch.setattr(domain_policy, "single_domain", lambda: True)


# --- the per-role schema: schema_gen._build_visible_tables --------------------------------------


@pytest.mark.parametrize("held,reached", CASES)
def test_the_roles_schema_holds_only_the_domains_it_lists(multi_domain, held, reached):
    domains = {t.domain_id for t in _build_visible_tables(_schema_input(_role(held)))}
    # meta is an implicit TRAVERSAL domain: its tables stay in a schema as targets, never as
    # roots (asserted on the root fields below), so it is set aside here.
    assert domains - {META_DOMAIN_ID} == reached - {META_DOMAIN_ID}


@pytest.mark.parametrize("held,reached", CASES)
def test_the_roles_root_fields_are_only_the_domains_it_lists(multi_domain, held, reached):
    role = _role(held)
    if not reached:
        # A role listing no domain has no root at all — not meta, not ops.
        roots = {
            t.domain_id
            for t in _build_visible_tables(_schema_input(role))
            if t.domain_id in set(held)
        }
        assert roots == set()
        return
    schema = generate_schema(_schema_input(role))
    assert schema.query_type is not None
    fields = set(schema.query_type.fields)
    for tid, domain in DOMAIN_OF.items():
        present = ROOT_OF[tid] in fields
        assert present == (domain in reached), (domain, sorted(fields))


def test_single_domain_mode_shows_every_table_whatever_the_list(single_domain):
    domains = {t.domain_id for t in _build_visible_tables(_schema_input(_role([])))}
    assert domains == set(DOMAIN_OF.values())


def test_a_columns_empty_visible_to_still_means_every_role(multi_domain):
    # The other list: visible_to=[] on a column is visible to every role that reaches the table.
    (info,) = [
        t for t in _build_visible_tables(_schema_input(_role(["sales"]))) if t.domain_id == "sales"
    ]
    assert [c["column_name"] for c in info.visible_columns] == ["id"]


# --- SQL: V001 in sql_validator ------------------------------------------------------------------


def _gov() -> GovernanceContext:
    gov = GovernanceContext()
    for tid in DOMAIN_OF:
        gov.table_map[NAME_OF[tid]] = tid
        gov.visible_columns[tid] = None
        gov.all_columns[tid] = [("id", "integer")]
    return gov


@pytest.mark.parametrize("held,reached", CASES)
def test_v001_refuses_every_table_outside_the_roles_list(multi_domain, held, reached):
    for tid, domain in DOMAIN_OF.items():
        violations = validate_sql(
            f"SELECT id FROM {NAME_OF[tid]}",
            _ctx(),
            _gov(),
            _role(held),
            [],
            bypass_relationship_guard=True,
        )
        refused = any(v.code == "V001" for v in violations)
        assert refused == (domain not in reached), (domain, [v.message for v in violations])


def test_v001_is_not_a_gate_in_single_domain_mode(single_domain):
    violations = validate_sql(
        "SELECT id FROM orders", _ctx(), _gov(), _role([]), [], bypass_relationship_guard=True
    )
    assert not [v for v in violations if v.code == "V001"]


# --- the meta catalog's rows: rights.compute_meta_row_scope --------------------------------------


@pytest.mark.parametrize(
    "held,scope",
    [
        pytest.param([], set(), id="empty-sees-no-catalog-rows"),
        pytest.param(["*"], None, id="wildcard-sees-the-whole-catalog"),
        pytest.param([META_DOMAIN_ID], None, id="meta-grant-sees-the-whole-catalog"),
        pytest.param(["sales"], {SALES}, id="listed-sees-its-own-tables"),
    ],
)
def test_meta_rows_follow_the_roles_list(multi_domain, held, scope):
    assert compute_meta_row_scope(_role(held), _tables(), []) == scope


def test_an_empty_list_gets_a_match_nothing_row_filter(multi_domain):
    ctx = CompilationContext()
    ctx.tables["registered_tables_meta"] = TableMeta(
        table_id=100,
        field_name="registered_tables_meta",
        type_name="RegisteredTablesMeta",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="registered_tables_meta",
        domain_id=META_DOMAIN_ID,
    )
    gov = build_governance_context("r", RLSContext.empty(), {}, ctx, _tables(), _role([]))
    assert gov.rls_rules[100] == "id IN (-1)"


def test_single_domain_mode_sees_the_whole_catalog(single_domain):
    assert compute_meta_row_scope(_role([]), _tables(), []) is None


# --- security.visibility.visible_tables ----------------------------------------------------------


@pytest.mark.parametrize("held,reached", CASES)
def test_visible_tables_follow_the_roles_list(multi_domain, held, reached):
    tables = [
        {"domain_id": d, "columns": [{"column_name": "id", "visible_to": ["r"]}]}
        for d in DOMAIN_OF.values()
    ]
    assert {t["domain_id"] for t in visible_tables(tables, _role(held))} == reached


# --- gRPC: the Query message's roots -------------------------------------------------------------


@pytest.mark.parametrize(
    "held,meta_is_a_root",
    [(["sales"], False), (["sales", META_DOMAIN_ID], True), (["*"], True)],
)
def test_the_proto_query_message_roots_follow_the_roles_list(multi_domain, held, meta_is_a_root):
    from provisa.grpc.proto_gen import generate_proto

    proto = generate_proto(_schema_input(_role(held)))
    query_message = proto.split("message Query {", 1)[1].split("}", 1)[0]
    assert "orders" in query_message
    assert (ROOT_OF[META] in query_message) == meta_is_a_root


def test_a_role_listing_no_domain_has_no_proto_roots(multi_domain):
    from provisa.grpc.proto_gen import generate_proto

    proto = generate_proto(_schema_input(_role([])))
    query_message = proto.split("message Query {", 1)[1].split("}", 1)[0]
    assert query_message.strip() == ""


# --- Cypher: the label map's meta roots ----------------------------------------------------------


@pytest.mark.parametrize(
    "held,meta_is_a_match_root",
    [([], False), (["sales"], False), ([META_DOMAIN_ID], True), (["*"], True)],
)
def test_the_catalog_is_a_match_root_only_for_a_role_that_reaches_it(
    multi_domain, held, meta_is_a_match_root
):
    from provisa.cypher.label_map import CypherLabelMap

    lm = CypherLabelMap.from_schema(_ctx(), domain_access=held)
    meta_nodes = [n for n in lm.nodes.values() if n.domain_id == META_DOMAIN_ID]
    assert meta_nodes, "the fixture has a meta table"
    assert all(n.traversal_only != meta_is_a_match_root for n in meta_nodes)


def test_a_label_map_is_never_built_without_the_acting_roles_list():
    from provisa.cypher.label_map import CypherLabelMap

    with pytest.raises(ValueError, match="acting role's domain_access"):
        CypherLabelMap.from_schema(_ctx(), domain_access=None)


# --- "Role: All": the union of the held roles' lists ---------------------------------------------


@pytest.mark.parametrize(
    "lists,union",
    [
        ([[], []], []),
        ([[], ["sales"]], ["sales"]),
        ([["sales"], ["finance"]], ["finance", "sales"]),
        ([[], ["*"]], ["*"]),
    ],
)
def test_role_all_is_the_union_and_an_all_empty_union_is_none(multi_domain, lists, union):
    from provisa.security import meta_role

    roles = {f"r{i}": {"id": f"r{i}", "domain_access": lst} for i, lst in enumerate(lists)}
    # "Role: All" acts as the set's meta-role: its domain_access is the union.
    effective = meta_role._role(list(roles.values()), meta_role.meta_role_id(list(roles)))
    assert effective["domain_access"] == union
    # ...and the union is read like any other list: empty reaches nothing.
    reached = {
        t["domain_id"]
        for t in visible_tables(
            [
                {
                    "domain_id": d,
                    "columns": [{"column_name": "id", "visible_to": [effective["id"]]}],
                }
                for d in ("sales", "finance")
            ],
            effective,
        )
    }
    assert reached == ({"sales", "finance"} if "*" in union else set(union))


# --- a missing role is an error, never a default -------------------------------------------------


def test_governance_refuses_to_run_without_the_acting_role():
    none: Any = None
    with pytest.raises(ValueError, match="acting role"):
        build_governance_context("ghost", RLSContext.empty(), {}, _ctx(), _tables(), none)


def test_a_role_id_that_is_not_loaded_raises():
    with pytest.raises(UnknownRoleError) as err:
        require_role({"analyst": _role(["*"])}, "ghost")
    assert err.value.role_id == "ghost"
    with pytest.raises(UnknownRoleError):
        require_role(None, "ghost")
    with pytest.raises(UnknownRoleError):
        effective_domain_access_role("ghost", {"analyst": _role(["*"])})


def test_a_role_dict_with_no_domain_list_is_not_read_as_any_scope(multi_domain):
    role: Any = {"id": "r", "capabilities": []}
    with pytest.raises(KeyError):
        validate_sql("SELECT id FROM orders", _ctx(), _gov(), role, [])
    with pytest.raises(KeyError):
        compute_meta_row_scope(role, _tables(), [])
    with pytest.raises(KeyError):
        visible_tables(_tables(), role)
    with pytest.raises(KeyError):
        _build_visible_tables(_schema_input(role))


# --- the per-role build: a role that lists no domain gets no data surface ------------------------


def _built_roles(monkeypatch, roles: list[dict]) -> Any:
    import provisa.api.app as appmod
    from provisa.api.app_loaders import _build_and_register_schemas

    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(state, "source_catalogs", {"pg": "pg"}, raising=False)
    _build_and_register_schemas(
        roles=roles,
        tables=_tables(),
        relationships=[],
        col_types_converted={
            tid: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for tid in DOMAIN_OF
        },
        naming_rules=[],
        domains=[{"id": d} for d in DOMAIN_OF.values()],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],  # REQ-1921: none out of service
    )
    return state


def test_a_role_listing_no_domain_is_built_no_schema_and_breaks_nobody_elses(
    multi_domain, monkeypatch
):
    empty = {"id": "newrole", "capabilities": ["usage"], "domain_access": []}
    scoped = {"id": "r", "capabilities": ["usage"], "domain_access": ["sales"]}
    state = _built_roles(monkeypatch, [empty, scoped])

    # Registered as a role (its rights still resolve), with no data surface of any kind.
    assert set(state.roles) == {"newrole", "r"}
    for surface in (state.schemas, state.contexts, state.rls_contexts, state.proto_files):
        assert "newrole" not in surface
        assert "r" in surface


def test_single_domain_mode_builds_the_same_role_its_schema(single_domain, monkeypatch):
    empty = {"id": "r", "capabilities": ["usage"], "domain_access": []}
    state = _built_roles(monkeypatch, [empty])
    assert "r" in state.schemas and "r" in state.contexts


def test_a_role_whose_list_becomes_empty_loses_the_surface_an_earlier_build_gave_it(
    multi_domain, monkeypatch
):
    import provisa.api.app as appmod
    from provisa.api.app_loaders import _drop_data_surface

    state = _built_roles(
        monkeypatch, [{"id": "r", "capabilities": ["usage"], "domain_access": ["sales"]}]
    )
    surfaces = (
        state.schemas,
        state.contexts,
        state.rls_contexts,
        state.table_path_maps,
        state.proto_files,
    )
    assert all("r" in surface for surface in surfaces)

    # The same runtime, rebuilt after the role's list was emptied: nothing of the old build is
    # left to show it a catalog it no longer reaches.
    _drop_data_surface(appmod.state, "r")
    assert not any("r" in surface for surface in surfaces)


def test_the_build_itself_drops_a_stale_surface(multi_domain, monkeypatch):
    import provisa.api.app as appmod
    from provisa.api.app_loaders import _build_and_register_schemas

    state = _built_roles(
        monkeypatch, [{"id": "r", "capabilities": ["usage"], "domain_access": ["sales"]}]
    )
    assert "r" in state.schemas
    # Rebuild in place (the maps are NOT reset between builds of a live runtime).
    _build_and_register_schemas(
        roles=[
            {"id": "r", "capabilities": ["usage"], "domain_access": []},
            {"id": "s", "capabilities": ["usage"], "domain_access": ["sales"]},
        ],
        tables=_tables(),
        relationships=[],
        col_types_converted={
            tid: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for tid in DOMAIN_OF
        },
        naming_rules=[],
        domains=[{"id": d} for d in DOMAIN_OF.values()],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],  # REQ-1921: none out of service
    )
    assert appmod.state.roles["r"]["domain_access"] == []
    for surface in (state.schemas, state.contexts, state.rls_contexts, state.proto_files):
        assert "r" not in surface
        assert "s" in surface
