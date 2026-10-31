# Copyright (c) 2026 Kenneth Stott
# Canary: 6e3f0b52-8f1d-4a57-9c23-5d0a7b41e9c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A system domain is held as a capability, and flattening adds it to the role's domain_access.

pet_analyst (["pet-store", "shelter"], parent analyst) is the case the maintainer reported: analyst
reached the catalog and the glossary and pet_analyst did not. The roles here are flattened over the
real standard roles, so the rule is shown as it is built: meta is catalog_read, ontology is
glossary_read, ops is observability (REQ-1132). ``"*"`` means every data domain. Rows stay governed:
meta by REQ-1132's tiers, ontology by the glossary's rule. No write and no command comes with the
read.
"""

from __future__ import annotations

import asyncio
import json
import types

import pytest
import sqlglot
from graphql import get_named_type

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.naming import SYSTEM_SCHEMA_GQL_PREFIX
from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import _build_visible_tables, generate_schema
from provisa.compiler.schema_types import SchemaInput
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import build_governance_context
from provisa.compiler.write_admission import admit_write
from provisa.core import domain_policy
from provisa.core.db import STANDARD_ROLES, standard_role_dicts
from provisa.core.env_authority import domains_within
from provisa.gql.label_map import CypherLabelMap
from provisa.security import domain_tree
from provisa.security.default_grant import VIEWER_ROLE
from provisa.security.inheritance import expand_column_grants, flatten_role_dicts, role_chains
from provisa.security.meta_role import _role as meta_role_of
from provisa.security.mutation_authz import command_reachable
from provisa.security.rights import (
    META_DOMAIN_ID,
    ONTOLOGY_DOMAIN_ID,
    OPS_DOMAIN_ID,
    SYSTEM_DOMAIN_CAPABILITIES,
    TRAVERSABLE_DOMAINS,
    Capability,
    MissingDomainCapability,
    compute_meta_row_scope,
    reaches_domain,
    role_domain_problem,
    served_roles,
)
from provisa.security.visibility import visible_tables

ROLE, LONER, WILD, NO_GLOSSARY = "pet_analyst", "loner", "wild", "no_glossary"
PETS, CATALOG, TERMS, QUERIES = 1, 2, 3, 4
PETS_NAME, CATALOG_NAME, TERMS_NAME, QUERIES_NAME = (
    "pets",
    "registered_tables_meta",
    "_terms_ontology",
    "queries",
)
DOMAIN_OF = {
    PETS: "pet-store",
    CATALOG: META_DOMAIN_ID,
    TERMS: ONTOLOGY_DOMAIN_ID,
    QUERIES: OPS_DOMAIN_ID,
}
NAME_OF = {PETS: PETS_NAME, CATALOG: CATALOG_NAME, TERMS: TERMS_NAME, QUERIES: QUERIES_NAME}
COLUMNS = {
    PETS: ("id",),
    CATALOG: ("id", "table_name", "visible_to"),
    TERMS: ("name", "term_domains"),
    QUERIES: ("id",),
}
LISTED = ["pet-store", "shelter"]
OWN = [
    {"id": ROLE, "capabilities": [], "parents": ["analyst"], "domain_access": LISTED},
    # No viewer ancestor: it holds a data right and reaches a data domain.
    {"id": LONER, "capabilities": ["usage"], "parents": [], "domain_access": ["pet-store"]},
    # "*" and no capability opening a system domain.
    {"id": WILD, "capabilities": ["usage"], "parents": [], "domain_access": ["*"]},
    # REQ-2155 NO FLOOR: viewer's rights but glossary_read, as a child unselecting it holds them.
    {
        "id": NO_GLOSSARY,
        "capabilities": ["catalog_read", "data_product_read"],
        "parents": [],
        "domain_access": ["pet-store"],
    },
]


def _standard() -> list[dict]:
    return [
        {**r, "domain_access": list(STANDARD_ROLES[r["id"]].domain_access)}
        for r in standard_role_dicts()
    ]


def _flat(role_id: str = ROLE, *capabilities: str) -> dict:
    """``role_id`` as the build holds it, its stored ``capabilities`` added to."""
    rows = [
        {**r, "capabilities": [*r["capabilities"], *capabilities]} if r["id"] == role_id else r
        for r in [*_standard(), *OWN]
    ]
    return {**next(r for r in flatten_role_dicts(rows) if r["id"] == role_id), "description": "d"}


def _chains() -> dict[str, list[str]]:
    return role_chains([*_standard(), *OWN])


def _tables() -> list[dict]:
    """The tables as the seed registers them: meta and ontology columns granted to viewer alone
    (startup_seed.py), ops columns and data columns to every role here, each table also writable
    at the source and write-granted to the role, so that nothing but the rule under test keeps a
    read or a write out."""
    tables = [
        {
            "id": tid,
            "domain_id": DOMAIN_OF[tid],
            "source_id": "pg",
            "schema_name": "public",
            "table_name": NAME_OF[tid],
            "alias": None,
            "columns": [
                {
                    "column_name": name,
                    "visible_to": (
                        [VIEWER_ROLE]
                        if tid in (CATALOG, TERMS)
                        else [ROLE, LONER, WILD, NO_GLOSSARY, "analyst", "org_admin"]
                    ),
                    "native_filter_type": None,
                }
                for name in COLUMNS[tid]
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
            "write_grants": [
                {"role": rid, "operation": op, "columns": list(COLUMNS[tid])}
                for op in ("insert", "update", "delete")
                for rid in (ROLE, "org_admin")
            ],
        }
        for tid in DOMAIN_OF
    ]
    expand_column_grants(tables, _chains())
    return tables


def _schema_input(role: dict) -> SchemaInput:
    tables = _tables()
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types={
            t["id"]: [
                ColumnMetadata(column_name=c, data_type="varchar", is_nullable=True)
                for c in COLUMNS[t["id"]]
            ]
            for t in tables
        },
        naming_rules=[],
        role=role,
        domains=[{"id": d} for d in DOMAIN_OF.values()],
    )


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    for tid, name in NAME_OF.items():
        # REQ-2163: a system schema's field carries its SDL prefix, as the build names it.
        prefix = SYSTEM_SCHEMA_GQL_PREFIX.get(DOMAIN_OF[tid])
        field = f"{prefix}_{name}" if prefix else name
        ctx.tables[field] = TableMeta(
            table_id=tid,
            field_name=field,
            type_name=name.title().replace("_", ""),
            source_id="pg",
            catalog_name="pg",
            schema_name="public",
            table_name=name,
            domain_id=DOMAIN_OF[tid],
        )
        ctx.aggregate_columns[tid] = [(c, "varchar") for c in COLUMNS[tid]]
    return ctx


def _gov(role: dict):
    ctx = _ctx()
    return ctx, build_governance_context(role["id"], RLSContext.empty(), {}, ctx, _tables(), role)


def _violations(sql: str, role: dict) -> list:
    ctx, gov = _gov(role)
    return validate_sql(sql, ctx, gov, role, _tables(), bypass_relationship_guard=True)


def _system_fields(fields) -> set[str]:
    """The fields of a root type that belong to meta and ontology."""
    tags = ("registered", "terms")
    return {f for f in fields if any(tag in f.lower() for tag in tags)}


def _ops_fields(fields) -> set[str]:
    return {f for f in fields if "queries" in f.lower()}


@pytest.fixture(autouse=True)
def multi_domain(monkeypatch):
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)


# --- the one constant -----------------------------------------------------------------------------


def test_the_one_constant_names_each_system_domains_capability():
    assert SYSTEM_DOMAIN_CAPABILITIES == {
        "meta": "catalog_read",
        "ontology": "glossary_read",
        "ops": "observability",
    }
    assert Capability.CATALOG_READ.value == "catalog_read"


def test_every_other_module_derives_from_it():
    from provisa.api.data import sdl

    assert sdl._ALWAYS_VISIBLE_DOMAINS == set(SYSTEM_DOMAIN_CAPABILITIES)
    assert TRAVERSABLE_DOMAINS == {META_DOMAIN_ID, ONTOLOGY_DOMAIN_ID}


# --- flattening: THE place ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "role_id,reach",
    [
        pytest.param(ROLE, ["meta", "ontology", "pet-store", "shelter"], id="pet_analyst"),
        pytest.param("viewer", ["*", "meta", "ontology"], id="viewer"),
        pytest.param("analyst", ["*", "meta", "ontology"], id="analyst-has-no-ops"),
        pytest.param("org_operator", ["*", "meta", "ontology", "ops"], id="org_operator"),
        pytest.param("org_admin", ["*", "meta", "ontology", "ops"], id="org_admin"),
        pytest.param("platform_admin", [], id="platform_admin-reaches-none"),
        pytest.param(LONER, ["pet-store"], id="no-viewer-ancestor"),
        pytest.param(WILD, ["*"], id="wildcard-with-no-capability"),
        pytest.param(NO_GLOSSARY, ["meta", "pet-store"], id="without-glossary-read"),
    ],
)
def test_a_roles_flattened_list_is_its_data_domains_and_its_capabilities(role_id, reach):
    assert _flat(role_id)["domain_access"] == reach


def test_a_system_domain_comes_with_an_inherited_capability():
    assert _flat(LONER, "observability")["domain_access"] == ["ops", "pet-store"]


def test_data_domains_are_not_inherited():
    rows = [
        {"id": "p", "capabilities": ["catalog_read"], "parents": [], "domain_access": ["a"]},
        {"id": "c", "capabilities": [], "parents": ["p"], "domain_access": ["b"]},
    ]
    assert flatten_role_dicts(rows)[1]["domain_access"] == ["b", "meta"]


def test_a_stored_system_domain_is_no_access_and_the_input_is_not_changed():
    stored = {"id": "x", "capabilities": [], "parents": [], "domain_access": ["ops", "a"]}
    assert flatten_role_dicts([stored])[0]["domain_access"] == ["a"]
    assert stored["domain_access"] == ["ops", "a"]


def test_the_domain_tree_and_a_meta_role_keep_the_system_domains():
    assert domain_tree.expand(["*", "meta"], {}) == ["*", "meta"]
    members = [_flat("analyst"), {**_flat("org_operator"), "domain_access": ["ops", "sales"]}]
    assert meta_role_of(members, "meta:x")["domain_access"] == ["*", "meta", "ontology", "ops"]


@pytest.mark.parametrize("listed", [["meta"], ["sales", "ops"], ["ontology", "*"]])
def test_a_role_may_not_be_saved_listing_a_system_domain(listed):
    problem = role_domain_problem("r", ["usage"], listed)
    assert problem is not None and "capability" in problem


@pytest.mark.parametrize(
    "held,domain,reached",
    [
        (["*"], "pet-store", True),
        (["*"], "meta", False),
        (["*"], "ontology", False),
        (["*"], "ops", False),
        (["*", "meta"], "meta", True),
        (["*", "meta"], "ops", False),
        (["sales"], "meta", False),
    ],
)
def test_the_wildcard_is_every_data_domain(held, domain, reached):
    assert reaches_domain(held, domain) is reached


def test_a_system_domain_is_gated_by_capability_in_single_domain_mode(monkeypatch):
    monkeypatch.setattr(domain_policy, "single_domain", lambda: True)
    assert reaches_domain([], "pet-store")
    assert not reaches_domain([], "ops")
    assert reaches_domain(["ops"], "ops")


def test_nothing_is_changed_in_a_system_domain_by_reaching_it():
    assert domains_within(_flat()["domain_access"]) == {"pet-store", "shelter"}


# --- the grants reach a viewer descendant ---------------------------------------------------------


def test_the_seed_grant_to_viewer_reaches_pet_analyst_through_analyst():
    assert VIEWER_ROLE in _chains()[ROLE]
    catalog, terms = (t for t in _tables() if t["id"] in (CATALOG, TERMS))
    for table in (catalog, terms):
        assert all(ROLE in c["visible_to"] for c in table["columns"])


def test_a_capability_names_no_column():
    # The grants govern: holding catalog_read with no grant serves nothing.
    tables = _tables()
    for table in tables:
        for column in table["columns"]:
            column["visible_to"] = [VIEWER_ROLE]
    assert visible_tables(tables, _flat()) == []


# --- GraphQL --------------------------------------------------------------------------------------


def test_graphql_pet_analyst_reaches_meta_and_ontology_and_not_ops():
    query = generate_schema(_schema_input(_flat())).query_type
    assert query is not None
    wildcard = generate_schema(_schema_input(_flat("org_admin"))).query_type
    assert wildcard is not None
    assert _system_fields(query.fields) == _system_fields(wildcard.fields) != set()
    assert PETS_NAME in query.fields
    assert not _ops_fields(query.fields)
    assert _ops_fields(wildcard.fields)


def test_graphql_analyst_does_not_reach_ops():
    query = generate_schema(_schema_input(_flat("analyst"))).query_type
    assert query is not None and not _ops_fields(query.fields) and _system_fields(query.fields)


def test_graphql_a_role_with_no_viewer_ancestor_reaches_neither():
    query = generate_schema(_schema_input(_flat(LONER))).query_type
    assert query is not None
    assert PETS_NAME in query.fields
    assert not _system_fields(query.fields)


def test_graphql_a_wildcard_role_with_no_capability_reaches_no_system_domain():
    query = generate_schema(_schema_input(_flat(WILD))).query_type
    assert query is not None
    assert PETS_NAME in query.fields
    assert not _system_fields(query.fields) and not _ops_fields(query.fields)


def test_graphql_a_role_not_holding_glossary_read_has_no_ontology():
    query = generate_schema(_schema_input(_flat(NO_GLOSSARY))).query_type
    assert query is not None
    assert not {f for f in query.fields if "terms" in f.lower()}


def test_graphql_serves_ontology_terms_and_meta_columns():
    served = {
        t.domain_id: {c["column_name"] for c in t.visible_columns}
        for t in _build_visible_tables(_schema_input(_flat()))
    }
    assert served[ONTOLOGY_DOMAIN_ID] == {"name", "term_domains"}
    assert {"id", "table_name"} <= served[META_DOMAIN_ID]


@pytest.mark.parametrize(
    "capabilities,shown",
    [pytest.param((), False, id="without"), pytest.param(("view_governance",), True, id="with")],
)
def test_graphql_governance_columns_follow_view_governance(capabilities, shown):
    tables = _build_visible_tables(_schema_input(_flat(ROLE, *capabilities)))
    catalog = next(t for t in tables if t.domain_id == META_DOMAIN_ID)
    assert ("visible_to" in {c["column_name"] for c in catalog.visible_columns}) is shown


def test_grpc_query_roots_match():
    from provisa.grpc.proto_gen import generate_proto

    def query_message(role: dict) -> list[str]:
        proto = generate_proto(_schema_input(role))
        return proto.split("message Query {", 1)[1].split("}", 1)[0].split()

    assert _system_fields(query_message(_flat()))
    assert not _ops_fields(query_message(_flat()))
    assert not _system_fields(query_message(_flat(LONER)))


# --- join traversal -------------------------------------------------------------------------------


def _pets_fields(role: dict) -> set[str]:
    si = _schema_input(role)
    # The catalog table _meta joins to (context._register_meta_synthetic_joins).
    next(t for t in si.tables if t["id"] == CATALOG)["table_name"] = "registered_tables"
    query = generate_schema(si).query_type
    assert query is not None
    return set(get_named_type(query.fields[PETS_NAME].type).fields)  # type: ignore[union-attr]


def test_every_type_has_meta_for_a_role_holding_catalog_read():
    assert "_meta" in _pets_fields(_flat())


def test_a_role_with_no_catalog_has_no_meta_field_and_a_valid_schema():
    assert "_meta" not in _pets_fields(_flat(LONER))


def test_a_join_to_a_reached_system_table_needs_no_relationship_but_ops_does():
    role = _flat("org_admin")
    ctx, gov = _gov(role)

    def v002(sql: str) -> list:
        violations = validate_sql(sql, ctx, gov, role, _tables())
        return [v for v in violations if v.code == "V002"]

    assert not v002(f"SELECT p.id FROM {PETS_NAME} p JOIN {CATALOG_NAME} c ON p.id = c.table_name")
    assert v002(f"SELECT p.id FROM {PETS_NAME} p JOIN {QUERIES_NAME} q ON p.id = q.id")


# --- SQL ------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(f"SELECT table_name FROM {CATALOG_NAME}", id="meta"),
        pytest.param(f"SELECT name FROM {TERMS_NAME}", id="ontology"),
    ],
)
def test_sql_reads_a_system_table_directly(sql):
    assert _violations(sql, _flat()) == []
    assert _violations(sql, _flat("org_admin")) == []


@pytest.mark.parametrize("role_id", [ROLE, "analyst"])
def test_sql_refuses_ops_without_observability(role_id):
    violations = _violations(f"SELECT id FROM {QUERIES_NAME}", _flat(role_id))
    assert [v.code for v in violations if v.code == "V001"] == ["V001"]
    assert _violations(f"SELECT id FROM {QUERIES_NAME}", _flat("org_admin")) == []


def test_sql_governance_columns_follow_view_governance():
    sql = f"SELECT visible_to FROM {CATALOG_NAME}"
    assert [v for v in _violations(sql, _flat()) if v.code == "V003"]
    assert _violations(sql, _flat(ROLE, "view_governance")) == []


def test_sql_rows_stay_governed_by_the_tiers_and_the_glossary():
    _, gov = _gov(_flat())
    # meta: REQ-1132's default tier: the tables it reads, not the whole catalog. Ops is not read.
    assert gov.rls_rules[CATALOG] == f"id IN ({PETS},{CATALOG},{TERMS})"
    # ontology: the glossary's own rule, scoped to the role's data domains.
    assert "pet-store" in gov.rls_rules[TERMS] and "shelter" in gov.rls_rules[TERMS]
    assert "'meta'" not in gov.rls_rules[TERMS]
    # A role reaching every data domain has neither.
    _, whole = _gov(_flat("org_admin"))
    assert CATALOG not in whole.rls_rules and TERMS not in whole.rls_rules


def test_the_catalog_describes_what_the_role_reads():
    assert compute_meta_row_scope(_flat(), _tables(), []) == {PETS, CATALOG, TERMS}
    assert compute_meta_row_scope(_flat("org_admin"), _tables(), []) is None


# --- the refusal names the capability -------------------------------------------------------------


def _built(role: dict):
    si = _schema_input(role)
    return generate_schema(si), build_context(si)


@pytest.mark.parametrize(
    "query,domain,capability",
    [
        pytest.param(
            "{ registeredTablesMeta { id } }", "meta", "catalog_read", id="a-catalog-root"
        ),
        pytest.param("{ pets { id _meta { id } } }", "meta", "catalog_read", id="the-meta-field"),
        pytest.param("{ op_queries { id } }", "ops", "observability", id="an-ops-root"),
    ],
)
def test_graphql_refuses_naming_the_missing_capability(query, domain, capability):
    schema, ctx = _built(_flat(LONER))
    with pytest.raises(MissingDomainCapability) as refused:
        parse_query(schema, query, ctx=ctx)
    assert refused.value.capability == capability and refused.value.domain_id == domain
    assert capability in str(refused.value)


def test_graphql_an_unknown_field_is_still_a_validation_error():
    schema, ctx = _built(_flat(LONER))
    with pytest.raises(GraphQLValidationError):
        parse_query(schema, "{ nothingOfTheKind { id } }", ctx=ctx)


def test_sql_refuses_the_catalog_naming_the_missing_capability():
    role = _flat(LONER)
    _, ctx = _built(role)
    gov = build_governance_context(role["id"], RLSContext.empty(), {}, ctx, _tables(), role)
    violations = validate_sql(f"SELECT id FROM {CATALOG_NAME}", ctx, gov, role, _tables())
    assert [v.code for v in violations] == ["V001"]
    assert "catalog_read" in violations[0].message


# --- the rest of the one rule ---------------------------------------------------------------------


def test_visible_tables_and_served_roles_agree_with_the_schema():
    domains = {t["domain_id"] for t in visible_tables(_tables(), _flat())}
    assert domains == {"pet-store", META_DOMAIN_ID, ONTOLOGY_DOMAIN_ID}
    terms = next(t for t in _tables() if t["id"] == TERMS)
    assert served_roles({ROLE: _flat()}, _chains(), ONTOLOGY_DOMAIN_ID, terms["columns"][0]) == {
        ROLE
    }


def test_effective_rights_say_what_is_read_and_no_write_comes_with_it():
    from provisa.security.effective_rights import data_view

    view = data_view(
        ROLE,
        tables=_tables(),
        reach=_flat()["domain_access"],
        chain=_chains()[ROLE],
        parents={ROLE: ["analyst"]},
        stored={},
        masks={},
        row_rule=lambda _tid, _domain: None,
        readable=lambda _tid, _name: True,
    )
    by_domain = {row["domain"]: row for row in view}
    assert all(c["read"] for c in by_domain[ONTOLOGY_DOMAIN_ID]["columns"])
    assert all(c["write"] == [] for c in by_domain[ONTOLOGY_DOMAIN_ID]["columns"])
    assert all(c["write"] == [] for c in by_domain[META_DOMAIN_ID]["columns"])
    assert by_domain["pet-store"]["columns"][0]["write"]


def test_a_cypher_match_may_root_where_the_capability_is_held():
    def nodes(role: dict) -> dict:
        lm = CypherLabelMap.from_schema(_ctx(), domain_access=role["domain_access"])
        return {n.domain_id: n.traversal_only for n in lm.nodes.values()}

    held = nodes(_flat())
    assert held[META_DOMAIN_ID] is False and held[ONTOLOGY_DOMAIN_ID] is False
    assert held[OPS_DOMAIN_ID] is True
    lone = nodes(_flat(LONER))
    assert lone[META_DOMAIN_ID] is True and lone[ONTOLOGY_DOMAIN_ID] is True


# --- the HTTP surface the UI reads ----------------------------------------------------------------


def _domains_response(monkeypatch, role: dict) -> list[str]:
    from provisa.api import app as appmod
    from provisa.api.data.sdl import get_domains

    state = types.SimpleNamespace(
        roles={role["id"]: role},
        schema_build_cache={
            "domains": [{"id": d} for d in ("", "meta", "ontology", "ops", "pet-store", "hr")]
        },
    )
    monkeypatch.setattr(appmod, "state", state)
    request = types.SimpleNamespace(state=types.SimpleNamespace(role=role["id"]))
    response = asyncio.run(get_domains(request, role["id"], None))  # type: ignore[arg-type]
    return sorted(json.loads(response.body))


@pytest.mark.parametrize(
    "role_id,shown",
    [
        pytest.param(ROLE, ["meta", "ontology", "pet-store"], id="pet_analyst"),
        pytest.param("analyst", ["hr", "meta", "ontology", "pet-store"], id="wildcard-no-ops"),
        pytest.param(
            "org_admin", ["hr", "meta", "ontology", "ops", "pet-store"], id="wildcard-with-ops"
        ),
        pytest.param(LONER, ["pet-store"], id="no-viewer-ancestor"),
    ],
)
def test_data_domains_answers_the_flattened_list(monkeypatch, role_id, shown):
    assert _domains_response(monkeypatch, _flat(role_id)) == shown


def test_a_per_domain_schema_may_name_a_system_domain_the_role_holds():
    from provisa.api.data.sdl import require_reached_domains
    from provisa.api.errors import ApiError

    require_reached_domains(ROLE, _flat(), ["pet-store", "meta", "ontology"])
    with pytest.raises(ApiError):
        require_reached_domains(ROLE, _flat(), ["ops"])


# --- no write and no command comes with the read --------------------------------------------------


def test_no_mutation_field_on_a_system_table_for_a_listed_role():
    schema = generate_schema(_schema_input(_flat(ROLE, "write")))
    assert schema.mutation_type is not None
    assert not _system_fields(schema.mutation_type.fields)
    assert not _ops_fields(schema.mutation_type.fields)


def test_a_write_grant_on_a_system_table_admits_no_sql_write_for_a_listed_role():
    # write capability, a grant on the table, a table that takes writes: only the list is missing.
    _, gov = _gov(_flat(ROLE, "write"))
    for sql in (f"DELETE FROM {TERMS_NAME}", f"DELETE FROM {CATALOG_NAME}"):
        with pytest.raises(PermissionError):
            admit_write(sqlglot.parse_one(sql, read="postgres"), gov)


def test_a_write_within_reach_is_still_admitted():
    _, gov = _gov(_flat(ROLE, "write"))
    admit_write(sqlglot.parse_one(f"DELETE FROM {PETS_NAME}", read="postgres"), gov)


def test_a_role_reaching_every_domain_keeps_its_write_where_it_holds_the_capability():
    _, gov = _gov(_flat("org_admin"))
    admit_write(sqlglot.parse_one(f"DELETE FROM {TERMS_NAME}", read="postgres"), gov)


@pytest.mark.parametrize(
    "role_id,domain,admitted",
    [
        (ROLE, META_DOMAIN_ID, True),
        (ROLE, ONTOLOGY_DOMAIN_ID, False),
        (ROLE, OPS_DOMAIN_ID, False),
        (ROLE, "pet-store", True),
        (LONER, META_DOMAIN_ID, False),
        ("analyst", OPS_DOMAIN_ID, False),
        ("org_admin", OPS_DOMAIN_ID, True),
    ],
)
def test_command_admission_is_not_widened_by_a_capability(role_id, domain, admitted):
    command = {"domain_id": domain, "visible_to": [role_id]}
    assert command_reachable(command, _flat(role_id)) is admitted


def test_a_listed_role_holding_observability_is_admitted_no_ops_command():
    command = {"domain_id": OPS_DOMAIN_ID, "visible_to": [ROLE]}
    assert not command_reachable(command, _flat(ROLE, "observability"))
