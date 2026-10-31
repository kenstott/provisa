# Copyright (c) 2026 Kenneth Stott
# Canary: 6e3f0b52-8f1d-4a57-9c23-5d0a7b41e9c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role with a named domain list reads meta and ontology as a ``"*"`` role does.

pet_analyst (["pet-store", "shelter"], parent analyst) is the case the maintainer reported: analyst
reached the catalog and the glossary and pet_analyst did not. Each read surface is shown here with
the same role id twice, once listing its domains and once ``"*"``, so the only thing differing is
the list (REQ-1132, REQ-2163, REQ-2155). Rows stay governed: meta by REQ-1132's tiers, ontology by
the glossary's rule. No write and no command comes with the read.
"""

from __future__ import annotations

import asyncio
import json
import types

import pytest
import sqlglot

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.naming import SYSTEM_SCHEMA_GQL_PREFIX
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import _build_visible_tables, generate_schema
from provisa.compiler.schema_types import SchemaInput
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import build_governance_context
from provisa.compiler.write_admission import admit_write
from provisa.core import domain_policy
from provisa.core.db import standard_role_dicts
from provisa.gql.label_map import CypherLabelMap
from provisa.security.default_grant import VIEWER_ROLE
from provisa.security.inheritance import expand_column_grants, role_chains
from provisa.security.mutation_authz import command_reachable
from provisa.security.rights import (
    IMPLICIT_DOMAINS,
    META_DOMAIN_ID,
    ONTOLOGY_DOMAIN_ID,
    Capability,
    compute_meta_row_scope,
    reaches_implicitly,
    reads_domain,
    served_roles,
)
from provisa.security.visibility import visible_tables

ROLE = "pet_analyst"
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
    QUERIES: "ops",
}
NAME_OF = {PETS: PETS_NAME, CATALOG: CATALOG_NAME, TERMS: TERMS_NAME, QUERIES: QUERIES_NAME}
COLUMNS = {
    PETS: ("id",),
    CATALOG: ("id", "table_name", "visible_to"),
    TERMS: ("name", "term_domains"),
    QUERIES: ("id",),
}
LISTED = ["pet-store", "shelter"]


def _chains() -> dict[str, list[str]]:
    """pet_analyst's chain over the STANDARD roles: its parent analyst, whatever analyst inherits."""
    return role_chains([*standard_role_dicts(), {"id": ROLE, "parents": ["analyst"]}])


def _tables() -> list[dict]:
    """The tables as the seed registers them: meta and ontology columns granted to viewer alone
    (startup_seed.py), each table also writable at the source and write-granted to the role, so
    that nothing but the rule under test keeps a write out."""
    tables = [
        {
            "id": tid,
            "domain_id": DOMAIN_OF[tid],
            "source_id": "pg",
            "schema_name": "public",
            "table_name": NAME_OF[tid],
            "columns": [
                {
                    "column_name": name,
                    "visible_to": [VIEWER_ROLE] if tid in (CATALOG, TERMS) else [ROLE],
                    "native_filter_type": None,
                }
                for name in COLUMNS[tid]
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
            "write_grants": [
                {"role": ROLE, "operation": op, "columns": list(COLUMNS[tid])}
                for op in ("insert", "update", "delete")
            ],
        }
        for tid in DOMAIN_OF
    ]
    expand_column_grants(tables, _chains())
    return tables


def _role(domain_access: list[str], *capabilities: str) -> dict:
    return {
        "id": ROLE,
        "description": "Reads the pet domains.",
        "capabilities": list(capabilities),
        "domain_access": domain_access,
    }


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
    return ctx, build_governance_context(ROLE, RLSContext.empty(), {}, ctx, _tables(), role)


def _violations(sql: str, role: dict) -> list:
    ctx, gov = _gov(role)
    return validate_sql(sql, ctx, gov, role, _tables(), bypass_relationship_guard=True)


def _system_fields(fields) -> set[str]:
    """The fields of a root type that belong to meta and ontology."""
    tags = ("registered", "terms")
    return {f for f in fields if any(tag in f.lower() for tag in tags)}


@pytest.fixture(autouse=True)
def multi_domain(monkeypatch):
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)


# --- the one list ---------------------------------------------------------------------------------


def test_the_one_list_names_meta_and_ontology_and_not_ops():
    assert IMPLICIT_DOMAINS == {META_DOMAIN_ID, ONTOLOGY_DOMAIN_ID}


def test_every_other_module_reads_that_list():
    from provisa.api.data import sdl

    assert IMPLICIT_DOMAINS <= sdl._ALWAYS_VISIBLE_DOMAINS
    assert sdl._ALWAYS_VISIBLE_DOMAINS - IMPLICIT_DOMAINS == {"ops"}


@pytest.mark.parametrize(
    "held,domain,reads",
    [
        (LISTED, META_DOMAIN_ID, True),
        (LISTED, ONTOLOGY_DOMAIN_ID, True),
        (LISTED, "pet-store", True),
        (LISTED, "ops", False),
        (LISTED, "finance", False),
        ([], META_DOMAIN_ID, False),
        ([], ONTOLOGY_DOMAIN_ID, False),
        (["*"], "ops", True),
    ],
)
def test_who_reads_a_domain(held, domain, reads):
    assert reads_domain(held, domain) is reads


# --- the grants reach a viewer descendant ---------------------------------------------------------


def test_the_seed_grant_to_viewer_reaches_pet_analyst_through_analyst():
    assert VIEWER_ROLE in _chains()[ROLE]
    catalog, terms = (t for t in _tables() if t["id"] in (CATALOG, TERMS))
    for table in (catalog, terms):
        assert all(ROLE in c["visible_to"] for c in table["columns"])


def test_a_role_with_no_viewer_ancestor_is_served_no_system_column():
    # The grants govern: reaching the domain names no column.
    tables = _tables()
    for table in tables:
        for column in table["columns"]:
            column["visible_to"] = [VIEWER_ROLE]
    served = {t["domain_id"] for t in visible_tables(tables, _role(LISTED))}
    assert served == set()


# --- GraphQL --------------------------------------------------------------------------------------


def test_graphql_serves_the_same_system_roots_as_a_wildcard_role():
    listed = generate_schema(_schema_input(_role(LISTED)))
    wildcard = generate_schema(_schema_input(_role(["*"])))
    assert listed.query_type is not None and wildcard.query_type is not None
    roots = _system_fields(listed.query_type.fields)
    assert roots, "meta and ontology have root fields"
    assert roots == _system_fields(wildcard.query_type.fields)
    # And the role's own domains are as they were; ops stays a domain a role must list.
    assert PETS_NAME in listed.query_type.fields
    assert not {f for f in listed.query_type.fields if "queries" in f.lower()}


def test_graphql_serves_ontology_terms_and_meta_columns():
    served = {
        t.domain_id: {c["column_name"] for c in t.visible_columns}
        for t in _build_visible_tables(_schema_input(_role(LISTED)))
    }
    assert served[ONTOLOGY_DOMAIN_ID] == {"name", "term_domains"}
    assert {"id", "table_name"} <= served[META_DOMAIN_ID]


@pytest.mark.parametrize(
    "capabilities,shown",
    [pytest.param((), False, id="without"), pytest.param(("view_governance",), True, id="with")],
)
def test_graphql_governance_columns_follow_view_governance(capabilities, shown):
    assert Capability.VIEW_GOVERNANCE.value == "view_governance"
    tables = _build_visible_tables(_schema_input(_role(LISTED, *capabilities)))
    catalog = next(t for t in tables if t.domain_id == META_DOMAIN_ID)
    assert ("visible_to" in {c["column_name"] for c in catalog.visible_columns}) is shown


def test_an_empty_list_still_has_no_system_root():
    assert not [
        t
        for t in _build_visible_tables(_schema_input(_role([])))
        if t.domain_id in IMPLICIT_DOMAINS
    ]


def test_grpc_query_roots_match():
    from provisa.grpc.proto_gen import generate_proto

    def query_message(role: dict) -> str:
        return generate_proto(_schema_input(role)).split("message Query {", 1)[1].split("}", 1)[0]

    listed, wildcard = query_message(_role(LISTED)), query_message(_role(["*"]))
    assert _system_fields(listed.split()) == _system_fields(wildcard.split())
    assert _system_fields(listed.split())


# --- SQL ------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(f"SELECT table_name FROM {CATALOG_NAME}", id="meta"),
        pytest.param(f"SELECT name FROM {TERMS_NAME}", id="ontology"),
    ],
)
def test_sql_reads_a_system_table_directly(sql):
    assert _violations(sql, _role(LISTED)) == _violations(sql, _role(["*"])) == []


def test_sql_still_refuses_ops_and_an_unlisted_domain():
    violations = _violations(f"SELECT id FROM {QUERIES_NAME}", _role(LISTED))
    assert [v.code for v in violations if v.code == "V001"] == ["V001"]


def test_sql_governance_columns_follow_view_governance():
    sql = f"SELECT visible_to FROM {CATALOG_NAME}"
    assert [v for v in _violations(sql, _role(LISTED)) if v.code == "V003"]
    assert _violations(sql, _role(LISTED, "view_governance")) == []


def test_sql_rows_stay_governed_by_the_tiers_and_the_glossary():
    _, gov = _gov(_role(LISTED))
    # meta: REQ-1132's default tier. Its own tables and the system tables it is served, not the
    # whole catalog: ops is not described.
    assert gov.rls_rules[CATALOG] == f"id IN ({PETS},{CATALOG},{TERMS})"
    # ontology: the glossary's own rule, scoped to the role's list and not to a system domain.
    assert "pet-store" in gov.rls_rules[TERMS] and "shelter" in gov.rls_rules[TERMS]
    assert META_DOMAIN_ID not in gov.rls_rules[TERMS]
    # A "*" role has neither, so a listed role is not handed the unlisted domains' rows.
    _, whole = _gov(_role(["*"]))
    assert CATALOG not in whole.rls_rules and TERMS not in whole.rls_rules


def test_the_catalog_describes_what_the_role_now_reads():
    assert compute_meta_row_scope(_role(LISTED), _tables(), []) == {PETS, CATALOG, TERMS}
    assert compute_meta_row_scope(_role([]), _tables(), []) == set()


# --- the rest of the one rule ---------------------------------------------------------------------


def test_visible_tables_and_served_roles_agree_with_the_schema():
    domains = {t["domain_id"] for t in visible_tables(_tables(), _role(LISTED))}
    assert domains == {"pet-store", META_DOMAIN_ID, ONTOLOGY_DOMAIN_ID}
    terms = next(t for t in _tables() if t["id"] == TERMS)
    roles = {ROLE: {"domain_access": LISTED}}
    assert served_roles(roles, _chains(), ONTOLOGY_DOMAIN_ID, terms["columns"][0]) == {ROLE}


def test_effective_rights_say_what_is_read_and_no_write_outside_reach():
    from provisa.security.effective_rights import data_view

    view = data_view(
        ROLE,
        tables=_tables(),
        reach=LISTED,
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


def test_a_cypher_match_may_root_in_a_system_domain_for_a_listed_role():
    nodes = CypherLabelMap.from_schema(_ctx(), domain_access=LISTED).nodes.values()
    system = [n for n in nodes if n.domain_id in IMPLICIT_DOMAINS]
    assert {n.domain_id for n in system} == IMPLICIT_DOMAINS
    assert not [n for n in system if n.traversal_only]


def test_an_empty_list_keeps_system_nodes_traversal_only():
    nodes = CypherLabelMap.from_schema(_ctx(), domain_access=[]).nodes.values()
    system = [n for n in nodes if n.domain_id in IMPLICIT_DOMAINS]
    assert system and all(n.traversal_only for n in system)


# --- the HTTP surface the UI reads ----------------------------------------------------------------


def _domains_response(monkeypatch, domain_access: list[str]) -> list[str]:
    from provisa.api import app as appmod
    from provisa.api.data.sdl import get_domains

    state = types.SimpleNamespace(
        roles={ROLE: _role(domain_access)},
        schema_build_cache={
            "domains": [{"id": d} for d in ("", "meta", "ontology", "ops", "pet-store", "hr")]
        },
    )
    monkeypatch.setattr(appmod, "state", state)
    request = types.SimpleNamespace(state=types.SimpleNamespace(role=ROLE))
    return json.loads(asyncio.run(get_domains(request, ROLE, None)).body)  # type: ignore[arg-type]


def test_data_domains_adds_the_system_domains_for_a_named_list(monkeypatch):
    shown = _domains_response(monkeypatch, LISTED)
    assert sorted(shown) == ["meta", "ontology", "pet-store"]


def test_data_domains_gives_an_empty_list_nothing(monkeypatch):
    assert _domains_response(monkeypatch, []) == []


def test_data_domains_gives_a_wildcard_every_domain(monkeypatch):
    assert sorted(_domains_response(monkeypatch, ["*"])) == [
        "hr",
        "meta",
        "ontology",
        "ops",
        "pet-store",
    ]


def test_a_per_domain_schema_may_name_a_system_domain():
    from provisa.api.data.sdl import require_reached_domains
    from provisa.api.errors import ApiError

    require_reached_domains(ROLE, _role(LISTED), ["pet-store", "meta", "ontology"])
    with pytest.raises(ApiError):
        require_reached_domains(ROLE, _role(LISTED), ["ops"])


# --- no write and no command comes with the read --------------------------------------------------


def test_no_mutation_field_on_a_system_table_for_a_listed_role():
    schema = generate_schema(_schema_input(_role(LISTED, "write")))
    assert schema.mutation_type is not None
    assert not _system_fields(schema.mutation_type.fields)


def test_a_write_grant_on_a_system_table_admits_no_sql_write_for_a_listed_role():
    # write capability, a grant on the table, a table that takes writes: only the list is missing.
    _, gov = _gov(_role(LISTED, "write"))
    for sql in (f"DELETE FROM {TERMS_NAME}", f"DELETE FROM {CATALOG_NAME}"):
        with pytest.raises(PermissionError):
            admit_write(sqlglot.parse_one(sql, read="postgres"), gov)


def test_a_write_within_reach_is_still_admitted():
    _, gov = _gov(_role(LISTED, "write"))
    admit_write(sqlglot.parse_one(f"DELETE FROM {PETS_NAME}", read="postgres"), gov)


def test_a_role_that_lists_a_system_domain_keeps_its_write():
    _, gov = _gov(_role([*LISTED, ONTOLOGY_DOMAIN_ID], "write"))
    admit_write(sqlglot.parse_one(f"DELETE FROM {TERMS_NAME}", read="postgres"), gov)


@pytest.mark.parametrize(
    "domain,admitted",
    [(META_DOMAIN_ID, True), (ONTOLOGY_DOMAIN_ID, False), ("ops", False), ("pet-store", True)],
)
def test_command_admission_is_not_widened_to_ontology(domain, admitted):
    command = {"domain_id": domain, "visible_to": [ROLE]}
    assert command_reachable(command, _role(LISTED)) is admitted


def test_command_reach_lists_meta_alone():
    assert reaches_implicitly(LISTED, META_DOMAIN_ID)
    assert not reaches_implicitly(LISTED, ONTOLOGY_DOMAIN_ID)
