# Copyright (c) 2026 Kenneth Stott
# Canary: b4d19f6c-7e23-4c08-9a51-2f6e8d037c14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What each surface describes of the catalog (REQ-1132, REQ-2153).

Every role sees core metadata for the tables of non-system domains it can read, write or list, with
no meta grant, on the pgwire catalog and meta. List means metadata, never data, and applies to SQL:
the gRPC proto, JSON:API, GraphQL, Cypher, SPARQL and OpenAPI show what a role can read or write. A
catalog is scoped to a role's data domains, and to a system domain only where the role holds it.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from graphql import print_schema

from provisa.api.jsonapi.spec import generate_jsonapi_openapi_spec
from provisa.api.rest.openapi_spec import generate_rest_openapi_spec
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import build_table_path_map, generate_schema
from provisa.compiler.schema_types import SchemaInput
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import build_governance_context
from provisa.gql.label_map import CypherLabelMap
from provisa.grpc.proto_gen import generate_proto
from provisa.pgwire.catalog_facts import role_facts
from provisa.security.inheritance import expand_meta_grants
from provisa.security.rights import (
    META_DOMAIN_ID,
    OPS_DOMAIN_ID,
    SYSTEM_DOMAINS,
    VIEWER_ROLE,
    ListedOnly,
    compute_meta_row_scope,
    unlisted_columns,
)
from provisa.sparql_query.vocabulary import Grounding, build_vocabulary
from provisa.transpiler.metadata_oids import table_oid
from tests.rules import mentions

READER = "reader"
ORDERS, PAYROLL, SECRETS, DRAFTS, CATALOG, QUERIES, PRODUCTS = 1, 2, 3, 4, 5, 6, 7
ROLE = {
    "id": READER,
    "description": "Reads sales.",
    "capabilities": ["usage"],
    "domain_access": ["sales"],
    "acting_chain": [READER],  # REQ-2188: as the build records it
}


def _column(name: str, granted: list[str], scope: str = "domain") -> dict:
    return {"column_name": name, "visible_to": granted, "scope": scope, "native_filter_type": None}


def _table(tid: int, domain: str, name: str, columns: list[dict], **more) -> dict:
    return {
        "id": tid,
        "domain_id": domain,
        "source_id": "pg",
        "schema_name": "public",
        "table_name": name,
        "alias": None,
        "columns": columns,
        "write_ops": ["delete", "insert", "update"],
        "write_returns_rows": True,
        "write_refused_forms": [],
        "write_grants": [],
        **more,
    }


def _tables() -> list[dict]:
    tables = [
        _table(ORDERS, "sales", "orders", [_column("id", [READER])]),
        # Listed: its name is every role's, and its amount is nobody's here.
        _table(
            PAYROLL,
            "finance",
            "payroll",
            [_column("id", ["hr"], "listed"), _column("amount", ["hr"])],
        ),
        _table(SECRETS, "finance", "secrets", [_column("id", ["hr"])]),
        # A write grant and no read: it can write it, and it is described in the catalog.
        _table(
            DRAFTS,
            "sales",
            "drafts",
            [_column("id", ["hr"])],
            write_grants=[{"role": READER, "operation": "insert", "columns": ["id"]}],
        ),
        _table(
            CATALOG,
            META_DOMAIN_ID,
            "registered_tables_meta",
            [_column("id", [VIEWER_ROLE]), _column("visible_to", [VIEWER_ROLE])],
        ),
        _table(QUERIES, OPS_DOMAIN_ID, "queries", [_column("id", [VIEWER_ROLE])]),
        _table(PRODUCTS, META_DOMAIN_ID, "data_products_meta", [_column("id", [VIEWER_ROLE])]),
    ]
    expand_meta_grants(tables, [ROLE])
    return tables


def _input(role: dict = ROLE) -> SchemaInput:
    tables = _tables()
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types={
            t["id"]: [
                ColumnMetadata(column_name=c["column_name"], data_type="varchar", is_nullable=True)
                for c in t["columns"]
            ]
            for t in tables
        },
        naming_rules=[],
        role=role,
        domains=[{"id": d} for d in ("sales", "finance", META_DOMAIN_ID, OPS_DOMAIN_ID)],
    )


def _state(role: dict = ROLE) -> SimpleNamespace:
    si = _input(role)
    surface = SimpleNamespace(schema=generate_schema(si), table_path_map=build_table_path_map(si))
    ctx = build_context(si)
    return SimpleNamespace(
        surfaces={role["id"]: surface},
        contexts={role["id"]: ctx},
        roles={role["id"]: role},
        schema_build_cache={"tables": _tables()},
    )


@pytest.fixture
def rest_surface(monkeypatch):
    state = _state()
    for module in ("provisa.api.rest.openapi_spec", "provisa.api.jsonapi.spec"):
        monkeypatch.setattr(
            f"{module}.role_surface", lambda _s, rid: state.surfaces[rid], raising=False
        )
    return state


def _shown_on(surface: str, state: SimpleNamespace | None = None) -> str:
    """What ``surface`` describes to the role, as text."""
    si = _input()
    if surface == "graphql":
        return print_schema(generate_schema(si))
    if surface == "grpc":
        return generate_proto(si)
    if surface == "cypher":
        return " ".join(CypherLabelMap.from_schema(build_context(si), ROLE["domain_access"]).nodes)
    if surface == "sparql":
        lm = CypherLabelMap.from_schema(build_context(si), ROLE["domain_access"])
        vocabulary = build_vocabulary(lm, "https://example.test", {}, Grounding())
        return " ".join(vocabulary.classes)
    assert state is not None
    generate = generate_rest_openapi_spec if surface == "openapi" else generate_jsonapi_openapi_spec
    return " ".join(generate(state, READER)["paths"])


@pytest.mark.parametrize("surface", ["graphql", "grpc", "cypher", "sparql"])
def test_a_surface_shows_what_a_role_reads_and_no_table_it_only_lists(surface):
    shown = _shown_on(surface).lower()
    assert "orders" in shown
    assert "payroll" not in shown
    assert "secrets" not in shown


@pytest.mark.parametrize("surface", ["graphql", "grpc", "cypher", "sparql"])
def test_a_surface_offers_no_meta_table_to_a_role_with_no_meta_grant(surface):
    shown = _shown_on(surface).lower()
    assert "registered" not in shown and "products" not in shown


@pytest.mark.parametrize("surface", ["openapi", "jsonapi"])
def test_an_api_document_shows_what_a_role_reads_and_no_table_it_only_lists(surface, rest_surface):
    shown = _shown_on(surface, rest_surface)
    assert "/sales/orders" in shown
    assert "payroll" not in shown
    assert "secrets" not in shown


class TestTheDefaultTier:
    def test_meta_scope_is_what_it_reads_writes_or_lists(self):
        assert compute_meta_row_scope(ROLE, _tables(), []) == {ORDERS, DRAFTS, PAYROLL}

    def test_a_system_table_is_not_described_with_no_grant_to_its_domain(self):
        scope = compute_meta_row_scope(ROLE, _tables(), [])
        assert scope is not None and not scope & {CATALOG, QUERIES, PRODUCTS}

    def test_a_system_domain_is_described_to_a_role_holding_it(self):
        role = {**ROLE, "domain_access": ["sales", META_DOMAIN_ID]}
        served = [
            {**t, "columns": [{**c, "visible_to": [READER]} for c in t["columns"]]}
            if t["domain_id"] == META_DOMAIN_ID
            else t
            for t in _tables()
        ]
        assert CATALOG in (compute_meta_row_scope(role, served, []) or set())

    def test_every_data_domain_without_every_system_domain_is_not_the_whole_catalog(self):
        role = {**ROLE, "domain_access": ["*", META_DOMAIN_ID]}
        scope = compute_meta_row_scope(role, _tables(), [])
        assert scope == {ORDERS, PAYROLL, SECRETS, DRAFTS, CATALOG, PRODUCTS}

    def test_every_domain_of_both_kinds_is_the_whole_catalog(self):
        role = {**ROLE, "domain_access": ["*", *SYSTEM_DOMAINS]}
        assert compute_meta_row_scope(role, _tables(), []) is None

    def test_a_listed_tables_other_columns_are_not_named(self):
        assert unlisted_columns(ROLE, _tables()) == {(PAYROLL, "amount")}

    def test_a_table_it_reads_keeps_its_columns(self):
        hr = {**ROLE, "id": "hr", "domain_access": ["finance"]}
        assert unlisted_columns(hr, _tables()) == set()

    def test_its_core_columns_are_served_and_its_governance_columns_are_not(self):
        catalog = next(t for t in _tables() if t["id"] == CATALOG)
        id_column, governance = catalog["columns"]
        assert READER in id_column["visible_to"]
        assert READER not in governance["visible_to"]

    def test_a_meta_table_beyond_the_tier_is_not_served(self):
        products = next(t for t in _tables() if t["id"] == PRODUCTS)
        assert READER not in products["columns"][0]["visible_to"]

    def test_catalog_read_with_no_viewer_ancestor_is_served_meta(self):
        role = {**ROLE, "capabilities": ["catalog_read"]}
        tables = _tables()
        expand_meta_grants(tables, [role])
        products = next(t for t in tables if t["id"] == PRODUCTS)
        assert READER in products["columns"][0]["visible_to"]

    def test_a_column_taken_from_viewer_stays_taken(self):
        table = _table(CATALOG, META_DOMAIN_ID, "table_details", [_column("id", ["steward"])])
        expand_meta_grants([table], [ROLE])
        assert table["columns"][0]["visible_to"] == ["steward"]

    def test_a_role_with_no_capabilities_is_served_no_catalog(self):
        tables = _tables()
        expand_meta_grants(tables, [{**ROLE, "id": "empty", "capabilities": []}])
        catalog = next(t for t in tables if t["id"] == CATALOG)
        assert "empty" not in catalog["columns"][0]["visible_to"]


class TestListIsMetadataOnly:
    def _gov(self):
        si = _input()
        ctx = build_context(si)
        gov = build_governance_context(
            READER, RLSContext.empty(), {}, ctx, _tables(), ROLE, relationships=[]
        )
        return ctx, gov

    def test_a_statement_reading_a_listed_table_is_refused_by_name(self):
        ctx, gov = self._gov()
        violations = validate_sql("SELECT id FROM finance.payroll", ctx, gov, ROLE, _tables())
        refused = [v for v in violations if v.code == "V001"]
        assert refused and "can only list 'finance.payroll'" in refused[0].message

    def test_a_table_outside_reach_and_unlisted_is_not_said_to_be_listed(self):
        ctx, gov = self._gov()
        violations = validate_sql("SELECT id FROM finance.secrets", ctx, gov, ROLE, _tables())
        assert violations and not [v for v in violations if "list" in v.message]

    def test_a_graphql_field_for_a_listed_table_is_refused_by_name(self):
        si = _input()
        ctx = build_context(si)
        with pytest.raises(ListedOnly, match="can only list 'finance.payroll'"):
            parse_query(generate_schema(si), "{ payroll { id } }", ctx=ctx)

    def test_the_meta_scope_predicates_name_the_listed_table(self):
        _, gov = self._gov()
        assert mentions(gov.rls_rules[CATALOG], PAYROLL)
        assert gov.meta_scope == "{" + f"{ORDERS},{PAYROLL},{DRAFTS}" + "}"


class TestCatalogFunctions:
    def _oids(self) -> set[int]:
        return {oid for oid, _privilege in role_facts(READER, _state()).privileges}

    def test_a_privilege_answer_leaves_out_a_system_table_it_holds_no_grant_to(self):
        assert table_oid(CATALOG) not in self._oids()

    def test_a_privilege_answer_names_a_table_it_reads(self):
        assert table_oid(ORDERS) in self._oids()
