# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2b8f61-0c47-4e9a-a3f6-81b7e2c4d095
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An own-domain column is served only to a role whose own domain access names its table's domain
(REQ-2153, amended 2026-11-04).

legal > executive_litigation. executive_settlements, in executive_litigation, has an own-domain
settlement_amount granted to viewer and exec_counsel, and a domain-scoped case_id granted to both.
viewer lists legal, so it reaches executive_litigation through the domain tree (REQ-1363) but its
own list does not name it: it is not served settlement_amount on any surface, though its grant
names it, and it is still served case_id. exec_counsel lists executive_litigation and reads both.
A role listing ``*`` names every domain.

The roles load as the runtime build loads them (api.app): each role's own list is kept before the
tree folds the domains under each listed one in, the grants are folded down the role chains, and
an own-domain column's grant is narrowed to the roles whose own list names its domain.
"""

# Requirements: REQ-2153, REQ-1363, REQ-2154

from __future__ import annotations

import copy

import pytest
from graphql import GraphQLObjectType

from provisa.api.admin.catalog_scope import CatalogScope
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import _build_visible_tables, generate_schema
from provisa.compiler.schema_types import SchemaInput
from provisa.core.column_scope import (
    COLUMN_SCOPES,
    SCOPE_OWN_DOMAIN,
    require_column_scope,
)
from provisa.core.models import Column
from provisa.security import domain_tree
from provisa.security.inheritance import (
    expand_column_grants,
    flatten_role_dicts,
    narrow_own_domain_grants,
    role_chains,
    with_own_domains,
)
from provisa.security.rights import column_served, reaches_domain, served_roles
from provisa.security.visibility import visible_tables

LEGAL, EXEC_LIT = "legal", "executive_litigation"
PARENTS: dict[str, str | None] = {LEGAL: None, EXEC_LIT: LEGAL}

GRANTED = ["viewer", "exec_counsel", "auditor"]


def _column(name: str, scope: str, visible_to: list[str]) -> dict:
    return {
        "column_name": name,
        "visible_to": list(visible_to),
        "scope": scope,
        "native_filter_type": None,
    }


SETTLEMENTS = {
    "id": 1,
    "domain_id": EXEC_LIT,
    "source_id": "pg",
    "schema_name": "public",
    "table_name": "executive_settlements",
    "columns": [
        _column("case_id", "domain", GRANTED),
        _column("settlement_amount", SCOPE_OWN_DOMAIN, GRANTED),
    ],
    "write_ops": [],
    "write_returns_rows": True,
    "write_refused_forms": [],
}


def _stored_role(role_id: str, *domains: str) -> dict:
    return {
        "id": role_id,
        "description": "Test role.",
        "domain_access": list(domains),
        "capabilities": ["query_development"],
        "parents": [],
    }


STORED_ROLES = [
    _stored_role("viewer", LEGAL),
    _stored_role("exec_counsel", EXEC_LIT),
    _stored_role("auditor", "*"),
]


def _build() -> tuple[dict[str, dict], list[dict]]:
    """(role id → role, tables) as the runtime build loads them (api.app)."""
    roles = flatten_role_dicts(
        domain_tree.flatten_role_domains(with_own_domains(STORED_ROLES), PARENTS)
    )
    tables = [copy.deepcopy(SETTLEMENTS)]
    expand_column_grants(tables, role_chains(STORED_ROLES))
    narrow_own_domain_grants(tables, roles)
    return {r["id"]: r for r in roles}, tables


def _served_by_rule(role_id: str) -> set[str]:
    roles, tables = _build()
    role = roles[role_id]
    table = tables[0]
    reaches = reaches_domain(role["domain_access"], table["domain_id"])
    return {
        c["column_name"]
        for c in table["columns"]
        if column_served(role, table["domain_id"], c, reaches=reaches)
    }


def _schema_input(role_id: str) -> SchemaInput:
    roles, tables = _build()
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
        role=roles[role_id],
        domains=[{"id": LEGAL}, {"id": EXEC_LIT, "parent_domain_id": LEGAL}],
    )


def _graphql_fields(role_id: str) -> set[str]:
    schema = generate_schema(_schema_input(role_id))
    query = schema.query_type
    assert query is not None
    field = query.fields["executiveSettlements"]  # the default naming rule
    of = field.type
    while not isinstance(of, GraphQLObjectType):
        of = of.of_type  # type: ignore[union-attr]
    return set(of.fields)


def test_own_domain_is_a_column_scope():
    assert SCOPE_OWN_DOMAIN == "own-domain"
    assert SCOPE_OWN_DOMAIN in COLUMN_SCOPES
    assert require_column_scope("settlement_amount", "own-domain") == "own-domain"
    assert Column(name="settlement_amount", scope="own-domain", visible_to=[]).scope == (
        "own-domain"
    )


def test_viewer_reaches_the_child_domain_through_its_ancestor():
    """The premise: viewer reaches executive_litigation, so a domain column is its."""
    roles, _ = _build()
    assert reaches_domain(roles["viewer"]["domain_access"], EXEC_LIT)
    assert roles["viewer"]["own_domain_access"] == [LEGAL]


@pytest.mark.parametrize(
    ("role_id", "served"),
    [
        ("exec_counsel", {"case_id", "settlement_amount"}),
        ("viewer", {"case_id"}),
        ("auditor", {"case_id", "settlement_amount"}),
    ],
)
def test_column_served(role_id, served):
    assert _served_by_rule(role_id) == served


@pytest.mark.parametrize(
    ("role_id", "served"),
    [
        ("exec_counsel", ["case_id", "settlement_amount"]),
        ("viewer", ["case_id"]),
        ("auditor", ["case_id", "settlement_amount"]),
    ],
)
def test_the_role_schema_build(role_id, served):
    tables = _build_visible_tables(_schema_input(role_id))
    assert [[c["column_name"] for c in t.visible_columns] for t in tables] == [served]


def test_graphql_schema_hides_the_column_from_viewer():
    assert "settlementAmount" not in _graphql_fields("viewer")
    assert "caseId" in _graphql_fields("viewer")
    assert {"caseId", "settlementAmount"} <= _graphql_fields("exec_counsel")
    assert {"caseId", "settlementAmount"} <= _graphql_fields("auditor")


def test_sql_catalog_visibility():
    roles, tables = _build()
    columns = {
        rid: [c["column_name"] for t in visible_tables(tables, roles[rid]) for c in t["columns"]]
        for rid in ("viewer", "exec_counsel", "auditor")
    }
    assert columns == {
        "viewer": ["case_id"],
        "exec_counsel": ["case_id", "settlement_amount"],
        "auditor": ["case_id", "settlement_amount"],
    }


def test_admin_catalog_stored_columns():
    """A table no build compiled yet is judged on its stored columns, narrowed as the build
    narrows them."""
    roles, _ = _build()

    def stored(role_id: str) -> set[str]:
        role = roles[role_id]
        scope = CatalogScope(
            whole=False,
            reach=None,
            served={},
            governance=False,
            role=role_id,
            subjects=(
                {
                    "id": role_id,
                    "domain_access": role["domain_access"],
                    "own_domain_access": role["own_domain_access"],
                },
            ),
        )
        return scope.stored_columns(copy.deepcopy(SETTLEMENTS))

    assert stored("viewer") == {"case_id"}
    assert stored("exec_counsel") == {"case_id", "settlement_amount"}


def test_served_roles():
    """The served set the profiler and the metadata export read (REQ-2152)."""
    roles, _ = _build()
    chains = role_chains(STORED_ROLES)
    amount, case_id = SETTLEMENTS["columns"][1], SETTLEMENTS["columns"][0]
    assert served_roles(roles, chains, EXEC_LIT, amount) == {"exec_counsel", "auditor"}
    assert served_roles(roles, chains, EXEC_LIT, case_id) == {"viewer", "exec_counsel", "auditor"}


def test_a_child_role_granted_through_its_parent_is_narrowed_by_its_own_list():
    """A grant to a parent folds down to its children; an own-domain grant then keeps a child only
    when the child's own list names the domain."""
    stored = [
        _stored_role("exec_counsel", EXEC_LIT),
        {**_stored_role("junior", EXEC_LIT), "parents": ["exec_counsel"]},
        {**_stored_role("legal_child", LEGAL), "parents": ["exec_counsel"]},
    ]
    roles = flatten_role_dicts(domain_tree.flatten_role_domains(with_own_domains(stored), PARENTS))
    table = {
        "domain_id": EXEC_LIT,
        "columns": [_column("settlement_amount", SCOPE_OWN_DOMAIN, ["exec_counsel"])],
    }
    expand_column_grants([table], role_chains(stored))
    narrow_own_domain_grants([table], roles)
    assert sorted(table["columns"][0]["visible_to"]) == ["exec_counsel", "junior"]


def test_domain_listed_and_public_keep_their_grants():
    roles, _ = _build()
    table = {
        "domain_id": EXEC_LIT,
        "columns": [_column(f"c_{s}", s, ["viewer"]) for s in ("domain", "listed", "public")],
    }
    narrow_own_domain_grants([table], roles.values())
    assert [c["visible_to"] for c in table["columns"]] == [["viewer"]] * 3
