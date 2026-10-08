# Copyright (c) 2026 Kenneth Stott
# Canary: 0c8a5e21-4f79-4b16-a3d2-7e1b9c6f4d50
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A public column is served outside the domains a role reaches (REQ-1959).

A column's ``scope`` was stored and read by nothing. It now decides where the column is served:
``domain`` within the domains a role reaches; ``public`` there and outside them; ``restricted``
only to whom ``visible_to`` names. Publishing removes the domain condition for that column and
nothing else: its own grant still decides who, and a table outside a role's reach is served with
its PUBLIC columns only. A parameter column publishes nothing; a lockdown domain's columns are
never served across domains.

The rule is one function (``security.rights.column_served``); the schema build, SQL governance,
the direct-read check (V001) and the catalog's row scope read it.
"""

# Requirements: REQ-1959, REQ-039, REQ-1133

from __future__ import annotations

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import _build_visible_tables
from provisa.compiler.schema_types import SchemaInput
from provisa.core.column_scope import ColumnScopeInvalid, require_column_scope
from provisa.core.models import Column
from provisa.security.rights import (
    column_served,
    compute_meta_row_scope,
    served_across_domains,
)
from provisa.security.visibility import visible_tables

HR, SALES, OPS = "hr", "sales", "ops"


def _column(name: str, scope: str = "domain", visible_to: list[str] | None = None, **extra):
    return {
        "column_name": name,
        "visible_to": visible_to or [],
        "scope": scope,
        "native_filter_type": None,
        **extra,
    }


def _table(table_id: int, domain: str, name: str, columns: list[dict]) -> dict:
    return {
        "id": table_id,
        "domain_id": domain,
        "source_id": "pg",
        "schema_name": "public",
        "table_name": name,
        "columns": columns,
        "write_ops": [],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }


# hr.staff: a published name, a domain-only salary, a public column granted to one role only,
# and a restricted column granted to nobody.
STAFF = _table(
    1,
    HR,
    "staff",
    [
        _column("id", "public"),
        _column("name", "public"),
        _column("salary", "domain"),
        _column("badge", "public", ["auditor"]),
        _column("ssn", "restricted"),
    ],
)
# hr.reviews: nothing published.
REVIEWS = _table(2, HR, "reviews", [_column("id"), _column("score")])
# sales.orders: the seller's own table.
ORDERS = _table(3, SALES, "orders", [_column("id"), _column("staff_id")])
# An API table whose only "public" columns are its parameters — the register form's old default.
LOOKUP = _table(
    4,
    HR,
    "get_staff_by_id",
    [
        _column("title", "domain"),
        _column("staff_id", "public", native_filter_type="path_param"),
    ],
)
# ops is a lockdown domain: nothing in it is served across domains.
AUDIT = _table(5, OPS, "audit_log", [_column("id", "public", ["seller"]), _column("who", "public")])
TABLES = [STAFF, REVIEWS, ORDERS, LOOKUP, AUDIT]


def _role(role_id: str, *domains: str) -> dict:
    return {"id": role_id, "domain_access": list(domains), "capabilities": []}


def _served(role: dict) -> dict[str, list[str]]:
    """table → data columns, as the schema build serves ``role``."""
    si = SchemaInput(
        tables=TABLES,
        relationships=[],
        column_types={
            t["id"]: [
                ColumnMetadata(column_name=c["column_name"], data_type="varchar", is_nullable=True)
                for c in t["columns"]
            ]
            for t in TABLES
        },
        naming_rules=[],
        role=role,
        domains=[{"id": HR}, {"id": SALES}, {"id": OPS}],
    )
    return {
        info.table_name: [c["column_name"] for c in info.visible_columns]
        for info in _build_visible_tables(si)
    }


# --- the served set -------------------------------------------------------------------------------


def test_a_role_with_no_reach_is_served_the_public_columns_and_only_those():
    served = _served(_role("seller", SALES))
    assert served["staff"] == ["id", "name"]  # not salary (domain), badge (not its grant), ssn
    assert "reviews" not in served, "a table publishing nothing is not served"
    assert served["orders"] == ["id", "staff_id"]


def test_a_role_that_reaches_the_domain_is_served_as_before():
    served = _served(_role("hr_reader", HR))
    assert served["staff"] == ["id", "name", "salary"]  # badge is another role's; ssn nobody's
    assert served["reviews"] == ["id", "score"]
    assert "orders" not in served


def test_a_public_columns_own_grant_still_decides_who():
    assert "badge" not in _served(_role("seller", SALES))["staff"]
    assert _served(_role("auditor", SALES))["staff"] == ["id", "name", "badge"]
    assert _served(_role("auditor", HR))["staff"] == ["id", "name", "salary", "badge"]


def test_a_restricted_column_is_served_only_to_whom_it_names():
    """An empty grant is everyone on a domain column and nobody on a restricted one."""
    for role in (_role("hr_reader", HR), _role("anyone", "*")):
        assert "ssn" not in _served(role)["staff"]
    named = {**STAFF, "columns": [_column("ssn", "restricted", ["hr_reader"])]}
    assert column_served(_role("hr_reader", HR), HR, named["columns"][0], reaches=True)
    assert not column_served(_role("seller", SALES), HR, named["columns"][0], reaches=False)


def test_a_parameter_column_publishes_nothing():
    """Only data columns count: an API table whose parameters carry ``public`` (the register
    form's old default) is not served to a role outside its domain."""
    assert "get_staff_by_id" not in _served(_role("seller", SALES))
    assert not served_across_domains(_role("seller", SALES), LOOKUP)
    assert _served(_role("hr_reader", HR))["get_staff_by_id"] == ["title"]


def test_a_lockdown_domain_is_never_served_across_domains():
    assert "audit_log" not in _served(_role("seller", SALES))
    assert not served_across_domains(_role("seller", SALES), AUDIT)
    # Within it, the explicit grant is what counts (REQ-1133): `who` is public and granted to
    # nobody, so not even a role that reaches ops is served it.
    assert _served(_role("seller", SALES, OPS))["audit_log"] == ["id"]


def test_a_set_of_roles_is_served_the_union():
    """A meta-role is a role: its domains are its members' union, and a grant to a member
    reaches it (security/meta_role.py). The rule needs no case for it."""
    meta = _role("meta:auditor+seller", SALES)
    tables = [
        {**STAFF, "columns": [dict(c) for c in STAFF["columns"]]},
        REVIEWS,
        ORDERS,
        LOOKUP,
        AUDIT,
    ]
    # What expand_grants does when the meta-role is made: a grant naming a member names it.
    for column in tables[0]["columns"]:
        if "auditor" in column["visible_to"]:
            column["visible_to"] = [*column["visible_to"], meta["id"]]
    served = {
        t["table_name"]: [c["column_name"] for c in t["columns"]]
        for t in visible_tables(tables, meta)
    }
    assert served["staff"] == ["id", "name", "badge"]
    assert served["orders"] == ["id", "staff_id"]
    assert "reviews" not in served and "audit_log" not in served


def test_nothing_is_public_by_default():
    assert Column(name="c", visible_to=[]).scope == "domain"
    unpublished = _table(9, HR, "t", [{"column_name": "c", "visible_to": []}])  # no scope at all
    assert not served_across_domains(_role("seller", SALES), unpublished)


# --- the same rule everywhere it is read ---------------------------------------------------------


def test_the_shared_helper_and_the_schema_build_agree():
    for role in (_role("seller", SALES), _role("hr_reader", HR), _role("auditor", SALES)):
        by_helper = {
            t["table_name"]: [c["column_name"] for c in t["columns"] if not c["native_filter_type"]]
            for t in visible_tables(TABLES, role)
        }
        by_helper = {name: cols for name, cols in by_helper.items() if cols}
        assert by_helper == _served(role), role["id"]


def test_the_catalog_describes_a_published_table_to_a_role_outside_its_domain():
    """REQ-1132's row scope: a role's own tables, now including those published to it."""
    assert compute_meta_row_scope(_role("seller", SALES), TABLES, []) == {1, 3}
    assert compute_meta_row_scope(_role("hr_reader", HR), TABLES, []) == {1, 2, 4}
    assert compute_meta_row_scope(_role("anyone", "*"), TABLES, []) is None


# --- scope is one of three values, on every path --------------------------------------------------


@pytest.mark.parametrize("scope", ["domain", "public", "restricted"])
def test_the_three_scopes_are_accepted(scope):
    assert require_column_scope("c", scope) == scope
    assert Column(name="c", visible_to=[], scope=scope).scope == scope


@pytest.mark.parametrize("scope", ["Public", "everyone", "", None, "org"])
def test_anything_else_is_refused_naming_the_column(scope):
    with pytest.raises(ColumnScopeInvalid, match="'c'") as refused:
        require_column_scope("c", scope)
    assert refused.value.column == "c" and refused.value.scope == scope
    # The model refuses it too: by the same sentence for a string, by its type for anything else.
    with pytest.raises(ValueError):
        Column(name="c", visible_to=[], scope=scope)  # type: ignore[arg-type]
    if isinstance(scope, str):
        with pytest.raises(
            ValueError, match="a column's scope is one of domain, public, restricted"
        ):
            Column(name="c", visible_to=[], scope=scope)


# --- SQL governance: the same columns, the direct read, the row rule and the mask ----------------


def _sql_context(role: dict, *, row_rule: str | None = None, mask=None):
    import sqlglot  # noqa: F401  (the validator parses with it)

    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import CompilationContext, TableMeta
    from provisa.compiler.stage2 import build_governance_context

    ctx = CompilationContext()
    # The role's compiled context holds the tables it is served (the schema build above).
    served = _served(role)
    ctx.tables = {
        t["table_name"]: TableMeta(
            table_id=t["id"],
            field_name=t["table_name"],
            type_name=t["table_name"].capitalize(),
            source_id="pg",
            catalog_name="pg",
            schema_name="public",
            table_name=t["table_name"],
            domain_id=t["domain_id"],
        )
        for t in TABLES
        if t["table_name"] in served
    }
    rls = RLSContext(rules={1: row_rule} if row_rule else {}, domain_rules={}, action_rules={})
    masking = {(1, role["id"]): mask} if mask else {}
    gov = build_governance_context(role["id"], rls, masking, ctx, TABLES, role)
    return gov, {m.table_id: m for m in ctx.tables.values()}


def _v001(role: dict, sql: str) -> list[str]:
    import sqlglot

    from provisa.compiler.sql_validator import _check_domain_access

    gov, metas = _sql_context(role)
    tree = sqlglot.parse_one(sql, read="postgres")
    return [v.code for v in _check_domain_access(tree, gov, metas, role["domain_access"])]


def test_sql_serves_the_same_columns_as_the_schema():
    gov, _ = _sql_context(_role("seller", SALES))
    assert gov.visible_columns[1] == frozenset({"id", "name"})
    assert gov.public_tables == frozenset({1})
    assert gov.visible_columns[2] == frozenset()  # reviews: nothing of it
    assert gov.visible_columns[3] is None  # its own table: every column
    reader, _ = _sql_context(_role("hr_reader", HR))
    assert reader.visible_columns[1] == frozenset({"id", "name", "salary"})
    assert reader.public_tables == frozenset()


def test_a_published_table_is_read_directly_and_an_unpublished_one_is_not():
    seller = _role("seller", SALES)
    assert _v001(seller, "SELECT id, name FROM hr.staff") == []
    assert _v001(seller, "SELECT id FROM sales.orders") == []
    # Unpublished tables are not in a role's compiled context at all; the registry name still
    # resolves, and the direct read is the domain violation it always was.
    gov, metas = _sql_context(seller)
    import sqlglot

    from provisa.compiler.sql_gen import TableMeta
    from provisa.compiler.sql_validator import _check_domain_access

    metas[2] = TableMeta(
        table_id=2,
        field_name="reviews",
        type_name="Reviews",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="reviews",
        domain_id=HR,
    )
    tree = sqlglot.parse_one("SELECT id FROM hr.reviews", read="postgres")
    assert [v.code for v in _check_domain_access(tree, gov, metas, seller["domain_access"])] == [
        "V001"
    ]


def test_a_row_rule_and_a_mask_on_a_published_table_still_apply():
    """Publishing widens who is served; a row rule for the table and a mask for the role apply
    as they do inside the domain. With no row rule the published columns are read unfiltered."""
    from provisa.security.masking import MaskingRule, MaskType

    rule = MaskingRule(mask_type=MaskType.constant, value="***")
    gov, _ = _sql_context(
        _role("seller", SALES), row_rule="id < 100", mask={"name": (rule, "varchar")}
    )
    assert gov.rls_rules[1] == "id < 100"
    assert gov.masking_rules[(1, "name")] == (rule, "varchar")
    unfiltered, _ = _sql_context(_role("seller", SALES))
    assert 1 not in unfiltered.rls_rules


# --- every surface follows the compiled context ---------------------------------------------------
#
# The served set is compiled once per role (schema build → context); the surfaces read that. One
# case per kind of surface that it carries a published table, with its published columns only,
# for a role outside the table's domain — and nothing of an unpublished one.


def _compiled(role: dict):
    from provisa.compiler.context import build_context
    from provisa.compiler.schema_gen import generate_schema

    si = SchemaInput(
        tables=TABLES,
        relationships=[],
        column_types={
            t["id"]: [
                ColumnMetadata(column_name=c["column_name"], data_type="varchar", is_nullable=True)
                for c in t["columns"]
            ]
            for t in TABLES
        },
        naming_rules=[],
        role=role,
        domains=[{"id": HR}, {"id": SALES}, {"id": OPS}],
    )
    return si, generate_schema(si), build_context(si)


def _staff_and_reviews(ctx):
    by_id = {meta.table_id: meta for meta in ctx.tables.values()}
    return by_id.get(1), by_id.get(2)


def test_the_graphql_schema_carries_the_published_table_and_columns():
    _si, schema, ctx = _compiled(_role("seller", SALES))
    staff, reviews = _staff_and_reviews(ctx)
    assert staff is not None and reviews is None
    fields = schema.type_map[staff.type_name].fields
    assert {"id", "name"} <= set(fields) and not {"salary", "badge", "ssn"} & set(fields)
    assert staff.field_name in schema.query_type.fields, "a published table is a root field"


def test_the_grpc_proto_carries_the_published_table_and_columns():
    from provisa.grpc.proto_gen import generate_proto

    si, _schema, ctx = _compiled(_role("seller", SALES))
    staff, _ = _staff_and_reviews(ctx)
    proto = generate_proto(si)
    message = proto[proto.index(f"message {staff.type_name} {{") :]
    message = message[: message.index("}")]
    assert " id " in message and " name " in message
    assert "salary" not in message and "ssn" not in message
    assert "Reviews" not in proto


def test_the_cypher_labels_carry_the_published_table_as_a_match_root():
    from provisa.cypher.label_map import CypherLabelMap

    role = _role("seller", SALES)
    _si, _schema, ctx = _compiled(role)
    labels = CypherLabelMap.from_schema(ctx, role["domain_access"])
    by_table = {node.table_id: node for node in labels.nodes.values()}
    assert 1 in by_table and 2 not in by_table
    assert not by_table[1].traversal_only, "published is read directly, not only by traversal"
    assert {"id", "name"} <= set(by_table[1].properties)
    assert not {"salary", "badge", "ssn"} & set(by_table[1].properties)


def test_the_role_narrowed_catalog_carries_the_published_table_and_columns():
    """Flight's catalog, the MCP catalog tools, the JDBC driver's metadata and the admin
    listing all narrow by this one function over the role's compiled context."""
    import types

    from provisa.api.flight.catalog import role_visibility

    role = _role("seller", SALES)
    _si, _schema, ctx = _compiled(role)
    visible = role_visibility(types.SimpleNamespace(contexts={role["id"]: ctx}), role["id"])
    assert visible[1] == {"id", "name"}
    assert 2 not in visible and 4 not in visible and 5 not in visible
    assert visible[3] == {"id", "staff_id"}


def test_the_admin_catalog_lists_a_published_table_across_domains_with_its_public_columns():
    """REQ-1958 over REQ-1959: a caller with no administrative right is answered what its role
    is served — a published table of a domain it does not reach, with the published columns
    only, and that table's domain; never the domain's unpublished table."""
    import types

    from provisa.api.admin.catalog_scope import CatalogScope
    from provisa.api.flight.catalog import role_visibility

    role = _role("seller", SALES)
    _si, _schema, ctx = _compiled(role)
    served = role_visibility(types.SimpleNamespace(contexts={role["id"]: ctx}), role["id"])
    scope = CatalogScope(
        whole=False,
        admin=False,
        admin_reach=frozenset(),
        reach=frozenset({SALES}),
        served=served,
        governance=False,
        role=role["id"],
    )
    assert scope.lists_table(1, HR) and scope.columns(1, HR) == {"id", "name"}
    assert scope.lists_column(1, HR, "name") and not scope.lists_column(1, HR, "salary")
    assert not scope.lists_table(2, HR), "the domain's unpublished table"
    assert scope.lists_domain(HR, with_listed_table=True)
    assert not scope.lists_domain(HR, with_listed_table=False)
