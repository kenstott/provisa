# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3d1e94-2c6a-4f08-b5d1-8e0c4a7f2b16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: parsing, expansion, the mask and its checks."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from provisa.compiler.names_table import NameRow, build_names_table
from provisa.entitlements.index import build_index, check_lists
from provisa.entitlements.lists import (
    EntitlementClaimError,
    ExpiryRule,
    HeldEntitlements,
    HeldList,
    effective_lists,
    held_version,
    idp_lists_hash,
    parse_idp_entitlements,
)
from provisa.entitlements.mask import MaskCache, MemberInput, compute_mask

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)


def _names():
    def t(oid, name, dom):
        return NameRow(
            "table",
            str(oid),
            "sql",
            name,
            name,
            name,
            domain_id=dom,
            table_id=oid,
            scope=("sql", dom),
        )

    def c(tid, name, dom):
        return NameRow(
            "column",
            f"{tid}:{name}",
            "sql",
            name,
            name,
            name,
            domain_id=dom,
            table_id=tid,
            scope=("sql_column", str(tid)),
        )

    def rel(type_name, field, dom, tid):
        return NameRow(
            "relationship",
            f"{type_name}.{field}",
            "graphql_field",
            field,
            field,
            field,
            domain_id=dom,
            table_id=tid,
            scope=("graphql_type_field", type_name),
        )

    return build_names_table(
        [
            t(1, "revenue", "finance"),
            c(1, "id", "finance"),
            c(1, "amount", "finance"),
            t(2, "orders", "sales"),
            c(2, "id", "sales"),
            c(2, "customer_id", "sales"),
            t(3, "customers", "sales"),
            c(3, "id", "sales"),
            t(4, "secret", "hr"),
            c(4, "id", "hr"),
            rel("Orders", "customer", "sales", 2),
            NameRow(
                "command",
                "refund",
                "graphql_field",
                "refund",
                "refund",
                "refund",
                domain_id="sales",
                scope=("graphql_mutation",),
            ),
        ]
    )


def _ctx(tables=(1, 2, 3)):
    names = {1: "revenue", 2: "orders", 3: "customers"}
    type_names = {2: "Orders", 3: "Customers", 1: "Revenue"}
    columns = {1: ("id", "amount"), 2: ("id", "customer_id"), 3: ("id",)}
    target = SimpleNamespace(table_id=3)
    return SimpleNamespace(
        tables={names[t]: SimpleNamespace(table_id=t, type_name=type_names[t]) for t in tables},
        physical_to_sql={(t, c): c for t in tables for c in columns[t]},
        joins=(
            {
                ("Orders", "customer"): SimpleNamespace(
                    source_column="customer_id",
                    target_column="id",
                    target=target,
                )
            }
            if {2, 3} <= set(tables)
            else {}
        ),
    )


def _index(tables=(1, 2, 3), products=()):
    return build_index(_names(), _ctx(tables), products=products)


def _model_index(products=()):
    return build_index(_names(), None, products=products)


def _effective(idp=None, held=None, defaults=None, role="viewer"):
    return effective_lists(role, idp or {}, held or HeldEntitlements(), defaults or {})


def _mask(lists, role="viewer", index=None, expiries=(), now=NOW):
    index = index or _index()
    return compute_mask(
        role,
        index,
        [MemberInput(role, lists, index, tuple(expiries))],
        model_version=("b", 1),
        now=now,
    )


# ── parsing ─────────────────────────────────────────────────────────────────────────────────────


def test_nested_claim_is_read_per_role_and_kind():
    claims = {
        "provisa_entitlements": {
            "viewer": {"tables": ["finance.revenue"], "relationships": []},
            "analyst": {"columns": ["sales.*"]},
        }
    }
    assert parse_idp_entitlements(claims, "provisa_entitlements") == {
        "viewer": {"tables": ("finance.revenue",), "relationships": ()},
        "analyst": {"columns": ("sales.*",)},
    }


def test_flat_form_is_read_from_any_list_claim():
    claims = {
        "groups": [
            "plain-group",
            "provisa:viewer:tables:finance.revenue",
            "provisa:viewer:tables:sales.*",
            "provisa:analyst:columns:sales.orders.id",
        ],
        "email": "alice@example.com",
    }
    assert parse_idp_entitlements(claims, "provisa_entitlements") == {
        "viewer": {"tables": ("finance.revenue", "sales.*")},
        "analyst": {"columns": ("sales.orders.id",)},
    }


def test_flat_empty_identifier_is_an_empty_list():
    assert parse_idp_entitlements({"groups": ["provisa:viewer:tables:"]}, None) == {
        "viewer": {"tables": ()}
    }


def test_nested_and_flat_forms_combine():
    claims = {
        "provisa_entitlements": {"viewer": {"tables": ["sales.*"]}},
        "groups": ["provisa:viewer:tables:finance.revenue"],
    }
    assert parse_idp_entitlements(claims, "provisa_entitlements")["viewer"]["tables"] == (
        "sales.*",
        "finance.revenue",
    )


@pytest.mark.parametrize(
    "claims",
    [
        {"provisa_entitlements": ["not", "a", "map"]},
        {"provisa_entitlements": {"viewer": ["not a map"]}},
        {"provisa_entitlements": {"viewer": {"tables": "not-a-list"}}},
        {"groups": ["provisa:viewer:tables"]},
    ],
)
def test_a_malformed_claim_is_refused_and_never_read_as_no_list(claims):
    with pytest.raises(EntitlementClaimError):
        parse_idp_entitlements(claims, "provisa_entitlements")


def test_no_claim_means_no_lists():
    assert parse_idp_entitlements({"sub": "alice", "groups": ["analysts"]}, None) == {}


def test_idp_hash_is_order_independent_and_tells_lists_apart():
    a = {"viewer": {"tables": ("a.b", "c.d"), "columns": ("x.y.z",)}}
    b = {"viewer": {"columns": ("x.y.z",), "tables": ("c.d", "a.b")}}
    c = {"viewer": {"tables": ("a.b",)}}
    assert idp_lists_hash(a) == idp_lists_hash(b)
    assert idp_lists_hash(a) != idp_lists_hash(c)


# ── precedence ──────────────────────────────────────────────────────────────────────────────────


def test_idp_list_supersedes_provisa_per_role_and_kind():
    held = HeldEntitlements(
        lists=(
            HeldList("viewer", "tables", ("sales.*",)),
            HeldList("viewer", "columns", ("finance.revenue.id",)),
        )
    )
    idp = {"viewer": {"tables": ("finance.revenue",)}}
    out = _effective(idp, held)
    assert out["tables"].identifiers == ("finance.revenue",)
    assert out["tables"].origin == "idp"
    assert out["tables"].superseded == ("sales.*",)
    # The provider is silent on columns: the held list applies.
    assert out["columns"].identifiers == ("finance.revenue.id",)
    assert out["columns"].origin == "provisa"


def test_a_provisa_list_never_widens_an_empty_idp_list():
    held = HeldEntitlements(lists=(HeldList("viewer", "tables", ("*",)),))
    out = _effective({"viewer": {"tables": ()}}, held)
    assert out["tables"].identifiers == ()
    assert out["tables"].origin == "idp"


def test_precedence_is_per_role():
    held = HeldEntitlements(lists=(HeldList("analyst", "tables", ("sales.*",)),))
    out = _effective({"viewer": {"tables": ("a.b",)}}, held, role="analyst")
    assert out["tables"].origin == "provisa"


def test_role_default_applies_where_nothing_else_speaks():
    out = _effective(defaults={"domains": ["sales"]})
    assert out["domains"].identifiers == ("sales",)
    assert out["domains"].origin == "role"


def test_a_capability_scoped_held_row_is_not_a_list_for_its_kind():
    held = HeldEntitlements(
        lists=(HeldList("viewer", "data_products", ("sales.p1",), capability="data_product_rw"),)
    )
    assert "data_products" not in _effective(held=held)


def test_held_version_tracks_content():
    one = HeldEntitlements(lists=(HeldList("viewer", "tables", ("a.b",)),))
    same = HeldEntitlements(lists=(HeldList("viewer", "tables", ("a.b",)),))
    other = HeldEntitlements(lists=(HeldList("viewer", "tables", ("a.c",)),))
    rule = HeldEntitlements(lists=one.lists, expiries=(ExpiryRule("viewer", NOW, {}),))
    assert held_version(one) == held_version(same)
    assert held_version(one) != held_version(other)
    assert held_version(one) != held_version(rule)


# ── expansion ───────────────────────────────────────────────────────────────────────────────────


def test_no_list_covers_the_whole_role_and_is_full():
    mask = _mask({})
    assert mask.full
    assert mask.hash == ""


def test_table_list_selects_that_table_only():
    mask = _mask(_effective({"viewer": {"tables": ("finance.revenue",)}}))
    assert mask.has_table(1)
    assert mask.has_column(1, "amount")
    assert not mask.has_table(2)
    assert not mask.has_column(2, "id")
    assert not mask.full
    assert mask.hash


def test_wildcard_levels():
    assert _mask(_effective({"viewer": {"tables": ("*",)}})).full
    domain = _mask(_effective({"viewer": {"tables": ("sales.*",)}}))
    assert domain.has_table(2) and domain.has_table(3) and not domain.has_table(1)
    table = _mask(_effective({"viewer": {"columns": ("finance.revenue.*",)}}))
    assert table.has_column(1, "amount") and table.has_column(1, "id")
    assert not table.has_column(2, "id")


def test_a_grant_on_a_node_covers_everything_beneath_it():
    mask = _mask(_effective({"viewer": {"domains": ("sales",)}}))
    assert mask.has_table(2) and mask.has_column(3, "id")
    assert mask.has_relationship(2, 3, "customer_id", "id")
    assert not mask.has_table(1)


def test_grants_within_a_list_add_together():
    mask = _mask(_effective({"viewer": {"tables": ("sales", "finance.revenue")}}))
    assert mask.has_table(1) and mask.has_table(2) and mask.has_table(3)


def test_identifiers_match_ignoring_case():
    assert _mask(_effective({"viewer": {"tables": ("FINANCE.Revenue",)}})).has_table(1)


def test_kinds_narrow_each_other():
    mask = _mask(_effective({"viewer": {"tables": ("sales.*",), "columns": ("sales.orders.id",)}}))
    assert mask.has_column(2, "id")
    assert not mask.has_column(2, "customer_id")
    # customers is in the table list but no column of it is left.
    assert not mask.has_table(3)


def test_a_column_list_outside_the_table_list_reaches_nothing():
    mask = _mask(
        _effective({"viewer": {"tables": ("sales.orders",), "columns": ("finance.revenue.id",)}})
    )
    assert not mask.has_column(1, "id")
    assert not mask.has_table(1)
    assert not mask.has_table(2)


def test_a_relationship_needs_both_tables_read():
    mask = _mask(_effective({"viewer": {"tables": ("sales.orders",)}}))
    assert not mask.has_relationship(2, 3, "customer_id", "id")
    both = _mask(_effective({"viewer": {"tables": ("sales.*",)}}))
    assert both.has_relationship(2, 3, "customer_id", "id")


def test_relationship_list_selects_by_domain_qualified_and_bare_name():
    for name in ("sales.orders_customer", "orders_customer"):
        mask = _mask(_effective({"viewer": {"relationships": (name,)}}))
        assert mask.has_relationship(2, 3, "customer_id", "id"), name
    assert not _mask(_effective({"viewer": {"relationships": ()}})).has_relationship(
        2, 3, "customer_id", "id"
    )


def test_empty_list_means_nothing_of_that_kind():
    mask = _mask(_effective({"viewer": {"tables": ()}}))
    assert not mask.has_table(1) and not mask.has_table(2)
    assert mask.bits == 0 or not any(mask.has_table(t) for t in (1, 2, 3))


def test_the_ceiling_is_the_role_schema():
    # hr.secret exists in the model; this role's schema does not hold it.
    mask = _mask(_effective({"viewer": {"tables": ("hr.secret", "sales.*")}}))
    assert not mask.has_table(4)
    assert mask.has_table(2)


def test_role_default_narrows_where_no_list_is_given():
    mask = _mask(_effective(defaults={"domains": ["sales"]}))
    assert mask.has_table(2) and not mask.has_table(1)


def test_a_deny_by_default_role_reaches_nothing_without_a_list():
    mask = _mask(_effective(defaults={"domains": []}))
    assert not any(mask.has_table(t) for t in (1, 2, 3))


def test_products_are_a_kind_under_domain():
    index = _index(products=[("p1", "sales"), ("p2", "finance")])
    mask = _mask(_effective({"viewer": {"data_products": ("sales.*",)}}), index=index)
    assert mask.covers("data_product", "p1")
    assert not mask.covers("data_product", "p2")
    domain = _mask(_effective({"viewer": {"domains": ("finance",)}}), index=index)
    assert domain.covers("data_product", "p2") and not domain.covers("data_product", "p1")


def test_commands_follow_their_domain():
    mask = _mask(_effective({"viewer": {"commands": ("sales.refund",)}}))
    assert mask.covers("command", "refund")
    assert not _mask(_effective({"viewer": {"domains": ("finance",)}})).covers("command", "refund")


def test_several_roles_combine_by_union():
    index = _index()
    wide = _index()
    narrow = _index()
    mask = compute_mask(
        "meta",
        index,
        [
            MemberInput("a", _effective({"a": {"tables": ("finance.*",)}}, role="a"), wide, ()),
            MemberInput(
                "b", _effective({"b": {"tables": ("sales.orders",)}}, role="b"), narrow, ()
            ),
        ],
        model_version=("b", 1),
        now=NOW,
    )
    assert mask.has_table(1) and mask.has_table(2) and not mask.has_table(3)


def test_a_member_mask_never_reaches_outside_that_member():
    index = _index()
    only_sales = _index(tables=(2, 3))
    mask = compute_mask(
        "meta",
        index,
        [MemberInput("a", _effective({"a": {"tables": ("*",)}}, role="a"), only_sales, ())],
        model_version=("b", 1),
        now=NOW,
    )
    assert mask.has_table(2) and not mask.has_table(1)


# ── expiry ──────────────────────────────────────────────────────────────────────────────────────


def _held_sales():
    return _effective(held=HeldEntitlements(lists=(HeldList("viewer", "tables", ("sales.*",)),)))


def test_a_passed_expiry_with_no_scope_ends_the_grant():
    mask = _mask(_held_sales(), expiries=[ExpiryRule("viewer", NOW - timedelta(hours=1), {})])
    assert not mask.has_table(2)


def test_expiry_of_a_scoped_capability_removes_only_its_bits():
    rule = ExpiryRule("viewer", NOW - timedelta(hours=1), {"read": ("sales.orders",)})
    mask = _mask(_held_sales(), expiries=[rule])
    assert not mask.has_table(2) and mask.has_table(3)


def test_a_future_expiry_is_carried_as_the_next_expiry():
    later = NOW + timedelta(days=1)
    mask = _mask(_held_sales(), expiries=[ExpiryRule("viewer", later, {})])
    assert mask.has_table(2)
    assert mask.next_expiry == later


def test_expiry_never_touches_an_idp_list():
    lists = _effective({"viewer": {"tables": ("sales.*",)}})
    mask = _mask(lists, expiries=[ExpiryRule("viewer", NOW - timedelta(hours=1), {})])
    assert mask.has_table(2)


# ── the cache ──────────────────────────────────────────────────────────────────────────────────


def test_mask_cache_is_invalidated_by_each_key_part():
    cache = MaskCache()
    built = []

    def build():
        built.append(1)
        return _mask({})

    key = ("viewer", "idp1", "held1", ("b", 1))
    cache.get_or_build(key, build, now=NOW)
    cache.get_or_build(key, build, now=NOW)
    assert len(built) == 1
    cache.get_or_build(("viewer", "idp2", "held1", ("b", 1)), build, now=NOW)
    assert len(built) == 2
    cache.get_or_build(("viewer", "idp1", "held2", ("b", 1)), build, now=NOW)
    assert len(built) == 3
    cache.get_or_build(("viewer", "idp1", "held1", ("b", 2)), build, now=NOW)
    assert len(built) == 4


def test_a_cached_mask_past_its_expiry_is_recomputed():
    cache = MaskCache()
    later = NOW + timedelta(hours=1)
    built = []

    def build():
        built.append(1)
        return _mask(_held_sales(), expiries=[ExpiryRule("viewer", later, {})])

    key = ("viewer", "i", "h", ("b", 1))
    cache.get_or_build(key, build, now=NOW)
    cache.get_or_build(key, build, now=NOW + timedelta(minutes=5))
    assert len(built) == 1
    cache.get_or_build(key, build, now=later + timedelta(seconds=1))
    assert len(built) == 2


def test_two_different_masks_have_different_hashes():
    a = _mask(_effective({"viewer": {"tables": ("sales.*",)}}))
    b = _mask(_effective({"viewer": {"tables": ("finance.*",)}}))
    assert a.hash and b.hash and a.hash != b.hash


def test_the_mask_hash_changes_with_the_model_version():
    index = _index()
    lists = _effective({"viewer": {"tables": ("sales.*",)}})
    member = [MemberInput("viewer", lists, index, ())]
    one = compute_mask("viewer", index, member, model_version=("b", 1), now=NOW)
    two = compute_mask("viewer", index, member, model_version=("b", 2), now=NOW)
    assert one.hash != two.hash


# ── checks ──────────────────────────────────────────────────────────────────────────────────────


def _role(capabilities=(), domain_access=("*",)):
    return {"capabilities": list(capabilities), "domain_access": list(domain_access)}


def test_an_unknown_identifier_is_flagged_by_name():
    lists = _effective({"viewer": {"tables": ("finance.revenue", "finance.nope")}})
    findings = check_lists("viewer", _role(), lists, _model_index())
    assert [(f.code, f.kind, f.identifier) for f in findings] == [
        ("unknown_identifier", "tables", "finance.nope")
    ]
    assert "finance.nope" in findings[0].message


def test_an_identifier_outside_the_role_is_not_unknown():
    lists = _effective({"viewer": {"tables": ("hr.secret",)}})
    assert check_lists("viewer", _role(), lists, _model_index()) == []


def test_a_wildcard_matching_nothing_is_flagged():
    lists = _effective({"viewer": {"tables": ("nowhere.*",)}})
    assert [f.code for f in check_lists("viewer", _role(), lists, _model_index())] == [
        "unknown_identifier"
    ]


def test_a_list_for_a_kind_the_role_uses_nothing_of_is_flagged():
    lists = _effective({"viewer": {"environments": ("feature-*",)}})
    findings = check_lists("viewer", _role(), lists, _model_index())
    assert [(f.code, f.kind) for f in findings] == [("unused_kind", "environments")]


def test_a_kind_the_role_uses_is_not_flagged():
    lists = _effective({"viewer": {"environments": ("feature-*",)}})
    role = _role(capabilities=("environment_switch",))
    assert check_lists("viewer", role, lists, _model_index()) == []


def test_an_unrecognised_kind_is_flagged_as_unused():
    lists = _effective({"viewer": {"bananas": ("x",)}})
    assert [f.code for f in check_lists("viewer", _role(), lists, _model_index())] == [
        "unused_kind"
    ]


def test_a_data_kind_on_a_role_with_no_data_plane_is_unused():
    lists = _effective({"viewer": {"tables": ("sales.*",)}})
    role = _role(capabilities=("platform_settings",), domain_access=())
    assert [f.code for f in check_lists("viewer", role, lists, _model_index())] == ["unused_kind"]
