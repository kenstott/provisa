# Copyright (c) 2026 Kenneth Stott
# Canary: 3700b912-f127-4cbc-8b8c-d198a784de8a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A tree is validated WHOLE, and every reference that resolves to nothing is reported (REQ-1496).

A deploy refuses a tree that does not hold before any schema holds it. What is under test here is
that the refusal is a REPORT: every dangling reference in the tree is named, by the path that makes
it and the thing it names, in one answer -- not the first one found, and never a database's
foreign-key error after the apply began.
"""

# Requirements: REQ-1496, REQ-1502, REQ-1526

from __future__ import annotations

import pytest

from provisa.core.env_deploy import DeployError, _decompose

EXISTING: dict[str, dict[str, int]] = {
    "registered_tables": {},
    "glossary_terms": {},
    "commands": {},
}
ROLES = frozenset({"analyst"})


def _tree() -> dict[str, dict]:
    """A small tree that holds: one source, one domain, two tables and an edge between them."""
    return {
        "domain.yaml": {"origin": "seed"},
        "sales/domain.yaml": {"origin": "admin"},
        "sources/warehouse.yaml": {"origin": "admin", "type": "postgres"},
        "sales/tables/Customer.yaml": {
            "origin": "admin",
            "source_id": "warehouse",
            "schema_name": "public",
            "table_name": "customers",
            "alias": "Customer",
        },
        "sales/tables/Order.yaml": {
            "origin": "admin",
            "source_id": "warehouse",
            "schema_name": "public",
            "table_name": "orders",
            "alias": "Order",
            "relationships": [
                {
                    "id": "orders_customer",
                    "alias": "customer",
                    "target": "sales/tables/Customer.yaml",
                    "cardinality": "many-to-one",
                }
            ],
            "row_policies": [
                {"description": "Test rule.", "role_id": "analyst", "filter_expr": b"x"}
            ],
        },
    }


def _problems(tree: dict[str, dict], roles: frozenset[str] = ROLES) -> list[str]:
    with pytest.raises(DeployError) as refused:
        _decompose(tree, EXISTING, roles)
    return refused.value.problems


def test_a_tree_that_holds_decomposes():
    rows = _decompose(_tree(), EXISTING, ROLES)
    assert len(rows["registered_tables"]) == 2 and len(rows["relationships"]) == 1


def test_a_table_naming_a_source_the_tree_does_not_hold_is_reported():
    tree = _tree()
    del tree["sources/warehouse.yaml"]
    problems = _problems(tree)
    assert len(problems) == 2  # both tables name it, and both are reported
    assert all("warehouse" in p and "source" in p for p in problems)


def test_a_table_in_a_domain_the_tree_does_not_hold_is_reported():
    tree = _tree()
    del tree["sales/domain.yaml"]
    problems = _problems(tree)
    assert any("sales/tables/Order.yaml" in p and "domain" in p for p in problems)


def test_a_row_policy_naming_a_role_the_environment_does_not_have_is_reported():
    (problem,) = _problems(_tree(), roles=frozenset({"developer"}))
    assert "sales/tables/Order.yaml" in problem and "'analyst'" in problem and "role" in problem


def test_every_dangling_reference_is_reported_not_only_the_first():
    tree = _tree()
    del tree["sales/tables/Customer.yaml"]
    tree["sales/tables/Order.yaml"]["source_id"] = "gone"
    tree["sales/tables/Order.yaml"]["product_id"] = "no-such-product"
    problems = _problems(tree, roles=frozenset())
    said = " | ".join(problems)
    assert "sales/tables/Customer.yaml" in said  # the relationship's target
    assert "'gone'" in said  # the source
    assert "'no-such-product'" in said  # the data product
    assert "'analyst'" in said  # the role
    assert len(problems) == 4


def test_the_refusal_reads_as_every_problem_it_holds():
    tree = _tree()
    del tree["sources/warehouse.yaml"]
    with pytest.raises(DeployError) as refused:
        _decompose(tree, EXISTING, ROLES)
    for problem in refused.value.problems:
        assert problem in str(refused.value)


def test_a_data_product_in_a_domain_the_tree_does_not_hold_is_reported():
    tree = _tree()
    tree["data-products/orders.yaml"] = {"origin": "admin", "domain_id": "finance", "name": "O"}
    (problem,) = _problems(tree)
    assert "data-products/orders.yaml" in problem and "'finance'" in problem


def test_a_via_table_the_tree_does_not_hold_is_reported():
    tree = _tree()
    tree["sales/tables/Order.yaml"]["relationships"][0]["via"] = "sales/tables/Bridge.yaml"
    (problem,) = _problems(tree)
    assert "sales/tables/Bridge.yaml" in problem and "via" in problem


def test_a_tag_on_a_relationship_the_file_does_not_hold_is_reported():
    tree = _tree()
    tree["sales/tables/Order.yaml"]["tags"] = [{"tag_id": "pii", "on": "#rel:nope"}]
    (problem,) = _problems(tree)
    assert "#rel:nope" in problem


def test_a_relationship_tag_is_resolved_by_the_name_the_projection_wrote():
    # The projection writes #rel:<alias or id>; the row is keyed by the relationship's id.
    tree = _tree()
    tree["sales/tables/Order.yaml"]["tags"] = [{"tag_id": "pii", "on": "#rel:customer"}]
    rows = _decompose(tree, EXISTING, ROLES)
    (tag,) = rows["tag_assignments"]
    assert tag["relationship_id"] == "orders_customer"
    assert tag["object_key"] == "relationship:orders_customer"
