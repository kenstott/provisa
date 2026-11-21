# Copyright (c) 2026 Kenneth Stott
# Canary: 6f0c2d7e-41b9-4a35-9c1e-b87d5a3e2f14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-source row counts for a GraphQL field's stats count only the joins the query selected (#265).

The count used to pass over every result row once per join in the role's whole context, whether
or not the query selected a join: a 6M-row scan through a context holding joins spent a minute
counting nothing. The Rust encoder now counts each selected relationship while it writes the rows
(``FieldAnswer.joined``, REQ-2194): a to-many's elements summed over the rows, a to-one's non-null
objects. ``_count_rows_per_source`` gives each count to the source its join targets."""

from __future__ import annotations

import json
from types import SimpleNamespace

from provisa.api.data.endpoint_helpers import _count_rows_per_source
from provisa.executor import json_encoders as je
from tests.unit.json_encoder.cases import C, orders, project


def _join(source_id: str, cardinality: str) -> SimpleNamespace:
    return SimpleNamespace(target=SimpleNamespace(source_id=source_id), cardinality=cardinality)


def _context(n_unselected: int = 50) -> SimpleNamespace:
    joins = {("Order", "customer"): _join("crm", "many-to-one")}
    joins[("Order", "items")] = _join("warehouse", "one-to-many")
    for i in range(n_unselected):
        joins[(f"Type{i}", f"rel_{i}")] = _join(f"src_{i}", "one-to-many")
    return SimpleNamespace(joins=joins)


def test_a_query_that_selected_no_join_has_no_entry():
    assert _count_rows_per_source({}, _context(), "Order") == {}
    assert _count_rows_per_source(None, _context(), "Order") == {}


def test_selected_joins_are_counted_as_the_encoder_writes_the_rows():
    """The encoder counts each selected relationship of the rows it writes, in that one pass: a
    to-one's non-null objects, a to-many's elements; the context's other joins are not counted."""
    plan = [
        C("id"),
        C("cust_id", "customer", "many-to-one"),
        C("item_id", "items", "one-to-many"),
        C("item_qty", "items", "one-to-many"),
    ]
    answer = je.graphql_field(project(orders(), plan).to_batches(), plan)
    rows = json.loads(answer.body)
    assert (
        answer.joined
        == {
            "customer": sum(r["customer"] is not None for r in rows),
            "items": sum(len(r["items"]) for r in rows),
        }
        == {"customer": 3, "items": 5}
    )
    assert _count_rows_per_source(answer.joined, _context(), "Order") == {
        "crm": 3,
        "warehouse": 5,
    }


def test_a_source_no_selected_join_targets_has_no_entry():
    """Its caller then reports the field's own row count for it, not zero."""
    ctx = SimpleNamespace(joins={("Customer", "orders"): _join("sales", "one-to-many")})
    assert _count_rows_per_source({}, ctx, "Customer") == {}


def test_two_selected_joins_on_one_source_add_up():
    ctx = SimpleNamespace(
        joins={
            ("Order", "customer"): _join("crm", "many-to-one"),
            ("Order", "contacts"): _join("crm", "one-to-many"),
        }
    )
    assert _count_rows_per_source({"customer": 1, "contacts": 2}, ctx, "Order") == {"crm": 3}


def test_another_type_s_join_of_a_column_s_name_is_not_counted():
    """A view's column ``complaints`` (a count) is no join, though the permits type has a join of
    that name: the stats used to take it for one and fail the read, len() of an int."""
    ctx = SimpleNamespace(joins={("Permits", "complaints"): _join("drop", "one-to-many")})
    assert _count_rows_per_source({"complaints": 82}, ctx, "ViewComplaintsNearConstruction") == {}
