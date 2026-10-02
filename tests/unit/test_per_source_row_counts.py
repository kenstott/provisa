# Copyright (c) 2026 Kenneth Stott
# Canary: 6f0c2d7e-41b9-4a35-9c1e-b87d5a3e2f14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-source row counts for a GraphQL field's stats walk only the joins the query selected.

The count used to pass over every result row once per join in the role's whole context, whether
or not the query selected a join: a 6M-row scan through a context holding joins spent a minute
counting nothing."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.data.endpoint_helpers import _count_rows_per_source


def _join(source_id: str, cardinality: str) -> SimpleNamespace:
    return SimpleNamespace(target=SimpleNamespace(source_id=source_id), cardinality=cardinality)


class _Rows(list):
    """A row list that counts how many times it is walked."""

    walks = 0

    def __iter__(self):
        self.walks += 1
        return super().__iter__()


def _context(n_unselected: int = 50) -> SimpleNamespace:
    joins = {("Order", "customer"): _join("crm", "many-to-one")}
    joins[("Order", "items")] = _join("warehouse", "one-to-many")
    for i in range(n_unselected):
        joins[(f"Type{i}", f"rel_{i}")] = _join(f"src_{i}", "one-to-many")
    return SimpleNamespace(joins=joins)


def test_a_query_that_selected_no_join_walks_no_rows():
    rows = _Rows({"id": i, "region": "east"} for i in range(1000))
    assert _count_rows_per_source(rows, _context()) == {}
    assert rows.walks == 0


def test_selected_joins_are_counted_in_one_pass():
    rows = _Rows(
        [
            {"id": 1, "customer": {"id": 7}, "items": [{"sku": "a"}, {"sku": "b"}]},
            {"id": 2, "customer": None, "items": []},
            {"id": 3, "customer": {"id": 9}, "items": None},
            {"id": 4, "customer": {"id": 9}, "items": [{"sku": "c"}]},
        ]
    )
    assert _count_rows_per_source(rows, _context()) == {"crm": 3, "warehouse": 3}
    assert rows.walks == 1


def test_a_source_no_selected_join_targets_has_no_entry():
    """Its caller then reports the field's own row count for it, not zero."""
    ctx = SimpleNamespace(joins={("Customer", "orders"): _join("sales", "one-to-many")})
    assert _count_rows_per_source([{"id": 1}, {"id": 2}], ctx) == {}


def test_two_selected_joins_on_one_source_add_up():
    ctx = SimpleNamespace(
        joins={
            ("Order", "customer"): _join("crm", "many-to-one"),
            ("Order", "contacts"): _join("crm", "one-to-many"),
        }
    )
    rows = [{"customer": {"id": 1}, "contacts": [{"id": 1}, {"id": 2}]}]
    assert _count_rows_per_source(rows, ctx) == {"crm": 3}
