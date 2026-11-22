# Copyright (c) 2026 Kenneth Stott
# Canary: 8a3f1c62-4e07-4b9d-a5c8-2f6e9d0b7c31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2262: relationship review after registration. The review is a discovery run scoped to the
tables just registered against every table, either way round: declared foreign keys, matching key
names and types (no model asked), and joins to tables already in the model, across sources. Every
candidate waits for review in the run's result."""

# Requirements: REQ-2262

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.api.errors import ApiError
from provisa.discovery.columns import ColumnFacts, TableFacts, type_groups
from provisa.discovery.direction import Known, Sampled, from_answer, settle
from provisa.discovery.key_names import CONFIDENCE, key_name_proposals
from tests.unit.test_discovery_run_api import (  # noqa: F401 -- the autouse fixture
    _ASSIGNED,
    _World,
    _candidates,
    _nothing_published,
    _run,
    _scope,
    _store,
    _table,
)


def col(name: str, data_type: str = "integer", *, pk: bool = False) -> ColumnFacts:
    return ColumnFacts(
        name=name,
        data_type=data_type,
        description=None,
        tags=(),
        classes=("unknown",),
        sensitive=False,
        samples=(),
        is_primary_key=pk,
        groups=type_groups(data_type, subtype=None, array_depth=0),
    )


def tbl(table_id: int, name: str, *columns: ColumnFacts) -> TableFacts:
    return TableFacts(
        table_id=table_id,
        domain_id="sales",
        schema_name="public",
        table_name=name,
        description=None,
        tags=(),
        role=None,
        columns=tuple(columns),
    )


def edges(proposals) -> set[tuple[int, str, int, str]]:
    return {p.edge for p in proposals}


# --- key names and types ---------------------------------------------------------------------------


def test_a_column_named_as_another_tables_key_is_proposed_to_refer_to_it():
    orders = tbl(1, "orders", col("id", pk=True), col("customer_id"))
    customers = tbl(2, "customers", col("customer_id", pk=True), col("name", "text"))
    found = key_name_proposals([orders, customers], None, either_way=False)
    assert edges(found) == {(1, "customer_id", 2, "customer_id")}
    (p,) = found
    assert p.kind == "key_names"
    assert p.cardinality == "many-to-one"
    assert p.confidence == CONFIDENCE
    assert p.reasoning == "name and type match the primary key customers.customer_id"


@pytest.mark.parametrize("named", ["customer_id", "customerid", "customers_id"])
def test_a_column_named_as_the_key_prefixed_by_its_table_refers_to_a_generic_id(named):
    orders = tbl(1, "orders", col("id", pk=True), col(named))
    customers = tbl(2, "customers", col("id", pk=True))
    assert edges(key_name_proposals([orders, customers], None, either_way=False)) == {
        (1, named, 2, "id")
    }


def test_a_plural_in_ies_is_read_as_its_singular():
    cities = tbl(1, "cities", col("id", pk=True))
    stores = tbl(2, "stores", col("id", pk=True), col("city_id"))
    assert edges(key_name_proposals([cities, stores], None, either_way=False)) == {
        (2, "city_id", 1, "id")
    }


def test_two_tables_keyed_by_a_generic_id_are_not_proposed_to_join():
    a = tbl(1, "orders", col("id", pk=True))
    b = tbl(2, "stores", col("id", pk=True))
    assert key_name_proposals([a, b], None, either_way=False) == []


def test_a_name_match_of_another_type_group_is_not_proposed():
    orders = tbl(1, "orders", col("id", pk=True), col("customer_id", "text"))
    customers = tbl(2, "customers", col("id", pk=True))
    assert key_name_proposals([orders, customers], None, either_way=False) == []


def test_a_composite_key_is_not_matched_by_name():
    lines = tbl(1, "lines", col("order_id", pk=True), col("line_no", pk=True))
    notes = tbl(2, "notes", col("id", pk=True), col("order_id"), col("line_no"))
    assert key_name_proposals([lines, notes], None, either_way=False) == []


def test_without_either_way_only_source_columns_refer_to_target_keys():
    new = tbl(1, "customers", col("id", pk=True))
    old = tbl(2, "orders", col("id", pk=True), col("customer_id"))
    assert key_name_proposals([new], [new, old], either_way=False) == []
    assert edges(key_name_proposals([new], [new, old], either_way=True)) == {
        (2, "customer_id", 1, "id")
    }


def test_either_way_proposes_each_match_once():
    a = tbl(1, "customers", col("id", pk=True), col("order_id"))
    b = tbl(2, "orders", col("id", pk=True), col("customer_id"))
    found = key_name_proposals([a, b], [a, b], either_way=True)
    assert sorted(edges(found)) == [(1, "order_id", 2, "id"), (2, "customer_id", 1, "id")]


# --- the review's scope: either way round ------------------------------------------------------------


class _NoSample:
    async def __call__(self, table, column):
        return Sampled(rows=0, repeats=False)


async def test_a_relationship_from_an_existing_table_to_a_new_one_is_in_the_reviews_scope():
    new = tbl(1, "customers", col("id", pk=True))
    old = tbl(2, "orders", col("id", pk=True), col("customer_id"))
    p = from_answer("key_names", (2, "customer_id"), (1, "id"), "many-to-one", 0.9, "r")
    tables = {1: new, 2: old}
    narrow = await settle([p], tables, {1}, {1, 2}, Known(), _NoSample())
    assert narrow.kept == [] and narrow.removed.out_of_scope == 1
    wide = await settle([p], tables, {1}, {1, 2}, Known(), _NoSample(), either_way=True)
    assert edges(wide.kept) == {(2, "customer_id", 1, "id")}


async def test_either_way_still_leaves_out_a_pair_between_two_existing_tables():
    new = tbl(1, "customers", col("id", pk=True))
    old = tbl(2, "orders", col("id", pk=True), col("store_id"))
    stores = tbl(3, "stores", col("id", pk=True))
    p = from_answer("key_names", (2, "store_id"), (3, "id"), "many-to-one", 0.9, "r")
    out = await settle(
        [p], {1: new, 2: old, 3: stores}, {1}, {1, 2, 3}, Known(), _NoSample(), either_way=True
    )
    assert out.kept == [] and out.removed.out_of_scope == 1


def test_either_way_with_the_target_the_same_as_the_source_is_refused():
    scope = _scope(kinds=["key_names"], either_way=True)
    with pytest.raises(ApiError) as refused:
        scope.spec("steward", None)
    assert refused.value.code == "discovery.either_way_needs_target"


def test_key_names_is_a_kind_a_run_can_ask_for():
    assert _scope(kinds=["key_names"]).spec("steward", None).kinds == frozenset({"key_names"})
    with pytest.raises(ValidationError):
        _scope(kinds=["names_and_keys"])


# --- the review run end to end on a real model store ---------------------------------------------------


async def test_the_review_proposes_declared_keys_name_matches_and_joins_to_existing_tables(
    monkeypatch, tmp_path
):
    """Tables 3 (stores) and 4 (regions) were just registered; 1 (orders) and 2 (customers) were
    already in the model. The review asks foreign keys and key names, no model: the declared key
    from stores to regions, stores.customer_id to customers (a new table to an existing one),
    orders.store_id to stores (an existing table to a new one), and never orders.customer_id to
    customers, a pair between two existing tables."""
    from provisa.discovery.analyzer import RelationshipCandidate

    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _table(
        db,
        1,
        "sales",
        "orders",
        [
            ("id", "integer", True),
            ("customer_id", "integer", False),
            ("store_id", "integer", False),
        ],
    )
    await _table(db, 2, "crm", "customers", [("id", "integer", True), ("name", "text", False)])
    await _table(
        db,
        3,
        "sales",
        "stores",
        [("id", "integer", True), ("region", "integer", False), ("customer_id", "integer", False)],
    )
    await _table(db, 4, "sales", "regions", [("id", "integer", True)])
    world = _World(monkeypatch, db, config=_ASSIGNED, api_keys={})
    world.fk_found = [
        RelationshipCandidate(3, "region", 4, "id", "many-to-one", 1.0, "Foreign key constraint")
    ]
    scope = _scope(
        kinds=["foreign_keys", "key_names"],
        source_domains=["sales", "crm"],
        source_tables=[3, 4],
        target_same_as_source=False,
        target_domains=["sales", "crm"],
        target_tables=None,
        either_way=True,
    )
    run = await _run(world, scope)
    assert run["status"] == "done", run
    assert world.model_calls == []
    report = run["report"]
    assert report["foreign_keys"] == {"found": 1, "suggested": 1}
    assert report["key_names"] == {"found": 2, "suggested": 2}
    stored = {
        (c["source_table_id"], c["source_column"], c["target_table_id"], c["target_column"])
        for c in await _candidates(db)
    }
    assert stored == {(3, "region", 4, "id"), (3, "customer_id", 2, "id"), (1, "store_id", 3, "id")}
    # Every candidate waits for review, the declared key included.
    assert {c["status"] for c in await _candidates(db)} == {"suggested"}
