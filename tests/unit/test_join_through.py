# Copyright (c) 2026 Kenneth Stott
# Canary: 9e3b7c15-2a6d-4f80-b4c9-1d8e5a3f7c62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1586 (amended 2026-11-16): two relationships that share a middle table are joined through
it into one junction-backed relationship between their other ends.

They qualify when they refer from one junction (J.a -> A, J.b -> B; J's other columns become the
relationship's attributes) or to one shared dimension (A.x -> D, B.y -> D). Joining them creates
one via_table relationship, many-to-many, named as generated names are (REQ-415); the two stay.
Qualification is pure; planning and creating run on a real model store (SQLite) with the save
faked at createRelationship's body."""

# Requirements: REQ-1586, REQ-415

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import join_through
from provisa.core import schema_org as so
from provisa.core.database import Database, create_engine_from_url
from provisa.core.join_through import Hop, Through, qualify

_PRODUCTS, _TAGS, _PRODUCT_TAGS, _REGIONS, _STORES, _CUSTOMERS = 1, 2, 3, 4, 5, 6


def _hop(
    rel_id: str,
    source: tuple[int, str],
    target: tuple[int, str] | None,
    cardinality: str = "many-to-one",
    via_table_id: int | None = None,
) -> Hop:
    return Hop(
        rel_id=rel_id,
        source_table_id=source[0],
        source_column=source[1],
        target_table_id=None if target is None else target[0],
        target_column="" if target is None else target[1],
        cardinality=cardinality,
        via_table_id=via_table_id,
    )


_TAGGED_PRODUCT = _hop("pt-product", (_PRODUCT_TAGS, "product_id"), (_PRODUCTS, "id"))
_TAGGED_TAG = _hop("pt-tag", (_PRODUCT_TAGS, "tag_id"), (_TAGS, "id"))
_STORE_REGION = _hop("store-region", (_STORES, "region_code"), (_REGIONS, "code"))
_CUSTOMER_REGION = _hop("customer-region", (_CUSTOMERS, "home_region"), (_REGIONS, "code"))


# --- qualification ---------------------------------------------------------------------------


def test_two_relationships_referring_from_one_junction_join_its_other_ends():
    assert qualify(_TAGGED_PRODUCT, _TAGGED_TAG) == Through(
        shape="junction",
        via_table_id=_PRODUCT_TAGS,
        source_table_id=_PRODUCTS,
        source_column="id",
        target_table_id=_TAGS,
        target_column="id",
        via_source_column="product_id",
        via_target_column="tag_id",
    )


def test_a_junction_leg_declared_from_its_one_side_qualifies_the_same():
    from_product = _hop(
        "product-pt", (_PRODUCTS, "id"), (_PRODUCT_TAGS, "product_id"), "one-to-many"
    )
    assert qualify(from_product, _TAGGED_TAG) == qualify(_TAGGED_PRODUCT, _TAGGED_TAG)


def test_two_relationships_referring_to_one_dimension_join_the_referring_ends():
    assert qualify(_STORE_REGION, _CUSTOMER_REGION) == Through(
        shape="shared_dimension",
        via_table_id=_REGIONS,
        source_table_id=_STORES,
        source_column="region_code",
        target_table_id=_CUSTOMERS,
        target_column="home_region",
        via_source_column="code",
        via_target_column="code",
    )


def test_the_first_ticked_is_the_source_end():
    through = qualify(_TAGGED_TAG, _TAGGED_PRODUCT)
    assert through is not None
    assert (through.source_table_id, through.target_table_id) == (_TAGS, _PRODUCTS)


@pytest.mark.parametrize(
    ("first", "second"),
    [
        # Nothing shared: the junction's leg and the dimension's.
        (_TAGGED_PRODUCT, _STORE_REGION),
        # One relationship ticked twice.
        (_TAGGED_PRODUCT, _TAGGED_PRODUCT),
        # A relationship and its reverse on the same columns.
        (
            _TAGGED_PRODUCT,
            _hop("product-pt", (_PRODUCTS, "id"), (_PRODUCT_TAGS, "product_id"), "one-to-many"),
        ),
        # A computed relationship has no target table.
        (_TAGGED_PRODUCT, _hop("computed", (_PRODUCT_TAGS, "tag_id"), None)),
        # A relationship that is junction-backed already joins nothing further.
        (
            _TAGGED_PRODUCT,
            _hop("joined", (_PRODUCT_TAGS, "tag_id"), (_TAGS, "id"), "one-to-many", _REGIONS),
        ),
        # One column referring to two tables is no junction.
        (_TAGGED_PRODUCT, _hop("pt-tag-too", (_PRODUCT_TAGS, "product_id"), (_TAGS, "id"))),
        # The shared table is an end of the other: a chain, not a middle table.
        (_TAGGED_PRODUCT, _hop("self", (_PRODUCT_TAGS, "parent_id"), (_PRODUCT_TAGS, "id"))),
        (_STORE_REGION, _hop("region-parent", (_REGIONS, "parent"), (_REGIONS, "code"))),
    ],
)
def test_pairs_that_share_no_middle_table_do_not_qualify(first: Hop, second: Hop):
    assert qualify(first, second) is None


# --- planning and creating on a model store ---------------------------------------------------

_TABLES = ("sources", "domains", "registered_tables", "table_columns", "relationships")

_SHAPES: dict[int, tuple[str, list[str]]] = {
    _PRODUCTS: ("products", ["id", "name"]),
    _TAGS: ("tags", ["id", "label"]),
    _PRODUCT_TAGS: ("product_tags", ["product_id", "tag_id", "weight", "tagged_at"]),
    _REGIONS: ("regions", ["code"]),
    _STORES: ("stores", ["id", "region_code"]),
    _CUSTOMERS: ("customers", ["id", "home_region"]),
}


async def _store(tmp_path) -> Database:
    db = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'store.db'}"), name="through"
    )
    with db.engine.begin() as conn:
        so.metadata.create_all(conn, tables=[so.metadata.tables[n] for n in _TABLES])
    async with db.acquire() as conn:
        await conn.execute_core(insert(so.sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(so.domains).values(id="shop"))
        for tid, (name, columns) in _SHAPES.items():
            await conn.execute_core(
                insert(so.registered_tables).values(
                    id=tid, source_id="pg", domain_id="shop", schema_name="public", table_name=name
                )
            )
            for col in columns:
                await conn.execute_core(
                    insert(so.table_columns).values(
                        table_id=tid,
                        domain_id="shop",
                        column_name=col,
                        data_type="integer",
                        is_primary_key=col in ("id", "code"),
                    )
                )
        for hop in (_TAGGED_PRODUCT, _TAGGED_TAG, _STORE_REGION, _CUSTOMER_REGION):
            await conn.execute_core(
                insert(so.relationships).values(
                    id=hop.rel_id,
                    source_table_id=hop.source_table_id,
                    source_column=hop.source_column,
                    target_table_id=hop.target_table_id,
                    target_column=hop.target_column,
                    cardinality=hop.cardinality,
                )
            )
    return db


class _Saved:
    """createRelationship's body, faked: each input stored as the registry stores it."""

    def __init__(self, monkeypatch, db: Database):
        from provisa.api.admin.types import MutationResult

        self.inputs: list[Any] = []

        async def save(info, input, *, creating):
            assert creating
            self.inputs.append(input)
            async with db.acquire() as conn:
                ids = {
                    r.table_name: r.id
                    for r in (
                        await conn.execute_core(
                            select(so.registered_tables.c.id, so.registered_tables.c.table_name)
                        )
                    ).fetchall()
                }
                await conn.execute_core(
                    insert(so.relationships).values(
                        id=input.id,
                        source_table_id=ids[input.source_table_id],
                        source_column=input.source_column,
                        target_table_id=ids[input.target_table_id],
                        target_column=input.target_column,
                        cardinality=input.cardinality,
                        alias=input.alias,
                        graphql_alias=input.graphql_alias,
                        via_table_id=ids[input.via_table],
                        via_source_column=input.via_source_column,
                        via_target_column=input.via_target_column,
                        via_label_source=input.via_label_source,
                    )
                )
            return MutationResult(success=True, message="saved")

        monkeypatch.setattr("provisa.api.admin.schema_mutation._save_relationship_as_caller", save)


_INFO = SimpleNamespace(context={})


async def test_a_junction_pair_plans_one_relationship_with_the_junctions_other_columns(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        planned = await join_through.plan(conn, "pt-product", "pt-tag")
    assert planned is not None
    assert planned.through.shape == "junction"
    assert (planned.source_table, planned.via_table, planned.target_table) == (
        "products",
        "product_tags",
        "tags",
    )
    assert planned.named.rel_id == "products-tags-via-product_tags"
    # The junction's columns that are no key of the join are the relationship's attributes.
    assert planned.attributes == ("weight", "tagged_at")


async def test_a_shared_dimension_pair_plans_a_relationship_through_the_dimension(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        planned = await join_through.plan(conn, "store-region", "customer-region")
    assert planned is not None
    assert planned.through.shape == "shared_dimension"
    assert (planned.source_table, planned.via_table, planned.target_table) == (
        "stores",
        "regions",
        "customers",
    )
    assert planned.attributes == ()


async def test_a_pair_sharing_no_middle_table_plans_nothing(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        assert await join_through.plan(conn, "pt-product", "store-region") is None
        assert await join_through.plan(conn, "pt-product", "no-such-relationship") is None


async def test_joining_creates_one_via_table_relationship_and_keeps_both_hops(
    tmp_path, monkeypatch
):
    db = await _store(tmp_path)
    saved = _Saved(monkeypatch, db)
    async with db.acquire() as conn:
        planned = await join_through.plan(conn, "pt-product", "pt-tag")
        assert planned is not None
        result, made = await join_through.create(_INFO, conn, planned.through, planned.named.rel_id)
    assert result.success and made == "products-tags-via-product_tags"
    (given,) = saved.inputs
    assert (given.source_table_id, given.source_column) == ("products", "id")
    assert (given.target_table_id, given.target_column) == ("tags", "id")
    assert (given.via_table, given.via_source_column, given.via_target_column) == (
        "product_tags",
        "product_id",
        "tag_id",
    )
    # Many-to-many: each product reaches a list of tags through the junction.
    assert given.cardinality == "one-to-many"
    assert (given.via_label_source, given.alias) == ("fixed", "PRODUCT_TAGS")
    async with db.acquire() as conn:
        ids = {r.id for r in (await conn.execute_core(select(so.relationships.c.id))).fetchall()}
    assert {"pt-product", "pt-tag", made} <= ids
    # Joined once, the pair is not offered again, either way round.
    async with db.acquire() as conn:
        assert await join_through.plan(conn, "pt-product", "pt-tag") is None
        assert await join_through.plan(conn, "pt-tag", "pt-product") is None


async def test_a_generated_name_that_is_taken_is_told_apart(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.relationships).values(
                id="products-tags-via-product_tags",
                source_table_id=_TAGS,
                source_column="id",
                target_table_id=_TAGS,
                target_column="id",
                cardinality="one-to-one",
            )
        )
        planned = await join_through.plan(conn, "pt-product", "pt-tag")
    assert planned is not None
    assert planned.named.rel_id == "products-tags-via-product_tags_by_id"


async def test_a_typed_name_that_is_taken_is_refused(tmp_path, monkeypatch):
    from provisa.core.relationship_naming import RelationshipNameTaken

    db = await _store(tmp_path)
    _Saved(monkeypatch, db)
    async with db.acquire() as conn:
        planned = await join_through.plan(conn, "pt-product", "pt-tag")
        assert planned is not None
        with pytest.raises(RelationshipNameTaken):
            await join_through.create(_INFO, conn, planned.through, "pt-tag")


async def test_two_relationships_through_one_junction_get_different_cypher_types(
    tmp_path, monkeypatch
):
    """A junction with a third leg: products-tags and products-<third> both start at products and
    are labelled for the junction; the second is told apart where it would share a target."""
    db = await _store(tmp_path)
    _Saved(monkeypatch, db)
    async with db.acquire() as conn:
        planned = await join_through.plan(conn, "pt-product", "pt-tag")
        assert planned is not None
        await join_through.create(_INFO, conn, planned.through, None)
        # The same two ends through the junction on other columns: a second junction-backed
        # relationship between products and tags, labelled for the same junction.
        other = Through(
            shape="junction",
            via_table_id=_PRODUCT_TAGS,
            source_table_id=_PRODUCTS,
            source_column="id",
            target_table_id=_TAGS,
            target_column="id",
            via_source_column="product_id",
            via_target_column="weight",
        )
        _, second = await join_through.create(_INFO, conn, other, None)
        rows = {
            r.id: r
            for r in (
                await conn.execute_core(
                    select(so.relationships.c.id, so.relationships.c.alias).where(
                        so.relationships.c.via_table_id == _PRODUCT_TAGS
                    )
                )
            ).fetchall()
        }
    assert rows["products-tags-via-product_tags"].alias == "PRODUCT_TAGS"
    assert second != "products-tags-via-product_tags"
    assert rows[second].alias.upper() != "PRODUCT_TAGS"
