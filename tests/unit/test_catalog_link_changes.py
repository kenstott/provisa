# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2e8b17-9a46-4c03-81f7-e6b3a0c4d952
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122: what a catalog holds differently from a table, as the Import and the scheduled
import read it. A value edited in Provisa since its last import is marked a local edit; a column
reads the catalog column it was last imported from, else the one of its name; tags and terms are
offered only where Provisa has one of the same name."""

from __future__ import annotations

import pytest

from provisa.api.metadata_export import linking
from provisa.api.metadata_export.reader import AssetMetadata, CatalogAsset
from provisa.core.repositories.catalog_link import Imported

ASSET = CatalogAsset(ref="o", name="pg.sales.public.orders", physical=("sales", "public", "orders"))


def _ctx(**overrides) -> linking.TableContext:
    base = {
        "id": 1,
        "domain_id": "sales",
        "semantic_uri": "provisa://sales/tables/orders",
        "name": "orders",
        "physical": ("sales", "public", "orders"),
        "description": "Orders placed",
        "columns": {"id": "", "total": "Old total", "region": ""},
        "tags": frozenset(),
        "terms": {},
    }
    return linking.TableContext(**{**base, **overrides})


def _meta(**overrides) -> AssetMetadata:
    base = {
        "asset": ASSET,
        "description": "Every order",
        "columns": {"ID": "The id", "amount": "The total", "total": "Gross"},
        "tags": ("Tier.gold", "Tier.platinum"),
        "column_tags": {"ID": ("Governance.pii",)},
        "column_terms": {"amount": ("Business.order total",), "ID": ("Business.Patron",)},
    }
    return AssetMetadata(**{**base, **overrides})


@pytest.fixture(autouse=True)
def registry(monkeypatch):
    async def _registry(_conn):
        return {"pii": "pii", "gold": "gold"}, {"order total": "order total"}

    monkeypatch.setattr(linking, "_registry", _registry)


def _held(values: dict) -> Imported:
    return Imported(catalog="om", imported_at=None, values=values, locked=False)


def test_a_column_reads_the_catalog_column_it_was_imported_from_else_its_namesake():
    held = _held({"column_map": {"total": "amount"}})
    assert linking.column_map(_ctx(), _meta(), held) == {"id": "ID", "total": "amount"}
    assert linking.column_map(_ctx(), _meta(), None) == {"id": "ID", "total": "total"}


@pytest.mark.asyncio
async def test_every_difference_is_found_and_only_namesake_tags_and_terms():
    found = await linking.changes(None, _ctx(), _meta(), _held({"column_map": {"total": "amount"}}))
    seen = {(c.field, c.column, c.catalog) for c in found}
    assert seen == {
        ("description", None, "Every order"),
        ("column", "id", "The id"),
        ("column", "total", "The total"),
        ("tag", "id", "pii"),
        ("term", "total", "order total"),
        ("tag", None, "gold"),
    }


@pytest.mark.asyncio
async def test_a_value_edited_since_its_import_is_a_local_edit():
    held = _held({"description": "Orders as imported", "columns": {"total": "Old total"}})
    found = {(c.field, c.column): c for c in await linking.changes(None, _ctx(), _meta(), held)}
    assert found[("description", None)].local_edit is True
    # The total still holds what was imported: replacing it loses nothing of Provisa's.
    assert found[("column", "total")].local_edit is False
    # Never imported: nothing to have edited since.
    assert found[("column", "id")].local_edit is False


@pytest.mark.asyncio
async def test_nothing_held_already_is_offered_again():
    ctx = _ctx(
        description="Every order",
        columns={"id": "The id"},
        tags=frozenset({("id", "pii"), (None, "gold")}),
    )
    assert await linking.changes(None, ctx, _meta(column_terms={}), None) == []


@pytest.mark.asyncio
async def test_accepting_all_takes_every_change():
    held = _held({"column_map": {"total": "amount"}})
    found = await linking.changes(None, _ctx(), _meta(), held)
    accepted = linking.accept_all(_ctx(), _meta(), held, found)
    assert accepted.description is True
    assert accepted.columns == {"id": "ID", "total": "amount"}
    assert set(accepted.tags) == {("id", "pii"), (None, "gold")}
    assert accepted.terms == {"total": "order total"}
