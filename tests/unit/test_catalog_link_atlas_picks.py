# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7b2a93-0c58-4d16-b9f3-6a1d8e5c2f07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122: in Atlas-shaped catalogs a picked table publishes no entity of Provisa's own, its
lineage and product membership are dropped and reported by name, and a locked table's
descriptions and classifications are not sent."""

from __future__ import annotations

import httpx
import pytest

from provisa.api.metadata_export.atlas import AtlasEntity, AtlasExport
from provisa.api.metadata_export.model import AssetKind, AssetRef
from provisa.api.metadata_export.provider import PublishResult
from provisa.core.models import MetadataExportTarget

URI = "provisa://sales/tables/orders"


def _entities() -> list[AtlasEntity]:
    table = AssetRef(kind=AssetKind.TABLE, parts=("pg", "public", "orders"))
    view = AssetRef(kind=AssetKind.TABLE, parts=("__derived__", "views", "totals"))
    return [
        AtlasEntity(
            asset=table,
            kind="table",
            type_name="provisa_table",
            guid="-1",
            attributes={"qualifiedName": "orders@provisa", "provisaUri": URI, "description": "x"},
            classifications=[{"typeName": "provisa_masked"}],
        ),
        AtlasEntity(
            asset=AssetRef(kind=AssetKind.COLUMN, parts=("pg", "public", "orders", "id")),
            kind="column",
            type_name="provisa_column",
            guid="-2",
            attributes={"qualifiedName": "orders.id@provisa", "description": "y"},
            relationships={"table": {"guid": "-1"}},
        ),
        AtlasEntity(
            asset=view,
            kind="table",
            type_name="provisa_table",
            guid="-3",
            attributes={
                "qualifiedName": "totals@provisa",
                "provisaUri": "provisa://sales/tables/totals",
            },
        ),
        AtlasEntity(
            asset=view,
            kind="lineage",
            type_name="Process",
            guid="-4",
            attributes={
                "qualifiedName": "totals.derivation@provisa",
                "inputs": [{"guid": "-1"}],
                "outputs": [{"guid": "-3"}],
            },
        ),
        AtlasEntity(
            asset=AssetRef(kind=AssetKind.DATA_PRODUCT, parts=("c360",)),
            kind="data_product",
            type_name="provisa_data_product",
            guid="-5",
            attributes={"qualifiedName": "c360@provisa"},
            relationships={"tables": [{"guid": "-1"}, {"guid": "-3"}]},
        ),
    ]


def _export() -> AtlasExport:
    return AtlasExport(MetadataExportTarget(id="atlas", provider="atlas", endpoint="http://atlas"))


def test_a_picked_table_and_its_columns_are_set_aside_and_references_to_it_dropped():
    export = _export()
    export.picked = {URI: ("guid-customer", "pg.sales.public.orders@pg")}
    result = PublishResult(provider_name="atlas")
    kept, picks = export._set_aside_picked(_entities(), result)

    assert [e.guid for e in kept] == ["-3", "-5"]
    (pick,) = picks
    assert (pick.ref, pick.uri, [c.guid for c in pick.columns]) == ("guid-customer", URI, ["-2"])
    product = kept[1]
    assert product.relationships["tables"] == [{"guid": "-3"}]
    messages = [e.message for e in result.errors]
    assert len(messages) == 2
    assert all("pg.public.orders" in m and "picked asset" in m for m in messages)


def test_nothing_is_set_aside_without_a_pick():
    kept, picks = _export()._set_aside_picked(_entities(), PublishResult(provider_name="atlas"))
    assert len(kept) == 5 and picks == []


@pytest.mark.asyncio
async def test_a_locked_table_and_its_columns_send_no_descriptions_or_classifications():
    export = _export()
    export.locked = frozenset({URI})
    entities = _entities()
    async with httpx.AsyncClient() as client:
        await export._keep_locked_descriptions(client, {}, entities)

    table, column, view = entities[0], entities[1], entities[2]
    assert "description" not in table.attributes and table.classifications == []
    assert "description" not in column.attributes
    assert view.attributes["provisaUri"] == "provisa://sales/tables/totals"
