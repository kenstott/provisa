# Copyright (c) 2026 Kenneth Stott
# Canary: 2e9b5d41-7a03-4c68-9f2e-8b1c6d0a3f57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2247, REQ-2122: an entry's differences with its catalog, and a steward's decisions.

The review lists each value the catalog holds differently from Provisa, with both values and the
value last exchanged, grouped by kind then by table or term, with counts. A decision takes a side
per row or per group: the catalog's is written into Provisa, Provisa's is published, and either is
recorded as last exchanged, decided by the steward. A publish that succeeds records what it sent.

What is asserted is what a wrong surface gets wrong silently: listing values that agree, losing the
last-exchanged value, applying a decision on a difference that is gone, taking a side whose
direction is off, writing a kind Provisa cannot take, and recording a publish that failed.
"""

# Requirements: REQ-2247, REQ-2122

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.api.errors import ApiError
from provisa.api.metadata_export import sync_values as sv
from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    ColumnAsset,
    DomainAsset,
    GlossaryTermAsset,
    MetadataSnapshot,
    OwnerRef,
    TableAsset,
)
from provisa.api.metadata_export.provider import AssetError, AssetRefStub, PublishResult
from provisa.core.repositories import catalog_exchange
from tests.unit.test_syncing_admin_surface import (  # noqa: F401 - the shared fixtures
    ORG_ID,
    _catalog,
    _request,
    _served,
    _stub_publish,
    surface,
)

TABLE = AssetRef(AssetKind.TABLE, ("wh", "public", "orders"))
COL_ID = AssetRef(AssetKind.COLUMN, ("wh", "public", "orders", "id"))
COL_TOTAL = AssetRef(AssetKind.COLUMN, ("wh", "public", "orders", "total"))
GROUP = "wh.public.orders"


def _snapshot() -> MetadataSnapshot:
    return MetadataSnapshot(
        org_id=ORG_ID,
        domains=[
            DomainAsset(
                id="sales",
                description="Sales",
                steward=OwnerRef(id="alice", kind="steward"),
                pending=False,
            )
        ],
        tables=[
            TableAsset(
                ref=TABLE,
                name="orders",
                source_id="wh",
                domain_id="sales",
                description="Orders placed",
                columns=[
                    ColumnAsset(ref=COL_ID, name="id", data_type="integer", description="Order id"),
                    ColumnAsset(
                        ref=COL_TOTAL, name="total", data_type="numeric", description="Total"
                    ),
                ],
            )
        ],
        glossary_terms=[
            GlossaryTermAsset(
                term_id=1,
                name="Order",
                definition="A purchase",
                is_abstract=False,
                deprecated=False,
                status="approved",
                grounding="grounded",
            )
        ],
    )


def _catalog_values(**changed: object) -> dict:
    """The catalog's values: Provisa's, with the ``(kind, object)`` pairs in ``changed`` replaced."""
    values = sv.provisa_values(_snapshot())
    for name, value in changed.items():
        kind, obj = name.split("__")
        key = (kind, obj.replace("_", "."))
        values[key] = sv.SyncValue(kind, values[key].group, values[key].object, value)  # type: ignore[arg-type]
    return values


class _Catalog:
    """A provider whose catalog holds ``values``, publishing ``result``."""

    needs_export_views = False
    binds_products = False

    def __init__(self, values: dict, result: PublishResult | None = None):
        self.values = values
        self.result = result or PublishResult(provider_name="openmetadata", published={"t": 1})
        self.published = 0

    async def read(self, snapshot):
        assert snapshot.tables[0].ref == TABLE, "read for what the entry is sent"
        return self.values

    async def publish(self, snapshot):
        self.published += 1
        return self.result


@pytest.fixture
def entry(surface, monkeypatch):  # noqa: F811 - the imported fixture, requested
    """One OpenMetadata entry published as ``steward``, its snapshot the one above, its catalog
    the provider ``entry.catalog`` (replace its ``values`` to set a case)."""
    import provisa.api.admin.syncing_router as mod
    import provisa.api.metadata_export.publishing as sync_mod

    surface.settings["catalogs"] = [_catalog(provider="openmetadata", role="steward")]
    catalog = _Catalog(_catalog_values())
    _stub_publish(monkeypatch, catalog)
    monkeypatch.setattr(sync_mod, "build_snapshot", lambda config, **kwargs: _snapshot())
    monkeypatch.setattr(mod, "metadata_export", lambda target: catalog)
    surface.catalog = catalog
    return surface


def _conn():
    from provisa.api.app import state

    db = state.model_db
    assert db is not None
    return db.acquire()


async def _latest() -> dict:
    async with _conn() as conn:
        return await catalog_exchange.latest(conn, "mz")


async def _review() -> dict:
    from provisa.api.admin.syncing_router import review_entry

    return await review_entry(_request(), "mz")


async def _apply(*decisions: dict) -> dict:
    from provisa.api.admin.syncing_router import apply_review

    return await apply_review(_request({"decisions": list(decisions)}), "mz")


async def _register_orders() -> None:
    """Provisa's own rows for the snapshot's table, its columns and its term."""
    from provisa.core.schema_org import (
        domains,
        glossary_terms,
        registered_tables,
        sources,
        table_columns,
    )

    async with _conn() as conn:
        await conn.execute_core(insert(sources).values(id="wh", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(registered_tables).values(
                id=11, source_id="wh", domain_id="sales", schema_name="public", table_name="orders"
            )
        )
        for name in ("id", "total"):
            await conn.execute_core(insert(table_columns).values(table_id=11, column_name=name))
        await conn.execute_core(insert(glossary_terms).values(id=1, name="Order"))


async def _stored(table, column: str, **where):
    async with _conn() as conn:
        stmt = select(table.c[column])
        for name, value in where.items():
            stmt = stmt.where(table.c[name] == value)
        return (await conn.execute_core(stmt)).scalar_one()


# --- publish records what it sent -------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_publish_that_succeeds_records_what_it_sent(entry):
    from provisa.api.admin.syncing_router import publish_entry

    assert (await publish_entry(_request(), "mz"))["ok"] is True
    latest = await _latest()
    assert {k: (e.direction, e.value, e.decided_by) for k, e in latest.items()} == {
        k: ("export", v.value, catalog_exchange.PUBLISH)
        for k, v in sv.provisa_values(_snapshot()).items()
    }


@pytest.mark.asyncio
async def test_a_publish_that_failed_records_nothing(entry):
    from provisa.api.admin.syncing_router import publish_entry

    entry.catalog.result = PublishResult(
        provider_name="openmetadata",
        errors=[AssetError(asset=AssetRefStub("wh.public.orders"), message="refused")],
    )
    assert (await publish_entry(_request(), "mz"))["ok"] is False
    assert await _latest() == {}


# --- the review --------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_review_lists_differences_grouped_with_counts_and_last_exchanged(entry):
    async with _conn() as conn:
        await catalog_exchange.record(
            conn, "mz", "export", [(sv.DESCRIPTION, GROUP, "Orders placed")], decided_by="publish"
        )
    entry.catalog.values = _catalog_values(
        description__wh_public_orders="Orders, as the catalog says",
        column_description__wh_public_orders_id="The id",
        column_description__wh_public_orders_total="Sum",
        tag__wh_public_orders=("PII.Sensitive",),
    )
    body = await _review()

    assert (body["id"], body["provider"], body["total"]) == ("mz", "openmetadata", 4)
    assert body["directions"] == {"export": True, "import": True}
    assert [(k["kind"], k["count"]) for k in body["kinds"]] == [
        (sv.DESCRIPTION, 1),
        (sv.COLUMN_DESCRIPTION, 2),
        (sv.TAG, 1),
    ]
    assert all(k["standing_choice"] == {"export": None, "import": None} for k in body["kinds"])
    columns = body["kinds"][1]
    assert [(g["group"], g["count"]) for g in columns["groups"]] == [(GROUP, 2)]
    assert [r["object"] for r in columns["groups"][0]["rows"]] == [
        "wh.public.orders.id",
        "wh.public.orders.total",
    ]
    (description,) = body["kinds"][0]["groups"][0]["rows"]
    assert {k: description[k] for k in ("provisa", "catalog", "changed")} == {
        "provisa": "Orders placed",
        "catalog": "Orders, as the catalog says",
        # The catalog moved off what was last sent.
        "changed": "catalog",
    }
    assert {k: description["last_exchanged"][k] for k in ("value", "direction", "decided_by")} == {
        "value": "Orders placed",
        "direction": "export",
        "decided_by": "publish",
    }
    tag = body["kinds"][2]["groups"][0]["rows"][0]
    assert (tag["provisa"], tag["catalog"], tag["last_exchanged"]) == ([], ["PII.Sensitive"], None)


@pytest.mark.asyncio
async def test_values_the_catalog_does_not_hold_are_not_differences(entry):
    entry.catalog.values = {}
    assert (await _review())["total"] == 0


@pytest.mark.asyncio
async def test_a_provider_that_does_not_read_is_refused_by_name(entry, monkeypatch):
    import provisa.api.admin.syncing_router as mod
    from provisa.api.metadata_export.provider import CatalogReadNotSupported

    class _NoRead(_Catalog):
        async def read(self, snapshot):
            raise CatalogReadNotSupported("atlas")

    monkeypatch.setattr(mod, "metadata_export", lambda target: _NoRead({}))
    with pytest.raises(ApiError) as caught:
        await _review()
    assert caught.value.status_code == 400
    assert "'atlas' is not supported for reading yet" in str(caught.value)


@pytest.mark.asyncio
async def test_a_refused_read_is_a_named_502_never_an_empty_review(entry, monkeypatch):
    import httpx

    import provisa.api.admin.syncing_router as mod

    class _Refused(_Catalog):
        async def read(self, snapshot):
            request = httpx.Request("GET", "https://catalog.example/api/v1/tables/name/x")
            raise httpx.HTTPStatusError(
                "403", request=request, response=httpx.Response(403, request=request)
            )

    monkeypatch.setattr(mod, "metadata_export", lambda target: _Refused({}))
    with pytest.raises(ApiError) as caught:
        await _review()
    assert caught.value.status_code == 502


# --- applying ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_taking_the_catalogs_value_per_row_writes_it_into_provisa_and_records_it(entry):
    from provisa.core.schema_org import registered_tables

    await _register_orders()
    entry.catalog.values = _catalog_values(description__wh_public_orders="Orders, edited")
    body = await _apply({"side": "catalog", "kind": sv.DESCRIPTION, "object": GROUP})

    assert body["applied"] == {"provisa": 0, "catalog": 1}
    assert body["publish"] is None
    assert entry.catalog.published == 0, "taking the catalog's value publishes nothing"
    assert await _stored(registered_tables, "description", id=11) == "Orders, edited"
    last = (await _latest())[(sv.DESCRIPTION, GROUP)]
    assert (last.direction, last.value, last.decided_by) == ("import", "Orders, edited", "alice")


@pytest.mark.asyncio
async def test_taking_the_catalogs_side_per_group_writes_every_row_of_the_group(entry):
    from provisa.core.schema_org import table_columns

    await _register_orders()
    entry.catalog.values = _catalog_values(
        column_description__wh_public_orders_id="The id",
        column_description__wh_public_orders_total="Sum",
        description__wh_public_orders="Not decided here",
    )
    body = await _apply({"side": "catalog", "kind": sv.COLUMN_DESCRIPTION, "group": GROUP})

    assert body["applied"] == {"provisa": 0, "catalog": 2}
    assert await _stored(table_columns, "description", table_id=11, column_name="id") == "The id"
    assert await _stored(table_columns, "description", table_id=11, column_name="total") == "Sum"
    latest = await _latest()
    assert {k: e.value for k, e in latest.items()} == {
        (sv.COLUMN_DESCRIPTION, "wh.public.orders.id"): "The id",
        (sv.COLUMN_DESCRIPTION, "wh.public.orders.total"): "Sum",
    }
    # The table's description was not in the group, and stays a difference.
    assert [k["kind"] for k in (await _review())["kinds"]] == [
        sv.DESCRIPTION,
        sv.COLUMN_DESCRIPTION,
    ]


@pytest.mark.asyncio
async def test_taking_the_catalogs_term_definition_writes_it_into_the_glossary(entry):
    from provisa.core.schema_org import glossary_terms

    await _register_orders()
    entry.catalog.values = _catalog_values(term_definition__Order="A customer's purchase")
    await _apply({"side": "catalog", "kind": sv.TERM_DEFINITION, "object": "Order"})
    assert await _stored(glossary_terms, "definition", id=1) == "A customer's purchase"


@pytest.mark.asyncio
async def test_taking_provisas_value_publishes_the_entry_and_records_the_steward(entry):
    entry.catalog.values = _catalog_values(description__wh_public_orders="Catalog's own")
    body = await _apply({"side": "provisa", "kind": sv.DESCRIPTION, "group": GROUP})

    assert body["applied"] == {"provisa": 1, "catalog": 0}
    assert entry.catalog.published == 1
    assert body["publish"]["ok"] is True
    last = (await _latest())[(sv.DESCRIPTION, GROUP)]
    assert (last.direction, last.value, last.decided_by) == ("export", "Orders placed", "alice")


@pytest.mark.asyncio
async def test_a_failed_publish_applies_nothing(entry):
    await _register_orders()
    entry.catalog.values = _catalog_values(
        description__wh_public_orders="Catalog's own", column_description__wh_public_orders_id="x"
    )
    entry.catalog.result = PublishResult(
        provider_name="openmetadata",
        errors=[AssetError(asset=AssetRefStub("wh.public.orders"), message="refused")],
    )
    with pytest.raises(ApiError) as caught:
        await _apply(
            {"side": "provisa", "kind": sv.DESCRIPTION, "object": GROUP},
            {"side": "catalog", "kind": sv.COLUMN_DESCRIPTION, "object": "wh.public.orders.id"},
        )
    assert caught.value.status_code == 502
    assert await _latest() == {}


@pytest.mark.asyncio
async def test_a_decision_on_a_difference_that_is_gone_is_refused(entry):
    with pytest.raises(ApiError) as caught:
        await _apply({"side": "catalog", "kind": sv.DESCRIPTION, "object": GROUP})
    assert caught.value.status_code == 409


@pytest.mark.asyncio
async def test_a_side_whose_direction_is_off_is_refused(entry):
    entry.settings["catalogs"] = [
        _catalog(provider="openmetadata", role="steward", direction="export")
    ]
    entry.catalog.values = _catalog_values(description__wh_public_orders="Catalog's own")
    with pytest.raises(ApiError) as caught:
        await _apply({"side": "catalog", "kind": sv.DESCRIPTION, "object": GROUP})
    assert (caught.value.status_code, "does not import" in str(caught.value)) == (400, True)

    entry.settings["catalogs"] = [
        _catalog(provider="openmetadata", role="steward", direction="import")
    ]
    with pytest.raises(ApiError) as caught:
        await _apply({"side": "provisa", "kind": sv.DESCRIPTION, "object": GROUP})
    assert (caught.value.status_code, "does not export" in str(caught.value)) == (400, True)
    assert entry.catalog.published == 0


@pytest.mark.asyncio
async def test_taking_a_catalogs_tag_is_refused_by_name(entry):
    entry.catalog.values = _catalog_values(tag__wh_public_orders=("PII.Sensitive",))
    with pytest.raises(ApiError) as caught:
        await _apply({"side": "catalog", "kind": sv.TAG, "object": GROUP})
    assert caught.value.status_code == 400
    assert "taking a catalog's tag into Provisa is not supported yet" in str(caught.value)
    assert await _latest() == {}
