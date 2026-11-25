# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e9a14-5b28-4f61-a0d3-8e2f6b91c457
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-24, EXPORT NAMES CATCH UP AT THE NEXT BUILD), REQ-1389: every export
provider records what it published in its bindings (``PublishResult.bindings``), so every export
gets its row in the names table (``export:<target>``, read from ``catalog_bindings``).

The registry is walked: each registered provider must have a publish case here, and each case
must bind every table it published under the table's Provisa URI with the target's own name for
it, and every column of those tables the same way when the provider exports columns. A new
provider that records no bindings fails here before it can ship."""

# Requirements: REQ-194, REQ-1389

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import replace
from types import SimpleNamespace

import boto3
import httpx
import pytest
from botocore.stub import Stubber

from provisa.api.metadata_export import build_snapshot, dataplex
from provisa.api.metadata_export.atlan import AtlanExport
from provisa.api.metadata_export.atlas import AtlasExport
from provisa.api.metadata_export.datahub import DataHubExport
from provisa.api.metadata_export.datazone import DataZoneExport
from provisa.api.metadata_export.provider import PublishResult
from provisa.api.metadata_export.purview import PurviewExport
from provisa.api.metadata_export.registry import metadata_export, registered_providers
from provisa.api.metadata_export.unity_catalog import UnityCatalogExport
from provisa.core.models import (
    Column,
    DataProduct,
    Domain,
    ProvisaConfig,
    Role,
    Source,
    SourceType,
    Table,
)
from tests.unit import test_metadata_export_bindings as vendors
from tests.unit import test_metadata_export_datazone as dz
from tests.unit import test_metadata_export_dataplex as dp
from tests.unit import test_metadata_export_purview as pv
from tests.unit import test_metadata_export_unity_catalog as uc
from tests.unit import test_snowflake_horizon_export as sf

pytestmark = pytest.mark.unit

# A case publishes a snapshot through its provider against a stand-in for the catalog and
# returns the result and the tables it published.
Case = Callable[[pytest.MonkeyPatch], Awaitable[tuple[PublishResult, list]]]


def _vendor_snapshot():
    """The REQ-1389 vendor tests' model: one table in one data product."""
    config = ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[Domain(id="sales", description="Sales")],
        tables=[
            Table(
                source_id="wh",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                columns=[
                    Column(name="id", data_type="integer", visible_to=["analyst"]),
                    Column(name="amount", data_type="numeric", visible_to=["analyst"]),
                ],
            )
        ],
        roles=[Role(id="analyst", description="Test role.", capabilities=[], domain_access=["*"])],
        data_products=[
            DataProduct(id="prod", domain_id="sales", name="prod", tables=["wh.public.orders"])
        ],
    )
    return build_snapshot(config, org_id="acme", dialect="postgres", contexts={})


async def _atlas(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(AtlasExport, "classification_merge", False)
    vendors._mock_atlas(monkeypatch)
    snapshot = _vendor_snapshot()
    return await AtlasExport(vendors._config("atlas")).publish(snapshot), snapshot.tables


async def _atlan(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(AtlanExport, "classification_merge", False)
    vendors._mock_atlas(monkeypatch)
    snapshot = _vendor_snapshot()
    return await AtlanExport(vendors._config("atlan")).publish(snapshot), snapshot.tables


def _atlas_api(request: httpx.Request) -> httpx.Response:
    """A stand-in Atlas API, as ``vendors._mock_atlas`` answers it: the bulk POST assigns guids,
    typedef probes miss, a search finds nothing and a live entity read has no attributes."""
    path = request.url.path
    if request.method == "POST" and path.endswith("/entity/bulk"):
        body = json.loads(request.content)
        assignments = {
            e["guid"]: f"created-{i}"
            for i, e in enumerate(body["entities"])
            if e["guid"].startswith("-")
        }
        return httpx.Response(200, json={"guidAssignments": assignments})
    if request.method == "POST" and "/search/basic" in path:
        return httpx.Response(200, json={"entities": []})
    if request.method == "POST":
        return httpx.Response(200, json={})  # typedef registration
    if "/entity/guid/" in path:
        return httpx.Response(200, json={"entity": {"attributes": {}}})
    return httpx.Response(404)  # typedef probes: nothing registered yet


async def _purview(monkeypatch: pytest.MonkeyPatch):
    # The Data Map pass (Atlas) publishes the tables; the Unified Catalog then publishes the
    # product over them.
    monkeypatch.setattr(AtlasExport, "classification_merge", False)
    catalog = pv._Purview()
    real = httpx.AsyncClient

    def _route(request: httpx.Request) -> httpx.Response:
        if "/api/atlas/" in request.url.path:
            return _atlas_api(request)
        return catalog.handle(request)

    monkeypatch.setattr(
        httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(_route), **kw)
    )
    snapshot = pv._snapshot()
    exporter = metadata_export(pv._target())
    assert isinstance(exporter, PurviewExport)
    return await exporter.publish(snapshot), snapshot.tables


async def _openmetadata(monkeypatch: pytest.MonkeyPatch):
    vendors._mock_openmetadata(monkeypatch)
    snapshot = _vendor_snapshot()
    return await metadata_export(vendors._config("openmetadata")).publish(snapshot), snapshot.tables


async def _collibra(monkeypatch: pytest.MonkeyPatch):
    vendors._mock_collibra(monkeypatch, lookup_results=vendors.COLLIBRA_ASSETS)
    snapshot = _vendor_snapshot()
    return await metadata_export(vendors._config("collibra")).publish(snapshot), snapshot.tables


async def _datahub(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(DataHubExport, "tag_merge", False)
    vendors._mock_datahub(monkeypatch)
    snapshot = _vendor_snapshot()
    return await metadata_export(vendors._config("datahub")).publish(snapshot), snapshot.tables


async def _datazone(monkeypatch: pytest.MonkeyPatch):
    client = boto3.client(
        "datazone",
        region_name="us-east-1",
        aws_access_key_id="AKIA-TEST",
        aws_secret_access_key="secret",
    )
    monkeypatch.setattr(DataZoneExport, "_client", lambda self: client)
    snapshot = dz._snapshot()
    (table,) = snapshot.tables
    with Stubber(client) as stub:
        dz._types_exist(stub)
        dz._search(stub, "ASSET", [])
        dz._search(stub, "DATA_PRODUCT", [])
        stub.add_response("create_asset", dz._asset("asset-1", "1", table), dz.ANY_PARAMS)
        dz._listing(stub, "asset-1", "ASSET", "1")
        stub.add_response("create_data_product", dz._product("dp-1", "1"), dz.ANY_PARAMS)
        dz._listing(stub, "dp-1", "DATA_PRODUCT", "1")
        result = await metadata_export(dz._target()).publish(snapshot)
        stub.assert_no_pending_responses()
    return result, snapshot.tables


async def _dataplex(monkeypatch: pytest.MonkeyPatch):
    fake = dp._Dataplex()
    real = httpx.AsyncClient
    monkeypatch.setattr(
        dataplex.httpx,
        "AsyncClient",
        lambda **kw: real(transport=httpx.MockTransport(fake.handle), **kw),
    )
    monkeypatch.setattr(dataplex, "bigquery_engine_project", lambda: "proj-x")
    snapshot = dp._snapshot()
    return await dp._export().publish(snapshot), snapshot.tables


async def _openlineage(monkeypatch: pytest.MonkeyPatch):
    async def _post(self, url, json=None, headers=None):
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    snapshot = _vendor_snapshot()
    return await metadata_export(vendors._config("openlineage")).publish(snapshot), snapshot.tables


async def _snowflake_horizon(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.configured_engine_url",
        lambda: "snowflake://user:pass@acct/db/schema",
    )
    rt = sf._runtime(existing_objects=True, kind="VIEW")
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.SnowflakeFederationRuntime",
        lambda url: rt,
    )
    monkeypatch.setattr(rt, "close", lambda: None)
    tables = [
        replace(
            sf._table_asset(
                "petstore-api",
                "public",
                name,
                name,
                columns=[
                    replace(
                        sf._column_asset("petstore-api", "public", name, column),
                        semantic_uri=f"uri:{name}#field:{column}",
                    )
                    for column in ("id", "status")
                ],
            ),
            semantic_uri=f"uri:{name}",
        )
        for name in ("pets", "orders")
    ]
    snapshot = SimpleNamespace(
        environment=None, data_products=[], governance_tags=[], model_tags=[], tables=tables
    )
    return await sf._exporter().publish(snapshot), tables  # type: ignore[arg-type]


async def _unity_catalog(monkeypatch: pytest.MonkeyPatch):
    built = uc._Warehouse()
    monkeypatch.setattr(UnityCatalogExport, "_runtime_or_none", lambda self: uc._Runtime(built))
    monkeypatch.setattr(
        "provisa.federation.replica_address.replica_schema", lambda org_id, **_: uc.REPLICAS
    )
    snapshot = uc._snapshot()
    return await uc._export().publish(snapshot), snapshot.tables


_CASES: dict[str, Case] = {
    "atlan": _atlan,
    "atlas": _atlas,
    "collibra": _collibra,
    "datahub": _datahub,
    "dataplex": _dataplex,
    "datazone": _datazone,
    "openlineage": _openlineage,
    "openmetadata": _openmetadata,
    "purview": _purview,
    "snowflake_horizon": _snowflake_horizon,
    "unity_catalog": _unity_catalog,
}


def test_every_registered_provider_has_a_binding_case():
    """A provider added to the registry without a case here fails: it must show it records
    what it publishes, or its exports never reach the names table."""
    missing = sorted(set(registered_providers()) - set(_CASES))
    assert not missing, f"export providers with no binding case: {missing}"
    assert not sorted(set(_CASES) - set(registered_providers()))


@pytest.mark.parametrize("provider", registered_providers())
async def test_every_provider_binds_each_table_it_published(provider, monkeypatch):
    result, tables = await _CASES[provider](monkeypatch)
    assert result.ok, result.errors
    assert tables
    unbound = [t.semantic_uri for t in tables if t.semantic_uri not in result.bindings]
    assert not unbound, f"{provider} published tables it recorded no binding for: {unbound}"
    for table in tables:
        vendor_ref, physical_key = result.bindings[table.semantic_uri]
        assert vendor_ref and physical_key, (provider, table.semantic_uri)


@pytest.mark.parametrize("provider", registered_providers())
async def test_every_provider_binds_each_column_it_published(provider, monkeypatch):
    """A column export reaches the names table only through its binding (``export:<target>`` on
    the column's row, keyed by the column's Provisa URI), as a table's does. Every registered
    provider exports its tables' columns, so the registry is walked: a new provider binds its
    columns or fails here."""
    result, tables = await _CASES[provider](monkeypatch)
    assert result.ok, result.errors
    columns = [c for t in tables for c in t.columns]
    assert columns, f"{provider}'s case publishes no column"
    unbound = [c.semantic_uri for c in columns if c.semantic_uri not in result.bindings]
    assert not unbound, f"{provider} published columns it recorded no binding for: {unbound}"
    for column in columns:
        vendor_ref, physical_key = result.bindings[column.semantic_uri]
        assert vendor_ref and physical_key, (provider, column.semantic_uri)


async def test_datazone_binds_each_column_as_an_entry_of_its_asset_form(monkeypatch):
    """REQ-194: a DataZone column is an entry of its asset's ProvisaTableForm columns."""
    result, (table,) = await _datazone(monkeypatch)
    assert table.columns
    for column in table.columns:
        assert result.bindings[column.semantic_uri] == (
            f"asset-1#ProvisaTableForm.columns.{column.name}",
            f"{table.semantic_uri}#ProvisaTableForm.columns.{column.name}",
        )


async def test_unity_catalog_binds_each_column_under_its_view_full_name(monkeypatch):
    """REQ-194: a Unity Catalog column is addressed by its view's full name and its own."""
    result, tables = await _unity_catalog(monkeypatch)
    columns = [(t, c) for t in tables for c in t.columns]
    assert columns
    for table, column in columns:
        view, _ = result.bindings[table.semantic_uri]
        address = f"{view}.{column.name}"
        assert result.bindings[column.semantic_uri] == (address, address)
