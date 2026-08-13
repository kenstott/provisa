# Copyright (c) 2026 Kenneth Stott
# Canary: 5b3f8a02-91d4-4c67-8e05-fa62c1d97b30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1389: every publish CAPTURES the vendor's own id per asset, and the next publish
USES the stored id to rebind a physically re-addressed asset to the same catalog entity.

Per vendor:

* Atlas — the bulk response's ``guidAssignments`` are captured; on the next publish the
  STORED guid outranks the laggy basic-search index for the URN-canonical rebind.
* OpenMetadata — the table PUT response's entity UUID is captured; PATCH cannot rename a
  table (documented in the adapter), so an FQN change reads the predecessor by its stored
  UUID, carries its steward enrichment forward, and marks it superseded.
* Collibra — the asset UUID is captured by exact-name lookup after the import; an FQN
  change PATCH-renames the live asset BEFORE the name-keyed import so the job matches the
  same asset.
* DataHub — the dataset URN is the vendor id; a changed URN publishes the successor and
  sets the deprecation aspect on the OLD bound URN.
"""

# Requirements: REQ-1389

from __future__ import annotations

import json

import httpx
import pytest

from provisa.api.metadata_export import build_snapshot
from provisa.api.metadata_export.atlas import AtlasExport
from provisa.api.metadata_export.collibra import CollibraExport
from provisa.api.metadata_export.datahub import DataHubExport
from provisa.api.metadata_export.openmetadata import OpenMetadataExport
from provisa.core.models import (
    Column,
    Domain,
    MetadataExportConfig,
    ProvisaConfig,
    Role,
    Source,
    SourceType,
    Table,
)

TABLE_FQN = "wh.public.orders"
OM_TABLE_FQN = "wh.default.public.orders"


@pytest.fixture
def snapshot():
    config = ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[Domain(id="sales", description="Sales")],
        tables=[
            Table(
                source_id="wh",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                data_product=True,
                columns=[
                    Column(name="id", data_type="integer", visible_to=["analyst"]),
                    Column(name="amount", data_type="numeric", visible_to=["analyst"]),
                ],
            )
        ],
        roles=[Role(id="analyst", capabilities=[], domain_access=["*"])],
    )
    return build_snapshot(config, org_id="acme", dialect="postgres")


def _config(provider: str) -> MetadataExportConfig:
    return MetadataExportConfig(
        enabled=True,
        provider=provider,
        endpoint="https://catalog.example",
        token="t",
        timeout_seconds=5,
    )


# --- Apache Atlas ---------------------------------------------------------------------------


def _mock_atlas(monkeypatch, *, search_entities=None, live_attributes=None):
    """Route the Atlas exporter's calls: typedef probes miss, the bulk POST assigns guids."""
    calls: dict[str, list] = {"posts": [], "gets": []}

    async def _post(self, url, json=None, headers=None):
        req = httpx.Request("POST", url)
        calls["posts"].append((url, json))
        if url.endswith("/entity/bulk"):
            assignments = {
                e["guid"]: f"created-{i}"
                for i, e in enumerate(json["entities"])
                if e["guid"].startswith("-")
            }
            return httpx.Response(200, json={"guidAssignments": assignments}, request=req)
        if "/search/basic" in url:
            return httpx.Response(200, json={"entities": search_entities or []}, request=req)
        return httpx.Response(200, json={}, request=req)  # typedef registration

    async def _get(self, url, params=None, headers=None):
        req = httpx.Request("GET", url)
        calls["gets"].append(url)
        if "/entity/guid/" in url:
            return httpx.Response(
                200, json={"entity": {"attributes": live_attributes or {}}}, request=req
            )
        return httpx.Response(404, request=req)  # typedef probes: nothing registered yet

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    return calls


@pytest.mark.asyncio
async def test_atlas_captures_the_guid_of_every_published_entity(snapshot, monkeypatch):
    monkeypatch.setattr(AtlasExport, "classification_merge", False)
    _mock_atlas(monkeypatch)
    result = await AtlasExport(_config("atlas")).publish(snapshot)
    assert result.ok
    table = snapshot.tables[0]
    assert result.bindings[table.semantic_uri] == ("created-1", f"{TABLE_FQN}@provisa")
    # Source and columns carry a provisaUri too: every published asset is bound.
    source_uri = snapshot.sources[0].semantic_uri
    assert result.bindings[source_uri] == ("created-0", "wh@provisa")
    column_uris = {c.semantic_uri for c in table.columns}
    assert column_uris <= set(result.bindings)


@pytest.mark.asyncio
async def test_atlas_prefers_the_stored_guid_over_the_laggy_search_index(
    snapshot, monkeypatch
):
    """The basic-search index lags commits; the stored binding is the identity of record."""
    monkeypatch.setattr(AtlasExport, "classification_merge", False)
    table = snapshot.tables[0]
    stale = [
        {
            "guid": "stale-guid",
            "attributes": {
                "provisaUri": table.semantic_uri,
                "qualifiedName": "stale.old.orders@provisa",
            },
        }
    ]
    calls = _mock_atlas(monkeypatch, search_entities=stale)
    export = AtlasExport(_config("atlas"))
    export.stored_bindings = {table.semantic_uri: ("stored-guid", "lake.sales.orders@provisa")}
    result = await export.publish(snapshot)
    assert result.ok
    bulk = next(body for url, body in calls["posts"] if url.endswith("/entity/bulk"))
    table_entity = next(
        e
        for e in bulk["entities"]
        if e["attributes"].get("qualifiedName") == f"{TABLE_FQN}@provisa"
    )
    # Rebound to the STORED guid — not the stale one the search index reported.
    assert table_entity["guid"] == "stored-guid"
    # The rebound guid is what the capture records for the next publish.
    assert result.bindings[table.semantic_uri] == ("stored-guid", f"{TABLE_FQN}@provisa")


@pytest.mark.asyncio
async def test_atlas_falls_back_to_the_search_index_only_without_a_binding(
    snapshot, monkeypatch
):
    monkeypatch.setattr(AtlasExport, "classification_merge", False)
    table = snapshot.tables[0]
    live = [
        {
            "guid": "indexed-guid",
            "attributes": {
                "provisaUri": table.semantic_uri,
                "qualifiedName": "lake.sales.orders@provisa",
            },
        }
    ]
    calls = _mock_atlas(monkeypatch, search_entities=live)
    result = await AtlasExport(_config("atlas")).publish(snapshot)
    assert result.ok
    bulk = next(body for url, body in calls["posts"] if url.endswith("/entity/bulk"))
    table_entity = next(
        e
        for e in bulk["entities"]
        if e["attributes"].get("qualifiedName") == f"{TABLE_FQN}@provisa"
    )
    assert table_entity["guid"] == "indexed-guid"


# --- OpenMetadata ---------------------------------------------------------------------------

STEWARD_TAG = {
    "tagFQN": "PII.Sensitive",
    "source": "Classification",
    "labelType": "Manual",
    "state": "Confirmed",
}


def _mock_openmetadata(monkeypatch, *, old_live=None):
    """PUTs echo an id+fqn; GET by stored UUID answers ``old_live``; by-name GETs 404."""
    calls: dict[str, list] = {"puts": [], "gets": [], "patches": []}

    async def _put(self, url, json=None, headers=None):
        calls["puts"].append((url, json))
        body = {"id": "om-uuid-new", "fullyQualifiedName": OM_TABLE_FQN}
        return httpx.Response(200, json=body, request=httpx.Request("PUT", url))

    async def _get(self, url, params=None, headers=None):
        calls["gets"].append(url)
        req = httpx.Request("GET", url)
        if old_live is not None and url.endswith(f"/api/v1/tables/{old_live['id']}"):
            return httpx.Response(200, json=old_live, request=req)
        return httpx.Response(404, request=req)

    async def _patch(self, url, content=None, headers=None):
        calls["patches"].append((url, json.loads(content), headers))
        return httpx.Response(200, json={}, request=httpx.Request("PATCH", url))

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    monkeypatch.setattr(httpx.AsyncClient, "patch", _patch)
    return calls


@pytest.mark.asyncio
async def test_openmetadata_captures_the_table_uuid_from_the_put_response(
    snapshot, monkeypatch
):
    _mock_openmetadata(monkeypatch)
    result = await OpenMetadataExport(_config("openmetadata")).publish(snapshot)
    assert result.ok
    uri = snapshot.tables[0].semantic_uri
    assert result.bindings[uri] == ("om-uuid-new", OM_TABLE_FQN)


@pytest.mark.asyncio
async def test_openmetadata_fqn_change_succeeds_the_old_entity_by_stored_uuid(
    snapshot, monkeypatch
):
    """PATCH cannot rename a table, so the rebind is succession: enrichment carried forward
    from the predecessor (read by stored UUID) and the predecessor marked superseded."""
    uri = snapshot.tables[0].semantic_uri
    old_live = {
        "id": "om-uuid-old",
        "fullyQualifiedName": "wh.default.legacy.orders",
        "description": "steward-written",
        "tags": [STEWARD_TAG],
        "columns": [{"name": "id", "tags": []}, {"name": "amount", "tags": []}],
        "extension": {"stewardNotes": "hand-curated"},
        "owners": [{"id": "om-user-1", "type": "user"}],
    }
    calls = _mock_openmetadata(monkeypatch, old_live=old_live)
    export = OpenMetadataExport(_config("openmetadata"))
    export.stored_bindings = {uri: ("om-uuid-old", "wh.default.legacy.orders")}
    result = await export.publish(snapshot)
    assert result.ok

    table_body = next(body for url, body in calls["puts"] if url.endswith("/tables"))
    # The predecessor's human enrichment rides the successor's first publish.
    assert STEWARD_TAG in table_body["tags"]
    assert table_body["extension"] == {"stewardNotes": "hand-curated"}
    assert table_body["owners"] == [{"id": "om-user-1", "type": "user"}]

    # The predecessor itself is marked superseded, pointing at the successor and the URN.
    url, ops, headers = calls["patches"][0]
    assert url.endswith("/api/v1/tables/om-uuid-old")
    assert headers["Content-Type"] == "application/json-patch+json"
    assert ops == [
        {
            "op": "replace",
            "path": "/description",
            "value": (
                f"[Superseded by Provisa] This table was physically re-addressed and is "
                f"now published as {OM_TABLE_FQN}. Provisa URI: {uri}."
            ),
        }
    ]
    # And the binding now names the successor.
    assert result.bindings[uri] == ("om-uuid-new", OM_TABLE_FQN)


@pytest.mark.asyncio
async def test_openmetadata_unchanged_fqn_never_touches_the_predecessor_path(
    snapshot, monkeypatch
):
    uri = snapshot.tables[0].semantic_uri
    calls = _mock_openmetadata(monkeypatch)
    export = OpenMetadataExport(_config("openmetadata"))
    export.stored_bindings = {uri: ("om-uuid-new", OM_TABLE_FQN)}
    result = await export.publish(snapshot)
    assert result.ok
    assert calls["patches"] == []
    assert not any(url.endswith("/api/v1/tables/om-uuid-new") for url in calls["gets"])


# --- Collibra -------------------------------------------------------------------------------


def _mock_collibra(monkeypatch, *, lookup_results=None):
    order: list[str] = []
    calls: dict[str, list] = {"posts": [], "gets": [], "patches": []}

    async def _post(self, url, json=None, files=None, data=None, headers=None):
        order.append("import")
        calls["posts"].append((url, files, data))
        return httpx.Response(200, json={}, request=httpx.Request("POST", url))

    async def _get(self, url, params=None, headers=None):
        order.append("lookup")
        calls["gets"].append((url, params))
        return httpx.Response(
            200, json={"results": lookup_results or []}, request=httpx.Request("GET", url)
        )

    async def _patch(self, url, json=None, headers=None):
        order.append("rename")
        calls["patches"].append((url, json))
        return httpx.Response(200, json={}, request=httpx.Request("PATCH", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    monkeypatch.setattr(httpx.AsyncClient, "patch", _patch)
    return calls, order


@pytest.mark.asyncio
async def test_collibra_captures_the_asset_uuid_after_a_first_import(snapshot, monkeypatch):
    calls, _ = _mock_collibra(
        monkeypatch, lookup_results=[{"id": "collibra-uuid-1", "name": TABLE_FQN}]
    )
    result = await CollibraExport(_config("collibra")).publish(snapshot)
    assert result.ok
    uri = snapshot.tables[0].semantic_uri
    assert result.bindings[uri] == ("collibra-uuid-1", TABLE_FQN)
    assert calls["gets"][0][1] == {"name": TABLE_FQN, "nameMatchMode": "EXACT"}


@pytest.mark.asyncio
async def test_collibra_steady_state_reconcile_makes_no_lookup_and_no_rename(
    snapshot, monkeypatch
):
    calls, _ = _mock_collibra(monkeypatch)
    export = CollibraExport(_config("collibra"))
    uri = snapshot.tables[0].semantic_uri
    export.stored_bindings = {uri: ("collibra-uuid-1", TABLE_FQN)}
    result = await export.publish(snapshot)
    assert result.ok
    assert calls["gets"] == [] and calls["patches"] == []
    # The known binding rides through, so the store never forgets a steady-state asset.
    assert result.bindings[uri] == ("collibra-uuid-1", TABLE_FQN)


@pytest.mark.asyncio
async def test_collibra_renames_the_bound_asset_before_the_name_keyed_import(
    snapshot, monkeypatch
):
    """The import upserts by full name; renaming first is what makes it hit the SAME asset."""
    calls, order = _mock_collibra(
        monkeypatch, lookup_results=[{"id": "collibra-uuid-1", "name": TABLE_FQN}]
    )
    export = CollibraExport(_config("collibra"))
    uri = snapshot.tables[0].semantic_uri
    export.stored_bindings = {uri: ("collibra-uuid-1", "legacy.public.orders")}
    result = await export.publish(snapshot)
    assert result.ok
    url, body = calls["patches"][0]
    assert url.endswith("/rest/2.0/assets/collibra-uuid-1")
    assert body["name"] == TABLE_FQN
    assert order.index("rename") < order.index("import")
    # The new name is looked up and captured after the import.
    assert result.bindings[uri] == ("collibra-uuid-1", TABLE_FQN)


@pytest.mark.asyncio
async def test_collibra_reports_a_capture_lookup_that_finds_nothing(snapshot, monkeypatch):
    _mock_collibra(monkeypatch, lookup_results=[])
    result = await CollibraExport(_config("collibra")).publish(snapshot)
    assert not result.ok
    assert any("found no asset" in e.message for e in result.errors)
    assert result.bindings == {}


# --- DataHub --------------------------------------------------------------------------------


def _datahub_urn(fqn: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:provisa,{fqn},PROD)"


def _mock_datahub(monkeypatch):
    posts: list[dict] = []

    async def _post(self, url, json=None, headers=None):
        posts.append(json)
        return httpx.Response(200, json={}, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    return posts


@pytest.mark.asyncio
async def test_datahub_captures_the_dataset_urn_as_the_vendor_id(snapshot, monkeypatch):
    monkeypatch.setattr(DataHubExport, "tag_merge", False)
    _mock_datahub(monkeypatch)
    result = await DataHubExport(_config("datahub")).publish(snapshot)
    assert result.ok
    uri = snapshot.tables[0].semantic_uri
    assert result.bindings[uri] == (_datahub_urn(TABLE_FQN), TABLE_FQN)


@pytest.mark.asyncio
async def test_datahub_deprecates_the_old_bound_urn_on_physical_readdress(
    snapshot, monkeypatch
):
    """A dataset URN is immutable identity: the successor publishes as a new URN, and the
    stored binding is what lets the old one be marked deprecated instead of lingering."""
    monkeypatch.setattr(DataHubExport, "tag_merge", False)
    posts = _mock_datahub(monkeypatch)
    export = DataHubExport(_config("datahub"))
    uri = snapshot.tables[0].semantic_uri
    old_urn = _datahub_urn("legacy.public.orders")
    export.stored_bindings = {uri: (old_urn, "legacy.public.orders")}
    result = await export.publish(snapshot)
    assert result.ok

    deprecations = [
        p["proposal"]
        for p in posts
        if p["proposal"]["aspectName"] == "deprecation" and p["proposal"]["entityUrn"] == old_urn
    ]
    assert len(deprecations) == 1
    aspect = json.loads(deprecations[0]["aspect"]["value"])
    assert aspect["deprecated"] is True
    assert _datahub_urn(TABLE_FQN) in aspect["note"]
    assert uri in aspect["note"]
    # The binding moves to the successor URN.
    assert result.bindings[uri] == (_datahub_urn(TABLE_FQN), TABLE_FQN)


# --- The publish path ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_snapshot_loads_persists_and_prunes_bindings(
    snapshot, tmp_path, monkeypatch
):
    """The binding lifecycle rides the single publish path (REQ-1389): stored bindings are
    handed to the provider before the publish, captured ones are persisted after, and a
    binding whose asset left the model is pruned."""
    import types

    from contextlib import asynccontextmanager

    from sqlalchemy.ext.asyncio import create_async_engine

    from provisa.api.metadata_export import publishing
    from provisa.core.database import Database
    from provisa.core.repositories import catalog_binding
    from provisa.core.schema_org import (
        catalog_bindings,
        glossary_term_edges,
        glossary_term_experts,
        glossary_term_refs,
        glossary_terms,
        registered_tables,
    )

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tenant.db'}")
    async with engine.begin() as c:
        # glossary_* + registered_tables: publish_snapshot hydrates the term graph (REQ-1387).
        await c.run_sync(
            lambda s: catalog_bindings.metadata.create_all(
                s,
                tables=[
                    catalog_bindings,
                    registered_tables,
                    glossary_terms,
                    glossary_term_refs,
                    glossary_term_edges,
                    glossary_term_experts,
                ],
            )
        )
    db = Database(engine, name="tenant")

    class _TenantDb:
        @asynccontextmanager
        async def acquire(self):
            async with db.acquire() as conn:
                yield conn

    uri = snapshot.tables[0].semantic_uri
    async with db.acquire() as conn:
        await catalog_binding.upsert_bindings(
            conn,
            "atlas",
            {
                uri: ("guid-1", "legacy.public.orders@provisa"),
                "provisa://acme/retired/tables/gone": ("guid-9", "wh.public.gone@provisa"),
            },
        )

    seen: dict = {}

    class _Exporter:
        stored_bindings: dict = {}

        async def publish(self, published_snapshot):
            seen["bindings"] = dict(self.stored_bindings)
            result = PublishResult(provider_name="atlas", published={"table": 1})
            result.bindings = {uri: ("guid-1", f"{TABLE_FQN}@provisa")}
            return result

    from provisa.api.metadata_export.provider import PublishResult

    exporter = _Exporter()

    async def _export_config(org_id):
        return MetadataExportConfig(
            enabled=True, provider="atlas", endpoint="https://catalog.example"
        )

    async def _model(*args, **kwargs):
        # An empty config: the publish path reads the DQ outcomes off it (REQ-1443), and a
        # config with no checker source has no results table to read.
        return ProvisaConfig(sources=[], domains=[], tables=[], roles=[])

    monkeypatch.setattr(publishing, "_export_config", _export_config)
    monkeypatch.setattr(publishing, "_model_for_export", _model)
    monkeypatch.setattr(
        publishing,
        "build_snapshot",
        lambda model, *, org_id, dialect, glossary=None, dq_outcomes=None: snapshot,
    )
    monkeypatch.setattr(publishing, "metadata_export", lambda config: exporter)
    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(tenant_db=_TenantDb()),
        raising=False,
    )

    result = await publishing.publish_snapshot("acme")

    assert result.ok
    # The provider was handed both stored bindings before it published.
    assert seen["bindings"][uri] == ("guid-1", "legacy.public.orders@provisa")
    async with db.acquire() as conn:
        stored = await catalog_binding.load_bindings(conn, "atlas")
    # The capture updated the physical key, and the departed asset's binding is pruned.
    assert stored == {uri: ("guid-1", f"{TABLE_FQN}@provisa")}
    await engine.dispose()


@pytest.mark.asyncio
async def test_datahub_unchanged_urn_proposes_no_deprecation(snapshot, monkeypatch):
    monkeypatch.setattr(DataHubExport, "tag_merge", False)
    posts = _mock_datahub(monkeypatch)
    export = DataHubExport(_config("datahub"))
    uri = snapshot.tables[0].semantic_uri
    export.stored_bindings = {uri: (_datahub_urn(TABLE_FQN), TABLE_FQN)}
    result = await export.publish(snapshot)
    assert result.ok
    assert not any(p["proposal"]["aspectName"] == "deprecation" for p in posts)
