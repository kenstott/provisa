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
from provisa.api.metadata_export.atlan import AtlanExport
from provisa.api.metadata_export.atlas import AtlasExport
from provisa.api.metadata_export.collibra import CollibraExport
from provisa.api.metadata_export.datahub import DataHubExport
from provisa.api.metadata_export.openmetadata import OpenMetadataExport
from provisa.core.models import (
    Column,
    Domain,
    MetadataExportConfig,
    MetadataExportTarget,
    ProvisaConfig,
    Role,
    Source,
    SourceType,
    Table,
)
from tests.role_state import RoleState

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
                product_id="prod",
                columns=[
                    Column(name="id", data_type="integer", visible_to=["analyst"]),
                    Column(name="amount", data_type="numeric", visible_to=["analyst"]),
                ],
            )
        ],
        roles=[Role(id="analyst", description="Test role.", capabilities=[], domain_access=["*"])],
    )
    return build_snapshot(config, org_id="acme", dialect="postgres", contexts={})


def _config(provider: str) -> MetadataExportTarget:
    return MetadataExportTarget(
        id="catalog",
        provider=provider,
        endpoint="https://catalog.example",
        token="t",
        timeout_seconds=5,
    )


# --- Apache Atlas ---------------------------------------------------------------------------


def _mock_atlas(monkeypatch, *, search_entities=None, live_attributes=None, unreadable=()):
    """Route the Atlas exporter's calls: typedef probes miss, the bulk POST assigns guids, and
    a live entity read of a guid in ``unreadable`` answers 503."""
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
        if "/entity/guid/" in url and url.rsplit("/", 1)[-1] in unreadable:
            return httpx.Response(503, request=req)
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
async def test_atlas_and_so_purview_bind_each_data_product(monkeypatch):
    """REQ-1634 STATUS: Atlas binds a product's entity under the product's URI, and Purview's
    publish runs the Atlas pass first (its Data Map), so a Purview target holds each product it
    published: a retirement is sent there (binds_products stays True for both)."""
    from provisa.api.metadata_export.purview import PurviewExport
    from provisa.core.models import DataProduct

    model = ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[Domain(id="sales", description="Sales")],
        tables=[
            Table(
                source_id="wh",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                product_id="prod",
                columns=[Column(name="id", data_type="integer", visible_to=["analyst"])],
            )
        ],
        data_products=[DataProduct(id="prod", domain_id="sales", name="Prod", status="live")],
        roles=[Role(id="analyst", description="Test role.", capabilities=[], domain_access=["*"])],
    )
    with_product = build_snapshot(model, org_id="acme", dialect="postgres", contexts={})
    (product,) = with_product.data_products
    monkeypatch.setattr(AtlasExport, "classification_merge", False)
    _mock_atlas(monkeypatch)
    result = await AtlasExport(_config("atlas")).publish(with_product)
    assert result.ok, result.errors
    assert product.semantic_uri in result.bindings
    assert issubclass(PurviewExport, AtlasExport) and PurviewExport.binds_products


@pytest.mark.asyncio
async def test_atlas_prefers_the_stored_guid_over_the_laggy_search_index(snapshot, monkeypatch):
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
async def test_atlas_republish_keeps_the_attributes_a_steward_wrote(snapshot, monkeypatch):
    """REQ-1389: Atlas's bulk update replaces the attribute map, so it starts from the live one;
    a steward's userDescription stands beside what Provisa authors."""
    monkeypatch.setattr(AtlasExport, "classification_merge", False)
    table = snapshot.tables[0]
    calls = _mock_atlas(monkeypatch, live_attributes={"userDescription": "Ask finance first."})
    export = AtlasExport(_config("atlas"))
    export.stored_bindings = {table.semantic_uri: ("table-guid", f"{TABLE_FQN}@provisa")}
    result = await export.publish(snapshot)
    assert result.ok
    bulk = next(body for url, body in calls["posts"] if url.endswith("/entity/bulk"))
    (sent,) = [
        e for e in bulk["entities"] if e["attributes"].get("provisaUri") == table.semantic_uri
    ]
    assert sent["attributes"]["userDescription"] == "Ask finance first."


@pytest.mark.asyncio
async def test_atlas_an_entity_whose_live_read_failed_is_not_written_and_the_rest_are(
    snapshot, monkeypatch
):
    """REQ-1389: sent with Provisa's attributes alone the table would lose the steward's; it is
    refused by name, and its columns still publish, attached to the table that stands."""
    monkeypatch.setattr(AtlasExport, "classification_merge", False)
    table = snapshot.tables[0]
    calls = _mock_atlas(monkeypatch, unreadable={"table-guid"})
    export = AtlasExport(_config("atlas"))
    export.stored_bindings = {table.semantic_uri: ("table-guid", f"{TABLE_FQN}@provisa")}
    result = await export.publish(snapshot)
    bulk = next(body for url, body in calls["posts"] if url.endswith("/entity/bulk"))
    uris = {e["attributes"].get("provisaUri") for e in bulk["entities"]}
    assert table.semantic_uri not in uris
    assert {c.semantic_uri for c in table.columns} <= uris
    columns = [
        e
        for e in bulk["entities"]
        if e["attributes"].get("provisaUri") in {c.semantic_uri for c in table.columns}
    ]
    assert all(c["relationshipAttributes"]["table"]["guid"] == "table-guid" for c in columns)
    refused = [e for e in result.errors if "not written" in e.message]
    assert [e.asset.fqn() for e in refused] == [TABLE_FQN]
    assert "HTTP 503" in refused[0].message


@pytest.mark.asyncio
async def test_atlas_falls_back_to_the_search_index_only_without_a_binding(snapshot, monkeypatch):
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
async def test_openmetadata_captures_the_table_uuid_from_the_put_response(snapshot, monkeypatch):
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
async def test_openmetadata_unchanged_fqn_never_touches_the_predecessor_path(snapshot, monkeypatch):
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
async def test_collibra_steady_state_reconcile_makes_no_lookup_and_no_rename(snapshot, monkeypatch):
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
async def test_collibra_renames_the_bound_asset_before_the_name_keyed_import(snapshot, monkeypatch):
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
async def test_datahub_deprecates_the_old_bound_urn_on_physical_readdress(snapshot, monkeypatch):
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


def _route_publish(monkeypatch, snapshot, db, exporters: dict) -> None:
    """The single publish path over ``db``, publishing ``snapshot`` to one target per entry of
    ``exporters`` (target id -> the exporter that target resolves to)."""
    import types

    from contextlib import asynccontextmanager

    from provisa.api.metadata_export import publishing

    class _TenantDb:
        @asynccontextmanager
        async def acquire(self):
            async with db.acquire() as conn:
                yield conn

    async def _export_config(org_id):
        # REQ-2118: the bindings are kept per target, under the target's id. REQ-2122: each
        # target's catalog serves the snapshot's domains, so its tables are published to it.
        domains = sorted({t.domain_id for t in snapshot.tables})
        return MetadataExportConfig(
            enabled=True,
            targets=[{"catalog": target_id, "role": "steward"} for target_id in exporters],
        ), [
            MetadataExportTarget(
                id=target_id,
                provider="atlas",
                endpoint="https://catalog.example",
                domains=domains,
                role="steward",
            )
            for target_id in exporters
        ]

    async def _reads_everything(state, role):
        # The role reads every column here: what a role is sent is test_metadata_export_scope.
        return lambda table: None

    async def _model(*args, **kwargs):
        # An empty config: the publish path reads the DQ outcomes off it (REQ-1443), and a
        # config with no checker source has no results table to read.
        return ProvisaConfig(sources=[], domains=[], tables=[], roles=[])

    monkeypatch.setattr(publishing, "_export_config", _export_config)
    monkeypatch.setattr(publishing, "role_reader", _reads_everything)
    monkeypatch.setattr(publishing, "role_reach", lambda state, role: lambda domain_id: True)
    monkeypatch.setattr(publishing, "_model_for_export", _model)
    monkeypatch.setattr(
        publishing,
        "build_snapshot",
        lambda model, *, org_id, dialect, contexts, glossary=None, dq_outcomes=None, uri_org_id=None, documentation_base_url=None, owner_emails=None: (
            snapshot
        ),
    )
    monkeypatch.setattr(publishing, "metadata_export", lambda config: exporters[config.id])
    monkeypatch.setattr(
        "provisa.api.app.state",
        RoleState(
            model_db=(_one_db := _TenantDb()),
            tenant_db=_one_db,
            contexts={},
            multitenancy=False,
            # REQ-1659: the listing links back at the org's public origin, read off config.mail.
            config=types.SimpleNamespace(
                mail=types.SimpleNamespace(base_url="https://provisa.test")
            ),
        ),
        raising=False,
    )


@pytest.mark.asyncio
async def test_publish_snapshot_loads_persists_and_prunes_bindings(
    snapshot, control_plane, monkeypatch
):
    """The binding lifecycle rides the single publish path (REQ-1389): stored bindings are
    handed to the provider before the publish, captured ones are persisted after, and a
    binding whose asset left the model is pruned."""
    from provisa.api.metadata_export import publishing
    from provisa.api.metadata_export.provider import PublishResult
    from provisa.core.repositories import catalog_binding

    db = control_plane.org()
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
        needs_export_views = False  # a remote catalog: it addresses nothing in the engine's store
        binds_products = True  # Atlas binds its product entities itself

        async def publish(self, published_snapshot):
            seen["bindings"] = dict(self.stored_bindings)
            result = PublishResult(provider_name="atlas", published={"table": 1})
            result.bindings = {uri: ("guid-1", f"{TABLE_FQN}@provisa")}
            return result

    _route_publish(monkeypatch, snapshot, db, {"atlas": _Exporter()})

    results = await publishing.publish_snapshot("acme")

    assert results["atlas"].ok
    # The provider was handed both stored bindings before it published.
    assert seen["bindings"][uri] == ("guid-1", "legacy.public.orders@provisa")
    async with db.acquire() as conn:
        stored = await catalog_binding.load_bindings(conn, "atlas")
    # The capture updated the physical key, and the departed asset's binding is pruned.
    assert stored == {uri: ("guid-1", f"{TABLE_FQN}@provisa")}


@pytest.mark.asyncio
async def test_a_binding_the_publish_captured_is_kept_though_it_names_no_snapshot_asset(
    snapshot, control_plane, monkeypatch
):
    """A provider records what it wrote that has no asset of its own in the snapshot (Purview's
    term relationships, REQ-1389); what this publish captured is current, so the prune after it
    keeps it, and drops it once a later publish no longer captures it."""
    from provisa.api.metadata_export import publishing
    from provisa.api.metadata_export.provider import PublishResult
    from provisa.core.repositories import catalog_binding

    db = control_plane.org()
    captured = {"purview:relationship/t1/DATAASSET/Related/a1": ("a1", "Related")}

    class _Exporter:
        stored_bindings: dict = {}
        needs_export_views = False
        binds_products = True

        async def publish(self, published_snapshot):
            result = PublishResult(provider_name="atlas", published={"table": 1})
            result.bindings = dict(captured)
            return result

    _route_publish(monkeypatch, snapshot, db, {"atlas": _Exporter()})

    await publishing.publish_snapshot("acme")
    async with db.acquire() as conn:
        assert await catalog_binding.load_bindings(conn, "atlas") == captured
    captured = {snapshot.tables[0].semantic_uri: ("guid-1", f"{TABLE_FQN}@provisa")}
    await publishing.publish_snapshot("acme")
    async with db.acquire() as conn:
        assert await catalog_binding.load_bindings(conn, "atlas") == captured


@pytest.mark.asyncio
async def test_a_refusal_an_adapter_does_not_report_is_its_targets_named_failure(
    snapshot, control_plane, monkeypatch
):
    """REQ-1068: a catalog refusal that escapes the adapter (its token, a listing) fails that
    target with the request and the catalog's answer, and the targets after it still publish."""
    from provisa.api.metadata_export import publishing
    from provisa.api.metadata_export.provider import PublishResult

    class _Refused:
        stored_bindings: dict = {}
        needs_export_views = False

        async def publish(self, published_snapshot):
            request = httpx.Request("GET", "https://catalog.example/businessdomains")
            raise httpx.HTTPStatusError(
                "refused",
                request=request,
                response=httpx.Response(403, text='{"error":"Forbidden"}', request=request),
            )

    class _Accepted:
        stored_bindings: dict = {}
        needs_export_views = False
        binds_products = True

        async def publish(self, published_snapshot):
            return PublishResult(provider_name="atlas", published={"table": 1})

    _route_publish(
        monkeypatch, snapshot, control_plane.org(), {"first": _Refused(), "second": _Accepted()}
    )

    results = await publishing.publish_snapshot("acme")

    (error,) = results["first"].errors
    assert error.asset.fqn() == "first"
    assert error.message == (
        'GET https://catalog.example/businessdomains: HTTP 403: {"error":"Forbidden"}'
    )
    assert results["second"].ok


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


# --- Atlan: the Atlas authorship merges on Atlan's routes (unproven against a live tenant) ---


ATLAN_TABLE_QN = "default/provisa/acme/wh/public/orders"


@pytest.mark.asyncio
async def test_atlan_republish_keeps_the_attributes_a_steward_wrote(snapshot, monkeypatch):
    """REQ-1389: an Atlan update starts from the live entity, read by its stored binding on
    Atlan's own route; the steward's attribute stands beside what Provisa authors."""
    monkeypatch.setattr(AtlanExport, "classification_merge", False)
    table = snapshot.tables[0]
    calls = _mock_atlas(monkeypatch, live_attributes={"announcementMessage": "Ask finance first."})
    export = AtlanExport(_config("atlan"))
    export.stored_bindings = {table.semantic_uri: ("table-guid", ATLAN_TABLE_QN)}
    result = await export.publish(snapshot)
    assert result.ok, result.errors
    assert any(url.endswith("/api/meta/entity/guid/table-guid") for url in calls["gets"])
    assert not any("/search/basic" in url for url, _ in calls["posts"])
    bulk = next(body for url, body in calls["posts"] if url.endswith("/api/meta/entity/bulk"))
    (sent,) = [
        e for e in bulk["entities"] if e["attributes"].get("provisaUri") == table.semantic_uri
    ]
    assert sent["attributes"]["announcementMessage"] == "Ask finance first."


@pytest.mark.asyncio
async def test_atlan_an_entity_whose_live_read_failed_is_not_written(snapshot, monkeypatch):
    monkeypatch.setattr(AtlanExport, "classification_merge", False)
    table = snapshot.tables[0]
    calls = _mock_atlas(monkeypatch, unreadable={"table-guid"})
    export = AtlanExport(_config("atlan"))
    export.stored_bindings = {table.semantic_uri: ("table-guid", ATLAN_TABLE_QN)}
    result = await export.publish(snapshot)
    bulk = next(body for url, body in calls["posts"] if url.endswith("/api/meta/entity/bulk"))
    assert table.semantic_uri not in {e["attributes"].get("provisaUri") for e in bulk["entities"]}
    refused = [e for e in result.errors if "not written" in e.message]
    assert [e.asset.fqn() for e in refused] == [TABLE_FQN]


@pytest.mark.asyncio
async def test_atlan_classification_merge_keeps_a_stewards_tag(snapshot, monkeypatch):
    """REQ-1389: only provisa_* classifications are Provisa's; a steward's tag on the entity is
    never removed. Atlan's per-guid routes are /api/meta."""
    calls = _mock_atlas(monkeypatch)
    deletes: list[str] = []
    get = httpx.AsyncClient.get

    async def _get(self, url, params=None, headers=None):
        if "/api/meta/entity/uniqueAttribute/type/" in url:
            live = {
                "guid": "live-guid",
                "classifications": [{"typeName": "Finance"}, {"typeName": "provisa_stale"}],
            }
            return httpx.Response(200, json={"entity": live}, request=httpx.Request("GET", url))
        return await get(self, url, params=params, headers=headers)

    async def _delete(self, url, headers=None):
        deletes.append(url)
        return httpx.Response(204, request=httpx.Request("DELETE", url))

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    monkeypatch.setattr(httpx.AsyncClient, "delete", _delete)
    await AtlanExport(_config("atlan")).publish(snapshot)
    assert deletes
    assert all(url.startswith("https://catalog.example/api/meta/entity/guid/") for url in deletes)
    assert all(url.endswith("/classification/provisa_stale") for url in deletes)
    assert calls["posts"]


@pytest.mark.asyncio
async def test_a_retired_product_is_sent_only_where_it_was_published(
    snapshot, control_plane, monkeypatch
):
    """REQ-1634 STATUS: a retired product is withdrawn from a target an earlier publish put it in,
    and never created, retired, where it was not. For a provider that keeps no product bindings
    (Horizon), the publish path records what it published."""
    import dataclasses
    import types
    from contextlib import asynccontextmanager

    from provisa.api.metadata_export import publishing
    from provisa.api.metadata_export.model import DataProductAsset
    from provisa.api.metadata_export.provider import PublishResult
    from provisa.api.metadata_export.refs import data_product_ref
    from provisa.core.repositories import catalog_binding

    db = control_plane.org()

    class _TenantDb:
        @asynccontextmanager
        async def acquire(self):
            async with db.acquire() as conn:
                yield conn

    def product(pid: str, status: str) -> DataProductAsset:
        return DataProductAsset(
            ref=data_product_ref(pid),
            id=pid,
            name=pid.upper(),
            domain_id="sales",
            owner=None,
            description="",
            semantic_uri=f"provisa://acme/sales/products/{pid}",
            status=status,
            # A retired product's members went to its successor.
            members=() if status == "retired" else (snapshot.tables[0].ref,),
        )

    sent: list[list[tuple[str, str]]] = []

    class _Horizon:
        stored_bindings: dict = {}
        needs_export_views = False
        binds_products = False  # a listing is found by its name, never by a stored id

        async def publish(self, published_snapshot):
            sent.append([(p.id, p.status) for p in published_snapshot.data_products])
            return PublishResult(provider_name="snowflake_horizon", published={})

    current: dict = {}

    async def _export_config(org_id):
        return MetadataExportConfig(
            enabled=True, targets=[{"catalog": "horizon", "role": "steward"}]
        ), [
            MetadataExportTarget(
                id="horizon",
                provider="snowflake_horizon",
                endpoint="https://catalog.example",
                domains=["sales"],
                role="steward",
            )
        ]

    async def _reads_everything(state, role):
        return lambda table: None

    async def _model(*args, **kwargs):
        return ProvisaConfig(sources=[], domains=[], tables=[], roles=[])

    monkeypatch.setattr(publishing, "_export_config", _export_config)
    monkeypatch.setattr(publishing, "role_reader", _reads_everything)
    monkeypatch.setattr(publishing, "role_reach", lambda state, role: lambda domain_id: True)
    monkeypatch.setattr(publishing, "_model_for_export", _model)
    monkeypatch.setattr(publishing, "build_snapshot", lambda *a, **k: current["snapshot"])
    exporter = _Horizon()
    monkeypatch.setattr(publishing, "metadata_export", lambda config: exporter)
    monkeypatch.setattr(
        "provisa.api.app.state",
        RoleState(
            model_db=(_one_db := _TenantDb()),
            tenant_db=_one_db,
            contexts={},
            multitenancy=False,
            config=types.SimpleNamespace(
                mail=types.SimpleNamespace(base_url="https://provisa.test")
            ),
        ),
        raising=False,
    )

    async def publish(*products: DataProductAsset) -> None:
        current["snapshot"] = dataclasses.replace(snapshot, data_products=list(products))
        assert (await publishing.publish_snapshot("acme"))["horizon"].ok

    # c360 is published live; old was retired before this target ever had it.
    await publish(product("c360", "live"), product("old", "retired"))
    assert sent[-1] == [("c360", "live")]
    async with db.acquire() as conn:
        bound = await catalog_binding.load_bindings(conn, "horizon")
    assert bound["provisa://acme/sales/products/c360"] == ("c360", "C360")
    assert "provisa://acme/sales/products/old" not in bound
    # c360 retires: it was published here, so it is sent, retired, to be withdrawn.
    await publish(product("c360", "retired"), product("old", "retired"))
    assert sent[-1] == [("c360", "retired")]
