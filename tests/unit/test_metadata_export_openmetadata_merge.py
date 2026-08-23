# Copyright (c) 2026 Kenneth Stott
# Canary: aa210a93-ad9a-4f45-90aa-0bf342e2a661
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1389: the OpenMetadata publish is a single-writer merge, not a replace.

A table PUT replaces the fields sent, and stewards enrich the SAME entity inside
OpenMetadata — so before every table PUT the exporter reads the live entity and carries
through what it does not own: tag labels outside the ``Provisa`` classification (table and
column level), extension keys other than the relationship property, and PUT-body fields it
never authors. Provisa's own tags track the snapshot: added, updated, and removed.
"""

# Requirements: REQ-1389

from __future__ import annotations

import httpx
import pytest

from provisa.api.metadata_export import build_snapshot
from provisa.api.metadata_export.openmetadata import (
    CLASSIFICATION,
    RELATIONSHIP_PROPERTY,
    OpenMetadataExport,
)
from provisa.core.models import (
    Column,
    Domain,
    MetadataExportConfig,
    ProvisaConfig,
    RLSRule,
    Role,
    Source,
    SourceType,
    Table,
)

TABLE_FQN = "wh.default.public.orders"

# A label a steward applied inside OpenMetadata: outside the Provisa classification, so the
# publish must never touch it.
STEWARD_TABLE_TAG = {
    "tagFQN": "PII.Sensitive",
    "source": "Classification",
    "labelType": "Manual",
    "state": "Confirmed",
}
STEWARD_COLUMN_TAG = {
    "tagFQN": "PII.SSN",
    "source": "Classification",
    "labelType": "Manual",
    "state": "Confirmed",
}
# A Provisa tag a previous publish applied that the current snapshot no longer carries: the
# merge must remove it, because Provisa authored it and the snapshot is its source of truth.
STALE_PROVISA_TAG = {
    "tagFQN": f"{CLASSIFICATION}.stale_signal",
    "source": "Classification",
    "labelType": "Automated",
    "state": "Confirmed",
}
LIVE_OWNERS = [{"id": "9c1d8e70-0000-0000-0000-000000000001", "type": "user"}]


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
                    Column(name="id", data_type="integer", visible_to=["analyst", "admin"]),
                    Column(
                        name="ssn",
                        data_type="text",
                        visible_to=["admin"],
                        mask_type="regex",
                        mask_pattern=r"(\d{3})-(\d{2})-(\d{4})",
                        mask_replace="XXX",
                        unmasked_to=["admin"],
                    ),
                ],
            )
        ],
        roles=[
            Role(id="analyst", capabilities=[], domain_access=["*"]),
            Role(id="admin", capabilities=[], domain_access=["*"]),
        ],
        rls_rules=[RLSRule(table_id="orders", role_id="analyst", filter="region_id = 'us'")],
    )
    return build_snapshot(config, org_id="acme", dialect="postgres")


def _export_config() -> MetadataExportConfig:
    return MetadataExportConfig(
        enabled=True,
        provider="openmetadata",
        endpoint="https://catalog.example/",
        timeout_seconds=5,
    )


def _live_table() -> dict:
    """The live entity as a steward left it: their tags, their custom property, an owner."""
    return {
        "id": "9c1d8e70-0000-0000-0000-0000000000aa",
        "fullyQualifiedName": TABLE_FQN,
        "tags": [STEWARD_TABLE_TAG, STALE_PROVISA_TAG],
        "columns": [
            {"name": "id", "tags": []},
            {"name": "ssn", "tags": [STEWARD_COLUMN_TAG, dict(STALE_PROVISA_TAG)]},
        ],
        "extension": {"stewardNotes": "hand-curated", RELATIONSHIP_PROPERTY: "[]"},
        "owners": LIVE_OWNERS,
    }


def _mock_transport(monkeypatch, live_response):
    """Route the exporter's own calls: PUTs are captured, the by-name GET answers ``live_response``.

    ``live_response`` is a dict (served as the live table), an int (served as that HTTP
    status), or an exception instance (raised, as a transport failure would).
    """
    puts: list[tuple[str, dict]] = []
    gets: list[str] = []

    async def _put(self, url, json=None, headers=None):
        puts.append((url, json))
        body = {"id": "9c1d8e70-0000-0000-0000-0000000000aa", "fullyQualifiedName": TABLE_FQN}
        return httpx.Response(200, json=body, request=httpx.Request("PUT", url))

    async def _get(self, url, params=None, headers=None):
        gets.append(url)
        request = httpx.Request("GET", url)
        if isinstance(live_response, Exception):
            raise live_response
        if isinstance(live_response, int):
            return httpx.Response(live_response, text="unavailable", request=request)
        return httpx.Response(200, json=live_response, request=request)

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    return puts, gets


def _table_put(puts) -> dict:
    return next(body for url, body in puts if url.endswith("/tables"))


def _tag_fqns(body: dict | None) -> list[str]:
    return [t["tagFQN"] for t in (body or {}).get("tags", [])]


@pytest.mark.asyncio
async def test_merge_preserves_steward_table_tag_and_removes_stale_provisa_tag(
    snapshot, monkeypatch
):
    puts, gets = _mock_transport(monkeypatch, _live_table())
    result = await OpenMetadataExport(_export_config()).publish(snapshot)
    assert result.ok
    body = _table_put(puts)
    # Ours track the snapshot; the steward's survives; the stale Provisa tag is gone.
    assert _tag_fqns(body) == [f"{CLASSIFICATION}.rls_restricted", "PII.Sensitive"]
    assert any(url.endswith(f"/api/v1/tables/name/{TABLE_FQN}") for url in gets)


@pytest.mark.asyncio
async def test_merge_preserves_steward_column_tag_and_removes_stale_provisa_tag(
    snapshot, monkeypatch
):
    puts, _ = _mock_transport(monkeypatch, _live_table())
    result = await OpenMetadataExport(_export_config()).publish(snapshot)
    assert result.ok
    columns = {c["name"]: c for c in _table_put(puts)["columns"]}
    assert _tag_fqns(columns["ssn"]) == [
        f"{CLASSIFICATION}.masked",
        f"{CLASSIFICATION}.visibility_restricted",
        "PII.SSN",
    ]
    assert _tag_fqns(columns["id"]) == []


@pytest.mark.asyncio
async def test_merge_carries_through_fields_the_exporter_does_not_author(snapshot, monkeypatch):
    puts, _ = _mock_transport(monkeypatch, _live_table())
    result = await OpenMetadataExport(_export_config()).publish(snapshot)
    assert result.ok
    body = _table_put(puts)
    # A PUT clears what it does not send: the live owner rides through unchanged.
    assert body["owners"] == LIVE_OWNERS
    # The steward's custom property survives; Provisa's relationship property tracks the
    # snapshot, which has no approved relationships — so it is gone.
    assert body["extension"] == {"stewardNotes": "hand-curated"}


@pytest.mark.asyncio
async def test_read_failure_falls_back_to_our_body_and_reports_it(snapshot, monkeypatch):
    puts, _ = _mock_transport(monkeypatch, 500)
    result = await OpenMetadataExport(_export_config()).publish(snapshot)
    assert result.published["table"] == 1
    body = _table_put(puts)
    # The publish went through with Provisa's own body — no live state to merge.
    assert _tag_fqns(body) == [f"{CLASSIFICATION}.rls_restricted"]
    assert "owners" not in body
    merge_errors = [e for e in result.errors if "tag merge" in e.message]
    assert len(merge_errors) == 1
    assert merge_errors[0].asset.fqn() == "wh.public.orders"
    assert "HTTP 500" in merge_errors[0].message


@pytest.mark.asyncio
async def test_transport_error_on_read_falls_back_and_reports_it(snapshot, monkeypatch):
    puts, _ = _mock_transport(monkeypatch, httpx.ConnectError("refused"))
    result = await OpenMetadataExport(_export_config()).publish(snapshot)
    assert result.published["table"] == 1
    assert _tag_fqns(_table_put(puts)) == [f"{CLASSIFICATION}.rls_restricted"]
    merge_errors = [e for e in result.errors if "tag merge" in e.message]
    assert len(merge_errors) == 1
    assert "refused" in merge_errors[0].message


@pytest.mark.asyncio
async def test_first_publish_of_a_table_merges_nothing_and_reports_nothing(snapshot, monkeypatch):
    puts, _ = _mock_transport(monkeypatch, 404)
    result = await OpenMetadataExport(_export_config()).publish(snapshot)
    assert result.ok
    assert _tag_fqns(_table_put(puts)) == [f"{CLASSIFICATION}.rls_restricted"]


@pytest.mark.asyncio
async def test_tag_merge_gate_off_skips_the_live_read(snapshot, monkeypatch):
    monkeypatch.setattr(OpenMetadataExport, "tag_merge", False)
    puts, gets = _mock_transport(monkeypatch, _live_table())
    result = await OpenMetadataExport(_export_config()).publish(snapshot)
    assert result.ok
    assert not any("/tables/name/" in url for url in gets)
    assert _tag_fqns(_table_put(puts)) == [f"{CLASSIFICATION}.rls_restricted"]
