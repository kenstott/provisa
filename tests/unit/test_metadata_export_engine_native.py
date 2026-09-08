# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2a8d13-4c95-4e07-8b1d-9a3c6e5f2b70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1636: the BigQuery Dataplex adapter resolves members via an injected runtime.

Unlike the vendor-neutral adapters (REQ-1069), BigQuery Dataplex needs a live federation
runtime to turn a member's ``(source_id, schema_name, table_name)`` into its actual warehouse
identity, and to confirm REQ-1633's landing terminal exists before publishing anything. The
adapter never opens a real BigQuery connection here — ``set_runtime_resolver`` injects a fake
runtime, exercising exactly the gate ``native_backend.py``'s ``reconcile_landed_tables`` uses
(``hasattr(runtime, "attach_landed_source")``).

Snowflake Horizon (REQ-1635) is engine-native by a different route — it opens its own
connection off ``configured_engine_url()`` rather than an injected runtime resolver — and is
covered by ``tests/unit/test_snowflake_horizon_export.py`` instead.
"""

# Requirements: REQ-1633, REQ-1634, REQ-1636

from __future__ import annotations

from typing import Any

import httpx
import pytest

from provisa.api.metadata_export import bigquery_dataplex
from provisa.api.metadata_export.bigquery_dataplex import BigQueryDataplexExport
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.registry import metadata_export, registered_providers
from provisa.core.models import DataProduct, MetadataExportConfig
from tests.integration.metadata_export_fixture import ORG_ID, governed_config


def _export_config(provider: str) -> MetadataExportConfig:
    return MetadataExportConfig(
        enabled=True, provider=provider, endpoint="https://catalog.example/", timeout_seconds=5
    )


def _snapshot_with_data_product():
    config = governed_config()
    config.data_products = [
        DataProduct(
            id="prod", domain_id="sales", name="Sales 360", owner_role="alice", purpose="Unified"
        )
    ]
    return build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})


class FakeRuntimeWithLandingTerminal:
    """Stands in for a federation runtime that has REQ-1633's landing terminal."""

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        return (f"{source.id}_db", source.schema_name, source.table_name)

    async def attach_landed_source(self, source: Any, columns: Any, *, pk_columns=None) -> None:
        raise AssertionError("publish() must never call attach_landed_source itself")


class FakeRuntimeWithoutLandingTerminal:
    """Stands in for today's real BigQuery runtime — REQ-1633 not yet implemented."""

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        raise AssertionError("must never resolve members when the landing terminal is absent")


@pytest.fixture(autouse=True)
def _reset_resolver():
    yield
    bigquery_dataplex.set_runtime_resolver(None)


def test_bigquery_dataplex_is_resolvable_by_name():
    assert "bigquery_dataplex" in registered_providers()
    assert isinstance(metadata_export(_export_config("bigquery_dataplex")), BigQueryDataplexExport)


@pytest.mark.asyncio
async def test_bigquery_dataplex_skips_when_no_resolver_is_wired():
    result = await BigQueryDataplexExport(_export_config("bigquery_dataplex")).publish(
        _snapshot_with_data_product()
    )
    assert result.ok
    assert result.published == {}


@pytest.mark.asyncio
async def test_bigquery_dataplex_skips_when_runtime_has_no_landing_terminal():
    bigquery_dataplex.set_runtime_resolver(lambda org_id: FakeRuntimeWithoutLandingTerminal())
    result = await BigQueryDataplexExport(_export_config("bigquery_dataplex")).publish(
        _snapshot_with_data_product()
    )
    assert result.ok
    assert result.published == {}


@pytest.mark.asyncio
async def test_bigquery_dataplex_publishes_a_listing_backed_by_the_resolved_members(monkeypatch):
    bigquery_dataplex.set_runtime_resolver(lambda org_id: FakeRuntimeWithLandingTerminal())
    requests: list[tuple[str, Any]] = []

    async def _put(self, url, json=None, headers=None):
        requests.append((url, json))
        return httpx.Response(200, request=httpx.Request("PUT", url))

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    result = await BigQueryDataplexExport(_export_config("bigquery_dataplex")).publish(
        _snapshot_with_data_product()
    )
    assert result.ok
    assert result.published == {"data_product": 1}
    url, payload = requests[0]
    assert url == "https://catalog.example/v1/dataProducts/prod"
    assert payload["displayName"] == "Sales 360"
    assert payload["owner"] == "alice"
    assert payload["bigQueryDataset"]["tables"] == [
        "wh_db.public.orders",
        "wh_db.public.customers",
        "wh_db.public.order_totals",
    ]


def test_bigquery_dataplex_listing_carries_the_product_page_as_documentation():
    """REQ-1659: the Analytics Hub listing's documentation is the product's page in Provisa."""
    from provisa.api.metadata_export.bigquery_dataplex import ListingSpec

    with_link = ListingSpec(
        product_id="prod",
        name="Sales 360",
        description="Unified",
        owner_id=None,
        dataset_tables=["wh_db.public.orders"],
        documentation="https://acme.example.test/data-products?product=prod",
    ).as_payload()
    assert with_link["documentation"] == "https://acme.example.test/data-products?product=prod"
    bare = ListingSpec("prod", "Sales 360", "Unified", None, ["wh_db.public.orders"]).as_payload()
    assert "documentation" not in bare
