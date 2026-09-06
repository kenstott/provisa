# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2a8d13-4c95-4e07-8b1d-9a3c6e5f2b70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1635/REQ-1636: the engine-native adapters resolve members via an injected runtime.

Unlike the vendor-neutral adapters (REQ-1069), Snowflake Horizon and BigQuery Dataplex need
a live federation runtime to turn a member's ``(source_id, schema_name, table_name)`` into
its actual warehouse identity, and to confirm REQ-1633's landing terminal exists before
publishing anything. Neither adapter ever opens a real Snowflake/BigQuery connection here —
``set_runtime_resolver`` injects a fake runtime, exercising exactly the gate
``native_backend.py``'s ``reconcile_landed_tables`` uses (``hasattr(runtime,
"attach_landed_source")``).
"""

# Requirements: REQ-1633, REQ-1634, REQ-1635, REQ-1636

from __future__ import annotations

from typing import Any

import httpx
import pytest

from provisa.api.metadata_export import bigquery_dataplex, snowflake_horizon
from provisa.api.metadata_export.bigquery_dataplex import BigQueryDataplexExport
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.registry import metadata_export, registered_providers
from provisa.api.metadata_export.snowflake_horizon import SnowflakeHorizonExport
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
            id="prod", domain_id="sales", name="Sales 360", owner="alice", description="Unified"
        )
    ]
    return build_snapshot(config, org_id=ORG_ID, dialect="postgres")


class FakeRuntimeWithLandingTerminal:
    """Stands in for a federation runtime that has REQ-1633's landing terminal."""

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        return (f"{source.id}_db", source.schema_name, source.table_name)

    async def attach_landed_source(self, source: Any, columns: Any, *, pk_columns=None) -> None:
        raise AssertionError("publish() must never call attach_landed_source itself")


class FakeRuntimeWithoutLandingTerminal:
    """Stands in for today's real Snowflake/BigQuery runtime — REQ-1633 not yet implemented."""

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        raise AssertionError("must never resolve members when the landing terminal is absent")


@pytest.fixture(autouse=True)
def _reset_resolvers():
    yield
    snowflake_horizon.set_runtime_resolver(None)
    bigquery_dataplex.set_runtime_resolver(None)


def test_both_engine_native_targets_are_resolvable_by_name():
    assert {"snowflake_horizon", "bigquery_dataplex"} <= set(registered_providers())
    assert isinstance(metadata_export(_export_config("snowflake_horizon")), SnowflakeHorizonExport)
    assert isinstance(metadata_export(_export_config("bigquery_dataplex")), BigQueryDataplexExport)


# --- Snowflake Horizon ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snowflake_horizon_skips_when_no_resolver_is_wired():
    result = await SnowflakeHorizonExport(_export_config("snowflake_horizon")).publish(
        _snapshot_with_data_product()
    )
    assert result.ok
    assert result.published == {}


@pytest.mark.asyncio
async def test_snowflake_horizon_skips_when_runtime_has_no_landing_terminal():
    snowflake_horizon.set_runtime_resolver(lambda org_id: FakeRuntimeWithoutLandingTerminal())
    result = await SnowflakeHorizonExport(_export_config("snowflake_horizon")).publish(
        _snapshot_with_data_product()
    )
    assert result.ok
    assert result.published == {}


@pytest.mark.asyncio
async def test_snowflake_horizon_publishes_a_listing_backed_by_the_resolved_members(monkeypatch):
    snowflake_horizon.set_runtime_resolver(lambda org_id: FakeRuntimeWithLandingTerminal())
    requests: list[tuple[str, Any]] = []

    async def _put(self, url, json=None, headers=None):
        requests.append((url, json))
        return httpx.Response(200, request=httpx.Request("PUT", url))

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    result = await SnowflakeHorizonExport(_export_config("snowflake_horizon")).publish(
        _snapshot_with_data_product()
    )
    assert result.ok
    assert result.published == {"data_product": 1}
    url, payload = requests[0]
    assert url == "https://catalog.example/api/v2/data-products/prod"
    assert payload["name"] == "Sales 360"
    assert payload["owner"] == "alice"
    assert payload["share_objects"] == [
        "wh_db.public.orders",
        "wh_db.public.customers",
        "wh_db.public.order_totals",
    ]


@pytest.mark.asyncio
async def test_snowflake_horizon_reports_a_rejected_listing(monkeypatch):
    snowflake_horizon.set_runtime_resolver(lambda org_id: FakeRuntimeWithLandingTerminal())

    async def _put(self, url, json=None, headers=None):
        return httpx.Response(422, text="bad listing", request=httpx.Request("PUT", url))

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    result = await SnowflakeHorizonExport(_export_config("snowflake_horizon")).publish(
        _snapshot_with_data_product()
    )
    assert not result.ok
    assert result.errors[0].asset.fqn() == "prod"
    assert "422" in result.errors[0].message


# --- BigQuery Dataplex -------------------------------------------------------------------


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
