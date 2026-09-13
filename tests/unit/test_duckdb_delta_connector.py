# Copyright (c) 2026 Kenneth Stott
# Canary: 2e3dd419-92a0-4d45-b69f-f58343f0abf1
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDBDeltaConnector (REQ-899): delta_scan scanner-view details() and the LOAD-ONLY probe
(``_DuckDBExtensionConnector.probe``) — install/load the core ``delta`` extension, then confirm
``delta_scan`` registered. Pure logic — the async ``probe(fetch)`` is driven by a fake fetch
callable; no live DuckDB, no real Delta table."""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import DuckDBDeltaConnector


def _src(sid: str, path: str = "/warehouse/orders", **kw) -> Source:
    return Source(id=sid, type=SourceType.delta_lake, path=path, **kw)


class _FakeFetch:
    """Async fetch that mirrors _DuckDBExtensionConnector.probe's two calls: LOAD <ext>, then the
    duckdb_functions() registration check."""

    def __init__(self, *, loads: bool, registered: bool):
        self._loads = loads
        self._registered = registered

    async def __call__(self, sql: str):
        if sql.startswith("LOAD") or sql.startswith("INSTALL"):
            if not self._loads:
                raise RuntimeError("extension not staged")
            return []
        if "duckdb_functions()" in sql:
            return [{"n": 1}] if self._registered else [{"n": 0}]
        return []


# ---- identity / packaging (REQ-899) -----------------------------------------


def test_delta_connector_identity():
    c = DuckDBDeltaConnector()
    assert c.engine == "duckdb"
    assert c.source_type == "delta_lake"
    assert c.key == "duckdb_delta"
    assert c.extension == "delta"
    assert c.install_from_community is False  # core extension, not community
    assert c.probe_symbol == "delta_scan"
    assert c.mechanism is Mechanism.SCAN  # read in place, no attach (REQ-951)


# ---- details() DDL (REQ-899) -------------------------------------------------


def test_details_emits_delta_scan_view_over_source_path():
    details = DuckDBDeltaConnector().details(_src("orders", path="s3://bucket/orders"))
    assert details == {
        "view_ddl": "CREATE VIEW orders AS SELECT * FROM delta_scan('s3://bucket/orders')"
    }


def test_details_emits_s3_secret_ddl_when_credentials_present():
    # REQ-1736: an S3-backed delta_lake source with federation_hints credentials must get a
    # DuckDB SECRET alongside its view — without this, httpfs falls back to default AWS creds and
    # a cloud-hosted table can never actually be read despite `path` and creds both being correct.
    src = _src(
        "orders",
        path="s3://bucket/orders",
        federation_hints={"access_key_id": "AKIAFAKE", "secret_access_key": "shh"},
    )
    details = DuckDBDeltaConnector().details(src)
    assert "secret_ddl" in details
    assert "KEY_ID 'AKIAFAKE'" in details["secret_ddl"]
    assert "SECRET 'shh'" in details["secret_ddl"]


def test_details_omits_secret_ddl_without_credentials():
    details = DuckDBDeltaConnector().details(_src("orders", path="s3://bucket/orders"))
    assert "secret_ddl" not in details


def test_details_omits_secret_ddl_for_local_path():
    details = DuckDBDeltaConnector().details(_src("orders", path="/local/orders"))
    assert "secret_ddl" not in details


# ---- LOAD-ONLY probe (REQ-904 / REQ-899) ------------------------------------


@pytest.mark.asyncio
async def test_probe_available_when_extension_loads_and_delta_scan_registered():
    r = await DuckDBDeltaConnector().probe(_FakeFetch(loads=True, registered=True))
    assert r.available is True
    assert "delta_scan" in r.reason


@pytest.mark.asyncio
async def test_probe_unavailable_when_delta_scan_not_registered():
    # The extension loads, but delta_scan isn't registered — fail closed, never silently proceed.
    r = await DuckDBDeltaConnector().probe(_FakeFetch(loads=True, registered=False))
    assert r.available is False
    assert "delta_scan" in r.reason
    assert r.remediation and "delta" in r.remediation.lower()


@pytest.mark.asyncio
async def test_probe_unavailable_when_extension_fails_to_load():
    r = await DuckDBDeltaConnector().probe(_FakeFetch(loads=False, registered=True))
    assert r.available is False
    assert "did not load" in r.reason


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
