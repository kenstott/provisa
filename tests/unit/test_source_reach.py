# Copyright (c) 2026 Kenneth Stott
# Canary: 5aaeb344-6240-4ddc-9133-dc418e5cfb3f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Sources form's type list is gated and annotated by the reach of the engine serving the
org (REQ-947). It reads that reach from what opens Sources -- the right to register a source --
never from the platform's engine settings, which only a platform administrator reads: a sandbox
visitor or an org administrator was refused those, and the page showed the refusal."""

# Requirements: REQ-947

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.errors import ApiError


def _anyone(identity_caps: set[str]):
    def gate(request, capability):
        if capability not in identity_caps:
            raise ApiError(403, "auth.missing_capability", f"Missing capability: {capability!r}")

    return gate


@pytest.mark.asyncio
async def test_a_role_that_registers_sources_reads_the_reach_of_its_orgs_engine(monkeypatch):
    import provisa.api.app as appmod
    from provisa.api.admin import source_kinds_router as kinds
    from provisa.federation.engine import build_engine

    # The right that opens Sources, and none of the platform's.
    monkeypatch.setattr(kinds, "require_capability_request", _anyone({"source_registration"}))
    monkeypatch.setattr(appmod.state, "federation_engine", build_engine("duckdb"), raising=False)
    reach = await kinds.source_reach(SimpleNamespace())
    assert reach["engine"] == "duckdb"
    duckdb = next(e for e in reach["engines"] if e["key"] == "duckdb")
    assert set(duckdb) == {
        "key",
        "label",
        "live_source_types",
        "reachable_source_types",
        "unavailable",
    }
    assert duckdb["unavailable"] is None
    assert "postgresql" in duckdb["live_source_types"]
    assert set(duckdb["live_source_types"]) <= set(duckdb["reachable_source_types"])


@pytest.mark.asyncio
async def test_an_engine_whose_driver_library_is_absent_is_unavailable_and_the_rest_answer(
    monkeypatch,
):
    """An image without the ODBC driver manager could not import Fabric/Synapse's driver
    (pyodbc: libodbc.so.2), and the whole reach answered 500, so the Sources page of every sandbox
    showed the error. That engine is reported unavailable, naming the library; the rest answer."""
    import provisa.api.app as appmod
    import provisa.federation.engine as engmod
    from provisa.api.admin import source_kinds_router as kinds
    from provisa.federation.engine import build_engine

    missing = "libodbc.so.2: cannot open shared object file: No such file or directory"

    def _no_driver(name):
        raise ImportError(missing)

    monkeypatch.setattr(engmod, "_build_mssql_warehouse_engine", _no_driver)
    monkeypatch.setattr(kinds, "require_capability_request", _anyone({"source_registration"}))
    monkeypatch.setattr(appmod.state, "federation_engine", build_engine("duckdb"), raising=False)
    reach = await kinds.source_reach(SimpleNamespace())
    engines = {e["key"]: e for e in reach["engines"]}
    for key in ("fabric", "synapse"):
        assert engines[key]["unavailable"] == missing
        assert engines[key]["reachable_source_types"] == []
        assert engines[key]["live_source_types"] == []
    assert engines["duckdb"]["unavailable"] is None
    assert "postgresql" in engines["duckdb"]["live_source_types"]


@pytest.mark.asyncio
async def test_a_role_that_registers_no_source_is_refused_the_reach(monkeypatch):
    from provisa.api.admin import source_kinds_router as kinds

    monkeypatch.setattr(kinds, "require_capability_request", _anyone(set()))
    with pytest.raises(ApiError) as refused:
        await kinds.source_reach(SimpleNamespace())
    assert refused.value.status_code == 403


def test_the_reach_carries_none_of_the_engine_settings():
    """Its route is the source kinds', not the platform settings' (/admin/federation-engine)."""
    from provisa.api.admin.source_kinds_router import router

    paths = {getattr(route, "path", None) for route in router.routes}
    assert "/admin/source-kinds" in paths
