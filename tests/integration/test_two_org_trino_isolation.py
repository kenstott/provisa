# Copyright (c) 2026 Kenneth Stott
# Canary: b4e17a92-8c05-4d3f-a1e6-2f9b7c0d5a41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1266: two orgs seeded with the SAME demo source_id are simultaneously queryable
against ONE live Trino coordinator, each through its own org-prefixed catalog — no process
restart, no cross-org catalog collision.

This is the data-plane half the Postgres-only onboarding test (test_create_org_onboarding.py)
stubs out. It boots the real app against a live Trino + Postgres and drives the actual
``build_org_runtime`` path (never stubbed): two orgs' catalogs are created dynamically in the
running coordinator, coexist in ``SHOW CATALOGS`` at the same time, and each returns rows when
queried under its own bound org.

The fixture source host is the compose-internal service name ``postgres`` so the dynamically
created ``org_<id>__sales_pg`` catalog is reachable FROM the Trino container. The pytest-host
direct-driver pool cannot reach ``postgres`` and fails silently by design
(tolerate_startup_failure); every assertion here runs through the ENGINE terminal only.
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "two_org_trino_config.yaml")
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(scope="module")
async def app_state():
    from _pytest.monkeypatch import MonkeyPatch

    from provisa.api.app import create_app, state
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    # Scoped to this fixture, never module import: pytest imports every test module before running
    # anything, so an import-time PROVISA_CONFIG/PROVISA_CONFIG_REPLACE would repoint — and with
    # REPLACE=1, delete rows from — the shared control plane every other in-process test uses.
    # Host/port default so an isolated-stack run (ephemeral ports, set by conftest) still wins.
    mp = MonkeyPatch()
    mp.setenv("PROVISA_ENGINE", "trino")
    mp.setenv("PROVISA_CONFIG", _CONFIG)
    mp.setenv("PROVISA_CONFIG_REPLACE", "1")
    mp.setenv("TRINO_HOST", os.environ.get("TRINO_HOST", "localhost"))
    mp.setenv("TRINO_PORT", os.environ.get("TRINO_PORT", "8080"))
    mp.setenv("PG_PASSWORD", os.environ.get("PG_PASSWORD", "provisa"))
    mp.setenv("PROVISA_SKIP_TRINO_WAIT", os.environ.get("PROVISA_SKIP_TRINO_WAIT", "1"))

    try:
        # If another test imported the app earlier under a different engine, rebind to Trino so this
        # module always exercises the shared-coordinator catalog namespace it is written to prove.
        if getattr(state.federation_engine.engine, "name", None) != "trino":
            state.federation_engine = EngineRuntime(build_engine(), state)

        app = create_app()
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as _c:
                assert _c is not None
                yield state
    finally:
        mp.undo()


async def _drop_catalog(state, name: str) -> None:
    # Dev-stack Trino persists across runs; drop the org catalogs so a rerun starts clean.
    await state.federation_engine.execute_engine(f"DROP CATALOG IF EXISTS {name}")


async def test_two_orgs_isolated_and_simultaneously_queryable(app_state):
    state = app_state
    from provisa.api.app import build_org_runtime
    from provisa.api.org_runtime import set_current_org, reset_current_org

    assert state.federation_engine.engine.name == "trino"
    # The default/bootstrap org keeps the bare catalog name (never org-prefixed).
    assert state.org_registry.get(state.org_id).source_catalogs["sales-pg"] == "sales_pg"

    acme = await build_org_runtime("acme", include_demo=True)
    beta = await build_org_runtime("beta", include_demo=True)
    try:
        # Identical demo source_id ("sales-pg") → distinct org-prefixed catalogs, no collision.
        assert acme.source_catalogs["sales-pg"] == "org_acme__sales_pg"
        assert beta.source_catalogs["sales-pg"] == "org_beta__sales_pg"
        assert acme.source_catalogs["sales-pg"] != beta.source_catalogs["sales-pg"]

        # Both org catalogs exist in the ONE live coordinator at the SAME time — no restart.
        cats = await state.federation_engine.execute_engine("SHOW CATALOGS")
        names = {row[0] for row in cats.rows}
        assert "org_acme__sales_pg" in names, names
        assert "org_beta__sales_pg" in names, names

        # Each org, bound via the ContextVar, resolves ITS OWN catalog and is independently
        # queryable through it. Same physical table → identical, non-zero counts, proving both
        # org catalogs really reach Postgres (not an empty/misrouted catalog).
        counts = {}
        for org_id, expected_cat in (
            ("acme", "org_acme__sales_pg"),
            ("beta", "org_beta__sales_pg"),
        ):
            token = set_current_org(org_id)
            try:
                assert state.catalog_for("sales-pg") == expected_cat
                res = await state.federation_engine.execute_engine(
                    f"SELECT count(*) FROM {expected_cat}.public.orders"
                )
                counts[org_id] = res.rows[0][0]
            finally:
                reset_current_org(token)

        assert counts["acme"] > 0
        assert counts["acme"] == counts["beta"]
    finally:
        await _drop_catalog(state, "org_acme__sales_pg")
        await _drop_catalog(state, "org_beta__sales_pg")
