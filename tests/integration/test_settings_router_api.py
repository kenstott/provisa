# Copyright (c) 2026 Kenneth Stott
# Canary: 2fc98bcd-14be-4432-985f-a113ad194f11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Admin REST settings endpoints (settings_router.py)."""

import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestDownloadConfig:
    async def test_get_config(self, client):
        resp = await client.get("/admin/config")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/x-yaml")


class TestLiveConfigDisabledByDefault:
    async def test_live_config_404(self, client):
        resp = await client.get("/admin/config/live")
        assert resp.status_code == 404

    async def test_config_diff_404(self, client):
        resp = await client.get("/admin/config/diff")
        assert resp.status_code == 404

    async def test_config_patch_404(self, client):
        resp = await client.post("/admin/config/patch", content=b"sources: []")
        assert resp.status_code == 404


class TestLiveConfigEnabled:
    async def test_live_config_and_diff_and_patch(self, client):
        from provisa.api.app import state

        prev = getattr(state, "config_live_export", False)
        state.config_live_export = True
        try:
            live_resp = await client.get("/admin/config/live")
            assert live_resp.status_code == 200
            assert live_resp.headers["content-type"].startswith("application/x-yaml")

            diff_resp = await client.get("/admin/config/diff")
            assert diff_resp.status_code == 200
            diff = diff_resp.json()
            assert "original" in diff
            assert "current" in diff

            patch_resp = await client.post("/admin/config/patch", content=live_resp.content)
            assert patch_resp.status_code == 200
            assert patch_resp.headers["content-type"].startswith("text/x-patch")
        finally:
            state.config_live_export = prev


class TestGetSettings:
    async def test_get_settings_shape(self, client):
        resp = await client.get("/admin/settings")
        assert resp.status_code == 200
        body = resp.json()
        for key in (
            "features",
            "engine",
            "redirect",
            "limits",
            "cache",
            "naming",
            "relationships",
            "cdc",
            "materialize",
            "sampling",
            "otel",
            "graphql_remote",
        ):
            assert key in body


class TestUpdateSettings:
    async def test_update_settings_scalars(self, client):
        resp = await client.put(
            "/admin/settings",
            json={
                "redirect": {
                    "enabled": True,
                    "threshold": 1000,
                    "default_format": "arrow",
                    "ttl": 60,
                },
                "limits": {"default_row_limit": 500},
                "cache": {"default_ttl": 120},
                "sampling": {"default_sample_size": 5000},
                "relationships": {"auto_track_fk": False},
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "redirect.enabled" in body["updated"]
        assert "limits.default_row_limit" in body["updated"]
        assert "cache.default_ttl" in body["updated"]
        assert "sampling.default_sample_size" in body["updated"]
        assert "relationships.auto_track_fk" in body["updated"]
        # Restore auto_track_fk to its default so later tests aren't affected.
        await client.put("/admin/settings", json={"relationships": {"auto_track_fk": True}})

    async def test_update_settings_naming_invalid_convention(self, client):
        resp = await client.put(
            "/admin/settings", json={"naming": {"convention": "not-a-real-convention"}}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is False
        assert "Invalid convention" in body["message"]

    async def test_update_settings_naming_valid(self, client):
        resp = await client.put(
            "/admin/settings",
            json={"naming": {"domain_prefix": True, "convention": "apollo_graphql"}},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "naming.domain_prefix" in body["updated"]
        assert "naming.convention" in body["updated"]

    async def test_update_settings_otel(self, client):
        resp = await client.put(
            "/admin/settings",
            json={"otel": {"service_name": "provisa-test", "sample_rate": 0.5}},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "otel.service_name" in body["updated"]
        assert "otel.sample_rate" in body["updated"]

    async def test_update_settings_cdc(self, client):
        resp = await client.put(
            "/admin/settings", json={"cdc": {"consumer_group_id": "smtest-group"}}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "cdc.consumer_group_id" in body["updated"]
        assert body["restart_required"] is True
        # Clear it again so later runs don't depend on this state.
        await client.put("/admin/settings", json={"cdc": {"consumer_group_id": ""}})

    async def test_update_settings_graphql_remote(self, client):
        resp = await client.put(
            "/admin/settings",
            json={
                "graphql_remote": {"max_object_depth": 6, "max_list_depth": 3, "max_list_items": 50}
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "graphql_remote.max_object_depth" in body["updated"]

    async def test_update_settings_engine(self, client):
        resp = await client.put("/admin/settings", json={"engine": {"jvm_heap_gb": 2}})
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "engine.jvm_heap_gb" in body["updated"]
        assert body["restart_required"] is True


class TestFederationEngine:
    async def test_get_federation_engine(self, client):
        resp = await client.get("/admin/federation-engine")
        assert resp.status_code == 200
        body = resp.json()
        assert "current" in body
        assert "engines" in body

    async def test_set_federation_engine_unknown(self, client):
        resp = await client.put("/admin/federation-engine", json={"engine": "no-such-engine-xyz"})
        assert resp.status_code == 400


class TestCacheStorage:
    async def test_get_cache_storage(self, client):
        resp = await client.get("/admin/cache-storage")
        assert resp.status_code == 200
        body = resp.json()
        assert "cache" in body
        assert "hot_tables" in body

    async def test_set_cache_storage(self, client):
        resp = await client.put(
            "/admin/cache-storage",
            json={"cache": {"default_ttl": 300}, "hot_tables": {"max_rows": 10000}},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "cache.default_ttl" in body["updated"]
        assert "hot_tables.max_rows" in body["updated"]


class TestEncryption:
    async def test_get_encryption(self, client):
        resp = await client.get("/admin/encryption")
        assert resp.status_code == 200
        body = resp.json()
        assert "provider" in body
        assert "providers" in body

    async def test_set_encryption_unknown_provider(self, client):
        resp = await client.put("/admin/encryption", json={"provider": "no-such-provider-xyz"})
        assert resp.status_code == 400

    async def test_generate_encryption_key(self, client):
        resp = await client.post("/admin/encryption/generate-key", json={})
        assert resp.status_code == 200
        body = resp.json()
        assert "stored" in body
        assert body["key_id"] == "master"


class TestAuth:
    async def test_get_auth(self, client):
        resp = await client.get("/admin/auth")
        assert resp.status_code == 200
        body = resp.json()
        assert "provider" in body
        assert "providers" in body

    async def test_set_auth_unknown_provider(self, client):
        resp = await client.put("/admin/auth", json={"provider": "no-such-provider-xyz"})
        assert resp.status_code == 400


class TestSchemaClusters:
    async def test_recompute_schema_clusters(self, client):
        resp = await client.post("/admin/schema-clusters/recompute")
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "tables_clustered" in body


class TestRecentTraces:
    async def test_get_recent_traces(self, client):
        resp = await client.get("/admin/traces/recent")
        assert resp.status_code == 200
        body = resp.json()
        assert "traces" in body
        assert isinstance(body["traces"], list)

    async def test_get_recent_traces_limit(self, client):
        resp = await client.get("/admin/traces/recent", params={"limit": 5})
        assert resp.status_code == 200
        assert "traces" in resp.json()


class TestDomainPolicyValidation:
    """Validation-only branches — the success path is destructive (resets the live config)
    and is exercised last, in its own class, so it doesn't disturb earlier tests."""

    async def test_invalid_use_domains_type(self, client):
        resp = await client.post("/admin/domain-policy", json={"use_domains": "yes"})
        assert resp.status_code == 400

    async def test_missing_default_domain_when_use_domains_false(self, client):
        resp = await client.post(
            "/admin/domain-policy", json={"use_domains": False, "default_domain": ""}
        )
        assert resp.status_code == 400


class TestDomainPolicyApply:
    """Runs last: exercises the destructive success path of POST /admin/domain-policy."""

    async def test_set_domain_policy_success(self, client):
        resp = await client.post("/admin/domain-policy", json={"use_domains": True})
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["use_domains"] is True

        # Config is now reset (no user sources/domains/tables/relationships) — verify via
        # GraphQL. Sources still includes provisa's own bootstrap-internal sources (e.g.
        # "__provisa__", the OTel self-monitoring source), and domains still includes the
        # internal "meta"/"ops" domains (config_export._INTERNAL_DOMAINS) — both are seeded
        # independently of config.yaml and are not cleared by this endpoint.
        gql_resp = await client.post(
            "/admin/graphql", json={"query": "{ sources { id } domains { id } }"}
        )
        assert gql_resp.status_code == 200
        gql_body = gql_resp.json()["data"]
        assert "pet-store-pg" not in {s["id"] for s in gql_body["sources"]}
        assert {"meta", "ops"} >= {d["id"] for d in gql_body["domains"]}
