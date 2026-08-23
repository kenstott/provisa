# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2a1e1e-2161-45d1-bd58-ad4a74e64c31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""New admin config surfaces: security posture, AI models / vector models / NL
rate limit, warm-tier + MV defaults on cache-storage, extended OTel tuning,
remote-GraphQL limits, editable sample size, and the S3 exchange-spool engine
fields.

Requirements: REQ-693, REQ-464, REQ-419, REQ-500, REQ-370, REQ-240, REQ-543,
REQ-545, REQ-165.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.api.admin._config_io import read_config
from provisa.api.admin.ai_models_router import router as ai_models_router
from provisa.api.admin.security_router import router as security_router
from provisa.api.admin.settings_router import router as settings_router


@pytest.fixture
def cfg_env(tmp_path: Path):
    """A temp config file wired to PROVISA_CONFIG for the duration of the test."""
    cfg_path = tmp_path / "provisa.yaml"
    cfg_path.write_text("sources: []\n")
    with patch.dict(os.environ, {"PROVISA_CONFIG": str(cfg_path)}):
        yield cfg_path


@pytest.fixture
def org_overrides(monkeypatch):
    """REQ-1349: the acting org's override rows, held in memory instead of its schema.

    The AI-models surface writes the ORG's storage, not the deployment config file. Standing in for
    that storage keeps this module a unit test of the router while still asserting where the write
    lands — ``tests/integration/test_org_settings_overrides.py`` drives the same functions against
    a real org schema.
    """
    import types

    import provisa.core.org_secrets as org_secrets_mod
    import provisa.core.org_settings as org_settings_mod

    store: dict = {}

    async def _read(_db):
        return store

    async def _write(_db, updates, *, updated_by):
        for key, value in updates.items():
            if value is None:
                store.pop(key, None)
            else:
                store[key] = value
        return list(updates)

    async def _read_api_keys(_db):
        return {}

    monkeypatch.setattr(org_settings_mod, "read_org_overrides", _read)
    monkeypatch.setattr(org_settings_mod, "write_org_overrides", _write)
    monkeypatch.setattr(org_secrets_mod, "read_org_api_keys", _read_api_keys)
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(tenant_db=object()), raising=False
    )
    return store


@pytest.fixture
def client(cfg_env):
    app = FastAPI()
    app.include_router(security_router)
    app.include_router(ai_models_router)
    app.include_router(settings_router)
    return TestClient(app)


# --- Security posture (REQ-693) -------------------------------------------------


class TestSecurityPosture:
    def test_get_defaults_to_standard(self, client):
        r = client.get("/admin/security")
        assert r.status_code == 200
        body = r.json()
        assert body["mode"] == "standard"
        assert {m["key"] for m in body["modes"]} == {"standard", "high"}

    def test_put_high_persists(self, client, cfg_env):
        r = client.put("/admin/security", json={"mode": "high"})
        assert r.status_code == 200
        assert r.json()["restart_required"] is True
        assert read_config()["security"]["mode"] == "high"
        assert client.get("/admin/security").json()["mode"] == "high"

    def test_put_unknown_mode_rejected(self, client):
        assert client.put("/admin/security", json={"mode": "paranoid"}).status_code == 400


# --- AI models / vector models / NL rate limit (REQ-464/419/500/370) ------------


class TestAiModels:
    """REQ-1349: this surface is ORG-scoped — it reads the deployment config as the base and
    writes the acting org's overrides, never the deployment config file."""

    def test_get_returns_field_defaults(self, client, org_overrides):
        body = client.get("/admin/ai-models").json()
        assert body["ai_models"]["sql_generation"]  # defaulted from AIModelsConfig
        assert body["vector_models"] == []
        assert body["nl"]["rate_limit"] is None

    def test_get_shows_the_orgs_override_over_the_deployment_value(self, client, org_overrides):
        org_overrides["ai_models"] = {"sql_generation": "claude-opus-4-8"}
        assert client.get("/admin/ai-models").json()["ai_models"]["sql_generation"] == (
            "claude-opus-4-8"
        )

    def test_put_persists_assignment_vector_and_rate_limit(self, client, cfg_env, org_overrides):
        r = client.put(
            "/admin/ai-models",
            json={
                "ai_models": {"sql_generation": "claude-opus-4-8"},
                "vector_models": [
                    {"id": "text-embedding-3-small", "provider": "openai", "dimensions": 1536}
                ],
                "nl": {"rate_limit": 60},
            },
        )
        assert r.status_code == 200
        assert org_overrides["ai_models"]["sql_generation"] == "claude-opus-4-8"
        assert org_overrides["vector_models"][0]["provider"] == "openai"
        assert org_overrides["nl"]["rate_limit"] == 60
        # The deployment config is the BASE for every org and is not the org's to edit.
        assert yaml.safe_load(cfg_env.read_text()) == {"sources": []}

    def test_blank_assignment_drops_the_org_override(self, client, org_overrides):
        # Reverting means "use the deployment's choice", so the override goes away entirely rather
        # than being rewritten with a value copied out of the router.
        client.put("/admin/ai-models", json={"ai_models": {"sql_generation": "x"}})
        client.put("/admin/ai-models", json={"ai_models": {"sql_generation": ""}})
        assert "ai_models" not in org_overrides

    def test_invalid_vector_model_rejected(self, client, org_overrides):
        r = client.put("/admin/ai-models", json={"vector_models": [{"id": "x"}]})
        assert r.status_code == 400


# --- Cache-storage: warm tier + MV default TTL (REQ-240, REQ-543) ---------------


class TestCacheStorageWarmAndMv:
    def test_put_warm_tables_and_mv_default_persist(self, client):
        r = client.put(
            "/admin/cache-storage",
            json={
                "warm_tables": {
                    "query_threshold": 250,
                    "max_rows": 5_000_000,
                    "fs_cache_enabled": True,
                    "fs_cache_max_sizes": "20GB",
                },
                "materialized_views": {"default_ttl": 900},
            },
        )
        assert r.status_code == 200
        cfg = read_config()
        assert cfg["warm_tables"]["query_threshold"] == 250
        assert cfg["warm_tables"]["fs_cache_enabled"] is True
        assert cfg["warm_tables"]["fs_cache_max_sizes"] == "20GB"
        assert cfg["materialized_views"]["default_ttl"] == 900


# --- Extended OTel tuning via _apply_otel (REQ-545) -----------------------------


class TestOtelExtended:
    def test_apply_otel_persists_pipeline_fields(self, cfg_env):
        from provisa.api.admin.settings_router import _apply_otel

        updated: list[str] = []
        _apply_otel(
            {
                "log_level": "DEBUG",
                "compact_batch_size": 42,
                "s3_endpoint": "http://localhost:9000",
                "ops_snapshot_retention_hours": 24,
                "collector_batch_timeout_ms": 500,
            },
            updated,
        )
        obs = read_config()["observability"]
        assert obs["log_level"] == "DEBUG"
        assert obs["compact_batch_size"] == 42
        assert obs["s3_endpoint"] == "http://localhost:9000"
        assert obs["ops_snapshot_retention_hours"] == 24
        assert obs["collector_batch_timeout_ms"] == 500
        assert "otel.log_level" in updated

    def test_apply_otel_blank_retention_is_none(self, cfg_env):
        from provisa.api.admin.settings_router import _apply_otel

        _apply_otel({"ops_snapshot_retention_hours": ""}, [])
        assert read_config()["observability"]["ops_snapshot_retention_hours"] is None


# --- Remote-GraphQL limits + editable sample size (REQ-165) ---------------------


class TestSettingsGraphqlRemoteAndSampling:
    def test_get_settings_reports_graphql_remote_defaults(self, client):
        body = client.get("/admin/settings").json()
        assert body["graphql_remote"]["max_object_depth"] == 5
        assert body["graphql_remote"]["max_list_items"] == 100

    def test_put_graphql_remote_persists(self, client):
        r = client.put("/admin/settings", json={"graphql_remote": {"max_object_depth": 9}})
        assert r.status_code == 200
        assert read_config()["graphql_remote"]["max_object_depth"] == 9

    def test_put_sample_size_sets_env(self, client):
        with patch.dict(os.environ, {}, clear=False):
            client.put("/admin/settings", json={"sampling": {"default_sample_size": 555}})
            assert os.environ["PROVISA_SAMPLE_SIZE"] == "555"


# --- Encryption provider registry (REQ-918) -------------------------------------


class TestEncryptionProviders:
    def test_get_lists_registry_providers_with_fields_and_availability(self, client):
        body = client.get("/admin/encryption").json()
        by_key = {p["key"]: p for p in body["providers"]}
        assert by_key["null"]["available"] is True
        assert by_key["local"]["available"] is True
        # AWS KMS is available (boto3 is a base dependency) and declares its config fields.
        assert by_key["aws_kms"]["available"] is True
        assert {"key_arn", "region", "endpoint_url"} <= {
            f["config_key"] for f in by_key["aws_kms"]["config_fields"]
        }
        # Vault/Azure/GCP appear regardless of whether their SDK is installed.
        assert {"hashicorp_vault", "gcp_kms", "azure_key_vault"} <= set(by_key)
        assert "config" in body

    def test_put_unavailable_provider_rejected(self, client):
        # An unavailable provider (its runtime probe is False) must be rejected — fail closed.
        # Registered here so the assertion is independent of which optional SDKs are installed.
        from provisa.encryption import EncryptionProviderSpec, register_encryption_provider
        from provisa.encryption.service import NullEncryption

        register_encryption_provider(
            EncryptionProviderSpec(
                key="unavailable_kms",
                label="Unavailable",
                description="test",
                available=lambda: False,
                build=lambda cfg, key_id, ttl: NullEncryption(),
            )
        )
        r = client.put("/admin/encryption", json={"provider": "unavailable_kms"})
        assert r.status_code == 400
        assert "not available" in r.json()["detail"]

    def test_put_unknown_provider_rejected(self, client):
        assert client.put("/admin/encryption", json={"provider": "rot13"}).status_code == 400

    def test_put_aws_kms_persists_config_block(self, client):
        r = client.put(
            "/admin/encryption",
            json={
                "provider": "aws_kms",
                "config": {"key_arn": "arn:aws:kms:us-east-1:1:key/x", "region": "us-east-1"},
            },
        )
        assert r.status_code == 200
        enc = read_config()["encryption"]
        assert enc["provider"] == "aws_kms"
        assert enc["aws_kms"]["key_arn"] == "arn:aws:kms:us-east-1:1:key/x"
        assert enc["aws_kms"]["region"] == "us-east-1"

    def test_alias_persists_canonical_key(self, client):
        r = client.put("/admin/encryption", json={"provider": "none"})
        assert r.status_code == 200
        assert read_config()["encryption"]["provider"] == "null"

    def test_available_provider_persists_and_config_roundtrips(self, client):
        r = client.put("/admin/encryption", json={"provider": "local", "key_id": "k1"})
        assert r.status_code == 200
        enc = read_config()["encryption"]
        assert enc["provider"] == "local"
        assert enc["key_id"] == "k1"
        assert client.get("/admin/encryption").json()["provider"] == "local"

    def test_custom_provider_registration_surfaces_in_api(self, client):
        from provisa.encryption import EncryptionProviderSpec, register_encryption_provider
        from provisa.encryption.service import NullEncryption

        register_encryption_provider(
            EncryptionProviderSpec(
                key="acme_hsm",
                label="ACME HSM",
                description="Enterprise custom endpoint.",
                build=lambda cfg, key_id, ttl: NullEncryption(),
                config_fields=[
                    {
                        "config_key": "endpoint",
                        "label": "Endpoint",
                        "type": "string",
                        "required": True,
                    },
                ],
            )
        )
        by_key = {p["key"]: p for p in client.get("/admin/encryption").json()["providers"]}
        assert by_key["acme_hsm"]["available"] is True
        assert by_key["acme_hsm"]["config_fields"][0]["config_key"] == "endpoint"
        # And it is now selectable (available) — persists its config block.
        r = client.put(
            "/admin/encryption",
            json={"provider": "acme_hsm", "config": {"endpoint": "https://hsm.internal"}},
        )
        assert r.status_code == 200
        assert read_config()["encryption"]["acme_hsm"]["endpoint"] == "https://hsm.internal"


# --- Secrets service registry (REQ-1557, REQ-1558) ------------------------------


class TestSecretsService:
    """Which secrets service the deployment resolves ``${secret:NAME}`` through."""

    def test_get_lists_every_backend_with_availability_and_library(self, client):
        body = client.get("/admin/secrets-service").json()
        by_key = {p["key"]: p for p in body["providers"]}
        # Unset selects Provisa's own store; it needs nothing installed and is the writable one.
        assert body["provider"] == "provisa"
        assert by_key["provisa"]["available"] is True
        assert by_key["provisa"]["requires"] is None
        assert by_key["provisa"]["writable"] is True
        # Every central backend is listed whether or not its SDK is installed, and each names the
        # distribution the page renders as "(requires hvac import)".
        assert {
            "hashicorp_vault",
            "aws_secrets_manager",
            "gcp_secret_manager",
            "azure_key_vault",
        } <= set(by_key)
        assert by_key["hashicorp_vault"]["requires"] == "hvac"
        assert by_key["aws_secrets_manager"]["requires"] == "boto3"
        assert by_key["hashicorp_vault"]["writable"] is False
        assert {"url", "token", "mount", "namespace"} == {
            f["config_key"] for f in by_key["hashicorp_vault"]["config_fields"]
        }

    def test_put_unknown_backend_rejected(self, client):
        assert client.put("/admin/secrets-service", json={"provider": "rot13"}).status_code == 400

    def test_put_unavailable_backend_rejected_and_names_the_library(self, client):
        from provisa.core.secrets import SecretsProvider
        from provisa.core.secrets_registry import (
            SecretsProviderSpec,
            register_secrets_provider,
        )

        class _Never(SecretsProvider):
            def resolve(self, reference: str) -> str:
                raise AssertionError("never built")

        register_secrets_provider(
            SecretsProviderSpec(
                key="absent_vault",
                label="Absent",
                description="test",
                build=lambda cfg: _Never(),
                available=lambda: False,
                requires="absent-sdk",
            )
        )
        r = client.put("/admin/secrets-service", json={"provider": "absent_vault"})
        assert r.status_code == 400
        assert "absent-sdk" in r.json()["detail"]
        # Fail closed: the refused selection did not become the deployment's.
        assert client.get("/admin/secrets-service").json()["provider"] == "provisa"

    def test_put_persists_config_block_and_rebinds_the_process(self, client):
        from provisa.core.secrets_runtime import reset_secrets, secrets_backend_spec

        try:
            r = client.put(
                "/admin/secrets-service",
                json={
                    "provider": "aws",  # alias
                    "config": {"region": "us-east-1", "not_a_field": "dropped"},
                },
            )
            assert r.status_code == 200
            sec = read_config()["secrets"]
            # The canonical key is persisted, so an alias resolves the same way at boot.
            assert sec["provider"] == "aws_secrets_manager"
            assert sec["aws_secrets_manager"] == {"region": "us-east-1"}
            # Applied to THIS process, not deferred to a restart.
            assert secrets_backend_spec().key == "aws_secrets_manager"
            assert client.get("/admin/secrets-service").json()["provider"] == "aws_secrets_manager"
        finally:
            reset_secrets()

    def test_get_requires_platform_settings(self, client, monkeypatch):
        """The SERVICE is the deployment's. An org's secret NAMES are a different endpoint."""
        import provisa.api.admin.settings_router as sr

        def _deny(_request):
            from provisa.api.errors import ApiError

            raise ApiError(403, "auth.forbidden", "platform_settings required")

        monkeypatch.setattr(sr, "require_platform_settings", _deny)
        assert client.get("/admin/secrets-service").status_code == 403
        assert client.put("/admin/secrets-service", json={"provider": "provisa"}).status_code == 403


# --- Engine registry exposes the S3 exchange-spool fields -----------------------


class TestEngineSpoolFields:
    def test_trino_engine_declares_s3_spool_fields(self):
        from provisa.federation.engine import ENGINE_REGISTRY

        trino = next(e for e in ENGINE_REGISTRY if e["key"] == "trino")
        keys = {f["config_key"] for f in trino["config_fields"]}
        assert {
            "exchange_spool_s3_endpoint",
            "exchange_spool_bucket",
            "exchange_spool_s3_region",
            "exchange_spool_s3_access_key",
            "exchange_spool_s3_secret_key",
        } <= keys


def test_config_files_are_valid_yaml_after_writes(client, cfg_env, org_overrides):
    """Every write path leaves parseable YAML.

    The AI-models write is included deliberately: since REQ-1349 it lands in the org's storage, so
    what this asserts of it is that it leaves the deployment file alone.
    """
    client.put("/admin/security", json={"mode": "high"})
    client.put("/admin/ai-models", json={"nl": {"rate_limit": 30}})
    client.put("/admin/cache-storage", json={"materialized_views": {"default_ttl": 120}})
    parsed = yaml.safe_load(cfg_env.read_text())
    assert "nl" not in parsed
    assert org_overrides["nl"] == {"rate_limit": 30}


def teardown_module(_mod):
    # Drop test-registered providers so global registry state doesn't leak to other modules.
    import provisa.encryption.registry as reg

    for k in ("acme_hsm", "unavailable_kms"):
        reg._REGISTRY.pop(k, None)


# --- Live vendor model listing for the model picker (REQ-1399) ------------------


class TestVendorModelListing:
    """The Model field's options come from the vendor's own list-models API, so a model the
    vendor started serving after this build is selectable without a release."""

    def test_lists_the_models_the_vendor_serves(self, client, org_overrides, monkeypatch):
        import provisa.core.org_secrets as org_secrets_mod
        import provisa.llm.vendor_models as vendor_models_mod

        async def _keys(_db):
            return {"anthropic": "sk-ant-org"}

        seen: dict = {}

        async def _fetch(vendor, api_key, **_kw):
            seen["vendor"], seen["api_key"] = vendor, api_key
            return ["claude-haiku-4-5-20251001", "claude-opus-4-6"]

        monkeypatch.setattr(org_secrets_mod, "read_org_api_keys", _keys)
        monkeypatch.setattr(vendor_models_mod, "fetch_vendor_models", _fetch)

        r = client.get("/admin/ai-models/vendors/anthropic/models")
        assert r.status_code == 200
        assert r.json() == {
            "vendor": "anthropic",
            "models": ["claude-haiku-4-5-20251001", "claude-opus-4-6"],
        }
        # The ORG's key, not the deployment's — the picker lists what the org's queries reach.
        assert seen == {"vendor": "anthropic", "api_key": "sk-ant-org"}

    def test_vendor_without_a_listing_api_is_rejected(self, client, org_overrides):
        r = client.get("/admin/ai-models/vendors/ollama/models")
        assert r.status_code == 400
        # This bare app registers no ApiError handler (provisa.api.app does), so the machine
        # code is not in the body here — the message is.
        assert "publishes no list-models API" in r.json()["detail"]

    def test_vendor_with_no_key_anywhere_is_rejected(self, client, org_overrides, monkeypatch):
        monkeypatch.delenv("COHERE_API_KEY", raising=False)
        r = client.get("/admin/ai-models/vendors/cohere/models")
        assert r.status_code == 400
        assert "set an API key for 'cohere'" in r.json()["detail"]

    def test_a_vendor_rejecting_the_key_surfaces_as_502(self, client, org_overrides, monkeypatch):
        import httpx

        import provisa.core.org_secrets as org_secrets_mod
        import provisa.llm.vendor_models as vendor_models_mod

        async def _keys(_db):
            return {"openai": "sk-bad"}

        async def _fetch(_vendor, _api_key, **_kw):
            raise httpx.HTTPStatusError(
                "401",
                request=httpx.Request("GET", "https://api.openai.com/v1/models"),
                response=httpx.Response(401),
            )

        monkeypatch.setattr(org_secrets_mod, "read_org_api_keys", _keys)
        monkeypatch.setattr(vendor_models_mod, "fetch_vendor_models", _fetch)

        r = client.get("/admin/ai-models/vendors/openai/models")
        assert r.status_code == 502
        assert "rejected the model listing: HTTP 401" in r.json()["detail"]

    @pytest.mark.parametrize(
        "payload,expected",
        [
            ({"data": [{"id": "b"}, {"id": "a"}]}, ["a", "b"]),  # OpenAI / Anthropic
            ({"models": [{"name": "cmd-r"}]}, ["cmd-r"]),  # Cohere
            ([{"id": "z"}, "y"], ["y", "z"]),  # Together's bare list
        ],
    )
    def test_parses_every_list_models_response_shape(self, payload, expected):
        from provisa.llm.vendor_models import parse_model_ids

        assert parse_model_ids(payload) == expected

    def test_an_unrecognized_shape_raises_rather_than_listing_nothing(self):
        from provisa.llm.vendor_models import parse_model_ids

        # An empty catalog would read as "this vendor serves no models"; the shape is the defect.
        with pytest.raises(ValueError):
            parse_model_ids({"result": []})
