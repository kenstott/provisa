# Copyright (c) 2026 Kenneth Stott
# Canary: 8066ccbb-a4cd-42f6-9cb0-13ac1153014d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MetadataExport port, registry and config tests (REQ-1068)."""

from __future__ import annotations

import logging

import pytest
from pydantic import ValidationError

from provisa.api.metadata_export import registry as registry_module
from provisa.api.metadata_export.model import AssetKind, AssetRef, MetadataSnapshot
from provisa.api.metadata_export.provider import (
    AssetError,
    MetadataExport,
    MetadataExportNotConfiguredError,
    PublishResult,
)
from provisa.api.metadata_export.registry import (
    metadata_export,
    register_provider,
    registered_providers,
)
from provisa.core.models import MetadataExportConfig


@pytest.fixture()
def isolated_registry(monkeypatch):
    """A registry with only the providers a test registers itself."""
    monkeypatch.setattr(registry_module, "_PROVIDERS", {})
    return registry_module


def _stub(name: str) -> type[MetadataExport]:
    class _Stub(MetadataExport):
        provider_name = name

        async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
            return PublishResult(provider_name=name, published=snapshot.asset_count())

        async def health(self) -> None:
            return None

    _Stub.__name__ = f"Stub_{name}"
    return _Stub


def _enabled(**overrides) -> MetadataExportConfig:
    base = {"enabled": True, "provider": "stub", "endpoint": "https://catalog.example.com"}
    base.update(overrides)
    return MetadataExportConfig(**base)


# ---------------------------------------------------------------------------
# Registry (REQ-1068)
# ---------------------------------------------------------------------------


def test_factory_resolves_each_registered_name(isolated_registry):
    for name in ("openlineage", "openmetadata", "atlas"):
        isolated_registry.register_provider(_stub(name))
    assert isolated_registry.registered_providers() == ["atlas", "openlineage", "openmetadata"]
    for name in ("openlineage", "openmetadata", "atlas"):
        provider = isolated_registry.metadata_export(_enabled(provider=name))
        assert provider.provider_name == name


def test_unknown_provider_is_refused_at_construction(isolated_registry):
    isolated_registry.register_provider(_stub("openlineage"))
    with pytest.raises(MetadataExportNotConfiguredError) as exc:
        isolated_registry.metadata_export(_enabled(provider="nope"))
    assert "nope" in str(exc.value)
    assert "openlineage" in str(exc.value)


def test_disabled_config_refuses_rather_than_no_opping(isolated_registry):
    isolated_registry.register_provider(_stub("openlineage"))
    with pytest.raises(MetadataExportNotConfiguredError) as exc:
        isolated_registry.metadata_export(
            MetadataExportConfig(enabled=False, provider="openlineage")
        )
    assert "metadata_export.enabled" in str(exc.value)


def test_provider_without_a_name_fails_registration(isolated_registry):
    class Nameless(MetadataExport):
        async def publish(self, snapshot):  # pragma: no cover - never constructed
            ...

        async def health(self):  # pragma: no cover - never constructed
            ...

    with pytest.raises(ValueError, match="provider_name"):
        isolated_registry.register_provider(Nameless)


def test_duplicate_provider_name_fails_registration(isolated_registry):
    isolated_registry.register_provider(_stub("atlas"))
    with pytest.raises(ValueError, match="already registered"):
        isolated_registry.register_provider(_stub("atlas"))


def test_register_provider_is_idempotent_for_the_same_class(isolated_registry):
    cls = _stub("atlas")
    isolated_registry.register_provider(cls)
    isolated_registry.register_provider(cls)
    assert isolated_registry.registered_providers() == ["atlas"]


def test_module_level_factory_is_the_same_function():
    # The package re-exports the factory; tests that patch the module must patch the one
    # callers reach.
    assert metadata_export is registry_module.metadata_export
    assert register_provider is registry_module.register_provider
    assert registered_providers is registry_module.registered_providers


# ---------------------------------------------------------------------------
# Config (REQ-1068)
# ---------------------------------------------------------------------------


def test_enabled_without_provider_or_endpoint_is_rejected():
    with pytest.raises(ValidationError, match="provider, endpoint"):
        MetadataExportConfig(enabled=True)
    with pytest.raises(ValidationError, match="endpoint"):
        MetadataExportConfig(enabled=True, provider="atlas")
    with pytest.raises(ValidationError, match="provider"):
        MetadataExportConfig(enabled=True, endpoint="https://catalog.example.com")


def test_disabled_config_needs_nothing_else():
    assert MetadataExportConfig().enabled is False


def test_credentials_are_redacted_in_repr_and_logs(caplog):
    config = _enabled(
        api_key="sk-live-do-not-print",
        token="tok-do-not-print",
        entra_client_secret="entra-do-not-print",
    )
    rendered = f"{config!r} {config!s} {config.model_dump()}"
    for secret in ("sk-live-do-not-print", "tok-do-not-print", "entra-do-not-print"):
        assert secret not in rendered
    assert config.api_key.get_secret_value() == "sk-live-do-not-print"

    with caplog.at_level(logging.INFO):
        logging.getLogger(__name__).info("export config: %s", config)
    assert "sk-live-do-not-print" not in caplog.text


def test_config_is_mounted_on_provisa_config():
    from provisa.core.models import ProvisaConfig

    assert "metadata_export" in ProvisaConfig.model_fields


# ---------------------------------------------------------------------------
# PublishResult (REQ-1068)
# ---------------------------------------------------------------------------


def test_publish_result_surfaces_partial_failure():
    ref = AssetRef(kind=AssetKind.COLUMN, parts=("pg", "orders", "total"))
    result = PublishResult(
        provider_name="atlas",
        published={"table": 10, "column": 160},
        errors=[AssetError(asset=ref, message="type not registered")],
    )
    assert result.ok is False
    assert result.total_published() == 170
    assert result.errors[0].asset.fqn() == "pg.orders.total"


def test_publish_result_with_no_errors_is_ok():
    assert PublishResult(provider_name="atlas", published={"table": 1}).ok is True
