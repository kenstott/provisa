# Copyright (c) 2026 Kenneth Stott
# Canary: 9f55e27d-e5e9-4a41-b9d4-3478dd873a69
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which service ``${secret:NAME}`` asks, and what it refuses (REQ-1557).

The store's round trip is covered against a real registry elsewhere. What is under test here is
the selection and the resolution around it: that a reference reaches the CONFIGURED backend rather
than a hardcoded one, that every way of being unconfigured raises instead of yielding an empty
string, and that a central backend's own credential cannot be written against the store it opens.
"""

# Requirements: REQ-125, REQ-557, REQ-1557

from __future__ import annotations

import pytest

from provisa.core import secrets_store
from provisa.core.secrets import SecretsProvider, resolve_secrets
from provisa.core.secrets_registry import (
    SecretsProviderSpec,
    register_secrets_provider,
    secrets_provider_registry,
)
from provisa.core.secrets_runtime import (
    configure_secrets,
    reset_secrets,
    secrets_backend,
    secrets_backend_spec,
)


class _Fixed(SecretsProvider):
    def __init__(self, values):
        self.values = values

    def resolve(self, reference: str) -> str:
        return self.values[reference]


@pytest.fixture(autouse=True)
def _clean():
    reset_secrets()
    yield
    reset_secrets()


@pytest.fixture
def registered():
    """A backend registered the way an extension would register one."""
    built = []
    register_secrets_provider(
        SecretsProviderSpec(
            key="fake_vault",
            label="Fake",
            description="test double",
            build=lambda cfg: built.append(cfg) or _Fixed({"TOKEN": "s3cret"}),
            aliases=("fake",),
        )
    )
    return built


class TestWhichBackendAReferenceReaches:
    def test_the_configured_one_answers(self, registered):
        configure_secrets("fake_vault")
        assert resolve_secrets("Bearer ${secret:TOKEN}") == "Bearer s3cret"

    def test_an_alias_selects_the_same_backend(self, registered):
        configure_secrets("fake")
        assert secrets_backend_spec().key == "fake_vault"

    def test_it_is_built_once_and_only_when_asked(self, registered):
        configure_secrets("fake_vault", config={"url": "https://vault.test"})
        assert registered == []  # selecting is not constructing
        secrets_backend()
        secrets_backend()
        assert registered == [{"url": "https://vault.test"}]

    def test_unconfigured_means_provisa_s_own_store(self):
        """REQ-1557: the default is the built-in store, not an absent service."""
        assert secrets_backend_spec().key == "provisa"
        assert isinstance(secrets_backend(), secrets_store.StoredSecretsProvider)

    def test_only_the_built_in_store_is_writable(self):
        """A central service owns its own lifecycle; Provisa does not create names in it."""
        writable = [s.key for s in secrets_provider_registry() if s.writable]
        assert writable == ["provisa"]


class TestWhatRaisesRatherThanResolving:
    def test_an_unknown_backend_raises(self):
        configure_secrets("no_such_service")
        with pytest.raises(ValueError, match="Unknown secrets provider"):
            secrets_backend()

    def test_a_backend_whose_sdk_is_absent_raises(self):
        register_secrets_provider(
            SecretsProviderSpec(
                key="absent",
                label="Absent",
                description="sdk not installed",
                build=lambda cfg: _Fixed({}),
                available=lambda: False,
            )
        )
        configure_secrets("absent")
        with pytest.raises(ValueError, match="not available"):
            secrets_backend()

    def test_an_unknown_provider_name_raises(self):
        with pytest.raises(ValueError, match="Unknown secrets provider"):
            resolve_secrets("${nowhere:TOKEN}")

    def test_a_name_the_backend_does_not_hold_raises(self, registered):
        configure_secrets("fake_vault")
        with pytest.raises(KeyError):
            resolve_secrets("${secret:MISSING}")


class TestWhereABackendsOwnCredentialMayComeFrom:
    """REQ-1557: a store whose credential lives inside the store cannot be opened."""

    def test_config_values_resolve_out_of_the_environment(self, monkeypatch):
        from provisa.core.secrets_registry import _cfg

        monkeypatch.setenv("VAULT_TOKEN_TEST", "hv.abc")
        assert _cfg({"token": "${env:VAULT_TOKEN_TEST}"}, "token") == "hv.abc"

    def test_a_config_value_may_not_name_the_secrets_service(self):
        from provisa.core.secrets_registry import _cfg

        with pytest.raises(ValueError, match="not permitted here"):
            _cfg({"token": "${secret:VAULT_TOKEN}"}, "token")

    def test_a_missing_required_value_raises(self):
        from provisa.core.secrets_registry import _cfg

        with pytest.raises(ValueError, match="missing 'url'"):
            _cfg({}, "url")


class TestNamesTheStoreAccepts:
    def test_a_name_is_what_the_reference_grammar_can_carry_back(self):
        for good in ("GIT_TOKEN", "_x", "a1"):
            assert secrets_store.validate_name(good) == good
        for bad in ("", "1abc", "a-b", "a b", "a}b", "a:b"):
            with pytest.raises(ValueError):
                secrets_store.validate_name(bad)


class TestResolvingOutsideAnOrg:
    def test_unbound_resolution_raises_rather_than_reaching_into_an_org(self):
        """REQ-1557: every read is scoped to one org, so no org means no read."""
        with pytest.raises(KeyError, match="no organization is bound"):
            secrets_store.StoredSecretsProvider().resolve("GIT_TOKEN")

    def test_a_bound_org_resolves_only_its_own_names(self):
        token = secrets_store._bound.set(("acme", {"GIT_TOKEN": "ghp_x"}))
        try:
            assert secrets_store.StoredSecretsProvider().resolve("GIT_TOKEN") == "ghp_x"
            with pytest.raises(KeyError, match="no secret named"):
                secrets_store.StoredSecretsProvider().resolve("OTHER")
        finally:
            secrets_store._bound.reset(token)
