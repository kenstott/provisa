# Copyright (c) 2026 Kenneth Stott
# Canary: b2046c69-a897-4c77-a073-104dd4285208
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A central secrets service is read inside the organisation the operation is bound to
(REQ-1557: "a name is looked up only in the namespace of the org the request is bound to").

The service is a stand-in holding two organisations' secrets; each provider is given a client
that reads it the way its SDK does.
"""

# Requirements: REQ-1557

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.core import secrets_providers as sp
from provisa.core import secrets_store

A_SECRET, B_SECRET = "password-of-a", "password-of-b"


@pytest.fixture
def bound_to_a():
    token = secrets_store._bound.set(secrets_store._Binding("a", {}, None, {}))
    yield
    secrets_store._bound.reset(token)


def _vault(held: dict) -> sp.VaultSecretsProvider:
    def read_secret_version(*, path, mount_point, raise_on_deleted_version):
        return {"data": {"data": held[path]}}

    provider = object.__new__(sp.VaultSecretsProvider)
    provider._mount = "secret"
    provider._client = SimpleNamespace(
        secrets=SimpleNamespace(
            kv=SimpleNamespace(v2=SimpleNamespace(read_secret_version=read_secret_version))
        )
    )
    return provider


def _aws(held: dict) -> sp.AwsSecretsManagerProvider:
    provider = object.__new__(sp.AwsSecretsManagerProvider)
    provider._client = SimpleNamespace(
        get_secret_value=lambda SecretId: {"SecretString": held[SecretId]}
    )
    return provider


def _gcp(held: dict) -> sp.GcpSecretManagerProvider:
    def access_secret_version(*, name):
        secret = name.split("/secrets/")[1].split("/versions/")[0]
        return SimpleNamespace(payload=SimpleNamespace(data=held[secret].encode()))

    provider = object.__new__(sp.GcpSecretManagerProvider)
    provider._project = "p"
    provider._client = SimpleNamespace(access_secret_version=access_secret_version)
    return provider


def _azure(held: dict) -> sp.AzureKeyVaultSecretsProvider:
    provider = object.__new__(sp.AzureKeyVaultSecretsProvider)
    provider._client = SimpleNamespace(
        get_secret=lambda name, version: SimpleNamespace(value=held[name])
    )
    return provider


# kind, provider over a service holding both organisations' secret "db", and what organisation
# a's reference to its own is.
def _services():
    return [
        (
            "vault",
            _vault({"orgs/a/db": {"value": A_SECRET}, "orgs/b/db": {"value": B_SECRET}}),
            "orgs/b/db",
        ),
        ("aws", _aws({"orgs/a/db": A_SECRET, "orgs/b/db": B_SECRET}), "orgs/b/db"),
        ("gcp", _gcp({"org-a--db": A_SECRET, "org-b--db": B_SECRET}), "org-b--db"),
        ("azure", _azure({"o1-a-db": A_SECRET, "o1-b-db": B_SECRET}), "o1-b-db"),
    ]


class _Deployment:
    multitenancy = True


@pytest.fixture(autouse=True)
def deployment(monkeypatch):
    """The running deployment's state, as the provider asks it each time: many organisations."""
    state = _Deployment()
    monkeypatch.setattr(sp, "_many_organisations", lambda: state.multitenancy)
    return state


@pytest.mark.parametrize(("kind", "provider", "_theirs"), _services())
def test_an_organisations_reference_reads_its_own_secret(bound_to_a, kind, provider, _theirs):
    assert provider.resolve("db") == A_SECRET


@pytest.mark.parametrize(("kind", "provider", "theirs"), _services())
def test_a_reference_naming_another_organisations_secret_does_not_resolve(
    bound_to_a, kind, provider, theirs
):
    """Organisation a states the full name organisation b's secret has in the service."""
    with pytest.raises(KeyError):
        provider.resolve(theirs)


@pytest.mark.parametrize("name", ["../b/db", "/orgs/b/db", "x/../../b/db", "", "a\\..\\b"])
@pytest.mark.parametrize(("kind", "provider", "_theirs"), _services())
def test_a_name_that_would_leave_the_organisation_is_refused(
    bound_to_a, kind, provider, _theirs, name
):
    with pytest.raises(sp.SecretNameRefused, match="cannot leave it") as refused:
        provider.resolve(name)
    assert B_SECRET not in str(refused.value)


@pytest.mark.parametrize(("kind", "provider", "_theirs"), _services())
def test_with_no_organisation_bound_nothing_is_read(kind, provider, _theirs):
    with pytest.raises(KeyError, match="no organization is bound"):
        provider.resolve("db")


def test_a_key_within_a_secret_is_still_named_after_the_hash(bound_to_a):
    vault = _vault({"orgs/a/db": {"password": A_SECRET}})
    assert vault.resolve("db#password") == A_SECRET
    aws = _aws({"orgs/a/db": json.dumps({"password": A_SECRET})})
    assert aws.resolve("db#password") == A_SECRET


def test_a_deployment_of_one_organisation_reads_a_reference_as_written(deployment):
    """Without multi-tenancy the service is that one organisation's: its references name
    secrets where they already are, and nothing has to be moved."""
    deployment.multitenancy = False
    assert _vault({"teams/db": {"value": A_SECRET}}).resolve("teams/db") == A_SECRET
    assert _aws({"prod/db": A_SECRET}).resolve("prod/db") == A_SECRET
    assert _gcp({"db": A_SECRET}).resolve("db") == A_SECRET
    assert _azure({"db": A_SECRET}).resolve("db") == A_SECRET


def test_the_deployments_kind_is_asked_each_time_not_kept(bound_to_a, deployment):
    """One answer, the running deployment's: when it changes, the next read follows it."""
    vault = _vault({"orgs/a/db": {"value": A_SECRET}, "db": {"value": "unscoped"}})
    assert vault.resolve("db") == A_SECRET
    deployment.multitenancy = False
    assert vault.resolve("db") == "unscoped"
    deployment.multitenancy = True
    assert vault.resolve("db") == A_SECRET


def test_with_nothing_said_about_the_deployment_a_central_service_is_not_read(
    bound_to_a, monkeypatch
):
    monkeypatch.setattr(sp, "_many_organisations", None)
    with pytest.raises(RuntimeError, match="holds many organizations"):
        _vault({"db": {"value": A_SECRET}}).resolve("db")


@pytest.mark.parametrize(
    ("kind", "limit", "prefix"), [("gcp", 255, "org-a--"), ("azure", 127, "o1-a-")]
)
def test_a_name_too_long_once_prefixed_is_refused_with_the_limit(bound_to_a, kind, limit, prefix):
    fits = "n" * (limit - len(prefix))
    held = {prefix + fits: A_SECRET}
    provider = _gcp(held) if kind == "gcp" else _azure(held)
    assert provider.resolve(fits) == A_SECRET
    with pytest.raises(sp.SecretNameRefused, match=f"{limit + 1} characters.*at most {limit}"):
        provider.resolve(fits + "n")


# ------------------------------------------------------------------ at the save of a reference


@pytest.fixture
def wired_to(monkeypatch):
    """Select a central backend by its registry key, as a deployment wired to it has."""
    from provisa.core.secrets_registry import get_secrets_provider_spec

    def select(key: str) -> None:
        spec = get_secrets_provider_spec(key)
        monkeypatch.setattr("provisa.core.secrets_runtime.secrets_backend_spec", lambda: spec)

    return select


def _insert(**values):
    from provisa.core.env_secrets import guard_statement
    from provisa.core.schema_org import sources

    return guard_statement(sources.insert().values(**values))


def test_a_reference_too_long_for_its_service_is_refused_when_it_is_saved(bound_to_a, wired_to):
    wired_to("azure_key_vault")
    long_name = "n" * 127  # fits Azure alone, not with the organisation's prefix
    with pytest.raises(sp.SecretNameRefused, match="at most 127"):
        _insert(id="s", password_ref=f"${{secret:{long_name}}}")
    _insert(id="s", password_ref="${secret:db-password}")  # one that fits is stored


def test_a_reference_that_would_leave_the_organisation_is_refused_when_it_is_saved(
    bound_to_a, wired_to
):
    wired_to("hashicorp_vault")
    with pytest.raises(sp.SecretNameRefused, match="cannot leave it"):
        _insert(id="s", password_ref="${secret:../b/db#password}")
    with pytest.raises(sp.SecretNameRefused, match="cannot leave it"):
        _insert(id="s", mapping={"token": "${secret:/orgs/b/db}"})  # inside a structure too


def test_the_organisation_a_request_runs_as_is_the_one_a_save_is_checked_for(wired_to):
    from provisa.core.request_context import current_org

    wired_to("azure_key_vault")
    token = current_org.set("a_much_longer_organisation_id")
    try:
        with pytest.raises(sp.SecretNameRefused, match="at most 127"):
            _insert(id="s", password_ref="${secret:" + "n" * 100 + "}")
    finally:
        current_org.reset(token)


def test_a_save_is_not_checked_on_a_deployment_of_one_organisation_or_the_built_in_store(
    bound_to_a, wired_to, deployment
):
    _insert(id="s", password_ref="${secret:../anything}")  # the built-in store: its own rules apply
    wired_to("azure_key_vault")
    deployment.multitenancy = False
    _insert(
        id="s", password_ref="${secret:" + "n" * 127 + "}"
    )  # read as written: Azure's own limit
