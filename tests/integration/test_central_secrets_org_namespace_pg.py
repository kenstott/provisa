# Copyright (c) 2026 Kenneth Stott
# Canary: 08ce0446-9be6-49a6-acd6-3c6c91af2f37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A central secrets service is read inside the namespace of the bound organization (REQ-1557).

``${secret:NAME}`` names a secret of the organization the operation is bound to. With a central
service configured (Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) the same
rule holds as with Provisa's own store: the name is looked up under that organization's
namespace in the service, so two organizations holding ``GIT_TOKEN`` hold two unrelated secrets,
and a reference resolved with no organization bound is refused. A deployment of one
organization reads a reference as written.

The organization binding is the real one (``secrets_store.bound`` on a PostgreSQL platform
plane); each service's client is stood in for by a record of what it was asked. Lands on the
TEST instance's PostgreSQL only.
"""

# Requirements: REQ-1557

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import delete, insert

from provisa.core import secrets_providers as sp
from provisa.core import secrets_runtime, secrets_store
from provisa.core.schema_admin import orgs
from provisa.core.secrets import resolve_secrets

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest.fixture(autouse=True)
def many_organisations(monkeypatch):
    """The running deployment's answer, as the provider asks it: it holds many organisations."""
    held = SimpleNamespace(multitenancy=True)
    monkeypatch.setattr(sp, "_many_organisations", lambda: held.multitenancy)
    return held


@pytest.fixture
async def two_orgs(platform_admin_db):
    suffix = uuid.uuid4().hex[:8]
    one, two = f"one{suffix}", f"two{suffix}"
    async with platform_admin_db.acquire() as conn:
        for org_id in (one, two):
            await conn.execute_core(insert(orgs).values(id=org_id, name=org_id))
    yield SimpleNamespace(one=one, two=two, db=platform_admin_db)
    async with platform_admin_db.acquire() as conn:
        await conn.execute_core(delete(orgs).where(orgs.c.id.in_([one, two])))


def _vault(asked: list[str]) -> sp.VaultSecretsProvider:
    provider = sp.VaultSecretsProvider.__new__(sp.VaultSecretsProvider)
    provider._mount = "secret"

    def read_secret_version(*, path, mount_point, raise_on_deleted_version):
        asked.append(path)
        return {"data": {"data": {"value": f"vault:{path}", "user": f"user:{path}"}}}

    provider._client = SimpleNamespace(
        secrets=SimpleNamespace(
            kv=SimpleNamespace(v2=SimpleNamespace(read_secret_version=read_secret_version))
        )
    )
    return provider


def _aws(asked: list[str]) -> sp.AwsSecretsManagerProvider:
    provider = sp.AwsSecretsManagerProvider.__new__(sp.AwsSecretsManagerProvider)

    def get_secret_value(*, SecretId):  # noqa: N803 — the SDK's own parameter name
        asked.append(SecretId)
        return {"SecretString": f"aws:{SecretId}"}

    provider._client = SimpleNamespace(get_secret_value=get_secret_value)
    return provider


def _gcp(asked: list[str]) -> sp.GcpSecretManagerProvider:
    provider = sp.GcpSecretManagerProvider.__new__(sp.GcpSecretManagerProvider)
    provider._project = "proj"

    def access_secret_version(*, name):
        asked.append(name)
        return SimpleNamespace(payload=SimpleNamespace(data=f"gcp:{name}".encode()))

    provider._client = SimpleNamespace(access_secret_version=access_secret_version)
    return provider


def _azure(asked: list[str]) -> sp.AzureKeyVaultSecretsProvider:
    provider = sp.AzureKeyVaultSecretsProvider.__new__(sp.AzureKeyVaultSecretsProvider)

    def get_secret(name, version):
        asked.append(name)
        return SimpleNamespace(value=f"azure:{name}")

    provider._client = SimpleNamespace(get_secret=get_secret)
    return provider


@pytest.fixture(params=["vault", "aws", "gcp", "azure"])
def central(request, monkeypatch):
    """One central service selected as the deployment's secrets backend, recording its reads."""
    asked: list[str] = []
    provider = {"vault": _vault, "aws": _aws, "gcp": _gcp, "azure": _azure}[request.param](asked)
    monkeypatch.setattr(secrets_runtime, "_backend", provider)
    monkeypatch.setattr(secrets_runtime, "_selected", (request.param, {}))
    return SimpleNamespace(kind=request.param, asked=asked)


async def test_each_organization_reads_the_name_in_its_own_namespace(two_orgs, central):
    async with secrets_store.bound(two_orgs.db, two_orgs.one):
        first = resolve_secrets("${secret:GIT_TOKEN}")
    async with secrets_store.bound(two_orgs.db, two_orgs.two):
        second = resolve_secrets("${secret:GIT_TOKEN}")

    assert first != second  # two organizations, two unrelated secrets
    read_for_one, read_for_two = central.asked
    assert two_orgs.one in read_for_one and two_orgs.two not in read_for_one
    assert two_orgs.two in read_for_two and two_orgs.one not in read_for_two


async def test_a_reference_cannot_name_another_organizations_namespace(two_orgs, central):
    """Whatever the reference says, what is read is under the bound organization's namespace."""
    for reference in (
        f"{two_orgs.two}/GIT_TOKEN",
        f"../{two_orgs.two}/GIT_TOKEN",
        f"/{two_orgs.two}/GIT_TOKEN",
        f"orgs/{two_orgs.two}/GIT_TOKEN",
    ):
        central.asked.clear()
        async with secrets_store.bound(two_orgs.db, two_orgs.one):
            try:
                resolve_secrets("${secret:" + reference + "}")
            except KeyError:
                assert central.asked == [], reference  # refused before the service was asked
                continue
        (read,) = central.asked
        # What was read is a name under the bound organization's own namespace, whatever the
        # reference spelled after it.
        assert sp.org_namespace(central.kind, two_orgs.one) in read, reference
        assert (
            sp.org_namespace(central.kind, two_orgs.two)
            not in read.split(sp.org_namespace(central.kind, two_orgs.one), 1)[0]
        ), reference
        assert ".." not in read, reference


async def test_with_no_organization_bound_nothing_is_read(two_orgs, central):
    with pytest.raises(KeyError, match="no organization is bound"):
        resolve_secrets("${secret:GIT_TOKEN}")
    assert central.asked == []


async def test_the_key_part_of_a_vault_reference_is_kept(two_orgs, monkeypatch):
    asked: list[str] = []
    monkeypatch.setattr(secrets_runtime, "_backend", _vault(asked))
    monkeypatch.setattr(secrets_runtime, "_selected", ("vault", {}))
    async with secrets_store.bound(two_orgs.db, two_orgs.one):
        assert resolve_secrets("${secret:db/main#user}").startswith("user:")
    assert asked == [f"orgs/{two_orgs.one}/db/main"]


async def test_a_deployment_of_one_organization_reads_the_reference_as_written(
    two_orgs, central, many_organisations
):
    many_organisations.multitenancy = False
    async with secrets_store.bound(two_orgs.db, two_orgs.one):
        resolve_secrets("${secret:GIT_TOKEN}")
    (read,) = central.asked
    assert two_orgs.one not in read and "GIT_TOKEN" in read
