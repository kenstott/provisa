# Copyright (c) 2026 Kenneth Stott
# Canary: 78582e05-421b-46dc-8bf9-ea167147afa4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registering a table re-provisions its source on the engine under the org's vault (REQ-1695,
REQ-1730): the source's password is a ``${secret:...}`` reference, resolved only while the vault
is bound — outside it the provisioning failed ("no organization is bound to this context")."""

# Requirements: REQ-1695, REQ-1730

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from provisa.core import secrets_store


@pytest.fixture
def vault(monkeypatch):
    """The org vault ``bound_to_request_org`` binds: one secret, the source's password."""

    @asynccontextmanager
    async def _bound():
        token = secrets_store._bound.set(
            secrets_store._Binding("acme", {"source_src_password": "s3cret"}, None, {})
        )
        try:
            yield
        finally:
            secrets_store._bound.reset(token)

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _bound)


async def test_the_source_is_provisioned_with_its_password_resolved_in_the_org_vault(
    vault, monkeypatch
):
    from provisa.api.admin.schema_mutation_ops import reprovision_source_after_registration

    source = SimpleNamespace(id="src", password="${secret:source_src_password}")
    registered: list[tuple] = []

    async def _sources(_state):
        return [source]

    async def _synthesize(_pool, _model):
        return None

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr(
        "provisa.api.admin.schema_common._synthesize_mapping_dsl_tables", _synthesize
    )
    state = SimpleNamespace(
        federation_engine=SimpleNamespace(
            register_source=lambda model, password, *, catalog_name: registered.append(
                (model.id, password, catalog_name)
            )
        ),
        source_catalogs={"src": "src_catalog"},
    )
    await reprovision_source_after_registration(object(), state, "src")
    assert registered == [("src", "s3cret", "src_catalog")]


def test_outside_the_vault_the_reference_does_not_resolve():
    from provisa.core.secrets import resolve_secrets

    with pytest.raises(KeyError, match="no organization is bound to this context"):
        resolve_secrets("${secret:source_src_password}")
