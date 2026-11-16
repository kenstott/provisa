# Copyright (c) 2026 Kenneth Stott
# Canary: 0f6f6c0e-6a0b-4f0e-9a63-6e3a1f0f7c31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On a server of many organizations the process environment is the operator's (REQ-1557): a
``${env:NAME}`` reference in text an organization member writes is refused, and NAME is never
read. A single-organization install keeps ``${env:}`` as REQ-557 states it."""

# Requirements: REQ-557, REQ-1557

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.core.request_context import reset_current_org, set_current_org
from provisa.core.secrets import resolve_secrets

_VARIABLE = "PROVISA_TEST_SERVER_ONLY_VARIABLE"
_VALUE = "the-servers-own-value-7f3a"


@pytest.fixture
def server(monkeypatch):
    """A server whose environment holds one variable; say whether it holds many organizations."""
    from provisa.core import secrets_providers
    from provisa.security import sandbox_egress

    monkeypatch.setenv(_VARIABLE, _VALUE)

    def _say(many: bool) -> None:
        monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: many)
        monkeypatch.setattr(sandbox_egress, "_deployment_org", lambda: "default")

    return _say


@pytest.fixture
def as_member():
    """Bind an ordinary organization, as a member's request does."""
    token = set_current_org("acme")
    yield
    reset_current_org(token)


class _Pools:
    def __init__(self) -> None:
        self.added: list[dict] = []

    async def add(self, **fields) -> None:
        self.added.append(fields)


def _source_input(**written) -> SimpleNamespace:
    fields = {
        "id": "theirs",
        "type": "postgresql",
        "host": "db.example.com",
        "port": 5432,
        "database": "d",
        "username": "u",
        "password": "",
        "path": None,
        "federation_hints": {},
    }
    return SimpleNamespace(**{**fields, **written})


def test_an_unmarked_resolution_refuses_env_on_a_server_of_many_organizations(server, as_member):
    server(True)
    with pytest.raises(ValueError) as refused:
        resolve_secrets(f"${{env:{_VARIABLE}}}")
    assert _VALUE not in str(refused.value)


@pytest.mark.parametrize(
    "written",
    [
        {"host": f"${{env:{_VARIABLE}}}"},
        {"password": f"${{env:{_VARIABLE}}}"},
        {"federation_hints": {"account": f"${{env:{_VARIABLE}}}"}},
    ],
    ids=["host", "password", "hint"],
)
def test_a_members_new_source_is_refused_its_env_reference(server, as_member, written):
    from provisa.api.admin.schema_common import _add_source_pool

    server(True)
    pools = _Pools()
    state = SimpleNamespace(source_pools=pools)
    with pytest.raises(ValueError):
        asyncio.run(_add_source_pool(state, _source_input(**written)))
    assert _VALUE not in repr(pools.added)


def test_a_single_organization_install_resolves_env_as_before(server, as_member):
    server(False)
    assert resolve_secrets(f"${{env:{_VARIABLE}}}") == _VALUE


def test_the_refusal_names_the_reference_and_never_reads_the_variable(server, as_member):
    from provisa.core.secrets import EnvReferenceRefused

    server(True)
    with pytest.raises(EnvReferenceRefused) as refused:
        resolve_secrets(f"https://example.com/${{env:{_VARIABLE}}}")
    assert refused.value.code == "secrets.env_reference_refused"
    assert refused.value.params == {"name": _VARIABLE}
    assert "${env:" in str(refused.value)
    assert _VALUE not in str(refused.value)


@pytest.mark.unbound
def test_no_organization_bound_is_refused_on_a_server_of_many_organizations(server):
    from provisa.core.secrets import EnvReferenceRefused

    server(True)
    with pytest.raises(EnvReferenceRefused):
        resolve_secrets(f"${{env:{_VARIABLE}}}")


def test_text_the_operator_wrote_resolves_env_on_a_server_of_many_organizations(server, as_member):
    server(True)
    assert resolve_secrets(f"${{env:{_VARIABLE}}}", operator_written=True) == _VALUE


def test_the_deployments_own_organization_resolves_env(server):
    server(True)
    token = set_current_org("default")
    try:
        assert resolve_secrets(f"${{env:{_VARIABLE}}}") == _VALUE
    finally:
        reset_current_org(token)


def test_a_bare_variable_name_is_refused_as_its_reference_is(server, as_member):
    from provisa.core.secrets import EnvReferenceRefused, resolve_api_key_field

    server(True)
    with pytest.raises(EnvReferenceRefused):
        asyncio.run(resolve_api_key_field(_VARIABLE))
    assert asyncio.run(resolve_api_key_field(_VARIABLE, operator_written=True)) == _VALUE


def test_a_resolved_env_value_is_redacted_from_text_that_echoes_it(server, as_member):
    from provisa.security import redaction

    server(True)
    redaction.reset()
    try:
        resolve_secrets(f"${{env:{_VARIABLE}}}", operator_written=True)
        echoed = redaction.redact(f"could not connect with password {_VALUE!r}")
        assert _VALUE not in echoed
        assert f"[REDACTED:auto:env:{_VARIABLE}]" in echoed
    finally:
        redaction.reset()


def test_a_written_default_is_the_references_own_text_and_is_not_redacted(server, as_member):
    from provisa.security import redaction

    server(False)
    redaction.reset()
    try:
        assert resolve_secrets("${env:PROVISA_TEST_UNSET_VARIABLE:-localhost}") == "localhost"
        assert redaction.redact("connecting to localhost") == "connecting to localhost"
    finally:
        redaction.reset()
