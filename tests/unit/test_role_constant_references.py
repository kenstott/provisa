# Copyright (c) 2026 Kenneth Stott
# Canary: 78e10702-bed8-48ef-9809-9f7a0893d513
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role constant may be a ${env:...} or ${secret:...} reference, resolved at use (REQ-2086)."""

# Requirements: REQ-2086

from __future__ import annotations

import pytest

from provisa.core import secrets_runtime
from provisa.core.request_context import session_vars_for
from provisa.core.secrets import resolve_role_constant


class _Vault:
    def __init__(self, held: dict[str, str]) -> None:
        self._held = held

    def resolve(self, reference: str) -> str:
        if reference not in self._held:
            raise KeyError(f"no secret named {reference!r}")
        return self._held[reference]


@pytest.fixture
def vault(monkeypatch):
    monkeypatch.setattr(secrets_runtime, "secrets_backend", lambda: _Vault({"tenant": "acme-7"}))


def test_a_plain_constant_is_read_as_it_is():
    assert resolve_role_constant("region", "eu") == "eu"


def test_an_env_reference_is_resolved_at_use(monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_REGION", "eu-west")
    assert resolve_role_constant("region", "${env:PROVISA_TEST_REGION}") == "eu-west"


def test_a_secret_reference_is_resolved_at_use(vault):
    assert resolve_role_constant("tenant", "${secret:tenant}") == "acme-7"


@pytest.mark.parametrize("reference", ["${env:PROVISA_TEST_NOT_SET}", "${secret:nobody}"])
def test_a_reference_that_resolves_to_nothing_is_refused_by_name(monkeypatch, vault, reference):
    monkeypatch.delenv("PROVISA_TEST_NOT_SET", raising=False)
    with pytest.raises(ValueError, match=r"role constant 'tenant' \(\$\{"):
        resolve_role_constant("tenant", reference)


def test_a_role_is_read_with_its_constants_resolved_and_keeps_the_reference(monkeypatch, vault):
    monkeypatch.setenv("PROVISA_TEST_REGION", "eu-west")
    role = {"session_vars": {"region": "${env:PROVISA_TEST_REGION}", "tenant": "${secret:tenant}"}}
    resolved = session_vars_for(role)
    assert resolved["region"] == "eu-west" and resolved["tenant"] == "acme-7"
    # What the model holds is the reference: nothing resolved is stored back.
    assert role["session_vars"] == {
        "region": "${env:PROVISA_TEST_REGION}",
        "tenant": "${secret:tenant}",
    }


def test_a_meta_role_inlines_each_members_constant_resolved(monkeypatch, vault):
    from provisa.security.meta_role import _inlined

    assert _inlined(
        "tenant = current_setting('provisa.tenant')", {"tenant": "${secret:tenant}"}
    ) == ("tenant = 'acme-7'")
