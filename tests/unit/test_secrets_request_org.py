# Copyright (c) 2026 Kenneth Stott
# Canary: 0f1a7c34-9d62-4b8e-a5f1-6c20e9b3d774
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1580: which org's vault a stored ``${secret:NAME}`` resolves against.

``core`` cannot see the API layer's ``current_org``, so the answer arrives through a resolver the
API layer installs at import. What is under test is that it is installed, and that it names the
org the request is running as rather than the process's boot org.
"""

# Requirements: REQ-1580

from __future__ import annotations

import pytest

from provisa.api import app as app_module
from provisa.api.org_runtime import current_org
from provisa.core import secrets_store


@pytest.fixture
def admin_db(monkeypatch):
    """A stand-in control plane, put back afterwards -- ``state`` is process-wide."""
    sentinel = object()
    monkeypatch.setattr(app_module.state, "admin_db", sentinel)
    return sentinel


def test_importing_the_api_installs_a_resolver():
    assert secrets_store._request_org is not None


def test_the_vault_is_the_bound_orgs(admin_db):
    token = current_org.set("acme")
    try:
        resolved_db, org_id = secrets_store._request_org()
    finally:
        current_org.reset(token)
    assert admin_db is resolved_db
    assert org_id == "acme"


def test_an_unbound_request_reads_the_boot_org(admin_db):
    # Startup, a background refresh, a single-org install: no ContextVar is set and the one org
    # the process was built for is the only org there is.
    assert secrets_store._request_org()[1] == app_module.state.org_id


async def test_resolving_with_no_resolver_installed_says_so(monkeypatch):
    monkeypatch.setattr(secrets_store, "_request_org", None)
    with pytest.raises(RuntimeError, match="No request-org resolver"):
        async with secrets_store.bound_to_request_org():
            pass
