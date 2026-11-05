# Copyright (c) 2026 Kenneth Stott
# Canary: 23682169-411f-4a2d-9e02-ed142eb08e05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155, REQ-1939: the synthetic datasets are read with environment_management, or by the
owner of the environment the request acts in holding environment_switch -- a sandbox visitor reads
its own environment's generation report. Writes stay environment_management's."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import synthetic_router as router
from provisa.core.request_context import reset_current_env, set_current_env


class _Refused(Exception):
    pass


@pytest.fixture
def plane(monkeypatch):
    """Ada owns `ada-env`; the request holds the rights it is given."""
    from provisa.api.admin import capabilities, environments_router
    from provisa.core import env_authority

    async def _owns(_admin, _org, name, user_id):
        return (name, user_id) == ("ada-env", "ada")

    def _refuse(request, right):
        if right not in request.held:
            raise _Refused(right)

    monkeypatch.setattr(env_authority, "owns_environment", _owns)
    monkeypatch.setattr(environments_router, "_caller_user_id", lambda request: request.user)
    monkeypatch.setattr(environments_router, "_admin_pool", lambda: None)
    monkeypatch.setattr(
        capabilities, "has_capability_request", lambda request, right: right in request.held
    )
    monkeypatch.setattr(router, "require_capability_request", _refuse)

    def request(user: str, env: str, *rights: str):
        token = set_current_env(env)
        return SimpleNamespace(
            user=user, held=set(rights), state=SimpleNamespace(active_org_id="acme")
        ), token

    return request


async def test_the_owner_reads_its_own_environments_datasets(plane):
    request, token = plane("ada", "ada-env", "environment_switch")
    try:
        await router._require_read(request)
    finally:
        reset_current_env(token)


async def test_an_owner_reads_no_other_environments_datasets(plane):
    request, token = plane("ada", "bob-env", "environment_switch")
    try:
        with pytest.raises(_Refused):
            await router._require_read(request)
    finally:
        reset_current_env(token)


async def test_owning_without_the_right_reads_nothing(plane):
    request, token = plane("ada", "ada-env")
    try:
        with pytest.raises(_Refused):
            await router._require_read(request)
    finally:
        reset_current_env(token)


async def test_environment_management_reads_any(plane):
    request, token = plane("bob", "ada-env", "environment_management")
    try:
        await router._require_read(request)
    finally:
        reset_current_env(token)
