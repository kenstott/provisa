# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2a91c4-8d3b-4c17-b0ae-51e9d7c4a882
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a membership PINNED to one environment may reach (REQ-1596, REQ-1624).

A sandbox visitor's membership exists in the ephemeral environment minted for them
(``user_org_memberships.env_name``). The rights check answers what their ROLE may do; it cannot
answer which environment they may do it in, and two ways round it were open: the environments
listing named every visitor's environment, and a merge is guarded on its SOURCE -- which the
visitor owns -- so owning theirs authorized merging it into anybody else's.
"""

# Requirements: REQ-1596, REQ-1624

from __future__ import annotations

import pytest
from fastapi import Request

from provisa.api.admin import environments_router as router
from provisa.api.errors import ApiError

pytestmark = pytest.mark.asyncio

PIN = "ephemeral_1918c50d"
OTHER = "ephemeral_3d10fd41"


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Conn:
    """Answers the membership lookup with one pinned row."""

    def __init__(self, pin):
        self.pin = pin

    async def execute_core(self, statement):
        return _Result(None if self.pin is None else (self.pin,))


class _Pool:
    def __init__(self, pin):
        self.conn = _Conn(pin)

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                return pool.conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


@pytest.fixture
def pinned(monkeypatch):
    def wire(pin):
        monkeypatch.setattr(router, "_admin_pool", lambda: _Pool(pin))
        monkeypatch.setattr(router, "_member", _allowed)
        monkeypatch.setattr(router, "_guard", _allowed)

    return wire


async def _allowed(request, org_id, *rights):
    return "visitor"


def _request() -> Request:
    request = Request({"type": "http", "headers": [], "method": "POST", "path": "/x"})
    request.state.identity = type("I", (), {"user_id": "visitor"})()
    return request


def _env(name: str) -> dict:
    return {"name": name, "deployed_sha": None, "origin_sha": None, "redo_sha": None}


class TestTheListingIsTheirOwnEnvironment:
    async def test_other_visitors_environments_are_not_named(self, pinned, monkeypatch):
        pinned(PIN)
        monkeypatch.setattr(
            router, "list_envs", lambda pool, org_id: _rows([_env(PIN), _env(OTHER)])
        )
        answer = await router.list_environments(_request(), "sandbox")
        assert [row["name"] for row in answer["environments"]] == [PIN]

    async def test_an_unpinned_member_still_sees_the_org(self, pinned, monkeypatch):
        # The confinement is the PIN, not the sandbox: a developer with environment_management and
        # no pin keeps the whole listing (REQ-1573).
        pinned(None)
        monkeypatch.setattr(
            router, "list_envs", lambda pool, org_id: _rows([_env("prod"), _env("dev")])
        )
        answer = await router.list_environments(_request(), "acme")
        assert [row["name"] for row in answer["environments"]] == ["prod", "dev"]


async def _rows(rows):
    return rows


class TestActingOutsideThePinIsRefused:
    async def test_merging_into_another_visitors_environment(self, pinned):
        pinned(PIN)
        with pytest.raises(ApiError) as exc:
            await router.merge_into_environment(
                _request(), "sandbox", OTHER, router.MergeBody(from_env=PIN)
            )
        assert exc.value.status_code == 403
        assert exc.value.code == "environments.pinned"

    async def test_merging_another_visitors_environment_into_their_own(self, pinned):
        # The source is guarded too: a visitor may not pull somebody else's model into their
        # sandbox any more than they may push into it.
        pinned(PIN)
        with pytest.raises(ApiError) as exc:
            await router.merge_into_environment(
                _request(), "sandbox", PIN, router.MergeBody(from_env=OTHER)
            )
        assert exc.value.code == "environments.pinned"

    async def test_deleting_another_visitors_environment(self, pinned):
        pinned(PIN)
        with pytest.raises(ApiError) as exc:
            await router.delete_environment(_request(), "sandbox", OTHER)
        assert exc.value.code == "environments.pinned"

    async def test_creating_a_second_environment(self, pinned):
        pinned(PIN)
        with pytest.raises(ApiError) as exc:
            await router.create_environment(
                _request(), "acme", router.CreateEnvBody(name="mine", from_env="prod")
            )
        assert exc.value.code == "environments.pinned"

    async def test_within_the_pin_the_rights_check_still_answers(self, pinned, monkeypatch):
        # Confinement adds a second question; it does not replace the first. Inside the pin the
        # call proceeds to the guard, which is what refuses or allows it.
        pinned(PIN)
        seen: list = []
        monkeypatch.setattr(router, "_guard_within", _record(seen))
        monkeypatch.setattr(router, "_known", _record(seen))
        with pytest.raises(TypeError):
            # _known is stubbed, so the route fails LATER -- past the pin, which is the assertion.
            await router.delete_environment(_request(), "sandbox", PIN)
        assert seen


def _record(seen):
    async def call(*args, **kwargs):
        seen.append(args)
        raise TypeError("stub")

    return call
