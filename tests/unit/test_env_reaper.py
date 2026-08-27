# Copyright (c) 2026 Kenneth Stott
# Canary: 6b4e0d92-1c73-4a58-8ef1-3d9a26c5b704
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1523: an environment whose expiry has passed is deleted with its schema and its store, and
an environment with no expiry is never deleted for being idle."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from provisa.core import env_reaper
from provisa.core.env_reaper import ReapError, reap_expired

NOW = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)


class _Retirements:
    """Records what ``retire_environment`` was asked to do, and fails the names it is told to."""

    def __init__(self, fail: set[str] | None = None):
        self.calls: list[tuple[str, str, bool]] = []
        self.fail = fail or set()

    async def __call__(self, pool, admin_db, org_id, name, *, drop_branch):
        self.calls.append((org_id, name, drop_branch))
        if name in self.fail:
            raise RuntimeError(f"store for {name} unreachable")
        return {"retired": name, "branch_deleted": drop_branch}


@pytest.fixture
def retire(monkeypatch):
    def _install(expired: list[tuple[str, str]], fail: set[str] | None = None) -> _Retirements:
        async def _expired_envs(_admin_db, now):
            assert now == NOW  # the reaper measures against the instant it was given
            return expired

        recorder = _Retirements(fail)
        monkeypatch.setattr(env_reaper, "expired_envs", _expired_envs)
        monkeypatch.setattr(env_reaper, "retire_environment", recorder)
        return recorder

    return _install


class TestReapExpired:
    @pytest.mark.asyncio
    async def test_every_expired_environment_is_retired(self, retire):
        recorder = retire([("acme", "dev_1"), ("globex", "sandbox_7")])
        outcomes = await reap_expired(object(), object(), now=NOW)
        assert recorder.calls == [
            ("acme", "dev_1", True),
            ("globex", "sandbox_7", True),
        ]
        assert outcomes == [
            {"org_id": "acme", "retired": "dev_1", "branch_deleted": True},
            {"org_id": "globex", "retired": "sandbox_7", "branch_deleted": True},
        ]

    @pytest.mark.asyncio
    async def test_nothing_expired_retires_nothing(self, retire):
        recorder = retire([])
        assert await reap_expired(object(), object(), now=NOW) == []
        assert recorder.calls == []

    @pytest.mark.asyncio
    async def test_the_branch_goes_with_the_environment(self, retire):
        # An expiry says the work is over on a date chosen at creation, which is the merge-retires-
        # its-source case rather than the delete door, where a person has not asked to lose history.
        recorder = retire([("acme", "dev_1")])
        await reap_expired(object(), object(), now=NOW)
        assert recorder.calls[0][2] is True

    @pytest.mark.asyncio
    async def test_one_unreachable_store_does_not_block_the_ones_behind_it(self, retire):
        recorder = retire([("acme", "broken"), ("acme", "fine")], fail={"broken"})
        with pytest.raises(ReapError) as caught:
            await reap_expired(object(), object(), now=NOW)
        # Attempted, not skipped: the sweep would otherwise fail at the same place every tick and
        # nothing queued behind it would ever be reaped.
        assert [c[1] for c in recorder.calls] == ["broken", "fine"]
        assert [(o, n) for o, n, _ in caught.value.failures] == [("acme", "broken")]

    @pytest.mark.asyncio
    async def test_a_failure_is_raised_and_never_swallowed(self, retire):
        retire([("acme", "broken")], fail={"broken"})
        with pytest.raises(ReapError, match="broken"):
            await reap_expired(object(), object(), now=NOW)

    @pytest.mark.asyncio
    async def test_each_retirement_is_recorded_in_the_orgs_own_trail(self, retire):
        retire([("acme", "dev_1")])
        seen: list[tuple[str, str, dict]] = []

        async def _audit(org_id, name, outcome):
            seen.append((org_id, name, outcome))

        await reap_expired(object(), object(), now=NOW, audit=_audit)
        assert seen == [("acme", "dev_1", {"retired": "dev_1", "branch_deleted": True})]

    @pytest.mark.asyncio
    async def test_a_failed_retirement_is_not_audited_as_one(self, retire):
        retire([("acme", "broken")], fail={"broken"})
        seen: list[str] = []

        async def _audit(org_id, name, outcome):
            seen.append(name)

        with pytest.raises(ReapError):
            await reap_expired(object(), object(), now=NOW, audit=_audit)
        assert seen == []

    @pytest.mark.asyncio
    async def test_no_instant_given_measures_against_now(self, monkeypatch):
        # The default is the wall clock, so the scheduled job needs no clock of its own.
        asked: list[datetime] = []

        async def _expired_envs(_admin_db, now):
            asked.append(now)
            return []

        monkeypatch.setattr(env_reaper, "expired_envs", _expired_envs)
        monkeypatch.setattr(env_reaper, "utcnow", lambda: NOW)
        await reap_expired(object(), object())
        assert asked == [NOW]


class TestExpiryPredicate:
    """The SQL is ``expires_at <= now``, and a NULL never satisfies it (REQ-1523)."""

    def test_a_null_expiry_is_permanent(self):
        from provisa.core.schema_admin import environments

        clause = str(environments.c.expires_at <= NOW)
        assert "expires_at <=" in clause
        # No IS NULL branch is needed or wanted: an environment with no expiry is never reaped for
        # being idle, and NULL <= anything is NULL, which no row is selected by.
        assert "IS NULL" not in clause

    def test_the_sweep_takes_the_most_overdue_first(self):
        import inspect

        source = inspect.getsource(env_reaper.expired_envs)
        assert "order_by(environments.c.expires_at)" in source


class TestNotIdleReaping:
    @pytest.mark.asyncio
    async def test_an_environment_with_no_expiry_is_never_offered_for_reaping(self, monkeypatch):
        # The query is the whole gate: a row with a NULL expiry cannot come back from it, so the
        # reaper is never in a position to retire a quiet pre-prod.
        recorder = _Retirements()

        async def _expired_envs(_admin_db, now):
            return []

        monkeypatch.setattr(env_reaper, "expired_envs", _expired_envs)
        monkeypatch.setattr(env_reaper, "retire_environment", recorder)
        await reap_expired(object(), object(), now=NOW + timedelta(days=365))
        assert recorder.calls == []
