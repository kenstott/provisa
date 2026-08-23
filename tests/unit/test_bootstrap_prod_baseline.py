# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9a15-4d38-4b71-8fa0-6e5b13c94d27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every environment of the booted org starts its history at boot, however it came to exist.

REQ-1543: an environment's line is never started by an edit. ``create_org`` gives prod its row and
its first commit for an org made through the admin API; the booted org is assembled from the config
file instead, and an environment can also outlive the process that created it on a restored volume.
So the guarantee is enforced against the environments that are ACTUALLY THERE: any whose branch has
no ref is given its baseline, and one already standing somewhere is left alone.
"""

# Requirements: REQ-1487, REQ-1524, REQ-1543

from __future__ import annotations

import pytest

from provisa.api import app as app_module
from provisa.core import env_repo, env_store

pytestmark = pytest.mark.asyncio

ORG = "default"


class _Conn:
    async def __aenter__(self):
        return "conn"

    async def __aexit__(self, *exc):
        return False


class _Db:
    def acquire(self):
        return _Conn()


@pytest.fixture
def booted(monkeypatch):
    """The planes a finished boot leaves behind, recording what the baseline step wrote."""
    written: dict = {"prod_rows": [], "repos": [], "commits": []}
    rows: list[dict] = [{"name": "prod"}, {"name": "dev"}]
    started: set[str] = set()

    async def _ensure_prod(db, org_id, created_by=None):
        written["prod_rows"].append(org_id)

    def _ensure_repo(org_id):
        written["repos"].append(org_id)
        return "repo"

    async def _write_through(conn, admin_db, org_id, env, schema, message, actor):
        written["commits"].append((org_id, env, message))
        return "e" * 40

    async def _list_envs(db, org_id):
        return rows

    monkeypatch.setattr(env_repo, "has_branch", lambda repo, env: env in started)
    monkeypatch.setattr(env_store, "list_envs", _list_envs)
    monkeypatch.setattr(env_store, "ensure_prod", _ensure_prod)
    monkeypatch.setattr(env_repo, "ensure_repo", _ensure_repo)
    monkeypatch.setattr(env_repo, "write_through", _write_through)
    monkeypatch.setattr(app_module.state, "admin_db", _Db())
    monkeypatch.setattr(app_module.state, "tenant_db", _Db())
    monkeypatch.setattr(app_module.state, "_org_id", ORG)
    written["rows"] = rows
    written["started"] = started
    return written


class TestBootstrapProd:
    async def test_prod_is_registered_for_the_org_this_process_boots(self, booted):
        await app_module._ensure_environment_baselines()
        assert booted["prod_rows"] == [ORG]

    async def test_the_repository_exists_before_anything_is_committed_into_it(self, booted):
        await app_module._ensure_environment_baselines()
        assert booted["repos"] == [ORG]

    async def test_prod_gets_the_same_first_commit_a_created_org_gets(self, booted):
        await app_module._ensure_environment_baselines()
        assert (ORG, "prod", "provisioned") in booted["commits"]


class TestEveryEnvironment:
    async def test_a_branch_that_never_got_one_is_given_its_baseline_too(self, booted):
        """However it got started is irrelevant -- what decides is whether its line has begun."""
        await app_module._ensure_environment_baselines()
        assert [c[1] for c in booted["commits"]] == ["prod", "dev"]

    async def test_prod_goes_first_so_a_branch_can_continue_its_line(self, booted):
        booted["rows"][:] = [{"name": "dev"}, {"name": "prod"}]
        await app_module._ensure_environment_baselines()
        assert [c[1] for c in booted["commits"]] == ["prod", "dev"]

    async def test_an_environment_already_standing_somewhere_is_left_alone(self, booted):
        booted["started"].add("prod")
        await app_module._ensure_environment_baselines()
        assert [c[1] for c in booted["commits"]] == ["dev"]
