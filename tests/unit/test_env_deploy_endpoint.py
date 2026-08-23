# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a6c02-9d71-4be5-8c14-77e0b2d95af6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The door a build passes through to become a model (REQ-1496).

``env_deploy`` itself is proved against real schemas in ``tests/integration``; what is checked here is
the layer above it -- that the ref is PINNED to a sha before anything is planned, that a protected
target turns the call into a proposal instead of a write, that seeding is carried rather than
assumed, and that a tree which does not hold arrives as a status rather than a traceback.
"""

# Requirements: REQ-1496, REQ-1504, REQ-1524, REQ-1528, REQ-1539

from __future__ import annotations

import pytest

from provisa.api.admin import environments_router as er
from provisa.api.errors import ApiError
from provisa.core import env_approvals
from provisa.core.env_deploy import DeployDelta, DeployError, DeployReport

pytestmark = pytest.mark.asyncio

ORG = "acme"
SHA = "1f6d9c4b" + "0" * 32


class _Request:
    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


def _report(env: str = "prod", ref: str = SHA, seed: bool = False) -> DeployReport:
    return DeployReport(env, ref, seed, DeployDelta(added=["sales/tables/Order.yaml"], unchanged=4))


@pytest.fixture
def wired(monkeypatch):
    """Every collaborator replaced and recorded, so a test asserts on the seam."""
    calls: dict[str, list] = {
        k: []
        for k in (
            "guard_within",
            "known",
            "audit",
            "resolve",
            "files",
            "plan",
            "deploy",
            "request",
            "position",
        )
    }

    async def _guard_within(request, org_id, name):
        calls["guard_within"].append((org_id, name))
        return "uid-deployer"

    async def _known(org_id, name):
        calls["known"].append((org_id, name))
        return {"name": name}

    async def _audit(org_id, actor, action, name, detail):
        calls["audit"].append((org_id, actor, action, name, detail))

    async def _org_tenant_db(org_id):
        return "tenant-db-for-" + org_id

    async def _member_count(org_id):
        return 4

    async def is_protected(admin_db, org_id, name, member_count):
        return name == "prod"

    def resolve_sha(org_id, ref):
        calls["resolve"].append((org_id, ref))
        return SHA

    def files_at(org_id, ref):
        calls["files"].append((org_id, ref))
        return {"sources/warehouse.yaml": "type: postgres\n"}

    async def plan_deploy(db, org_id, env, tree, *, ref, seed=False):
        calls["plan"].append((org_id, env, ref, seed))
        return _report(env, ref, seed)

    async def deploy_tree(db, org_id, env, tree, *, ref, seed=False):
        calls["deploy"].append((org_id, env, ref, seed))
        return _report(env, ref, seed)

    async def request_deploy(admin_db, tenant_db, org_id, **kw):
        calls["request"].append(kw)
        return {"id": 12, "target_env": kw["target_env"], "report": _report().as_dict()}

    monkeypatch.setattr(er, "_guard_within", _guard_within)
    monkeypatch.setattr(er, "_known", _known)
    monkeypatch.setattr(er, "_audit", _audit)
    monkeypatch.setattr(er, "_member_count", _member_count)
    monkeypatch.setattr(er, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(er, "plan_deploy", plan_deploy)
    monkeypatch.setattr(er, "deploy_tree", deploy_tree)
    monkeypatch.setattr(env_approvals, "is_protected", is_protected)
    monkeypatch.setattr(env_approvals, "request_deploy", request_deploy)

    from provisa.api.admin import orgs_router
    from provisa.core import env_repo

    async def set_position(db, org_id, name, *, deployed_sha, redo_sha):
        # REQ-1543: an apply records WHERE the environment now is and ends any run of undos.
        calls["position"].append((org_id, name, deployed_sha, redo_sha))

    monkeypatch.setattr("provisa.core.env_store.set_position", set_position)
    monkeypatch.setattr(orgs_router, "_org_tenant_db", _org_tenant_db)
    monkeypatch.setattr(env_repo, "resolve_sha", resolve_sha)
    monkeypatch.setattr(env_repo, "files_at", files_at)
    return calls


class TestTheRefIsPinned:
    async def test_a_branch_is_resolved_and_the_sha_is_what_loads(self, wired):
        out = await er.deploy_into_environment(
            _Request(), ORG, "dev", er.DeployBody(ref="refs/heads/dev")
        )
        assert wired["resolve"] == [(ORG, "refs/heads/dev")]
        assert wired["deploy"] == [(ORG, "dev", SHA, False)]
        assert out["report"]["ref"] == SHA

    async def test_the_tree_is_read_at_the_sha_not_at_the_branch(self, wired):
        # A branch that moves between resolving and reading would put a different model behind a
        # report that named the first one.
        await er.deploy_into_environment(_Request(), ORG, "dev", er.DeployBody(ref="dev"))
        assert wired["files"] == [(ORG, SHA)]

    async def test_a_ref_this_repository_cannot_resolve_is_404(self, wired, monkeypatch):
        from provisa.core import env_repo
        from provisa.core.env_repo import RepositoryError

        def resolve_sha(org_id, ref):
            raise RepositoryError(f"{ref!r} is neither a branch nor a commit in this repository")

        monkeypatch.setattr(env_repo, "resolve_sha", resolve_sha)
        with pytest.raises(ApiError) as exc:
            await er.deploy_into_environment(_Request(), ORG, "dev", er.DeployBody(ref="nope"))
        assert exc.value.status_code == 404
        assert exc.value.code == "environments.unknown_ref"


class TestNothingLoadsItself:
    async def test_a_dry_run_plans_and_writes_nothing(self, wired):
        out = await er.deploy_into_environment(
            _Request(), ORG, "dev", er.DeployBody(ref="dev", dry_run=True)
        )
        assert wired["plan"] and wired["deploy"] == []
        assert out["applied"] is False
        assert wired["audit"] == []

    async def test_applying_is_audited_in_the_org_s_own_trail(self, wired):
        await er.deploy_into_environment(_Request(), ORG, "dev", er.DeployBody(ref="dev"))
        (entry,) = wired["audit"]
        assert entry[2] == "environment.deploy"
        assert entry[4]["ref"] == SHA

    async def test_the_environment_that_will_hold_it_is_the_one_guarded(self, wired):
        # REQ-1496: the deploy is performed where it LANDS, so the authority checked is the target's.
        await er.deploy_into_environment(_Request(), ORG, "dev", er.DeployBody(ref="other"))
        assert wired["guard_within"] == [(ORG, "dev")]
        assert wired["known"] == [(ORG, "dev")]


class TestAProtectedTargetIsProposedTo:
    async def test_a_load_into_prod_becomes_a_request(self, wired):
        out = await er.deploy_into_environment(
            _Request(), ORG, "prod", er.DeployBody(ref="dev", message="release 4")
        )
        assert out == {
            "request": {"id": 12, "target_env": "prod", "report": _report().as_dict()},
            "applied": False,
            "requires_approval": True,
        }
        assert wired["deploy"] == []

    async def test_the_request_pins_both_the_ref_and_the_sha(self, wired):
        # The ref is what a person recognises and the sha is what gets applied; a request holding
        # only the name would let the branch move out from under the approval.
        await er.deploy_into_environment(_Request(), ORG, "prod", er.DeployBody(ref="main"))
        (kw,) = wired["request"]
        assert kw["ref"] == "main"
        assert kw["sha"] == SHA
        assert kw["requested_by"] == "uid-deployer"

    async def test_a_dry_run_against_a_protected_target_stays_a_dry_run(self, wired):
        # It writes nothing, so there is nothing for an approver to hold.
        out = await er.deploy_into_environment(
            _Request(), ORG, "prod", er.DeployBody(ref="dev", dry_run=True)
        )
        assert wired["request"] == [] and wired["plan"]
        assert out["applied"] is False and out["requires_approval"] is True

    async def test_the_proposal_is_audited_as_a_request_not_as_a_load(self, wired):
        await er.deploy_into_environment(_Request(), ORG, "prod", er.DeployBody(ref="dev"))
        (entry,) = wired["audit"]
        assert entry[2] == "environment.deploy_requested"
        assert entry[4]["request_id"] == 12


class TestSeeding:
    async def test_seeding_is_off_unless_asked_for(self, wired):
        # REQ-1539: a tree carries the roles of whatever control plane projected it, and a desktop's
        # self-granted rights must not arrive with it.
        await er.deploy_into_environment(_Request(), ORG, "dev", er.DeployBody(ref="dev"))
        assert wired["deploy"] == [(ORG, "dev", SHA, False)]

    async def test_seeding_is_carried_to_the_load(self, wired):
        await er.deploy_into_environment(
            _Request(), ORG, "dev", er.DeployBody(ref="dev", seed=True)
        )
        assert wired["deploy"] == [(ORG, "dev", SHA, True)]

    async def test_seeding_is_carried_onto_a_request(self, wired):
        # An approver approves a specific operation, and whether roles arrive is part of it.
        await er.deploy_into_environment(
            _Request(), ORG, "prod", er.DeployBody(ref="dev", seed=True)
        )
        assert wired["request"][0]["seed"] is True


class TestATreeThatDoesNotHold:
    async def test_it_is_refused_as_a_status_a_caller_can_render(self, wired, monkeypatch):
        async def deploy_tree(db, org_id, env, tree, *, ref, seed=False):
            raise DeployError("'x.yaml' names 'gone.yaml' as its target")

        monkeypatch.setattr(er, "deploy_tree", deploy_tree)
        with pytest.raises(ApiError) as exc:
            await er.deploy_into_environment(_Request(), ORG, "dev", er.DeployBody(ref="dev"))
        assert exc.value.status_code == 422
        assert exc.value.code == "environments.tree_does_not_hold"
        assert "gone.yaml" in exc.value.detail

    async def test_a_plan_that_does_not_hold_is_refused_the_same_way(self, wired, monkeypatch):
        async def plan_deploy(db, org_id, env, tree, *, ref, seed=False):
            raise DeployError("the tree holds no such table")

        monkeypatch.setattr(er, "plan_deploy", plan_deploy)
        with pytest.raises(ApiError) as exc:
            await er.deploy_into_environment(
                _Request(), ORG, "dev", er.DeployBody(ref="dev", dry_run=True)
            )
        assert exc.value.status_code == 422

    async def test_a_refused_load_is_not_audited(self, wired, monkeypatch):
        async def deploy_tree(db, org_id, env, tree, *, ref, seed=False):
            raise DeployError("nope")

        monkeypatch.setattr(er, "deploy_tree", deploy_tree)
        with pytest.raises(ApiError):
            await er.deploy_into_environment(_Request(), ORG, "dev", er.DeployBody(ref="dev"))
        assert wired["audit"] == []
