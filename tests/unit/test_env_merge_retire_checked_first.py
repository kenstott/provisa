# Copyright (c) 2026 Kenneth Stott
# Canary: 9d04f311-d2b3-4476-89eb-64d59250e03c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A merge that ends its source is refused BEFORE it merges when the source cannot end (REQ-1542).

``prod`` is never retired, and an environment something still refers to is not either (REQ-1918).
Both are known before the merge runs, so both are answered then: a merge that applied and only
afterwards said its retirement was refused would leave the target changed behind a 409.
"""

# Requirements: REQ-1542, REQ-1918

from __future__ import annotations

import pytest

from provisa.api.admin import environments_router as er
from provisa.api.errors import ApiError
from provisa.core import env_approvals
from provisa.core.env_retire import EnvDependent

pytestmark = pytest.mark.asyncio

ORG = "acme"
_FEATURE_2 = EnvDependent("environment", "feature-2", "feature-2", "branched_from")


class _Request:
    # REQ-2216: a decision reads its Idempotency-Key header; these calls send none.
    headers: dict[str, str] = {}

    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


@pytest.fixture
def wired(monkeypatch):
    calls: dict[str, list] = {k: [] for k in ("copy", "plan", "request", "decide")}
    blocking: dict[str, list] = {}

    async def _passes(*args, **kwargs):
        return "uid-admin"

    async def _confined(*args, **kwargs):
        return None

    async def _known(org_id, name):
        return {"name": name, "deployed_sha": None}

    async def _org_model_db(org_id):
        return "model-db"

    async def _member_count(org_id):
        return 4

    async def is_protected(admin_db, org_id, name, member_count):
        return name == "staging"

    async def env_dependents(admin_db, org_id, name):
        return blocking.get(name, [])

    async def merge_model(*args, **kwargs):
        calls["copy"].append(args)
        raise AssertionError("the merge ran before its retirement was checked")

    async def plan_merge(*args, **kwargs):
        calls["plan"].append(args)
        raise AssertionError("the merge was planned before its retirement was checked")

    async def request_merge(*args, **kwargs):
        calls["request"].append(kwargs)
        raise AssertionError("the merge was proposed before its retirement was checked")

    async def decide(*args, **kwargs):
        calls["decide"].append(kwargs)
        raise AssertionError("the request was applied before its retirement was checked")

    async def _request_or_404(org_id, request_id):
        return {
            "id": request_id,
            "source_env": "feature",
            "source_ref": None,
            "target_env": "staging",
            "retire_source": True,
            "retire_remote": False,
        }

    from provisa.api.admin import orgs_router

    monkeypatch.setattr(er, "_guard", _passes)
    monkeypatch.setattr(er, "_guard_within", _passes)
    monkeypatch.setattr(er, "_confined", _confined)
    monkeypatch.setattr(er, "_known", _known)
    monkeypatch.setattr(er, "_member_count", _member_count)
    monkeypatch.setattr(er, "_request_or_404", _request_or_404)
    monkeypatch.setattr(er, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(er, "merge_model", merge_model)
    monkeypatch.setattr(er, "plan_merge", plan_merge)
    monkeypatch.setattr(er, "env_dependents", env_dependents)
    monkeypatch.setattr(env_approvals, "is_protected", is_protected)
    monkeypatch.setattr(env_approvals, "request_merge", request_merge)
    monkeypatch.setattr(env_approvals, "decide", decide)
    monkeypatch.setattr(orgs_router, "_org_model_db", _org_model_db)
    return calls, blocking


def _body(source: str, *, dry_run: bool = False) -> er.MergeBody:
    return er.MergeBody(from_env=source, message="done", retire_source=True, dry_run=dry_run)


async def test_a_merge_from_prod_that_would_retire_it_is_refused_before_it_merges(wired):
    calls, _ = wired
    with pytest.raises(ApiError) as exc:
        await er.merge_into_environment(_Request(), ORG, "dev", _body("prod"))
    assert exc.value.status_code == 409
    assert exc.value.code == "environments.prod_immutable"
    assert calls["copy"] == []


async def test_a_source_something_still_refers_to_is_refused_before_it_merges(wired):
    calls, blocking = wired
    blocking["feature"] = [_FEATURE_2]
    with pytest.raises(ApiError) as exc:
        await er.merge_into_environment(_Request(), ORG, "dev", _body("feature"))
    assert exc.value.status_code == 409
    assert exc.value.code == "environments.in_use"
    assert calls["copy"] == []


async def test_a_proposal_is_checked_too(wired):
    calls, _ = wired
    with pytest.raises(ApiError) as exc:
        await er.merge_into_environment(_Request(), ORG, "staging", _body("prod"))
    assert exc.value.code == "environments.prod_immutable"
    assert calls["request"] == []


async def test_an_approval_is_checked_before_it_applies(wired):
    calls, blocking = wired
    blocking["feature"] = [_FEATURE_2]
    with pytest.raises(ApiError) as exc:
        await er.decide_merge_request(_Request(), ORG, 7, er.DecideBody(approve=True))
    assert exc.value.code == "environments.in_use"
    assert calls["decide"] == []


async def test_a_dry_run_is_told_as_well(wired):
    """The report of a merge that cannot happen as asked is not a plan of it."""
    calls, _ = wired
    with pytest.raises(ApiError) as exc:
        await er.merge_into_environment(_Request(), ORG, "dev", _body("prod", dry_run=True))
    assert exc.value.code == "environments.prod_immutable"
    assert calls["plan"] == []
