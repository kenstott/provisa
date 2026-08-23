# Copyright (c) 2026 Kenneth Stott
# Canary: 72e1c3b9-cc2a-4c70-b8af-55b8a8e7da67
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the merge-request endpoints hand an approver, and who is allowed to ask (REQ-1504, REQ-1529).

The core modules these call are covered against real planes in ``tests/integration``; what is
checked here is the layer above them -- which guard each endpoint runs, that a listed request
carries the DERIVED state rather than the stored one, that timestamps leave as strings, and that a
refusal from ``env_approvals`` arrives as an HTTP status a caller can render
instead of a traceback.
"""

# Requirements: REQ-1504, REQ-1523, REQ-1527, REQ-1528, REQ-1529

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from provisa.api.admin import environments_router as er
from provisa.api.errors import ApiError
from provisa.core import env_approvals

pytestmark = pytest.mark.asyncio

ORG = "acme"
#: Where the source environment sits in its own history -- what the squash of REQ-1545 names.
SOURCE_SHA = "9c1d4e2f00000000000000000000000000000000"
REQUESTED_AT = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)


def _row(request_id: int = 7, **over) -> dict:
    row = {
        "id": request_id,
        "org_id": ORG,
        "source_env": "dev",
        # A merge names an environment; a LOAD names a ref and the sha it was pinned to instead
        # (REQ-1496), which is what tells the approval code which of the two it is holding.
        "source_ref": None,
        "source_sha": None,
        "seed": False,
        "target_env": "prod",
        "state": "requested",
        "requested_by": "uid-dev",
        "requested_at": REQUESTED_AT,
        "decided_by": None,
        "decided_at": None,
        "decision_note": None,
        "applied_at": None,
        # A merge request stores a CopyReport, which is grouped by the registry it wrote and so
        # always carries ``tables`` -- REQ-1544 reads it to decide what the apply has to refresh.
        "report": {"added": 1, "changed": 0, "removed": 0, "tables": []},
        # REQ-1542: on the row because it is part of what an approver approves. A stored row always
        # carries it -- the column is NOT NULL -- so the double carries it too.
        "retire_source": False,
        # REQ-1549: and whether the retirement reaches the remote. NOT NULL on the row, so the
        # double carries it for the same reason.
        "retire_remote": False,
        "message": "ship it",
    }
    row.update(over)
    return row


class _Request:
    """Only ``state.identity`` is read by the guards, and every guard here is monkeypatched."""

    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


@pytest.fixture
def wired(monkeypatch):
    """Every collaborator replaced, and every call to one recorded, so a test asserts on the seam.

    The guards are stubbed rather than exercised because who may call is settled by
    ``_member``/``_guard`` themselves; what is under test is WHICH of the two each endpoint runs.
    """
    calls: dict[str, list] = {
        k: [] for k in ("member", "guard", "known", "audit", "decide", "squash")
    }

    async def _member(request, org_id):
        calls["member"].append(org_id)
        return "uid-reviewer"

    async def _guard(request, org_id):
        calls["guard"].append(org_id)
        return "uid-reviewer"

    async def _known(org_id, name):
        calls["known"].append((org_id, name))
        # REQ-1543: every environment row carries where it is in its own history; the squash of
        # REQ-1545 reads the source's position to name the sha it merged from.
        return {"name": name, "deployed_sha": SOURCE_SHA, "redo_sha": None}

    async def _audit(org_id, actor, action, name, detail):
        calls["audit"].append((org_id, actor, action, name, detail))

    async def _org_tenant_db(org_id):
        return "tenant-db-for-" + org_id

    monkeypatch.setattr(er, "_member", _member)
    monkeypatch.setattr(er, "_guard", _guard)
    monkeypatch.setattr(er, "_known", _known)

    async def _squash(org_id, source, target, actor, message):
        # REQ-1545: the one commit a merge lands on the target's branch. The real one projects the
        # model and commits it; what a router test asserts is that it happened, and from where.
        # REQ-1550: the operator's comment reaches it, which is what the commit subject leads with.
        calls["squash"].append((org_id, source, target, message))
        return "5ea6d1c000000000000000000000000000000000"

    monkeypatch.setattr(er, "_audit", _audit)
    monkeypatch.setattr(er, "_squash", _squash)
    monkeypatch.setattr(er, "_admin_pool", lambda: "admin-db")

    class _Registry:
        """Nothing is cached in a unit test, which is the REQ-1544 "uncached" case: the next
        request would build the runtime from the rows the call just wrote."""

        def get(self, key):
            return None

        def invalidate(self, key):
            raise AssertionError("nothing is cached, so nothing can be invalidated")

    class _State:
        org_registry = _Registry()

    monkeypatch.setattr(er, "_state", _State)
    from provisa.api.admin import orgs_router

    monkeypatch.setattr(orgs_router, "_org_tenant_db", _org_tenant_db)
    return calls


class TestListMergeRequests:
    async def test_state_is_the_derived_one_not_the_stored_one(self, wired, monkeypatch):
        """REQ-1504: staleness is never stored, so a listing that echoed the row would hide it."""

        async def list_requests(admin_db, org_id, open_only=False):
            return [_row(state="requested")]

        async def effective_state(tenant_db, org_id, request):
            return "stale"

        monkeypatch.setattr(env_approvals, "list_requests", list_requests)
        monkeypatch.setattr(env_approvals, "effective_state", effective_state)
        out = await er.list_merge_requests(_Request(), ORG)
        assert [r["state"] for r in out["requests"]] == ["stale"]

    async def test_timestamps_leave_as_iso_strings(self, wired, monkeypatch):
        async def list_requests(admin_db, org_id, open_only=False):
            return [_row(decided_at=REQUESTED_AT, applied_at=None)]

        async def effective_state(tenant_db, org_id, request):
            return "requested"

        monkeypatch.setattr(env_approvals, "list_requests", list_requests)
        monkeypatch.setattr(env_approvals, "effective_state", effective_state)
        out = await er.list_merge_requests(_Request(), ORG)
        row = out["requests"][0]
        assert row["requested_at"] == REQUESTED_AT.isoformat()
        assert row["decided_at"] == REQUESTED_AT.isoformat()
        assert row["applied_at"] is None

    async def test_open_only_reaches_the_store(self, wired, monkeypatch):
        seen: list[bool] = []

        async def list_requests(admin_db, org_id, open_only=False):
            seen.append(open_only)
            return []

        monkeypatch.setattr(env_approvals, "list_requests", list_requests)
        await er.list_merge_requests(_Request(), ORG, open_only=True)
        assert seen == [True]

    async def test_listing_asks_membership_not_org_admin(self, wired, monkeypatch):
        """REQ-1528: a member proposes merges, so a member must be able to see their own."""

        async def list_requests(admin_db, org_id, open_only=False):
            return []

        monkeypatch.setattr(env_approvals, "list_requests", list_requests)
        await er.list_merge_requests(_Request(), ORG)
        assert wired["member"] == [ORG] and wired["guard"] == []


class TestGetMergeRequest:
    async def test_unknown_request_is_404(self, wired, monkeypatch):
        async def get_request(admin_db, org_id, request_id):
            return None

        monkeypatch.setattr(env_approvals, "get_request", get_request)
        with pytest.raises(ApiError) as exc:
            await er.get_merge_request(_Request(), ORG, 99)
        assert exc.value.status_code == 404
        assert exc.value.code == "environments.unknown_merge_request"

    async def test_report_is_returned_as_it_was_produced(self, wired, monkeypatch):
        """The approver reviews the report the request carries, not a fresh one (REQ-1504)."""

        async def get_request(admin_db, org_id, request_id):
            return _row(report={"added": 3})

        async def effective_state(tenant_db, org_id, request):
            return "stale"

        monkeypatch.setattr(env_approvals, "get_request", get_request)
        monkeypatch.setattr(env_approvals, "effective_state", effective_state)
        out = await er.get_merge_request(_Request(), ORG, 7)
        assert out["request"]["report"] == {"added": 3}
        assert out["request"]["state"] == "stale"


class TestDecideMergeRequest:
    async def test_deciding_is_an_org_admin_act(self, wired, monkeypatch):
        """REQ-1504: the second person is an org_admin, and refusing the requester is env_approvals'."""

        async def get_request(admin_db, org_id, request_id):
            return _row()

        async def decide(admin_db, tenant_db, org_id, request_id, **kw):
            return _row(state="applied", decided_by=kw["decided_by"])

        monkeypatch.setattr(env_approvals, "get_request", get_request)
        monkeypatch.setattr(env_approvals, "decide", decide)
        out = await er.decide_merge_request(_Request(), ORG, 7, er.DecideBody(approve=True))
        assert wired["guard"] == [ORG] and wired["member"] == []
        assert out["request"]["state"] == "applied"

    async def test_undecidable_request_is_409(self, wired, monkeypatch):
        """A rejected request decided again is a conflict, not a 500 (REQ-1504)."""

        async def get_request(admin_db, org_id, request_id):
            return _row(state="rejected")

        async def decide(admin_db, tenant_db, org_id, request_id, **kw):
            raise env_approvals.MergeRequestError("merge request 7 is rejected")

        monkeypatch.setattr(env_approvals, "get_request", get_request)
        monkeypatch.setattr(env_approvals, "decide", decide)
        with pytest.raises(ApiError) as exc:
            await er.decide_merge_request(_Request(), ORG, 7, er.DecideBody(approve=True))
        assert exc.value.status_code == 409
        assert exc.value.code == "environments.merge_request_undecidable"

    async def test_unknown_request_is_404_before_deciding(self, wired, monkeypatch):
        reached = []

        async def get_request(admin_db, org_id, request_id):
            return None

        async def decide(*a, **kw):
            reached.append(True)

        monkeypatch.setattr(env_approvals, "get_request", get_request)
        monkeypatch.setattr(env_approvals, "decide", decide)
        with pytest.raises(ApiError) as exc:
            await er.decide_merge_request(_Request(), ORG, 7, er.DecideBody(approve=True))
        assert exc.value.status_code == 404 and reached == []

    @pytest.mark.parametrize(
        ("approve", "action"),
        [(True, "environment.merge_approved"), (False, "environment.merge_rejected")],
    )
    async def test_the_decision_is_audited_against_the_target(
        self, wired, monkeypatch, approve, action
    ):
        async def get_request(admin_db, org_id, request_id):
            return _row()

        async def decide(admin_db, tenant_db, org_id, request_id, **kw):
            return _row(state="applied" if approve else "rejected")

        monkeypatch.setattr(env_approvals, "get_request", get_request)
        monkeypatch.setattr(env_approvals, "decide", decide)
        await er.decide_merge_request(_Request(), ORG, 7, er.DecideBody(approve=approve))
        (org_id, actor, logged, name, detail) = wired["audit"][0]
        assert (org_id, logged, name) == (ORG, action, "prod")
        assert detail["request_id"] == 7 and detail["from"] == "dev"
        assert detail["added"] == 1  # the report is flattened into the entry, not nested

    async def test_the_decided_row_is_rendered_with_its_own_state(self, wired, monkeypatch):
        """No derived state here: the request has just been decided, so the stored one IS current."""

        async def get_request(admin_db, org_id, request_id):
            return _row()

        async def decide(admin_db, tenant_db, org_id, request_id, **kw):
            return _row(state="applied", applied_at=REQUESTED_AT)

        async def effective_state(tenant_db, org_id, request):
            raise AssertionError("a just-decided request is not re-derived")

        monkeypatch.setattr(env_approvals, "get_request", get_request)
        monkeypatch.setattr(env_approvals, "decide", decide)
        monkeypatch.setattr(env_approvals, "effective_state", effective_state)
        out = await er.decide_merge_request(_Request(), ORG, 7, er.DecideBody(approve=True))
        assert out["request"]["state"] == "applied"
        assert out["request"]["applied_at"] == REQUESTED_AT.isoformat()


class TestMergePreview:
    """The gate a pipeline asserts against (REQ-1527): the same plan, applying none of it."""

    @pytest.fixture
    def planned(self, monkeypatch, wired):
        seen: list[tuple] = []

        class _Report:
            # REQ-1544: the refresh reads the delta, so a report double carries the same answer a
            # real one would -- nothing here touches a connection registry.
            touches_connectivity = False

            def as_dict(self):
                return {"added": 2, "changed": 1, "removed": 0}

        async def plan_copy(db, org_id, source, target, *, mode, removals):
            seen.append((db, org_id, source, target, mode, removals))
            return _Report()

        async def copy_model(*_args, **_kwargs):
            raise AssertionError("a preview must never apply anything")

        async def is_protected(admin_db, org_id, name, members):
            return True

        async def _guard_within(request, org_id, name):
            wired["guard"].append(org_id)
            return "uid-dev"

        async def _member_count(org_id):
            return 3

        monkeypatch.setattr(er, "plan_copy", plan_copy)
        monkeypatch.setattr(er, "copy_model", copy_model)
        monkeypatch.setattr(er, "_guard_within", _guard_within)
        monkeypatch.setattr(er, "_member_count", _member_count)
        monkeypatch.setattr(env_approvals, "is_protected", is_protected)
        return seen

    async def test_it_returns_the_report_and_applies_nothing(self, planned):
        out = await er.preview_merge(_Request(), ORG, "prod", from_env="dev")
        assert out["report"] == {"added": 2, "changed": 1, "removed": 0}
        assert out["applied"] is False

    async def test_it_plans_the_merge_the_approved_one_would_perform(self, planned):
        await er.preview_merge(_Request(), ORG, "prod", from_env="dev", removals=True)
        (_db, org_id, source, target, mode, removals) = planned[0]
        assert (org_id, source, target, mode, removals) == (ORG, "dev", "prod", er.MERGE, True)

    async def test_it_says_the_target_would_need_an_approval(self, planned):
        assert (await er.preview_merge(_Request(), ORG, "prod", from_env="dev"))[
            "requires_approval"
        ] is True

    async def test_a_preview_writes_no_audit_entry(self, planned, wired):
        await er.preview_merge(_Request(), ORG, "prod", from_env="dev")
        assert wired["audit"] == []

    async def test_an_environment_cannot_preview_against_itself(self, planned):
        with pytest.raises(ApiError) as exc:
            await er.preview_merge(_Request(), ORG, "prod", from_env="prod")
        assert exc.value.status_code == 400 and planned == []


class TestRepoIntegrationEndpoints:
    """Where the org's mirror and status receiver are configured (REQ-1527)."""

    @pytest.fixture
    def stored(self, monkeypatch, wired):
        from provisa.core import env_ci

        state: dict = {"remote": None, "status_webhook": None}

        async def read_integration(admin_db, org_id):
            return env_ci.RepoIntegration(**state)

        async def write_integration(admin_db, org_id, *, remote, status_webhook):
            state.update(remote=remote, status_webhook=status_webhook)
            return env_ci.RepoIntegration(**state)

        monkeypatch.setattr(env_ci, "read_integration", read_integration)
        monkeypatch.setattr(env_ci, "write_integration", write_integration)
        return state

    async def test_reading_is_an_org_admin_act(self, stored, wired):
        await er.get_repo_integration(_Request(), ORG)
        assert wired["guard"] == [ORG]

    async def test_an_unconfigured_org_reads_as_unconfigured(self, stored):
        assert await er.get_repo_integration(_Request(), ORG) == {
            "remote": None,
            "status_webhook": None,
            "configured": False,
        }

    async def test_setting_both_halves_is_recorded(self, stored, wired):
        body = er.RepoIntegrationBody(
            remote="https://${env:GIT_TOKEN}@git.example/x.git", status_webhook="https://ci/x"
        )
        out = await er.set_repo_integration(_Request(), ORG, body)
        assert out["configured"] is True
        (_org, _actor, action, name, detail) = wired["audit"][0]
        assert action == "environment.repo_integration" and name == "-"
        assert detail["remote"] == "https://${env:GIT_TOKEN}@git.example/x.git"

    async def test_the_reference_is_never_resolved_on_this_door(self, stored, monkeypatch):
        monkeypatch.setenv("GIT_TOKEN", "s3cret")
        body = er.RepoIntegrationBody(remote="https://${env:GIT_TOKEN}@git.example/x.git")
        assert "s3cret" not in (await er.set_repo_integration(_Request(), ORG, body))["remote"]

    async def test_clearing_it_is_a_null_remote_and_not_an_omission(self, stored):
        await er.set_repo_integration(
            _Request(), ORG, er.RepoIntegrationBody(remote="https://git/x")
        )
        out = await er.set_repo_integration(_Request(), ORG, er.RepoIntegrationBody())
        assert out == {"remote": None, "status_webhook": None, "configured": False}


class TestRetiringTheSourceOfAMerge:
    """REQ-1542: a merge may end the environment it came from, and only when it was asked to."""

    @pytest.fixture
    def merged(self, monkeypatch, wired):
        """A merge that applies, with the retirement recorded rather than performed."""
        retired: list[tuple] = []

        class _Report:
            touches_connectivity = False

            def as_dict(self):
                return {"added": 1, "changed": 0, "removed": 0}

        async def copy_model(db, org_id, source, target, *, mode, removals):
            return _Report()

        async def plan_copy(db, org_id, source, target, *, mode, removals):
            return _Report()

        async def is_protected(admin_db, org_id, name, members):
            return False

        async def _guard_within(request, org_id, name):
            return "uid-dev"

        async def _member_count(org_id):
            return 3

        async def retire_environment(pool, admin_db, org_id, name, *, drop_branch):
            retired.append((org_id, name, drop_branch))
            return {"retired": name, "branch_deleted": drop_branch}

        monkeypatch.setattr(er, "copy_model", copy_model)
        monkeypatch.setattr(er, "plan_copy", plan_copy)
        monkeypatch.setattr(er, "_guard_within", _guard_within)
        monkeypatch.setattr(er, "_member_count", _member_count)
        monkeypatch.setattr(er, "retire_environment", retire_environment)
        monkeypatch.setattr(er, "_pool", lambda: "tenant-db")
        monkeypatch.setattr(env_approvals, "is_protected", is_protected)
        return retired

    async def test_a_plain_merge_leaves_the_source_standing(self, merged):
        out = await er.merge_into_environment(
            _Request(), ORG, "prod", er.MergeBody(message="ship it", from_env="dev")
        )
        assert out["retired"] is None and merged == []

    async def test_asking_for_it_ends_the_source_and_its_branch(self, merged):
        out = await er.merge_into_environment(
            _Request(),
            ORG,
            "prod",
            er.MergeBody(message="ship it", from_env="dev", retire_source=True),
        )
        assert merged == [(ORG, "dev", True)]
        assert out["retired"] == {
            "retired": "dev",
            "branch_deleted": True,
            "remote_branch_deleted": None,
        }

    async def test_a_dry_run_ends_nothing(self, merged):
        out = await er.merge_into_environment(
            _Request(),
            ORG,
            "prod",
            er.MergeBody(message="ship it", from_env="dev", retire_source=True, dry_run=True),
        )
        assert out["applied"] is False and out["retired"] is None and merged == []

    async def test_the_retirement_is_audited_against_the_environment_it_ended(self, merged, wired):
        await er.merge_into_environment(
            _Request(),
            ORG,
            "prod",
            er.MergeBody(message="ship it", from_env="dev", retire_source=True),
        )
        (_org, _actor, action, name, detail) = wired["audit"][-1]
        assert (action, name) == ("environment.retired", "dev")
        assert detail == {"retired": "dev", "branch_deleted": True, "remote_branch_deleted": None}

    async def test_refusing_to_retire_prod_arrives_as_a_conflict(self, merged, monkeypatch):
        async def retire_environment(pool, admin_db, org_id, name, *, drop_branch):
            raise er.RetirementError("prod cannot be retired")

        monkeypatch.setattr(er, "retire_environment", retire_environment)
        with pytest.raises(ApiError) as exc:
            await er.merge_into_environment(
                _Request(),
                ORG,
                "dev",
                er.MergeBody(message="ship it", from_env="prod", retire_source=True),
            )
        assert exc.value.status_code == 409

    async def test_an_approved_merge_retires_what_the_request_carried(self, merged, monkeypatch):
        async def get_request(admin_db, org_id, request_id):
            return _row(retire_source=True)

        async def decide(admin_db, tenant_db, org_id, request_id, **kw):
            return _row(state="applied", retire_source=True)

        monkeypatch.setattr(env_approvals, "get_request", get_request)
        monkeypatch.setattr(env_approvals, "decide", decide)
        out = await er.decide_merge_request(_Request(), ORG, 7, er.DecideBody(approve=True))
        assert merged == [(ORG, "dev", True)]
        assert out["retired"] == {
            "retired": "dev",
            "branch_deleted": True,
            "remote_branch_deleted": None,
        }

    async def test_a_rejected_request_retires_nothing(self, merged, monkeypatch):
        async def get_request(admin_db, org_id, request_id):
            return _row(retire_source=True)

        async def decide(admin_db, tenant_db, org_id, request_id, **kw):
            return _row(state="rejected", retire_source=True)

        monkeypatch.setattr(env_approvals, "get_request", get_request)
        monkeypatch.setattr(env_approvals, "decide", decide)
        out = await er.decide_merge_request(_Request(), ORG, 7, er.DecideBody(approve=False))
        assert out["retired"] is None and merged == []

    async def test_the_request_carries_the_flag_to_the_approver(self, merged, monkeypatch):
        """It is proposed, not performed: a pending request must leave its source alive."""
        seen: list[dict] = []

        async def is_protected(admin_db, org_id, name, members):
            return True

        async def request_merge(admin_db, db, org_id, **kw):
            seen.append(kw)
            return _row(retire_source=True)

        monkeypatch.setattr(env_approvals, "is_protected", is_protected)
        monkeypatch.setattr(env_approvals, "request_merge", request_merge)
        out = await er.merge_into_environment(
            _Request(),
            ORG,
            "prod",
            er.MergeBody(message="ship it", from_env="dev", retire_source=True),
        )
        assert seen[0]["retire_source"] is True
        assert out["applied"] is False and merged == []

    async def test_a_blank_comment_is_refused(self, merged, wired):
        with pytest.raises(ApiError) as exc:
            await er.merge_into_environment(
                _Request(), ORG, "prod", er.MergeBody(from_env="dev", message="   ")
            )
        assert exc.value.status_code == 400
        assert exc.value.code == "environments.message_required"
        assert wired["squash"] == [] and merged == []

    async def test_the_comment_reaches_the_commit_the_merge_lands(self, merged, wired):
        """REQ-1545/REQ-1550: one squash, and the operator's sentence is what it says."""
        await er.merge_into_environment(
            _Request(), ORG, "prod", er.MergeBody(from_env="dev", message="adds the tag domain")
        )
        assert wired["squash"] == [(ORG, "dev", "prod", "adds the tag domain")]

    async def test_deleting_the_remote_branch_alone_is_refused(self, merged, wired):
        """REQ-1550: the remote copy is what survives a lost volume (REQ-1546), so deleting it
        while the environment stands is not an option the API offers."""
        with pytest.raises(ApiError) as exc:
            await er.merge_into_environment(
                _Request(),
                ORG,
                "prod",
                er.MergeBody(from_env="dev", message="ship it", retire_remote=True),
            )
        assert exc.value.status_code == 400
        assert exc.value.code == "environments.remote_without_local"
        assert merged == []
