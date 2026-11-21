# Copyright (c) 2026 Kenneth Stott
# Canary: 4500a808-3aae-40a5-b97c-63d7a1893248
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every environment operation from the command line (REQ-1498).

The command line is a thin client of the admin API the UI calls, so no rule is restated here: what
is under test is the contract a script has with it -- which endpoint a command reaches, what it
sends, what it prints as text and as JSON, and what the EXIT CODE says, because a pipeline reads the
exit code and nothing else.
"""

# Requirements: REQ-1498

from __future__ import annotations

import json

import pytest

from provisa import cli

REPORT = {
    "source_env": "dev",
    "target_env": "prod",
    "mode": "merge",
    "removals": False,
    "added": 1,
    "changed": 2,
    "unchanged": 7,
    "removed": 0,
    "paths": {
        "added": ["metrics/revenue.yaml"],
        "changed": ["metrics/margin.yaml", "metrics/churn.yaml"],
        "removed": [],
    },
    "settings": {
        "table": "org_settings",
        "added": [],
        "changed": [],
        "removed": [],
        "unchanged": 0,
    },
    "selection": None,
    "pulled_in": [],
    "base": "abc",
    "compared": True,
    "conflicts": [],
}


@pytest.fixture
def api(monkeypatch):
    """The API, answering whatever a test queues and recording what was asked of it."""
    seen: list[tuple[str, str, dict | None]] = []
    answers: list = []

    def _api_call(args, method, path, body=None):
        seen.append((method, path, body))
        answer = answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    monkeypatch.setattr(cli, "_api_call", _api_call)
    return seen, answers


def _run(*argv: str) -> int:
    return cli.main(["env", *argv])


class TestListing:
    ENVS = {
        "environments": [
            {
                "name": "prod",
                "deployed_sha": "a" * 40,
                "protected": True,
                "parent": None,
                "data_mode": None,
                "expires_at": None,
            },
            {
                "name": "dev",
                "deployed_sha": None,
                "protected": False,
                "parent": "prod",
                "data_mode": "inherit",
                "expires_at": None,
            },
        ]
    }

    def test_it_lists_the_orgs_environments(self, api, capsys):
        seen, answers = api
        answers.append(self.ENVS)
        assert _run("list", "--org", "acme") == 0
        assert seen == [("GET", "/admin/orgs/acme/environments", None)]
        out = capsys.readouterr().out
        assert "prod" in out and "dev" in out and "aaaaaaaaaaaa" in out
        assert "from prod" in out and "data inherit" in out

    def test_json_is_the_answer_itself(self, api, capsys):
        _, answers = api
        answers.append(self.ENVS)
        assert _run("list", "--org", "acme", "--json") == 0
        assert json.loads(capsys.readouterr().out) == self.ENVS

    def test_show_gives_one_environments_current_build(self, api, capsys):
        _, answers = api
        answers.append(self.ENVS)
        assert _run("show", "--org", "acme", "--env", "prod", "--json") == 0
        assert json.loads(capsys.readouterr().out)["deployed_sha"] == "a" * 40

    def test_show_of_an_environment_the_org_lacks_exits_non_zero(self, api, capsys):
        _, answers = api
        answers.append(self.ENVS)
        assert _run("show", "--org", "acme", "--env", "nope") == 1
        assert "environments.unknown" in capsys.readouterr().err


class TestCreateAndDelete:
    def test_create_sends_what_the_ui_sends(self, api):
        seen, answers = api
        answers.append({"environment": {"name": "feature"}, "copy": REPORT})
        code = _run(
            "create", "--org", "acme", "--env", "feature", "--from", "dev", "--data-mode", "inherit"
        )
        assert code == 0
        assert seen == [
            (
                "POST",
                "/admin/orgs/acme/environments",
                {
                    "name": "feature",
                    "from_env": "dev",
                    "data_mode": "inherit",
                    "mutation_handling": "refused",
                    "expires_at": None,
                },
            )
        ]

    def test_delete_names_the_branches_only_when_asked(self, api):
        seen, answers = api
        answers.extend([{"retired": True}, {"retired": True}])
        assert _run("delete", "--org", "acme", "--env", "feature") == 0
        assert _run("delete", "--org", "acme", "--env", "feature", "--delete-branch") == 0
        assert seen[0] == (
            "DELETE",
            "/admin/orgs/acme/environments/feature?delete_branch=false&delete_remote_branch=false",
            None,
        )
        assert "delete_branch=true" in seen[1][1]


class TestDiffAndMerge:
    def test_a_diff_reads_and_cannot_write(self, api, capsys):
        seen, answers = api
        answers.append({"report": REPORT, "applied": False, "requires_approval": False})
        assert _run("diff", "--org", "acme", "--from", "dev", "--into", "prod") == 0
        assert seen == [
            (
                "GET",
                "/admin/orgs/acme/environments/prod/merge-preview?from_env=dev&removals=false",
                None,
            )
        ]
        out = capsys.readouterr().out
        assert (
            "added 1" in out and "changed 2" in out and "removed 0" in out and "unchanged 7" in out
        )
        assert "revenue" in out

    def test_a_dry_run_merge_prints_the_report_and_applies_nothing(self, api, capsys):
        seen, answers = api
        answers.append({"report": REPORT, "applied": False, "requires_approval": False})
        code = _run("merge", "--org", "acme", "--from", "dev", "--into", "prod", "--dry-run")
        assert code == 0
        method, path, body = seen[0]
        assert (method, path) == ("POST", "/admin/orgs/acme/environments/prod/merge")
        assert body["dry_run"] is True and body["from_env"] == "dev"
        assert "PLANNED" in capsys.readouterr().out

    def test_a_merge_applies_under_the_same_rules(self, api, capsys):
        seen, answers = api
        answers.append({"report": REPORT, "applied": True, "requires_approval": False})
        code = _run(
            "merge", "--org", "acme", "--from", "dev", "--into", "prod",
            "--message", "fix margin", "--removals",
        )  # fmt: skip
        assert code == 0
        assert seen[0][2] == {
            "from_env": "dev",
            "removals": True,
            "selection": None,
            "dry_run": False,
            "retire_source": False,
            "retire_remote": False,
            "message": "fix margin",
        }
        assert "APPLIED" in capsys.readouterr().out

    def test_a_scoped_merge_names_its_objects_and_what_they_pulled_in(self, api, capsys):
        # REQ-1506: the selection travels as paths, and the report says what it required.
        seen, answers = api
        scoped = {
            **REPORT,
            "selection": ["metrics/revenue.yaml"],
            "pulled_in": ["sales/domain.yaml"],
        }
        answers.extend(
            [
                {"report": scoped, "applied": False, "requires_approval": False},
                {"report": scoped, "applied": True, "requires_approval": False},
            ]
        )
        _run("diff", "--org", "acme", "--from", "dev", "--into", "prod", "--select", "metrics/revenue.yaml")  # fmt: skip
        assert seen[0][1].endswith("&select=metrics%2Frevenue.yaml")
        _run(
            "merge", "--org", "acme", "--from", "dev", "--into", "prod", "--message", "m",
            "--select", "metrics/revenue.yaml",
        )  # fmt: skip
        assert seen[1][2]["selection"] == ["metrics/revenue.yaml"]
        assert "sales/domain.yaml (required by the selection)" in capsys.readouterr().out

    def test_json_emits_the_same_report(self, api, capsys):
        _, answers = api
        answer = {"report": REPORT, "applied": False, "requires_approval": False}
        answers.append(answer)
        _run("merge", "--org", "acme", "--from", "dev", "--into", "prod", "--dry-run", "--json")
        assert json.loads(capsys.readouterr().out) == answer

    def test_a_proposal_is_not_a_merge(self, api, capsys):
        _, answers = api
        answers.append(
            {
                "request": {"id": 4, "target_env": "prod", "report": REPORT},
                "applied": False,
                "requires_approval": True,
            }
        )
        code = _run("merge", "--org", "acme", "--from", "dev", "--into", "prod", "--message", "m")
        assert code == 2
        assert "PROPOSED" in capsys.readouterr().out

    def test_a_refused_merge_exits_non_zero_naming_the_rule(self, api, capsys):
        _, answers = api
        answers.append(
            cli.ApiRefused(
                "POST",
                "/admin/orgs/acme/environments/prod/merge",
                400,
                json.dumps(
                    {
                        "detail": "A merge needs a comment.",
                        "code": "environments.message_required",
                        "params": {"org": "acme", "env": "prod"},
                    }
                ),
            )
        )
        code = _run("merge", "--org", "acme", "--from", "dev", "--into", "prod")
        assert code == 1
        err = capsys.readouterr().err
        assert "environments.message_required" in err and "A merge needs a comment." in err

    def test_a_refusal_under_json_is_json_too(self, api, capsys):
        _, answers = api
        body = {"detail": "nope", "code": "environments.pinned", "params": {}}
        answers.append(cli.ApiRefused("POST", "/x", 403, json.dumps(body)))
        code = _run("merge", "--org", "acme", "--from", "dev", "--into", "prod", "--json")
        assert code == 1
        assert json.loads(capsys.readouterr().out) == {"refused": {"status": 403, **body}}


class TestRevert:
    MOVED = {
        "report": {
            "env": "dev",
            "ref": "b" * 40,
            "added": [],
            "changed": ["sales/domain.yaml"],
            "removed": [],
            "unchanged": 3,
        },
        "deployed_sha": "b" * 40,
        "redo_sha": "c" * 40,
        "refreshed": "recompiled",
    }

    def test_revert_steps_back_one_build(self, api, capsys):
        seen, answers = api
        answers.append(self.MOVED)
        assert _run("revert", "--org", "acme", "--env", "dev") == 0
        assert seen == [("POST", "/admin/orgs/acme/environments/dev/undo", {})]
        assert "bbbbbbbbbbbb" in capsys.readouterr().out

    def test_revert_to_a_named_build_is_a_deploy_of_it(self, api):
        seen, answers = api
        answers.append(
            {"report": self.MOVED["report"], "applied": True, "requires_approval": False}
        )
        assert _run("revert", "--org", "acme", "--env", "dev", "--to", "abc1234") == 0
        assert seen == [
            (
                "POST",
                "/admin/orgs/acme/environments/dev/deploy",
                {"ref": "abc1234", "dry_run": False, "message": ""},
            )
        ]

    def test_a_dry_run_revert_reports_and_applies_nothing(self, api, capsys):
        # REQ-1496: the removals a revert would perform are named before it is taken.
        seen, answers = api
        report = {**self.MOVED["report"], "removed": ["sales/tables/Order.yaml"]}
        answers.append({"report": report, "applied": False, "requires_approval": False})
        assert _run("revert", "--org", "acme", "--env", "dev", "--dry-run") == 0
        assert seen == [("POST", "/admin/orgs/acme/environments/dev/undo?dry_run=true", {})]
        out = capsys.readouterr().out
        assert "PLANNED" in out and "R sales/tables/Order.yaml" in out

    def test_redo_steps_forward_again(self, api):
        seen, answers = api
        answers.append(self.MOVED)
        assert _run("redo", "--org", "acme", "--env", "dev") == 0
        assert seen[0][1].endswith("/dev/redo")

    def test_a_revert_of_a_protected_environment_is_a_proposal(self, api, capsys):
        _, answers = api
        answers.append(
            {
                "request": {"id": 9, "target_env": "prod", "report": self.MOVED["report"]},
                "applied": False,
                "requires_approval": True,
            }
        )
        assert _run("revert", "--org", "acme", "--env", "prod") == 2
        assert "PROPOSED" in capsys.readouterr().out


class TestTheRemote:
    def test_push_and_pull_reach_their_endpoints(self, api):
        seen, answers = api
        sync = {"local": "a", "remote": "a", "ahead": 0, "behind": 0, "diverged": False}
        answers.extend([{"pushed": "a" * 40, "sync": sync}, {"applied": False, "sync": sync}])
        assert _run("push", "--org", "acme", "--env", "dev") == 0
        assert _run("pull", "--org", "acme", "--env", "dev") == 0
        assert [s[1] for s in seen] == [
            "/admin/orgs/acme/environments/dev/push",
            "/admin/orgs/acme/environments/dev/pull",
        ]

    def test_deploy_and_fetch_speak_json_too(self, api, capsys):
        _, answers = api
        fetched = {"branches": {"main": "a" * 40}}
        answers.append(fetched)
        assert _run("fetch", "--org", "acme", "--json") == 0
        assert json.loads(capsys.readouterr().out) == fetched


class TestRebuild:
    def test_a_rebuild_that_cleared_the_drift_succeeds(self, api, capsys):
        seen, answers = api
        answers.append({"sha": "a" * 40, "drifted": False})
        assert _run("rebuild", "--org", "acme", "--env", "dev") == 0
        assert seen == [("POST", "/admin/orgs/acme/environments/dev/rebuild", {})]
        assert "aaaaaaaaaaaa" in capsys.readouterr().out

    def test_one_that_did_not_land_exits_non_zero(self, api, capsys):
        _, answers = api
        answers.append({"sha": None, "drifted": True})
        assert _run("rebuild", "--org", "acme", "--env", "dev") == 1
        assert "still drifted" in capsys.readouterr().err


class TestTheOrgIsNamed:
    """REQ-1327, REQ-1235: an environment route acts in the org its path names, and the request
    must be bound to that org. A session token names no org, so the CLI names it with the
    X-Org-Provisa header, as the UI does, rather than relying on a token issued for the org."""

    def test_every_call_sends_the_org_it_acts_in(self, monkeypatch, capsys):
        import io
        import json as _json
        import urllib.request

        sent: list[urllib.request.Request] = []

        def _urlopen(req, timeout=None):
            sent.append(req)
            return io.BytesIO(_json.dumps({"environments": []}).encode())

        monkeypatch.setattr(urllib.request, "urlopen", _urlopen)
        assert _run("list", "--org", "acme", "--token", "t", "--api", "http://x.invalid") == 0
        assert len(sent) == 1
        assert sent[0].full_url == "http://x.invalid/admin/orgs/acme/environments"
        assert sent[0].get_header("X-org-provisa") == "acme"
        assert sent[0].get_header("Authorization") == "Bearer t"
