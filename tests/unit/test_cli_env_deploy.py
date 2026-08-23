# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2b6d40-71ac-4e93-b5c8-1d90a3f7e264
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The command a CI pipeline runs to deploy (REQ-1496, REQ-1554).

Provisa polices no deployment process of its own: the git host's rules decide what may reach a
branch, and this command is how the pipeline that watched those rules pass tells one NAMED control
plane to ingest the result. So what is under test is the pipeline's contract with it -- which
control plane was addressed, what was sent, and what the EXIT CODE said -- because a pipeline reads
the exit code and nothing else, and a proposal reported as a success is a release that never
happened.
"""

# Requirements: REQ-1496, REQ-1504, REQ-1541, REQ-1554

from __future__ import annotations

import json


from provisa import cli

APPLIED = {
    "applied": True,
    "requires_approval": False,
    "report": {
        "env": "prod",
        "ref": "abc1234",
        "added": ["a.yaml"],
        "changed": [],
        "removed": [],
        "unchanged": 3,
    },
}
PROPOSED = {
    "applied": False,
    "requires_approval": True,
    "request": {
        "id": "req-1",
        "target_env": "prod",
        "report": {
            "env": "prod",
            "ref": "abc1234",
            "added": [],
            "changed": ["b.yaml"],
            "removed": [],
            "unchanged": 3,
        },
    },
}


def _run(argv: list[str]) -> int:
    return cli.main(argv)


def test_deploy_addresses_the_named_control_plane(monkeypatch, capsys):
    seen: list[dict] = []
    monkeypatch.setattr(
        cli,
        "_api_call",
        lambda a, m, p, b=None: (seen.append((m, p, b, a.api, a.token)), APPLIED)[1],
    )
    code = _run(
        [
            "env",
            "deploy",
            "--org",
            "acme",
            "--env",
            "prod",
            "--ref",
            "origin/main",
            "--api",
            "https://cp.example.com",
            "--token",
            "t0ken",
        ]
    )
    assert code == 0
    method, path, body, api, token = seen[0]
    assert method == "POST"
    assert path == "/admin/orgs/acme/environments/prod/deploy"
    assert body == {"ref": "origin/main", "dry_run": False, "seed": False, "message": ""}
    # Which control plane ingests is the invocation's to say -- there is no discovery and no default
    # target beyond the one the environment names (REQ-1554).
    assert api == "https://cp.example.com"
    assert token == "t0ken"


def test_a_proposal_is_not_a_deployment(monkeypatch, capsys):
    """A protected target answers with a request, and the pipeline must not read that as a release."""
    monkeypatch.setattr(cli, "_api_call", lambda a, m, p, b=None: PROPOSED)
    code = _run(["env", "deploy", "--org", "acme", "--env", "prod", "--ref", "main"])
    assert code == 2
    assert "PROPOSED" in capsys.readouterr().out


def test_a_dry_run_that_applied_nothing_still_succeeds(monkeypatch, capsys):
    """A pipeline's plan step reports what WOULD change; writing nothing is its correct outcome."""
    planned = {"applied": False, "requires_approval": False, "report": APPLIED["report"]}
    sent: list[dict] = []
    monkeypatch.setattr(cli, "_api_call", lambda a, m, p, b=None: (sent.append(b), planned)[1])
    code = _run(["env", "deploy", "--org", "acme", "--env", "prod", "--ref", "main", "--dry-run"])
    assert code == 0
    assert sent[0]["dry_run"] is True
    assert "PLANNED" in capsys.readouterr().out


def test_the_environment_and_the_control_plane_come_from_the_environment(monkeypatch):
    """With no --api and no --token, the values are the pipeline's environment, not a guess."""
    import io
    import urllib.request

    monkeypatch.setenv("PROVISA_API_URL", "https://from-env.example.com")
    monkeypatch.setenv("PROVISA_API_TOKEN", "env-token")
    captured: dict = {}

    class Answer:
        """Only what ``json.load`` needs of a response, since that is all the CLI does with one."""

        def __init__(self, payload: dict) -> None:
            self._buf = io.BytesIO(json.dumps(payload).encode())

        def __enter__(self):
            return self._buf

        def __exit__(self, *exc):
            return False

    def fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["auth"] = req.get_header("Authorization")
        captured["timeout"] = timeout
        return Answer(APPLIED)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert _run(["env", "deploy", "--org", "acme", "--env", "prod", "--ref", "main"]) == 0
    assert (
        captured["url"] == "https://from-env.example.com/admin/orgs/acme/environments/prod/deploy"
    )
    assert captured["auth"] == "Bearer env-token"
    assert captured["timeout"] == 300


def test_fetch_brings_the_remote_back_for_a_deploy_to_name(monkeypatch, capsys):
    """REQ-1541: the step before the deploy, so the ref a pipeline names is the merged one."""
    seen: list[tuple] = []
    monkeypatch.setattr(
        cli,
        "_api_call",
        lambda a, m, p, b=None: (
            seen.append((m, p)),
            {"branches": {"main": "abc1234def567", "release": "0f0f0f0f0f0f"}},
        )[1],
    )
    assert _run(["env", "fetch", "--org", "acme"]) == 0
    assert seen[0] == ("POST", "/admin/orgs/acme/environments/-/repo-integration/fetch")
    out = capsys.readouterr().out
    assert "origin/main  abc1234def56" in out
    assert "origin/release  0f0f0f0f0f0f" in out
