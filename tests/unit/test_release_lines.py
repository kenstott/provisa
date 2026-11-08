# Copyright (c) 2026 Kenneth Stott
# Canary: 6152a954-ebf3-4ee2-8ea9-5c87b8cdc9a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The CI side of the release lines (REQ-1989, amendments of 2026-11-08): what dev runs, the
nightly alpha, an alpha shipping with its failures listed, and the merge the lead runs after
publish. Workflows are read as YAML; the scripts run against real temporary Git repositories."""

# Requirements: REQ-1989
from __future__ import annotations

import importlib.util
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS = _ROOT / ".github" / "workflows"
_NIGHTLY = "alpha-nightly.yml"
_NEW_YORK = ZoneInfo("America/New_York")

_UNIVERSAL = {
    "unit-tests.yml",
    "ui-unit-tests.yml",
    "lint.yml",
    "cve-audit.yml",
    "docker-image.yml",
    "build-dmg.yml",
}
_FULL_SUITES = {
    "integration-suite.yml",
    "ui-e2e-core.yml",
    "ui-e2e-trino.yml",
    "ui-e2e-swap-amd64.yml",
    "conformance-bolt.yml",
    "conformance-cypher.yml",
    "conformance-drivers.yml",
    "conformance-flight-sql.yml",
    "conformance-functions.yml",
    "conformance-graphql.yml",
    "conformance-pgwire.yml",
}


def _module(name: str):
    spec = importlib.util.spec_from_file_location(name, _ROOT / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load(name: str) -> dict:
    return yaml.safe_load((_WORKFLOWS / name).read_text())


def _on(workflow: dict) -> dict:
    return workflow[True]  # YAML reads the bare key `on` as true


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _commit(repo: Path, subject: str, **files: str) -> None:
    for name, text in files.items():
        (repo / name).write_text(text)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "--allow-empty", "-m", subject)


def _remote(origin: Path, ref: str) -> str:
    return subprocess.run(
        ["git", "-C", str(origin), "rev-parse", "-q", "--verify", ref],
        capture_output=True,
        text=True,
    ).stdout.strip()


def _output(text: str) -> dict[str, str]:
    return dict(line.split("=", 1) for line in text.splitlines() if "=" in line)


# ── 1 and 6. The gate: an alpha reports, a beta or a production release refuses ─────────────


def _states(monkeypatch, gate, bad: dict[str, str]) -> None:
    monkeypatch.setattr(
        gate, "run_state", lambda workflow, branch, sha: bad.get(workflow, "completed success u")
    )


_GATE_ARGS = ["--branch", "release/v", "--sha", "abc"]


def test_an_alpha_is_gated_on_every_suite_except_requirements_ci():
    gate = _module("release_gate")
    assert set(gate.gates("v0.1.0-alpha.9")) == _UNIVERSAL | _FULL_SUITES


@pytest.mark.parametrize("version", ["v0.2.0", "v0.3.0-rc.1", "v0.3.0-beta.2"])
def test_a_beta_or_production_release_keeps_the_requirements_gate(version):
    gate = _module("release_gate")
    assert set(gate.gates(version)) == _UNIVERSAL | {"requirements-ci.yml"}


def test_an_alpha_with_failures_is_cut_and_every_failure_is_listed(tmp_path, monkeypatch, capsys):
    gate = _module("release_gate")
    bad = {"unit-tests.yml": "completed failure http://u", "ui-e2e-core.yml": "never run  "}
    _states(monkeypatch, gate, bad)
    out = tmp_path / "failures.md"
    assert gate.main(["--version", "v0.1.0-alpha.9", *_GATE_ARGS, "--out", str(out)]) == 0
    listing = out.read_text()
    assert "unit-tests.yml" in listing and "completed failure http://u" in listing
    assert "ui-e2e-core.yml" in listing and "never run" in listing
    assert "lint.yml" not in listing
    assert "::warning::" in capsys.readouterr().out


def test_an_alpha_with_nothing_failing_lists_nothing(tmp_path, monkeypatch):
    gate = _module("release_gate")
    _states(monkeypatch, gate, {})
    out = tmp_path / "failures.md"
    assert gate.main(["--version", "v0.1.0-alpha.9", *_GATE_ARGS, "--out", str(out)]) == 0
    assert not out.exists()


@pytest.mark.parametrize("version", ["v0.2.0", "v0.3.0-rc.1", "v0.3.0-beta.2"])
def test_a_failure_refuses_a_beta_or_production_cut_by_name(version, tmp_path, monkeypatch, capsys):
    gate = _module("release_gate")
    _states(monkeypatch, gate, {"build-dmg.yml": "in_progress  http://u"})
    out = tmp_path / "failures.md"
    assert gate.main(["--version", version, *_GATE_ARGS, "--out", str(out)]) == 2
    assert "::error::build-dmg.yml" in capsys.readouterr().out
    assert not out.exists()


def test_a_red_requirements_ci_does_not_touch_an_alpha(tmp_path, monkeypatch):
    gate = _module("release_gate")
    _states(monkeypatch, gate, {"requirements-ci.yml": "completed failure http://u"})
    out = tmp_path / "failures.md"
    assert gate.main(["--version", "v0.1.0-alpha.9", *_GATE_ARGS, "--out", str(out)]) == 0
    assert not out.exists()


def test_a_red_requirements_ci_refuses_a_production_cut(tmp_path, monkeypatch):
    gate = _module("release_gate")
    _states(monkeypatch, gate, {"requirements-ci.yml": "completed failure http://u"})
    assert gate.main(["--version", "v0.2.0", *_GATE_ARGS, "--out", str(tmp_path / "f")]) == 2


def test_a_lookup_that_cannot_answer_stops_an_alpha_too(tmp_path, monkeypatch):
    # "gh could not say" is not "never run": a listing built on it would be false.
    gate = _module("release_gate")

    def broken(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, "", "HTTP 502")

    monkeypatch.setattr(gate.subprocess, "run", broken)
    with pytest.raises(gate.LookupFailed, match="HTTP 502"):
        gate.run_state("lint.yml", "release/v", "abc")


def _cut_steps() -> list[dict]:
    return _load("release-cut.yml")["jobs"]["cut"]["steps"]


def _cut_index(text: str) -> int:
    return next(i for i, step in enumerate(_cut_steps()) if text in step.get("run", ""))


def test_the_cut_gates_through_the_script_before_the_notes_and_the_tag():
    gate = _cut_index("scripts/release_gate.py")
    run = _cut_steps()[gate]["run"]
    assert run.startswith("set -euo pipefail")
    for argument in ('--version "$VERSION"', '--branch "$GITHUB_REF_NAME"', '--sha "$GITHUB_SHA"'):
        assert argument in run
    assert "continue-on-error" not in _cut_steps()[gate]
    assert "if" not in _cut_steps()[gate]
    assert _cut_index('gen_release_notes.py --from "$PREVIOUS"') > gate
    assert _cut_index('git tag -a "$VERSION"') > gate


def test_the_cut_writes_an_alphas_failures_into_its_notes_before_the_commit():
    listed = _cut_index('>> "docs/releases/$VERSION.md"; fi')
    assert "gate-failures.md" in _cut_steps()[listed]["run"]
    assert _cut_index('gen_release_notes.py --from "$PREVIOUS"') < listed
    assert listed < _cut_index('git tag -a "$VERSION"')


# ── 2. The nightly alpha ────────────────────────────────────────────────────────────────────


def test_the_nightly_fires_at_1700_new_york_in_both_offsets():
    crons = {entry["cron"] for entry in _on(_load(_NIGHTLY))["schedule"]}
    assert crons == {"0 21 * * *", "0 22 * * *"}
    assert "workflow_dispatch" in _on(_load(_NIGHTLY))


@pytest.mark.parametrize(
    ("cron", "when", "expected"),
    [
        ("0 21 * * *", datetime(2026, 7, 1, 17, 4, tzinfo=_NEW_YORK), True),
        ("0 22 * * *", datetime(2026, 7, 1, 18, 4, tzinfo=_NEW_YORK), False),
        ("0 22 * * *", datetime(2026, 12, 1, 17, 4, tzinfo=_NEW_YORK), True),
        ("0 21 * * *", datetime(2026, 12, 1, 16, 4, tzinfo=_NEW_YORK), False),
        # GitHub starts a scheduled run late under load; the cron that fired still decides.
        ("0 21 * * *", datetime(2026, 7, 1, 18, 50, tzinfo=_NEW_YORK), True),
    ],
)
def test_only_the_cron_of_the_current_offset_runs(cron, when, expected):
    assert _module("release_nightly").due(cron, when) is expected


def test_the_nightly_may_push_and_dispatch_and_one_runs_at_a_time():
    workflow = _load(_NIGHTLY)
    assert workflow["permissions"] == {"contents": "write", "actions": "write"}
    assert workflow["concurrency"] == {"group": "release-nightly", "cancel-in-progress": False}


def _nightly_steps() -> list[dict]:
    return _load(_NIGHTLY)["jobs"]["start"]["steps"]


def test_the_nightly_passes_the_cron_that_fired_and_full_history():
    steps = _nightly_steps()
    assert steps[0]["uses"].startswith("actions/checkout@")
    assert steps[0]["with"]["fetch-depth"] == 0
    branch = next(s for s in steps if "release_nightly.py" in s.get("run", ""))
    assert branch["id"] == "branch"
    assert branch["env"]["SCHEDULE"] == "${{ github.event.schedule }}"
    assert '--schedule "$SCHEDULE" >> "$GITHUB_OUTPUT"' in branch["run"]


def test_the_nightly_starts_every_gated_suite_and_the_dry_run_explicitly():
    # A push by the workflow's token starts nothing, so each is dispatched on the new branch.
    start = next(s for s in _nightly_steps() if "gh workflow run" in s.get("run", ""))
    run = start["run"]
    assert start["if"] == "steps.branch.outputs.version != ''"
    assert run.startswith("set -euo pipefail")
    assert 'scripts/release_gate.py --version "$VERSION" --list' in run
    assert 'gh workflow run "$workflow" --ref "release/$VERSION"' in run
    assert (
        'gh workflow run build-dmg.yml --ref "release/$VERSION" '
        '-f version="$VERSION" -f publish=false'
    ) in run
    assert "publish=true" not in run


@pytest.mark.parametrize("name", sorted(_FULL_SUITES | _UNIVERSAL))
def test_every_workflow_the_nightly_starts_can_be_dispatched_without_required_inputs(name):
    dispatch = _on(_load(name))["workflow_dispatch"] or {}
    required = [
        key
        for key, spec in (dispatch.get("inputs") or {}).items()
        if spec.get("required") and "default" not in spec
    ]
    assert required == ([] if name != "build-dmg.yml" else ["version"])


_VERSION_5 = "v0.1.0-alpha.5"


@pytest.fixture
def lines(tmp_path) -> tuple[Path, Path]:
    """origin with main at alpha.5, and dev one landing ahead of it; a clone of it."""
    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    work = tmp_path / "work"
    _git(tmp_path, "clone", "-q", str(origin), str(work))
    _git(work, "config", "user.email", "t@example.test")
    _git(work, "config", "user.name", "t")
    _git(work, "checkout", "-q", "-b", "main")
    _commit(work, "first", **{"a.txt": "1\n"})
    _git(work, "tag", "-a", _VERSION_5, "-m", _VERSION_5)
    _git(work, "checkout", "-q", "-b", "dev")
    _commit(work, "feat: landed", **{"b.txt": "2\n"})
    _git(work, "push", "-q", "origin", "main", "dev", f"refs/tags/{_VERSION_5}")
    return origin, work


def _land_on_dev(work: Path, name: str) -> None:
    _git(work, "checkout", "-q", "dev")
    _commit(work, f"feat: {name}", **{name: "x\n"})
    _git(work, "push", "-q", "origin", "dev")


def test_a_moved_dev_gets_a_release_branch_from_main_with_dev_merged(lines, capsys):
    origin, work = lines
    nightly = _module("release_nightly")
    main_tip = _remote(origin, "refs/heads/main")
    assert nightly.main(["--repo", str(work)]) == 0
    assert _output(capsys.readouterr().out) == {"version": "v0.1.0-alpha.6"}
    branch = "refs/heads/release/v0.1.0-alpha.6"
    assert _git(origin, "rev-parse", f"{branch}^1") == main_tip
    assert _git(origin, "rev-parse", f"{branch}^2") == _remote(origin, "refs/heads/dev")
    assert _git(origin, "show", f"{branch}:b.txt") == "2"
    assert _remote(origin, "refs/heads/main") == main_tip  # main only ever takes a release


def _skipped(nightly, work, capsys, origin) -> None:
    before = _git(origin, "for-each-ref")
    assert nightly.main(["--repo", str(work)]) == 0
    assert _output(capsys.readouterr().out) == {"skip": "true"}
    assert _git(origin, "for-each-ref") == before


def test_a_dev_with_nothing_since_the_last_alpha_is_skipped(lines, capsys):
    origin, work = lines
    nightly = _module("release_nightly")
    # The last alpha holds dev: it was tagged on a commit that merged it.
    _git(work, "checkout", "-q", "dev")
    _git(work, "tag", "-a", "v0.1.0-alpha.6", "-m", "x")
    _git(work, "push", "-q", "origin", "refs/tags/v0.1.0-alpha.6")
    _skipped(nightly, work, capsys, origin)


def test_the_merge_back_of_a_release_does_not_count_as_dev_moving(lines, capsys):
    origin, work = lines
    nightly = _module("release_nightly")
    # alpha.6 shipped; the merge to dev adds a commit dev did not have but changes nothing there.
    _git(work, "checkout", "-q", "-b", "release/v0.1.0-alpha.6", "dev")
    _commit(work, "notes", **{"notes.md": "n\n"})
    _git(work, "tag", "-a", "v0.1.0-alpha.6", "-m", "x")
    _git(work, "checkout", "-q", "dev")
    _git(work, "merge", "-q", "--no-ff", "-m", "merge", "v0.1.0-alpha.6")
    _git(work, "push", "-q", "origin", "dev", "refs/tags/v0.1.0-alpha.6")
    _skipped(nightly, work, capsys, origin)


def test_a_landing_after_the_last_alpha_starts_the_next_one(lines, capsys):
    origin, work = lines
    nightly = _module("release_nightly")
    _git(work, "checkout", "-q", "dev")
    _git(work, "tag", "-a", "v0.1.0-alpha.6", "-m", "x")
    _git(work, "push", "-q", "origin", "refs/tags/v0.1.0-alpha.6")
    _land_on_dev(work, "c.txt")
    assert nightly.main(["--repo", str(work)]) == 0
    assert _output(capsys.readouterr().out) == {"version": "v0.1.0-alpha.7"}
    assert _remote(origin, "refs/heads/release/v0.1.0-alpha.7")


def test_an_open_release_branch_holding_dev_means_nothing_new(lines, capsys):
    origin, work = lines
    nightly = _module("release_nightly")
    _git(work, "push", "-q", "origin", "dev:refs/heads/release/v0.1.0-alpha.6")
    _skipped(nightly, work, capsys, origin)


def test_an_open_release_branch_takes_its_number_from_the_next_one(lines, capsys):
    origin, work = lines
    nightly = _module("release_nightly")
    _git(work, "push", "-q", "origin", "dev:refs/heads/release/v0.1.0-alpha.6")
    _land_on_dev(work, "c.txt")
    assert nightly.main(["--repo", str(work)]) == 0
    assert _output(capsys.readouterr().out) == {"version": "v0.1.0-alpha.7"}


def test_a_scheduled_run_outside_its_offset_does_nothing(lines, capsys, monkeypatch):
    origin, work = lines
    nightly = _module("release_nightly")
    monkeypatch.setattr(nightly, "due", lambda schedule, now: False)
    _skipped_args = ["--repo", str(work), "--schedule", "0 22 * * *"]
    before = _git(origin, "for-each-ref")
    assert nightly.main(_skipped_args) == 0
    assert _output(capsys.readouterr().out) == {"skip": "true"}
    assert _git(origin, "for-each-ref") == before


def test_a_conflict_between_main_and_dev_stops_and_pushes_nothing(lines, capsys):
    origin, work = lines
    nightly = _module("release_nightly")
    _git(work, "checkout", "-q", "main")
    _commit(work, "main's", **{"b.txt": "main\n"})
    _git(work, "push", "-q", "origin", "main")
    before = _git(origin, "for-each-ref")
    assert nightly.main(["--repo", str(work)]) == 2
    captured = capsys.readouterr()
    assert "b.txt" in captured.err
    assert "version=" not in captured.out
    assert _git(origin, "for-each-ref") == before


def test_alpha_numbering_stops_at_the_first_beta_release(lines, capsys):
    origin, work = lines
    nightly = _module("release_nightly")
    _git(work, "tag", "-a", "v0.2.0", "-m", "beta", "main")
    _git(work, "push", "-q", "origin", "refs/tags/v0.2.0")
    before = _git(origin, "for-each-ref")
    assert nightly.main(["--repo", str(work)]) == 2
    assert "v0.2.0" in capsys.readouterr().err
    assert _git(origin, "for-each-ref") == before


def test_the_nightly_never_force_pushes():
    text = (_ROOT / "scripts" / "release_nightly.py").read_text()
    assert '"--force"' not in text and '"-f"' not in text


# ── 3. dev runs the fast checks only ────────────────────────────────────────────────────────


def _pushed_on(branch: str) -> set[str]:
    names = set()
    for path in _WORKFLOWS.glob("*.yml"):
        triggers = yaml.safe_load(path.read_text())[True]
        push = triggers.get("push") if isinstance(triggers, dict) else None
        if branch in ((push or {}).get("branches") or []):
            names.add(path.name)
    return names


def test_dev_runs_unit_ui_unit_lint_requirements_and_the_site_only():
    assert _pushed_on("dev") == {
        "unit-tests.yml",
        "ui-unit-tests.yml",
        "lint.yml",
        "requirements-ci.yml",
        "docs.yml",
    }


@pytest.mark.parametrize(
    "name",
    ["cve-audit.yml", "docker-image.yml", "devcontainer-smoke.yml"],
)
def test_what_left_dev_still_runs_on_a_release_branch(name):
    assert "release/**" in _on(_load(name))["push"]["branches"]


# ── 4. Where the demos and the site publish from ────────────────────────────────────────────


def test_demo_bundles_publish_from_main():
    assert _on(_load("demo-bundles.yml"))["push"]["branches"] == ["main"]


def test_the_site_deploys_from_dev():
    triggers = _on(_load("docs.yml"))
    assert triggers["push"]["branches"] == ["dev"]
    assert "site/**" in triggers["push"]["paths"]


def test_the_site_still_deploys_to_cloudflares_production_branch():
    # wrangler's --branch names the Pages production branch, not the Git branch that was pushed.
    assert "wrangler pages deploy --branch=main" in (_WORKFLOWS / "docs.yml").read_text()


# ── 5. The final merge is a script the lead runs ────────────────────────────────────────────


def test_no_workflow_merges_the_release_or_needs_a_merge_token():
    for path in _WORKFLOWS.glob("*.yml"):
        text = path.read_text()
        assert "RELEASE_MERGE_TOKEN" not in text, path.name
        assert "release_merge.py" not in text, path.name
    leg = _load("release-publish.yml")
    assert "merge-release" not in leg["jobs"]
    assert "secrets" not in _on(leg)["workflow_call"]
    assert "secrets" not in _load("build-dmg.yml")["jobs"]["publish"]


_VERSION_6 = "v0.1.0-alpha.6"
_BRANCH_6 = f"release/{_VERSION_6}"


@pytest.fixture
def published(lines) -> tuple[Path, Path]:
    origin, work = lines
    _git(work, "checkout", "-q", "-b", _BRANCH_6, "main")
    _git(work, "merge", "-q", "--no-ff", "-m", "dev", "dev")
    _commit(work, "notes", **{"notes.md": "n\n"})
    _git(work, "tag", "-a", _VERSION_6, "-m", _VERSION_6)
    _git(work, "push", "-q", "origin", _BRANCH_6, f"refs/tags/{_VERSION_6}")
    return origin, work


def test_the_merge_prints_what_it_did(published, capsys):
    origin, work = published
    merging = _module("release_merge")
    old_main = _remote(origin, "refs/heads/main")
    old_dev = _remote(origin, "refs/heads/dev")
    assert merging.main(["--version", _VERSION_6, "--repo", str(work)]) == 0
    out = capsys.readouterr().out
    new_main = _remote(origin, "refs/heads/main")
    new_dev = _remote(origin, "refs/heads/dev")
    assert f"main: {old_main[:12]} -> {new_main[:12]}" in out
    assert f"dev: {old_dev[:12]} -> {new_dev[:12]}" in out
    assert f"deleted {_BRANCH_6}" in out
    assert _remote(origin, f"refs/heads/{_BRANCH_6}") == ""


def test_the_merge_works_from_a_clone_with_main_checked_out_and_leaves_it_alone(published):
    origin, work = published
    merging = _module("release_merge")
    _git(work, "checkout", "-q", "main")
    (work / "untracked.txt").write_text("the lead's")
    head = _git(work, "rev-parse", "HEAD")
    assert merging.main(["--version", _VERSION_6, "--repo", str(work)]) == 0
    assert _git(work, "symbolic-ref", "--short", "HEAD") == "main"
    assert _git(work, "rev-parse", "HEAD") == head
    assert (work / "untracked.txt").read_text() == "the lead's"
    assert _git(work, "worktree", "list").count("\n") == 0  # its own worktree is gone


def test_the_merge_never_force_pushes():
    text = (_ROOT / "scripts" / "release_merge.py").read_text()
    assert '"--force"' not in text and '"-f"' not in text


def test_a_nightly_alpha_version_reads_the_same_way_at_the_cut():
    assert _module("release_gate").gates("v0.1.0-alpha.1") == _module("release_gate").gates(
        "v9.9.9-alpha.99"
    )
