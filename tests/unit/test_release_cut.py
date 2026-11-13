# Copyright (c) 2026 Kenneth Stott
# Canary: 2aaca91e-2d5f-483e-8717-4f69ef6794ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A release is cut by the "Release: cut" workflow, from its own branch release/<version>: the
suites and the dry-run build pass on that branch first, its notes are generated and committed
before its tag is made, a generation failure leaves no commit and no tag, no hand-written release
page is overwritten, and the cut starts the release workflows on the new tag, which itself starts nothing. Once published,
the branch merges to main and to dev and is deleted.

The preflight runs against a real temporary Git repository; the workflow is read as YAML."""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS = _ROOT / ".github" / "workflows"


def _script():
    spec = importlib.util.spec_from_file_location(
        "release_cut_check", _ROOT / "scripts" / "release_cut_check.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cut = _script()


def cut_gate():
    spec = importlib.util.spec_from_file_location(
        "release_gate", _ROOT / "scripts" / "release_gate.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _merge_script():
    spec = importlib.util.spec_from_file_location(
        "release_merge", _ROOT / "scripts" / "release_merge.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _commit(repo: Path, subject: str, **files: str) -> None:
    for name, text in files.items():
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "--allow-empty", "-m", subject)


@pytest.fixture
def repo(tmp_path) -> Path:
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.email", "t@example.test")
    _git(tmp_path, "config", "user.name", "t")
    _commit(tmp_path, "first", **{"README.md": "x"})
    _git(tmp_path, "tag", "-a", "v0.1.0-alpha.479", "-m", "v0.1.0-alpha.479")
    _commit(tmp_path, "feat: something", **{"a.py": "1"})
    return tmp_path


# -- the preflight --------------------------------------------------------------------------------


def test_a_release_can_be_cut_and_its_notes_start_at_the_previous_tag(repo):
    assert cut.check(repo, "v0.1.0-alpha.480") == "v0.1.0-alpha.479"


@pytest.mark.parametrize(
    "version", ["0.1.0-alpha.480", "v0.1", "v0.1.0-alpha", "v0.1.0-nightly.3", "latest"]
)
def test_a_version_that_is_not_a_release_tag_is_refused(repo, version):
    with pytest.raises(cut.CutRefused, match="is not a release tag"):
        cut.check(repo, version)


def test_a_tag_that_exists_is_refused(repo):
    with pytest.raises(cut.CutRefused, match="already exists"):
        cut.check(repo, "v0.1.0-alpha.479")


def test_a_hand_written_page_for_the_release_is_never_overwritten(repo):
    _commit(
        repo, "docs: notes for the launch", **{"docs/releases/v0.1.0-alpha.480.md": "# by hand"}
    )
    with pytest.raises(cut.CutRefused, match="would overwrite it"):
        cut.check(repo, "v0.1.0-alpha.480")


def test_a_page_an_earlier_attempt_of_this_cut_wrote_is_written_again(repo):
    _commit(
        repo,
        cut.cut_subject("v0.1.0-alpha.480"),
        **{"docs/releases/v0.1.0-alpha.480.md": "# generated"},
    )
    assert cut.check(repo, "v0.1.0-alpha.480") == "v0.1.0-alpha.479"


def test_a_head_with_no_earlier_release_is_refused(tmp_path):
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.email", "t@example.test")
    _git(tmp_path, "config", "user.name", "t")
    _commit(tmp_path, "first", **{"README.md": "x"})
    with pytest.raises(cut.CutRefused, match="no earlier release tag"):
        cut.check(tmp_path, "v0.1.0")


def test_after_generation_only_this_release_and_earlier_cut_pages_may_change(repo):
    _commit(repo, cut.cut_subject("v0.1.0-alpha.470"), **{"docs/releases/v0.1.0-alpha.470.md": "g"})
    _commit(repo, "docs: a page written by hand", **{"docs/releases/v0.1.0-alpha.471.md": "h"})
    # The generator writes this release's page and drops an earlier cut's: allowed.
    (repo / "docs/releases/v0.1.0-alpha.480.md").write_text("# new")
    (repo / "docs/releases/v0.1.0-alpha.470.md").unlink()
    cut.check_written(repo, "v0.1.0-alpha.480")
    # It drops a hand-written one: refused, naming it.
    (repo / "docs/releases/v0.1.0-alpha.471.md").unlink()
    with pytest.raises(cut.CutRefused, match="v0.1.0-alpha.471.md"):
        cut.check_written(repo, "v0.1.0-alpha.480")


def test_the_preflight_exits_2_with_the_reason_and_prints_the_previous_tag(repo, capsys):
    assert cut.main(["--version", "v0.1.0-alpha.480", "--repo", str(repo)]) == 0
    assert capsys.readouterr().out.strip() == "previous=v0.1.0-alpha.479"
    assert cut.main(["--version", "bad", "--repo", str(repo)]) == 2
    assert "ERROR: 'bad' is not a release tag" in capsys.readouterr().err


# -- the workflow ---------------------------------------------------------------------------------


def _load(name: str) -> dict:
    return yaml.safe_load((_WORKFLOWS / name).read_text())


def _on(workflow: dict) -> dict:
    # PyYAML reads the bare key `on` as True.
    return workflow.get("on", workflow.get(True)) or {}


def _steps() -> list[dict]:
    return _load("release-cut.yml")["jobs"]["cut"]["steps"]


def test_a_cut_is_run_by_hand_with_a_version_and_never_cancelled():
    workflow = _load("release-cut.yml")
    dispatch = _on(workflow)["workflow_dispatch"]
    assert dispatch["inputs"]["version"]["required"] is True
    assert set(_on(workflow)) == {"workflow_dispatch"}
    assert workflow["concurrency"] == {"group": "release-cut", "cancel-in-progress": False}
    assert workflow["permissions"] == {"contents": "write", "actions": "write"}
    checkout = _steps()[0]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"] == {"fetch-depth": 0}  # the run's own commit, all of its history


def test_the_notes_are_generated_and_checked_before_anything_is_committed_or_tagged():
    runs = [step.get("run", "") for step in _steps()]
    order = [
        next(i for i, r in enumerate(runs) if needle in r)
        for needle in (
            "release_cut_check.py --version",
            'gen_release_notes.py --from "$PREVIOUS" --to HEAD --version "$VERSION"',
            "--written",
            "git commit",
            'git tag -a "$VERSION"',
            'git push --atomic origin "HEAD:refs/heads/release/$VERSION" "refs/tags/$VERSION"',
            "gh workflow run build-dmg.yml",
        )
    ]
    assert order == sorted(order)
    generate = next(r for r in runs if "gen_release_notes.py" in r)
    assert generate.startswith("set -euo pipefail")  # its exit 2 stops the job here
    assert 'if [ ! -f "docs/releases/$VERSION.md" ]' in generate
    commit = next(r for r in runs if "git commit" in r)
    assert commit.startswith("set -euo pipefail")
    assert (
        "git add -A provisa-ui/src/generated/releaseNotes.json docs/releases mkdocs.yml CHANGELOG.md"
        in commit
    )
    assert 'git commit -m "chore(release): $VERSION notes"' in commit
    assert cut.cut_subject("$VERSION") == "chore(release): $VERSION notes"


_RELEASE_WORKFLOWS = (
    "build-dmg.yml",
    "build-duckdb-extensions.yml",
    "build-pg-extensions.yml",
    "release-exports.yml",
)


def test_a_pushed_tag_starts_nothing_and_the_cut_publishes_from_it():
    for path in _WORKFLOWS.glob("*.yml"):
        push = _on(yaml.safe_load(path.read_text())).get("push") or {}
        assert not push.get("tags"), f"{path.name} starts on a tag"
    dispatch = next(r for r in (s.get("run", "") for s in _steps()) if "gh workflow run" in r)
    for name in _RELEASE_WORKFLOWS:
        assert "workflow_dispatch" in _on(_load(name)), f"{name} cannot be started on the tag"
        assert name in dispatch, f"{name} is not started by the cut"
    assert (
        'gh workflow run build-dmg.yml --ref "$VERSION" -f version="$VERSION" -f publish=true'
        in dispatch
    )
    assert 'gh workflow run "$workflow" --ref "$VERSION"' in dispatch


def test_the_release_body_is_the_tags_notes_page():
    action = (_ROOT / ".github" / "actions" / "ensure-release" / "action.yml").read_text()
    assert "--generate-notes" not in action
    assert 'notes="docs/releases/$TAG.md"' in action
    assert '--notes-file "$notes"' in action
    assert 'if [ ! -f "$notes" ]; then' in action


_UI_LANES = ("ui-e2e-core.yml", "ui-e2e-trino.yml", "ui-e2e-swap-amd64.yml")


def test_every_ui_e2e_lane_runs_on_dispatch_for_a_release_branch():
    # A dispatch on a release branch runs them; no push, no tag starts one (REQ-1989).
    for name in _UI_LANES:
        on = _on(_load(name))
        assert "push" not in on, name
        assert "workflow_dispatch" in on, name


def test_a_ui_lane_not_green_refuses_a_beta_or_production_cut_through_the_gate():
    gated = cut_gate().gates("v0.2.0")
    assert set(_UI_LANES) <= set(gated)


# -- the release branch ---------------------------------------------------------------------------

_VERSION = "v0.1.0-alpha.480"
#: The branches each workflow's push trigger names; a release branch's push starts only lint and
#: requirements-ci, the rest run there on dispatch.
_TRIGGERS = {
    "cve-audit.yml": ["main"],
    "docker-image.yml": ["main"],
    "lint.yml": ["main", "release/**"],
    "requirements-ci.yml": ["main", "release/**"],
    "ui-unit-tests.yml": ["main"],
    "unit-tests.yml": ["main"],
    "devcontainer-smoke.yml": ["main", "ci/**"],
    # Publishes a demo and gates no release.
    "demo-bundles.yml": ["main"],
}


@pytest.mark.parametrize("name", sorted(_TRIGGERS))
def test_a_push_runs_the_workflow_on_the_branches_the_release_lines_give_it(name):
    assert _on(_load(name))["push"]["branches"] == _TRIGGERS[name]


@pytest.mark.parametrize("name", sorted(_TRIGGERS))
def test_a_landing_on_main_cancels_nothing_on_a_release_branch(name):
    workflow = _load(name)
    concurrency = [workflow.get("concurrency")] + [
        job.get("concurrency") for job in workflow["jobs"].values()
    ]
    for entry in filter(None, concurrency):
        assert "github.ref" in entry["group"], f"{name}: {entry}"


@pytest.mark.parametrize(
    "ref",
    [
        "refs/heads/main",
        "refs/heads/fix/a-branch",
        "refs/heads/release/v0.1.0-alpha.479",
        "refs/heads/feature/release/v0.1.0-alpha.480",
        "refs/tags/v0.1.0-alpha.480",
    ],
)
def test_a_cut_refuses_any_ref_but_its_own_release_branch(ref):
    with pytest.raises(cut.CutRefused, match="its own branch"):
        cut.check_ref(ref, _VERSION)
    assert cut.main(["--version", _VERSION, "--ref", ref]) == 2


def test_a_cut_is_accepted_on_its_own_release_branch(capsys):
    cut.check_ref(f"refs/heads/release/{_VERSION}", _VERSION)
    assert cut.main(["--version", _VERSION, "--ref", f"refs/heads/release/{_VERSION}"]) == 0
    assert capsys.readouterr().out == ""  # nothing added to the step's outputs


def _index(needle: str) -> int:
    runs = [step.get("run", "") for step in _steps()]
    return next(i for i, r in enumerate(runs) if needle in r)


def test_the_ref_is_refused_before_anything_else_runs():
    assert "if" not in _load("release-cut.yml")["jobs"]["cut"]  # a skipped job refuses nothing
    refusal = _index('release_cut_check.py --ref "$GITHUB_REF" --version "$VERSION"')
    assert refusal < _index("scripts/release_gate.py")
    assert not any("run" in step for step in _steps()[:refusal])


def test_the_tag_comes_after_the_suites_and_the_dry_run_build_passed_on_the_branch():
    gate = _index("scripts/release_gate.py")
    assert _index('gen_release_notes.py --from "$PREVIOUS"') > gate
    assert _index('git tag -a "$VERSION"') > gate
    script = _steps()[gate]["run"]
    assert script.startswith("set -euo pipefail")
    assert '--branch "$GITHUB_REF_NAME"' in script and '--sha "$GITHUB_SHA"' in script
    assert "if" not in _steps()[gate]  # a beta or production release cannot waive it


def test_the_notes_commit_and_the_tag_reach_the_branch_together_and_nothing_is_forced():
    push = _steps()[_index('git tag -a "$VERSION"')]["run"]
    assert push.count("git push") == 1
    assert "HEAD:main" not in push
    for path in (
        _WORKFLOWS / "release-cut.yml",
        _WORKFLOWS / "release-publish.yml",
    ):
        text = path.read_text()
        assert "--force" not in text and "push -f" not in text, path.name
    # The script's one --force removes its own temporary worktree; its push has none.
    script = (_ROOT / "scripts" / "release_merge.py").read_text()
    push = script[script.index('"push"') : script.index("finally:")]
    assert "--force" not in push and '"-f"' not in push


def _branch(repo: Path, name: str, start: str = "HEAD") -> None:
    _git(repo, "checkout", "-q", "-b", name, start)


def test_notes_start_at_the_previous_release_merged_back_through_main(repo):
    # Release 480 was tagged on its branch and merged to main; 481 is cut from main.
    _branch(repo, "release/v0.1.0-alpha.480")
    _commit(repo, cut.cut_subject("v0.1.0-alpha.480"), **{"n.md": "notes"})
    _git(repo, "tag", "-a", "v0.1.0-alpha.480", "-m", "v0.1.0-alpha.480")
    _git(repo, "checkout", "-q", "main")
    _git(repo, "merge", "-q", "--no-ff", "--no-edit", "v0.1.0-alpha.480")
    _branch(repo, "release/v0.1.0-alpha.481")
    _commit(repo, "fix: on the branch", **{"b.py": "1"})
    assert cut.check(repo, "v0.1.0-alpha.481") == "v0.1.0-alpha.480"


def test_the_previous_tag_is_chosen_by_semantic_version_and_not_as_text(repo):
    _git(repo, "tag", "-a", "v0.1.0-alpha.9", "-m", "nine")
    _git(repo, "tag", "-a", "v0.1.0-alpha.10", "-m", "ten")
    assert cut.check(repo, "v0.1.0-alpha.11") == "v0.1.0-alpha.10"


@pytest.mark.parametrize(
    ("lower", "higher"),
    [
        ("v0.1.0-alpha.9", "v0.1.0-alpha.10"),
        ("v0.1.0-alpha.10", "v0.1.0-beta.1"),
        ("v0.1.0-beta.2", "v0.1.0-rc.1"),
        ("v0.1.0-rc.9", "v0.1.0-rc.10"),
        ("v0.1.0-rc.1", "v0.1.0"),
        ("v0.1.0", "v0.1.1-alpha.1"),
    ],
)
def test_tags_are_ordered_by_semantic_version(lower, higher):
    assert cut._order(lower) < cut._order(higher)


def test_a_prerelease_sorts_below_its_release_when_the_previous_tag_is_chosen(repo):
    _git(repo, "tag", "-a", "v0.2.0-rc.1", "-m", "rc")
    _git(repo, "tag", "-a", "v0.2.0", "-m", "final")
    _git(repo, "tag", "-a", "v0.1.0-alpha.9", "-m", "older number, lower order")
    assert cut.check(repo, "v0.2.1") == "v0.2.0"
    assert cut.check(repo, "v0.2.0-rc.2") == "v0.2.0-rc.1"


def test_a_release_started_while_the_one_before_published_must_take_that_tag_in_first(repo):
    # 481's branch was cut before 480 was tagged: its notes would repeat all of 480, and its
    # merge to main would conflict after it is published. It is refused while it can be fixed.
    _branch(repo, "release/v0.1.0-alpha.481")
    _git(repo, "checkout", "-q", "main")
    _branch(repo, "release/v0.1.0-alpha.480")
    _commit(repo, cut.cut_subject("v0.1.0-alpha.480"), **{"n.md": "notes"})
    _git(repo, "tag", "-a", "v0.1.0-alpha.480", "-m", "v0.1.0-alpha.480")
    _git(repo, "checkout", "-q", "release/v0.1.0-alpha.481")
    with pytest.raises(cut.CutRefused, match="git merge v0.1.0-alpha.480"):
        cut.check(repo, "v0.1.0-alpha.481")
    _git(repo, "merge", "-q", "--no-edit", "v0.1.0-alpha.480")
    assert cut.check(repo, "v0.1.0-alpha.481") == "v0.1.0-alpha.480"


# -- after the release is published ---------------------------------------------------------------

merging = _merge_script()
_BRANCH = f"release/{_VERSION}"


@pytest.fixture
def released(tmp_path) -> tuple[Path, Path]:
    """(clone on the tag, its bare origin): main moved on past a tagged release branch."""
    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    work = tmp_path / "work"
    _git(tmp_path, "clone", "-q", str(origin), str(work))
    _git(work, "config", "user.email", "t@example.test")
    _git(work, "config", "user.name", "t")
    _git(work, "checkout", "-q", "-b", "main")
    _commit(work, "first", **{"README.md": "x"})
    _commit(work, "feat: released", **{"a.py": "1"})
    _branch(work, _BRANCH)
    _commit(work, cut.cut_subject(_VERSION), **{"CHANGELOG.md": "notes"})
    _git(work, "tag", "-a", _VERSION, "-m", _VERSION)
    _git(work, "checkout", "-q", "main")
    _commit(work, "feat: newer than the release", **{"b.py": "2"})
    _git(work, "push", "-q", "origin", "main", _BRANCH, f"refs/tags/{_VERSION}")
    _git(work, "checkout", "-q", "--detach", _VERSION)
    return work, origin


def _remote(origin: Path, ref: str) -> str:
    return subprocess.run(
        ["git", "-C", str(origin), "rev-parse", "-q", "--verify", ref],
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_a_published_release_merges_to_main_and_its_branch_is_deleted(released):
    work, origin = released
    assert merging.main(["--version", _VERSION, "--repo", str(work)]) == 0
    assert _remote(origin, f"refs/heads/{_BRANCH}") == ""
    # main has the release and kept what landed since the cut.
    assert _git(origin, "show", "main:CHANGELOG.md") == "notes"
    assert _git(origin, "show", "main:b.py") == "2"


def _state(origin: Path) -> list[str | None]:
    return [_remote(origin, f"refs/heads/{ref}") for ref in ("main", _BRANCH)]


def test_a_merge_conflict_stops_names_the_files_and_pushes_nothing(released, capsys):
    work, origin = released
    _git(work, "checkout", "-q", "main")
    _commit(work, "docs: edited on main", **{"CHANGELOG.md": "main's version"})
    _git(work, "push", "-q", "origin", "main")
    _git(work, "checkout", "-q", "--detach", _VERSION)
    before = _state(origin)
    assert merging.main(["--version", _VERSION, "--repo", str(work)]) == 2
    err = capsys.readouterr().err
    assert "merging into main" in err and "CHANGELOG.md" in err
    assert _state(origin) == before


def test_commits_on_the_branch_after_the_tag_stop_the_merge(released, capsys):
    work, origin = released
    _git(work, "checkout", "-q", _BRANCH)
    _commit(work, "fix: after the tag", **{"c.py": "3"})
    _git(work, "push", "-q", "origin", _BRANCH)
    _git(work, "checkout", "-q", "--detach", _VERSION)
    before = _state(origin)
    assert merging.main(["--version", _VERSION, "--repo", str(work)]) == 2
    assert "after the tag" in capsys.readouterr().err
    assert _state(origin) == before


def test_a_landing_on_main_during_the_merge_refuses_the_whole_push(released, tmp_path, monkeypatch):
    work, origin = released
    other = tmp_path / "other"
    _git(tmp_path, "clone", "-q", "-b", "main", str(origin), str(other))
    _git(other, "config", "user.email", "t@example.test")
    _git(other, "config", "user.name", "t")
    real = merging._merged_into

    def land_then_merge(repo, target, version):
        result = real(repo, target, version)
        _commit(other, "feat: landed meanwhile", **{"e.py": "5"})
        _git(other, "push", "-q", "origin", "main")
        return result

    monkeypatch.setattr(merging, "_merged_into", land_then_merge)
    branch_before = _remote(origin, f"refs/heads/{_BRANCH}")
    assert merging.main(["--version", _VERSION, "--repo", str(work)]) == 2
    assert _remote(origin, f"refs/heads/{_BRANCH}") == branch_before
