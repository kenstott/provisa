# Copyright (c) 2026 Kenneth Stott
# Canary: 7f206041-a1fd-4f19-8f70-93b9240923f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A merge commit stamps only the files it added itself; an ordinary commit stamps the whole tree
and publishes the site (scripts/canary_stamp.py, REQ-1989 batch landings)."""

import importlib.util
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "canary_stamp.py"
CANARY_LINE = "# Canary: "


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "--no-verify", "-m", message)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q", "-b", "main")
    _git(root, "config", "user.email", "t@example.com")
    _git(root, "config", "user.name", "t")
    _git(root, "config", "core.hooksPath", "/dev/null")
    (root / "base.py").write_text("X = 1\n")
    _commit(root, "base")
    return root


@pytest.fixture
def stamper(repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    site = tmp_path / "site"
    monkeypatch.setenv("CANARY_SITE_DIR", str(site))
    spec = importlib.util.spec_from_file_location("canary_stamp_under_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "PROJECT_ROOT", repo)
    monkeypatch.setattr(module, "REGISTRY_PATH", repo / ".canary_registry.json")
    deploys: list[int] = []
    monkeypatch.setattr(module, "deploy_site", lambda: deploys.append(1))
    return SimpleNamespace(main=module.main, deploys=deploys, site=site)


def _canary(path: Path) -> str | None:
    for line in path.read_text().splitlines():
        if line.startswith(CANARY_LINE):
            return line[len(CANARY_LINE) :]
    return None


def _merge_two_sides(repo: Path, resolution_file: str | None) -> None:
    """Branch side adds side.py, main adds main_only.py, then merge; optionally the merge itself
    adds resolution_file (as a conflict resolution would)."""
    _git(repo, "checkout", "-q", "-b", "side")
    (repo / "side.py").write_text("S = 1\n")
    _commit(repo, "side")
    _git(repo, "checkout", "-q", "main")
    (repo / "main_only.py").write_text("M = 1\n")
    _commit(repo, "main")
    _git(repo, "merge", "-q", "--no-commit", "--no-ff", "side")
    if resolution_file:
        (repo / resolution_file).write_text("R = 1\n")
        _git(repo, "add", resolution_file)
    _git(repo, "commit", "-q", "--no-verify", "-m", "merge side")


def test_ordinary_commit_stamps_every_file_and_deploys(repo: Path, stamper) -> None:
    stamper.main()
    assert _canary(repo / "base.py")
    assert json.loads((repo / ".canary_registry.json").read_text())["files"]
    assert list(stamper.site.glob("*.json"))
    assert stamper.deploys == [1]


def test_merge_commit_is_not_stamped_or_deployed(repo: Path, stamper, capsys) -> None:
    _merge_two_sides(repo, None)
    stamper.main()
    for name in ("base.py", "side.py", "main_only.py"):
        assert _canary(repo / name) is None, name
    assert not (repo / ".canary_registry.json").exists()
    assert not stamper.site.exists() or not list(stamper.site.glob("*.json"))
    assert stamper.deploys == []
    assert "Merge commit: skipped full stamp and site deploy" in capsys.readouterr().out


def test_merge_stamps_only_the_files_it_added(repo: Path, stamper) -> None:
    _merge_two_sides(repo, "resolved.py")
    stamper.main()
    assert _canary(repo / "resolved.py")
    for name in ("base.py", "side.py", "main_only.py"):
        assert _canary(repo / name) is None, name
    assert stamper.deploys == []


def test_merge_added_file_with_a_copied_canary_is_reissued(repo: Path, stamper) -> None:
    stamper.main()
    _commit(repo, "stamps")
    base_canary = _canary(repo / "base.py")
    assert base_canary
    stamper.deploys.clear()
    _git(repo, "checkout", "-q", "-b", "side")
    (repo / "side.py").write_text("S = 1\n")
    _commit(repo, "side")
    _git(repo, "checkout", "-q", "main")
    (repo / "main_only.py").write_text("M = 1\n")
    _commit(repo, "main")
    _git(repo, "merge", "-q", "--no-commit", "--no-ff", "side")
    (repo / "copy.py").write_text((repo / "base.py").read_text())
    _git(repo, "add", "copy.py")
    _git(repo, "commit", "-q", "--no-verify", "-m", "merge side")
    stamper.main()
    copied = _canary(repo / "copy.py")
    assert copied and copied != base_canary
    assert _canary(repo / "base.py") == base_canary
    assert stamper.deploys == []


def test_merge_reissues_a_canary_both_sides_copied(repo: Path, stamper) -> None:
    stamper.main()
    _commit(repo, "stamps")
    base_canary = _canary(repo / "base.py")
    assert base_canary
    header = (repo / "base.py").read_text()
    _git(repo, "checkout", "-q", "-b", "side")
    (repo / "a_side.py").write_text(header)
    _commit(repo, "side copies base header")
    _git(repo, "checkout", "-q", "main")
    (repo / "a_main.py").write_text(header)
    _commit(repo, "main copies base header")
    _git(repo, "merge", "-q", "--no-ff", "--no-edit", "side")
    stamper.deploys.clear()
    stamper.main()
    canaries = [_canary(repo / n) for n in ("a_main.py", "a_side.py", "base.py")]
    assert all(canaries)
    assert len(set(canaries)) == 3
    assert canaries[0] == base_canary
    assert stamper.deploys == []
