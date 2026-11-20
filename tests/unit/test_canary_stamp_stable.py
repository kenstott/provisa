"""Canary site entries are stable across commits: a commit rewrites only new or changed entries and
deploys only when the site differs from the last deploy (scripts/canary_stamp.py)."""

import importlib.util
import json
import subprocess
from pathlib import Path
from types import ModuleType

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
    (root / ".gitignore").write_text(".canary_registry.json\n")
    (root / "base.py").write_text("X = 1\n")
    _commit(root, "base")
    return root


@pytest.fixture
def deploys() -> list[int]:
    return []


@pytest.fixture
def stamper(
    repo: Path, tmp_path: Path, deploys: list[int], monkeypatch: pytest.MonkeyPatch
) -> ModuleType:
    monkeypatch.setenv("CANARY_SITE_DIR", str(tmp_path / "site"))
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "token")
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "account")
    spec = importlib.util.spec_from_file_location("canary_stamp_under_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "PROJECT_ROOT", repo)
    monkeypatch.setattr(module, "REGISTRY_PATH", repo / ".canary_registry.json")
    monkeypatch.setattr(module, "IS_MAIN_CHECKOUT", True)
    monkeypatch.setattr(module, "wrangler_deploy", lambda: deploys.append(1))
    return module


def _canary(path: Path) -> str | None:
    for line in path.read_text().splitlines():
        if line.startswith(CANARY_LINE):
            return line[len(CANARY_LINE) :]
    return None


def _site(stamper: ModuleType) -> dict[str, tuple[str, int]]:
    return {f.name: (f.read_text(), f.stat().st_mtime_ns) for f in stamper.SITE_DIR.glob("*.json")}


def _stamp_and_commit(repo: Path, stamper: ModuleType) -> None:
    stamper.main()
    _commit(repo, "canary stamps")


def test_first_commit_stamps_writes_and_deploys(repo: Path, stamper, deploys) -> None:
    stamper.main()
    assert _canary(repo / "base.py")
    assert len(_site(stamper)) == 1
    assert deploys == [1]


def test_unchanged_commit_rewrites_nothing_and_does_not_deploy(
    repo: Path, stamper, deploys
) -> None:
    _stamp_and_commit(repo, stamper)
    before = _site(stamper)
    (repo / "README").write_text("docs\n")
    _commit(repo, "docs only")
    stamper.main()
    assert _site(stamper) == before
    assert deploys == [1]


def test_new_file_adds_one_entry_and_keeps_the_others(repo: Path, stamper, deploys) -> None:
    _stamp_and_commit(repo, stamper)
    before = _site(stamper)
    (repo / "new.py").write_text("N = 1\n")
    _commit(repo, "new file")
    stamper.main()
    after = _site(stamper)
    assert len(after) == 2
    for name, value in before.items():
        assert after[name] == value
    new_entry = json.loads((stamper.SITE_DIR / f"{_canary(repo / 'new.py')}.json").read_text())
    assert new_entry["path"] == "new.py"
    assert deploys == [1, 1]


def test_merge_commit_with_nothing_new_rewrites_nothing_and_does_not_deploy(
    repo: Path, stamper, deploys
) -> None:
    """Both sides were stamped on their own commits; the merge runs where the lander merges, a
    worktree, which never prunes."""
    (repo / "main_only.py").write_text("M = 1\n")
    _commit(repo, "main")
    _stamp_and_commit(repo, stamper)
    _git(repo, "checkout", "-q", "-b", "side")
    (repo / "side.py").write_text("S = 1\n")
    _commit(repo, "side")
    _stamp_and_commit(repo, stamper)
    _git(repo, "checkout", "-q", "main")
    (repo / "README").write_text("docs\n")
    _commit(repo, "main docs")
    stamper.IS_MAIN_CHECKOUT = False
    before = _site(stamper)
    deploys.clear()
    _git(repo, "merge", "-q", "--no-ff", "--no-edit", "side")
    stamper.main()
    assert _site(stamper) == before
    assert deploys == []


def test_merge_reissues_a_canary_both_sides_copied(repo: Path, stamper) -> None:
    _stamp_and_commit(repo, stamper)
    base_canary = _canary(repo / "base.py")
    header = (repo / "base.py").read_text()
    _git(repo, "checkout", "-q", "-b", "side")
    (repo / "z_side.py").write_text(header)
    _commit(repo, "side copies base header")
    _git(repo, "checkout", "-q", "main")
    (repo / "z_main.py").write_text(header)
    _commit(repo, "main copies base header")
    _git(repo, "merge", "-q", "--no-ff", "--no-edit", "side")
    stamper.main()
    canaries = [_canary(repo / n) for n in ("base.py", "z_main.py", "z_side.py")]
    assert canaries[0] == base_canary
    assert all(canaries) and len(set(canaries)) == 3


def test_entries_written_without_credentials_deploy_on_the_next_credentialed_run(
    repo: Path, stamper, deploys, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stamp_and_commit(repo, stamper)
    monkeypatch.delenv("CLOUDFLARE_API_TOKEN")
    (repo / "new.py").write_text("N = 1\n")
    _commit(repo, "new file")
    stamper.main()
    assert deploys == [1]
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "token")
    _commit(repo, "canary stamps")
    stamper.main()
    assert deploys == [1, 1]
