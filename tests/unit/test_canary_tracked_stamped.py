# Copyright (c) 2026 Kenneth Stott
# Canary: 917e1476-72ad-4012-8d89-454047fea610
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Every tracked source file carries its canary in the committed tree. The post-commit stamper
(scripts/canary_stamp.py) sweeps all tracked files, so an unstamped one is stamped, and left
modified, by an unrelated commit."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STAMPABLE_SUFFIXES = {".py", ".ts", ".tsx", ".js", ".jsx", ".mts", ".cts"}
EXCLUDE_DIRS = {
    ".venv",
    "venv",
    ".git",
    ".eggs",
    "canary-site",
    "__pycache__",
    "node_modules",
    "build",
    "dist",
    "worktrees",
}


def test_every_tracked_source_file_is_stamped() -> None:
    listed = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True
    ).stdout.decode("utf-8")
    unstamped = []
    for rel in filter(None, listed.split("\0")):
        p = ROOT / rel
        if set(Path(rel).parts) & EXCLUDE_DIRS or p.is_symlink() or not p.is_file():
            continue
        if p.suffix not in STAMPABLE_SUFFIXES:
            continue
        if "Canary: " not in p.read_text(encoding="utf-8"):
            unstamped.append(rel)
    assert unstamped == []


def test_no_two_tracked_source_files_share_a_canary() -> None:
    listed = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True
    ).stdout.decode("utf-8")
    owners: dict[str, list[str]] = {}
    for rel in filter(None, listed.split("\0")):
        p = ROOT / rel
        if set(Path(rel).parts) & EXCLUDE_DIRS or p.is_symlink() or not p.is_file():
            continue
        if p.suffix not in STAMPABLE_SUFFIXES:
            continue
        m = re.search(
            r"^(?:#|//) Canary:\s+([0-9a-f-]{36})\s*$", p.read_text(encoding="utf-8"), re.M
        )
        if m:
            owners.setdefault(m.group(1), []).append(rel)
    shared = {k: v for k, v in owners.items() if len(v) > 1}
    assert shared == {}, "\n".join(f"{k}: {' and '.join(v)}" for k, v in shared.items())
