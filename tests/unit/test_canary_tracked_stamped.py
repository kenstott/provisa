# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Every tracked source file carries its canary in the committed tree. The post-commit stamper
(scripts/canary_stamp.py) sweeps all tracked files, so an unstamped one is stamped, and left
modified, by an unrelated commit."""

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
