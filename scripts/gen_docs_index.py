#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 0d650b7b-e8ea-464f-9386-26674f37e9d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regenerate docs/index.md (and docs/<lng>/index.md) from README.md.

README.md is the single source of truth. This rewrites its repo-relative
paths to docs-site-relative ones and writes the result to docs/index.md
(gitignored, generated in CI before `mkdocs build`).

Translated homepages follow the same transform: a repo-root README.<lng>.md
(produced by the docs-translate skill, see docs/i18n/manifest.json) becomes
docs/<lng>/index.md — the file mkdocs-static-i18n's folder structure expects.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GH_BASE = "https://github.com/kenstott/provisa/blob/main/"


def render(src: str) -> str:
    # docs/images/x.png -> images/x.png ; docs/x.md -> x.md (paths are relative to docs/)
    src = src.replace("docs/images/", "images/")
    src = re.sub(r"\]\(docs/([^)]+\.md)\)", r"](\1)", src)
    # README quick-start self-reference -> site home
    src = src.replace("[README Quick Start](../README.md#quick-start)", "[Quick Start](index.md)")
    # repo-root files not in the docs tree -> absolute GitHub links
    for f in ("LICENSE", "NOTICE", "ai.txt", "robots.txt"):
        src = src.replace(f"]({f})", f"]({GH_BASE}{f})")
    return src


def write(out: Path, src: str) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(src))
    print(f"wrote {out.relative_to(ROOT)} ({len(src)} bytes)")


write(ROOT / "docs" / "index.md", (ROOT / "README.md").read_text())

for readme in ROOT.glob("README.*.md"):
    lng = readme.stem.removeprefix("README.")
    write(ROOT / "docs" / lng / "index.md", readme.read_text())
