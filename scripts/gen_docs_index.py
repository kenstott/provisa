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

"""Regenerate docs/index.md from README.md.

README.md is the single source of truth. This rewrites its repo-relative paths
to docs-site-relative ones and writes the result to docs/index.md (gitignored,
generated in CI before `mkdocs build`).
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
OUT = ROOT / "docs" / "index.md"
GH_BASE = "https://github.com/kenstott/provisa/blob/main/"

src = README.read_text()
# docs/images/x.png -> images/x.png ; docs/x.md -> x.md (paths are relative to docs/)
src = src.replace("docs/images/", "images/")
src = re.sub(r"\]\(docs/([^)]+\.md)\)", r"](\1)", src)
# README quick-start self-reference -> site home
src = src.replace("[README Quick Start](../README.md#quick-start)", "[Quick Start](index.md)")
# repo-root files not in the docs tree -> absolute GitHub links
for f in ("LICENSE", "NOTICE", "ai.txt", "robots.txt"):
    src = src.replace(f"]({f})", f"]({GH_BASE}{f})")

OUT.write_text(src)
print(f"wrote {OUT.relative_to(ROOT)} ({len(src)} bytes)")
