# Copyright (c) 2026 Kenneth Stott
# Canary: d4270f0f-04e1-4cd4-9e47-7ef5a68240c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The core UI lane runs with reduced motion, so no 3D stage turns while a spec reads its page.

A turning stage drew 20 frames a second for as long as it was on screen, and CI renders WebGL in
software: data-product-meta-panel.spec.ts sat on a turning product graph for the whole test, which
held SwiftShader's GPU process at 50-91% of a core on a 4-vCPU runner (batch 3). Under
``prefers-reduced-motion: reduce`` every stage holds still and jumps where it would fly
(profile3d/baseScene.ts), so a settled page draws nothing."""

from __future__ import annotations

import re
from pathlib import Path

CONFIG = (Path(__file__).resolve().parents[2] / "provisa-ui" / "playwright.config.ts").read_text()


def _project(name: str) -> str:
    """The body of the project named ``name``, up to the project that follows it."""
    found = re.search(rf'\{{\s*(?://[^\n]*\n\s*)*name: "{name}",(.*?)\n\s*\}},?\n', CONFIG, re.S)
    assert found, name
    return found.group(1)


def test_the_ui_projects_ask_for_reduced_motion_through_their_context_options():
    """``reducedMotion`` is a browser-context option, not a fixture of ``use``: given there
    directly, TypeScript refused it (TS2769) and Playwright ignored it, so every stage still
    turned. It is given in ``use.contextOptions``, which Playwright passes to each new context."""
    for name in ("core", "core-provisioning", "warehouse"):
        body = _project(name)
        assert re.search(
            r'use:\s*\{\s*contextOptions:\s*\{\s*reducedMotion:\s*"reduce" as const\s*\}', body
        ), name
        assert not re.search(r"use:\s*\{\s*reducedMotion:", body), name
