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


def test_both_core_projects_ask_for_reduced_motion():
    for name in ("core", "core-provisioning"):
        assert re.search(r'use:\s*\{[^}]*reducedMotion:\s*"reduce"', _project(name)), name
