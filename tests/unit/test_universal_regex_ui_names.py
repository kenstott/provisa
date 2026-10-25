# Copyright (c) 2026 Kenneth Stott
# Canary: b9648a15-edd0-4165-9ab7-c2eb8b94a907
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2142: the mask editor names each construct the universal-dialect check can report, by its
code, in every locale -- so a code added to the check cannot reach the editor unnamed."""

# Requirements: REQ-2142

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
CHECK = ROOT / "provisa" / "security" / "universal_regex.py"
LOCALES = ROOT / "provisa-ui" / "src" / "i18n" / "locales"


def _codes() -> set[str]:
    """Every construct code the check passes to NotUniversal, directly or through refuse()."""
    codes: set[str] = set()
    for node in ast.walk(ast.parse(CHECK.read_text())):
        if not isinstance(node, ast.Call):
            continue
        name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        if name in ("NotUniversal", "refuse") and node.args:
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                codes.add(first.value)
    return codes


def test_the_check_reports_codes_at_all() -> None:
    assert {"lookahead", "alternation_overlap", "replace_group_out_of_range"} <= _codes()


@pytest.mark.parametrize("locale", sorted(p.name for p in LOCALES.iterdir() if p.is_dir()))
def test_every_construct_code_is_named_in_every_locale(locale: str) -> None:
    named = json.loads((LOCALES / locale / "maskList.json").read_text())["maskList"]["construct"]
    assert sorted(_codes() - set(named)) == []
