# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-413 (amended 2026-11-18): every threshold and sample size a discovery run uses is set in
the discover dialog; none is a fixed constant in the code. Each setting's default constant is
defined once and read only by the defaults that prefill the dialog
(:data:`provisa.discovery.threshold.DEFAULTS`)."""

# Requirements: REQ-413

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2] / "provisa"
DEFAULTS_MODULE = ROOT / "discovery" / "threshold.py"

#: Each setting's default constant and the module that defines it.
DEFINED = {
    "CONTAINED_AT": ROOT / "discovery" / "context.py",
    "SHARED_CEILING": ROOT / "discovery" / "context.py",
    "SAMPLE_VALUES": ROOT / "discovery" / "context.py",
    "DISTINCT_BOUND": ROOT / "discovery" / "measure.py",
}


def _code_lines(path: Path) -> list[tuple[int, str]]:
    """The lines of ``path`` that are code: comments and docstring text left out."""
    out: list[tuple[int, str]] = []
    in_doc = False
    for n, line in enumerate(path.read_text().splitlines(), 1):
        stripped = line.strip()
        quotes = stripped.count('"""')
        if in_doc:
            in_doc = quotes % 2 == 0
            continue
        if stripped.startswith('"""'):
            in_doc = quotes == 1
            continue
        if stripped.startswith("#"):
            continue
        out.append((n, line.split("#", 1)[0]))
    return out


@pytest.mark.parametrize("name", sorted(DEFINED))
def test_a_settings_default_constant_is_read_only_by_the_dialogs_defaults(name):
    word = re.compile(rf"\b{name}\b")
    uses: list[str] = []
    for path in ROOT.rglob("*.py"):
        for n, line in _code_lines(path):
            if not word.search(line):
                continue
            if path == DEFINED[name] and re.match(rf"^{name}\s*=", line):
                continue
            if path == DEFAULTS_MODULE:
                continue
            uses.append(f"{path.relative_to(ROOT.parent)}:{n}: {line.strip()}")
    assert uses == [], "a setting's default decides a run outside the dialog's defaults"
