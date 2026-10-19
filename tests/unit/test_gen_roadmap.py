# Copyright (c) 2026 Kenneth Stott
# Canary: c6c46779-89b5-4f15-93e5-a3c804110121
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The roadmap's one-line brief of a requirement's description."""

from __future__ import annotations

import importlib.util
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _load():
    spec = importlib.util.spec_from_file_location(
        "gen_roadmap", REPO / "scripts" / "gen_roadmap.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_brief_leaves_out_inline_superseded_spans():
    brief = _load()._brief(
        "[SUPERSEDED by REQ-1 amendment, 2026-10-03 -- old wording. Kept here for history.] "
        "old text [END SUPERSEDED BLOCK] Explore has two tabs,\n[SUPERSEDED by REQ-1, "
        "2026-10-17 -- gone.] an extra [END SUPERSEDED BLOCK] Python and Notebook."
    )
    assert brief == "Explore has two tabs, Python and Notebook."


def test_brief_truncates_the_live_wording_to_140_characters():
    brief = _load()._brief("[SUPERSEDED by REQ-1 -- x] y [END SUPERSEDED BLOCK] " + "a" * 200)
    assert brief == "a" * 137 + "…"
