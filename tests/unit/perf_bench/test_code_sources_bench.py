# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2100: the code sources' throughput record (demo/named/perf/bench/code_sources.py) -- one
figure for the direct path and one for pgwire, each the rows it read and answered and their rate.
Nothing is asserted about the rate."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import code_sources  # noqa: E402

pytestmark = pytest.mark.unit


def test_each_path_is_recorded_with_what_it_read_and_answered(tmp_path):
    figures = code_sources.run(2000, tmp_path)
    assert [f["path"] for f in figures] == ["direct", "pgwire"]
    for figure in figures:
        assert (figure["rows"], figure["answered"]) == (2000, 1000)  # n % 2 = 0
        assert figure["seconds"] > 0 and figure["rows_per_second"] > 0
        assert figure["bounds"] == code_sources.BOUNDS
    assert json.loads((tmp_path / "code_sources.json").read_text()) == figures
