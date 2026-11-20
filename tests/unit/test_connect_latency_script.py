# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""conformance-drivers' connect latency check (scripts/conformance/connect_latency.py): Provisa's
median connect + SCRAM + select 1 against PostgreSQL's on the same runner, within a ratio."""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "conformance"))

import connect_latency  # noqa: E402


def test_within_the_ratio_passes():
    within, line = connect_latency.verdict([0.010, 0.011, 0.012], [0.005, 0.005, 0.006], 3.0)
    assert within
    assert "Provisa 11.0 ms, PostgreSQL 5.0 ms" in line and "2.20x, budget 3.00x -- within" in line


def test_over_the_ratio_fails():
    # The runner before the fix: ~20 ms against PostgreSQL's ~5 ms.
    within, line = connect_latency.verdict([0.019, 0.020, 0.021], [0.005, 0.005, 0.005], 3.0)
    assert not within
    assert "4.00x, budget 3.00x -- OVER" in line


def test_the_medians_decide_not_one_slow_round():
    within, _ = connect_latency.verdict([0.010, 0.010, 0.500], [0.005, 0.005, 0.005], 3.0)
    assert within


def test_the_workflow_runs_the_check_in_the_psycopg_job():
    workflow = (REPO / ".github" / "workflows" / "conformance-drivers.yml").read_text()
    psycopg_job = workflow.split("\n  psycopg:\n", 1)[1]
    assert "scripts/conformance/connect_latency.py --ratio" in psycopg_job
