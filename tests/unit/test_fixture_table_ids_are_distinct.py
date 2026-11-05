# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2c71-4d09-4a6f-9e13-7c2a0f6b8d45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A fixture's table ids are distinct, whatever the process's hash seed.

``hash(str)`` is salted per process (PYTHONHASHSEED), and ``hash(label) % 1000`` gave two labels
one id in one process in a thousand: under seed 1506 Person and Company shared a table, and
test_sparql_compiler's grounding read Company as an Employee too (main's unit run 40185489959).
Each case runs in a process of its own, under that seed."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_COLLIDING_SEED = "1506"  # hash("Person") % 1000 == hash("Company") % 1000 under it

_PROBES = {
    "cypher nodes": (
        "from tests.unit.test_cypher_min_hops import _node\n"
        "assert _node('Person', 'p').table_id != _node('Company', 'c').table_id\n"
    ),
    "registered tables": (
        "from tests.unit.table_ids import table_id\n"
        "assert table_id('Person') != table_id('Company')\n"
        "assert table_id('Person') == table_id('Person')\n"
    ),
}


@pytest.mark.parametrize("probe", sorted(_PROBES))
def test_distinct_names_get_distinct_table_ids_under_a_colliding_seed(probe):
    env = {**os.environ, "PYTHONHASHSEED": _COLLIDING_SEED}
    out = subprocess.run(  # noqa: S603 - the suite's own interpreter, a fixed probe
        [sys.executable, "-c", _PROBES[probe]], cwd=_REPO, env=env, capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr
