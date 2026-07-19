# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8b2d13-9c05-46ea-b7d1-2a6e8c0f95b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-885: the demo Provisa-hosted (python) command returns a set of random rows.

Guards the set-returning contract (list of uniform row dicts) and the deterministic-seed path used
by the demo/e2e so a "command that returns a table" is reproducible."""

from __future__ import annotations

from demo.py_functions import random_dataset

_COLS = {"id", "region", "amount", "active"}


def test_returns_requested_row_count():
    rows = random_dataset({"args": {"rows": 3, "seed": 1}}, None)
    assert len(rows) == 3
    assert all(set(r) == _COLS for r in rows)


def test_seed_is_deterministic():
    a = random_dataset({"args": {"rows": 5, "seed": 42}}, None)
    b = random_dataset({"args": {"rows": 5, "seed": 42}}, None)
    assert a == b


def test_defaults_to_five_rows():
    rows = random_dataset({"args": {}}, None)
    assert len(rows) == 5


def test_accepts_flat_payload():
    # dispatch may pass args at the top level rather than under "args"
    rows = random_dataset({"rows": 2, "seed": 1}, None)
    assert len(rows) == 2
