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

import pytest

from demo.py_functions import label_pets, random_dataset

_COLS = {"id", "region", "amount", "active"}

_PETS = {
    "kind": "result_set",
    "columns": ["id", "name"],
    "rows": [{"id": 1, "name": "Buddy"}, {"id": 2, "name": "Luna"}, {"id": 10, "name": "Max"}],
}


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


# --- label_pets ---


def test_label_pets_basic():
    rows = label_pets({"input": _PETS, "prefix": "DOG-", "min_score": 0}, None)
    assert len(rows) == 3
    assert {r["id"] for r in rows} == {1, 2, 10}
    buddy = next(r for r in rows if r["id"] == 1)
    assert buddy["label"] == "DOG-Buddy"
    # score is deterministic: (1 * 37) % 100 = 37, /100 = 0.37
    assert buddy["score"] == 0.37


def test_label_pets_min_score_filters():
    # id=1: score_pct=37, id=2: score_pct=74, id=10: score_pct=70
    rows = label_pets({"input": _PETS, "min_score": 70}, None)
    ids = {r["id"] for r in rows}
    assert 1 not in ids  # 37 < 70
    assert 2 in ids  # 74 >= 70
    assert 10 in ids  # 70 >= 70


def test_label_pets_default_prefix():
    rows = label_pets({"input": _PETS}, None)
    assert all(r["label"].startswith("P-") for r in rows)


def test_label_pets_output_columns():
    rows = label_pets({"input": _PETS}, None)
    assert all(set(r) == {"id", "score", "label"} for r in rows)


def test_label_pets_no_relation_raises():
    with pytest.raises(ValueError, match="label_pets expects a result_set"):
        label_pets({"prefix": "X-"}, None)
