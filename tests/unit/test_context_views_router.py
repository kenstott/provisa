# Copyright (c) 2026 Kenneth Stott
# Canary: b1e68872-c2f1-4521-8b68-45c779299d80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2259: the read endpoints behind the Context group's Tags and Classification pages.

Asserted here: every endpoint is gated on glossary_read, read from the router source so the assertion is
about the wiring; a table or column the caller does not read is counted and never named; a tag
lists the row filters keyed on it and the classification kinds that assign it; a kind lists the
columns it was detected on.
"""

# Requirements: REQ-2259

from __future__ import annotations

import re
import types
from pathlib import Path

from provisa.api.admin.context_views_router import (
    CLASSIFICATION_TAG,
    awaiting_by_table,
    kind_detections,
    placed,
    tag_usages,
)

_ROUTER = Path(__file__).resolve().parents[2] / "provisa/api/admin/context_views_router.py"

# The caller reads table 1 whole and column "email" of table 2; nothing of table 3.
_READS = {1: None, 2: {"email"}}


def _readable(table_id: int, column: str | None) -> bool:
    if table_id not in _READS:
        return False
    columns = _READS[table_id]
    return columns is None or column is None or column in columns


def _row(table_id: int, column: str | None, **extra) -> dict:
    return {
        "table_id": table_id,
        "table_ref": f"s.public.t{table_id}",
        "column_name": column,
        **extra,
    }


def test_every_endpoint_is_gated_on_glossary_read():
    source = _ROUTER.read_text()
    blocks = re.split(r"^@router\.", source, flags=re.M)[1:]
    assert len(blocks) == 4
    for block in blocks:
        assert "_require_glossary_read(request)" in block, block[:60]
    assert "Capability.GLOSSARY_READ.value" in source


def test_unreadable_places_are_counted_not_named():
    named, hidden = placed(
        [_row(1, None), _row(2, "email"), _row(2, "ssn"), _row(3, None), _row(3, "x")],
        _readable,
    )
    assert named == [
        {"tableId": 1, "tableRef": "s.public.t1", "column": None},
        {"tableId": 2, "tableRef": "s.public.t2", "column": "email"},
    ]
    assert hidden == 3
    assert all("t3" not in n["tableRef"] and n["column"] != "ssn" for n in named)


def test_tag_usages_name_dependents_and_hide_unreadable():
    out = tag_usages(
        ["finance", CLASSIFICATION_TAG, "unused"],
        [
            _row(1, "amount", base_tag_id="finance"),
            _row(3, None, base_tag_id="finance"),
            _row(2, "email", base_tag_id=CLASSIFICATION_TAG),
            _row(2, "ssn", base_tag_id=CLASSIFICATION_TAG),
        ],
        {"finance": 2},
        ["email", "ssn"],
        _readable,
    )
    by_id = {t["id"]: t for t in out}
    assert by_id["finance"]["usages"] == [
        {"tableId": 1, "tableRef": "s.public.t1", "column": "amount"}
    ]
    assert by_id["finance"]["hiddenUsages"] == 1
    assert by_id["finance"]["rowFilters"] == 2
    assert by_id["finance"]["classificationKinds"] == []
    assert by_id[CLASSIFICATION_TAG]["classificationKinds"] == ["email", "ssn"]
    assert by_id[CLASSIFICATION_TAG]["hiddenUsages"] == 1
    assert by_id["unused"] == {
        "id": "unused",
        "usages": [],
        "hiddenUsages": 0,
        "rowFilters": 0,
        "classificationKinds": [],
    }


def test_kind_detections_hide_unreadable_and_report_enabled():
    kinds = [
        types.SimpleNamespace(id="email", sensitive=True, threshold=0.9, mask_preset="email"),
        types.SimpleNamespace(id="zip", sensitive=False, threshold=0.8, mask_preset=None),
    ]
    out = kind_detections(
        kinds,
        frozenset({"zip"}),
        [_row(2, "email", kind="email"), _row(3, "contact", kind="email")],
        _readable,
    )
    assert out[0]["detections"] == [{"tableId": 2, "tableRef": "s.public.t2", "column": "email"}]
    assert out[0]["hiddenDetections"] == 1
    assert out[0]["enabled"] is True
    assert out[1] == {
        "id": "zip",
        "sensitive": False,
        "threshold": 0.8,
        "maskPreset": None,
        "enabled": False,
        "detections": [],
        "hiddenDetections": 0,
    }


def test_the_classification_tag_is_the_one_accepting_assigns():
    source = (
        Path(__file__).resolve().parents[2] / "provisa/profiler/classifications.py"
    ).read_text()
    assert f'tag_repo.get(conn, "{CLASSIFICATION_TAG}")' in source


def test_awaiting_detections_less_decisions_counted_per_readable_table():
    # REQ-2259 Context Health: the latest run's suggestions, less those decided since; a column
    # the caller does not read is counted and its table never named.
    suggested = [
        _row(1, "email", kind="email_address"),
        _row(1, "phone", kind="phone_number"),
        _row(1, "card", kind="payment_card"),
        _row(2, "email", kind="email_address"),
        _row(2, "ssn", kind="us_ssn"),
        _row(3, "x", kind="iban"),
    ]
    decided = {(1, "card", "payment_card")}
    named, hidden = awaiting_by_table(suggested, decided, _readable)
    assert named == [
        {"tableId": 1, "tableRef": "s.public.t1", "awaiting": 2},
        {"tableId": 2, "tableRef": "s.public.t2", "awaiting": 1},
    ]
    assert hidden == 2


def test_awaiting_is_empty_when_every_detection_is_decided():
    named, hidden = awaiting_by_table(
        [_row(1, "email", kind="email_address")], {(1, "email", "email_address")}, _readable
    )
    assert (named, hidden) == ([], 0)
