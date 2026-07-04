# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2d9a71-4b08-4e75-9f12-3c7a0d4f9c14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-874: delta-fetch uniform logic (field injection, PROBE==DELTA, cursor advance)."""

from __future__ import annotations

from provisa.federation.delta import (
    advance_cursor,
    delta_applies,
    delta_is_fresh,
    has_wm_placeholder,
    render_delta_fields,
)
from provisa.federation.strategy import Strategy


# ---- strategy gate ----------------------------------------------------------


def test_delta_only_for_materialized():
    assert delta_applies(Strategy.MATERIALIZED) is True
    assert delta_applies(Strategy.VIRTUAL) is False
    assert delta_applies(Strategy.SCAN) is False


# ---- field injection (Provisa substitutes, never parses) --------------------


def test_render_fields_substitutes_selection():
    tmpl = "query { orders(where: {updated_at: {_gt: $wm}}) { {{fields}} } }"
    out = render_delta_fields(tmpl, ["id", "updated_at", "total"])
    assert "{{fields}}" not in out
    assert "id, updated_at, total" in out
    assert "$wm" in out  # cursor placeholder left intact for native binding


def test_render_fields_custom_separator():
    assert render_delta_fields("SELECT {{fields}} FROM t", ["a", "b"], separator=" , ") == (
        "SELECT a , b FROM t"
    )


def test_has_wm_placeholder():
    assert has_wm_placeholder("WHERE updated > $wm") is True
    assert has_wm_placeholder("SELECT * FROM t") is False


# ---- PROBE == DELTA ---------------------------------------------------------


def test_empty_delta_is_fresh():
    assert delta_is_fresh([]) is True  # no rows changed → no-op


def test_nonempty_delta_is_not_fresh():
    assert delta_is_fresh([{"id": 1}]) is False  # rows changed → apply


# ---- cursor advance ---------------------------------------------------------


def test_cursor_advances_to_max():
    rows = [{"updated_at": 5}, {"updated_at": 9}, {"updated_at": 7}]
    assert advance_cursor(rows, "updated_at", current=3) == 9


def test_cursor_unchanged_on_empty():
    assert advance_cursor([], "updated_at", current=3) == 3


def test_cursor_ignores_rows_missing_the_field():
    rows = [{"updated_at": 5}, {"other": 1}]
    assert advance_cursor(rows, "updated_at", current=1) == 5


def test_cursor_works_with_string_values():
    rows = [{"ts": "2026-07-01"}, {"ts": "2026-07-03"}]
    assert advance_cursor(rows, "ts", current="2026-06-01") == "2026-07-03"
