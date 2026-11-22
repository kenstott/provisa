# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e8d41-7c3a-4f96-b0e2-1d9a6c4f8e73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin tables listing answers each table's ``timeTravel`` (REQ-1163). A view's kind is
judged by its ``schema.table`` key in the view reads (REQ-135), so the resolver hands
:func:`provisa.mv.time_travel.relation_kinds` the view's schema -- a view the reads do not key
travels in no time, and the listing answers it rather than failing."""

# Requirements: REQ-1163, REQ-135

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.api.admin.types import RegisteredTableType

pytestmark = pytest.mark.unit


def _view(**overrides: Any) -> RegisteredTableType:
    fields: dict[str, Any] = dict(
        id=7,
        registered_at="2026-11-20T00:00:00+00:00",
        source_id="__derived__",
        domain_id="sales",
        schema_name="views",
        table_name="order_totals",
        write_ops=[],
        alias=None,
        description=None,
        cache_ttl=None,
        replicate=None,
        region=None,
        withdrawn=False,
        source_withdrawn=False,
        row_materialize=False,
        load_protected=None,
        off_peak_window=None,
        off_peak_tz=None,
        gql_naming_convention=None,
        watermark_column=None,
        columns=[],
        view_sql="SELECT 1 AS n",
    )
    fields.update(overrides)
    return RegisteredTableType(**fields)


def _state(monkeypatch: pytest.MonkeyPatch, bitemporal_view_reads: dict[str, Any]) -> None:
    state = SimpleNamespace(bitemporal_view_reads=bitemporal_view_reads, tables=[], source_types={})
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)


def test_a_view_no_read_keys_travels_in_no_time(monkeypatch: pytest.MonkeyPatch) -> None:
    _state(monkeypatch, {"views.other_view": object()})
    assert cast(Any, _view()).time_travel() is None


def test_a_bitemporal_view_is_judged_under_its_own_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    _state(monkeypatch, {"views.order_totals": object()})
    assert cast(Any, _view()).time_travel() == "bitemporal"
    assert cast(Any, _view(schema_name="elsewhere")).time_travel() is None
