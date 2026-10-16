# Copyright (c) 2026 Kenneth Stott
# Canary: 5e25aa6a-7931-425c-ae1d-529041d6c798
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2066: a materialized view's load management window says when a user's read may read it."""

# Requirements: REQ-2066

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from provisa.audit.context import audit_identity_scope
from provisa.core.read_refusal import ReadRefused
from provisa.mv import load_window as lw

_VIEW = {
    "id": 7,
    "table_name": "view_daily_sales",
    "view_sql": "SELECT 1",
    "off_peak_window": "02:00-04:00",
    "off_peak_tz": "UTC",
}


def _at(hour: int, minute: int = 0) -> float:
    return datetime(2026, 10, 16, hour, minute, tzinfo=timezone.utc).timestamp()


def _state(*rows: dict, materialized: bool = True) -> SimpleNamespace:
    mvs = {f"view-{r['table_name']}": object() for r in rows} if materialized else {}
    return SimpleNamespace(tables=list(rows), mv_registry=SimpleNamespace(get=mvs.get))


@pytest.fixture
def at(monkeypatch):
    def _set(hour: int, minute: int = 0) -> None:
        monkeypatch.setattr(lw.time, "time", lambda: _at(hour, minute))

    return _set


def test_a_users_read_outside_the_window_is_refused_naming_the_view_and_when_it_opens(at):
    at(5)
    with audit_identity_scope("ada", "http"), pytest.raises(lw.ViewOutsideLoadWindow) as refused:
        lw.refuse_outside_window(_state(_VIEW), (7,))
    assert isinstance(refused.value, ReadRefused)
    assert refused.value.code == "data.view_outside_load_window"
    assert refused.value.params == {
        "view": "view_daily_sales",
        "opens_at": "2026-10-17T02:00+00:00",
    }


def test_a_users_read_inside_the_window_answers(at):
    at(2, 30)
    with audit_identity_scope("ada", "http"):
        lw.refuse_outside_window(_state(_VIEW), (7,))


@pytest.mark.parametrize("surface", ["http", "pgwire", "grpc", "flight", "mcp", "bolt"])
def test_every_user_surface_is_held_to_the_window(at, surface):
    at(5)
    with audit_identity_scope("ada", surface), pytest.raises(lw.ViewOutsideLoadWindow):
        lw.refuse_outside_window(_state(_VIEW), (7,))


def test_provisas_own_work_is_never_refused(at):
    """A refresh, a replica build or the cascade binds no identity; a scheduled job or trigger
    runs as scheduler:<id> on the scheduler surface."""
    at(5)
    lw.refuse_outside_window(_state(_VIEW), (7,))
    with audit_identity_scope("scheduler:nightly", "scheduler"):
        lw.refuse_outside_window(_state(_VIEW), (7,))


def test_a_view_with_no_window_a_view_not_stored_and_a_view_not_read_are_unaffected(at):
    at(5)
    with audit_identity_scope("ada", "http"):
        lw.refuse_outside_window(_state({**_VIEW, "off_peak_window": None}), (7,))
        lw.refuse_outside_window(_state(_VIEW, materialized=False), (7,))
        lw.refuse_outside_window(_state(_VIEW), (8,))


def test_the_window_is_read_in_the_views_own_zone(at):
    at(7)  # 02:00 in New York (EDT, UTC-4) is 06:00 UTC: 07:00 UTC is inside 02:00-04:00 local
    with audit_identity_scope("ada", "http"):
        lw.refuse_outside_window(_state({**_VIEW, "off_peak_tz": "America/New_York"}), (7,))


def test_a_view_outside_its_window_answers_none_of_a_users_reads_in_another_statements_place(at):
    at(5)
    mvs = [SimpleNamespace(id="view-view_daily_sales"), SimpleNamespace(id="view-view_other")]
    state = _state(_VIEW, {**_VIEW, "id": 8, "table_name": "view_other", "off_peak_window": None})
    with audit_identity_scope("ada", "http"):
        assert [m.id for m in lw.readable_now(mvs, state)] == ["view-view_other"]
    assert lw.readable_now(mvs, state) == mvs  # Provisa's own statement may be answered from it


def test_a_view_declares_its_window_in_the_config_and_only_a_stored_one_may():
    from provisa.core.config_loader import views_as_tables

    raw = views_as_tables(
        {
            "views": [
                {
                    "id": "daily_sales",
                    "sql": "SELECT 1",
                    "materialize": True,
                    "off_peak_window": "02:00-04:00",
                    "off_peak_tz": "Europe/London",
                }
            ]
        }
    )
    [entry] = raw["tables"]
    assert (entry["off_peak_window"], entry["off_peak_tz"]) == ("02:00-04:00", "Europe/London")
    with pytest.raises(ValueError, match="off_peak_window applies to a materialized view"):
        views_as_tables(
            {"views": [{"id": "v", "sql": "SELECT 1", "off_peak_window": "02:00-04:00"}]}
        )
