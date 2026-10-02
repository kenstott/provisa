# Copyright (c) 2026 Kenneth Stott
# Canary: 45cb81a8-09a8-4c11-8c55-0ceddf42677f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: what an operator sees of a replica's build — its state, how it copies (method and
load kind, known from the moment it starts), rows copied and the rate so far, its last error."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from provisa.api.admin._replica_builds import build_view

pytestmark = pytest.mark.unit

NOW = datetime(2026, 10, 2, 12, 0, 0, tzinfo=UTC)


def _record(**kw):
    base = dict(
        key=("src", "public", "orders"),
        build_state="idle",
        requested_reason=None,
        build_started_at=None,
        build_method=None,
        load_kind=None,
        rows_copied=None,
        completed_at=None,
        next_refresh_at=None,
        last_error=None,
        waiting_on=None,
        retired_at=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def test_a_running_row_copy_build_shows_its_method_progress_and_rate():
    view = build_view(
        _record(
            build_state="building",
            requested_reason="model",
            build_started_at=NOW - timedelta(seconds=100),
            build_method="stream_batches",
            load_kind="row_copy",
            rows_copied=3_800_000,
        ),
        NOW,
    )
    assert (view["state"], view["method"], view["load_kind"]) == (
        "building",
        "stream_batches",
        "row_copy",
    )
    assert view["rows_copied"] == 3_800_000 and view["rows_per_second"] == 38_000.0
    assert view["started_at"] == "2026-10-02T11:58:20+00:00"
    assert (view["source_id"], view["schema_name"], view["table_name"]) == (
        "src",
        "public",
        "orders",
    )


def test_a_build_with_no_progress_yet_has_no_rate():
    started = _record(build_state="building", build_started_at=NOW, rows_copied=0)
    assert build_view(started, NOW)["rows_per_second"] is None


def test_a_finished_build_has_no_running_rate_and_keeps_its_last_method():
    view = build_view(
        _record(
            build_started_at=NOW - timedelta(seconds=10),
            build_method="engine_statement",
            load_kind="bulk_stream",
            rows_copied=500,
            completed_at=NOW,
        ),
        NOW,
    )
    assert view["state"] == "idle" and view["rows_per_second"] is None
    assert view["method"] == "engine_statement" and view["completed_at"] is not None


def test_a_failed_build_shows_its_error_and_a_waiting_one_says_why():
    failed = build_view(_record(build_state="failed", last_error="source down"), NOW)
    assert (failed["state"], failed["last_error"]) == ("failed", "source down")
    waiting = build_view(_record(build_state="requested", waiting_on="engine_jobs"), NOW)
    assert (waiting["state"], waiting["waiting_on"]) == ("requested", "engine_jobs")


def test_a_replica_the_model_no_longer_declares_is_shown_as_retired():
    view = build_view(_record(build_state="idle", completed_at=NOW, retired_at=NOW), NOW)
    assert view["state"] == "retired"
