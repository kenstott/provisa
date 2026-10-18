# Copyright (c) 2026 Kenneth Stott
# Canary: 2333cd52-b3db-4abc-998e-72175c644316
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A user's read of a stored view outside its read window is refused on every surface (REQ-2066).

A statement is governed as ONE semantic statement: every view it reads is replaced by its SQL
before governance, so the tables a governed statement reads are the view's inputs, never the view.
The window is the view's, so it is judged on the statement as written -- the views it names -- on
each call, a kept (cached) governed plan included: the same statement is refused at 05:00 and
answered at 02:30."""

# Requirements: REQ-2066

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.audit.context import audit_identity_scope
from provisa.mv import load_window as lw
from tests.unit.test_table_read_as_caller import _Routed
from tests.unit.test_view_inputs_in_service import _OVER_A_SERVED_TABLE, _build, _view

pytestmark = pytest.mark.unit

_READ = 'SELECT id FROM "sales"."refund_notes" LIMIT 5'


def _at(monkeypatch, hour: int, minute: int = 0) -> None:
    at = datetime(2026, 10, 16, hour, minute, tzinfo=timezone.utc).timestamp()
    monkeypatch.setattr(lw.time, "time", lambda: at)


def _stored_view_state(monkeypatch):
    """A stored (materialized) view readable 02:00-04:00 UTC, over a table in service."""
    view = _view(_OVER_A_SERVED_TABLE, off_peak_window="02:00-04:00", off_peak_tz="UTC")
    state = _build(monkeypatch, view=view, drafts=[])
    stored = {
        f"view-{view['table_name']}": SimpleNamespace(
            region=None, enabled=True, status="stale", target_table=None
        )
    }
    monkeypatch.setattr(
        state, "mv_registry", SimpleNamespace(get=stored.get, get_fresh=list), raising=False
    )
    return state


async def _read_compiled(state) -> str:
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire import _pipeline

    async def _route(*args, **kwargs):
        raise _Routed(str(args[0]))

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)):
        with pytest.raises(_Routed) as routed:
            await _pipeline._govern_and_route_compiled(
                _READ, "r", state=state, cache_hint=NO_CACHE_HINT, sdl_joins=False
            )
    return routed.value.sql


async def test_a_users_compiled_read_of_a_view_outside_its_window_is_refused(monkeypatch):
    state = _stored_view_state(monkeypatch)
    _at(monkeypatch, 2, 30)
    with audit_identity_scope("ada", "http"):
        await _read_compiled(state)  # inside: answered, and its governed plan kept
        _at(monkeypatch, 5)
        with pytest.raises(lw.ViewOutsideLoadWindow) as refused:
            await _read_compiled(state)
    assert refused.value.params["view"] == "refund_notes"


async def test_a_users_sql_read_of_a_view_outside_its_window_is_refused(monkeypatch):
    from provisa.pgwire import _pipeline

    _stored_view_state(monkeypatch)  # the app's state, which govern_statement reads
    _at(monkeypatch, 2, 30)
    with audit_identity_scope("ada", "http"):
        await _pipeline.govern_statement(_READ, "r")  # inside: governed, and kept
        _at(monkeypatch, 5)
        with pytest.raises(lw.ViewOutsideLoadWindow) as refused:
            await _pipeline.govern_statement(_READ, "r")
    assert refused.value.params["view"] == "refund_notes"


async def test_provisas_own_read_of_the_view_is_answered_outside_its_window(monkeypatch):
    state = _stored_view_state(monkeypatch)
    _at(monkeypatch, 5)
    assert '"pg"."public"."orders"' in await _read_compiled(state)
