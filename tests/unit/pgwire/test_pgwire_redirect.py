# Copyright (c) 2026 Kenneth Stott
# Canary: 2a9e6c14-5f8b-4d37-b1c0-8e3f7a2d5b96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pgwire's forced redirect is a session setting (REQ-1194, REQ-1224 amended 2026-10-04).

A connection names ``provisa.redirect`` / ``provisa.redirect_format`` as startup parameters, in
``options``, or with SET / RESET. A redirected read answers one row -- url, format, row_count,
expires_at -- and is never delivered automatically on this streaming transport. A value that
cannot be read is refused by name."""

# Requirements: REQ-1194, REQ-1224

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from provisa.pgwire.server import (
    _REDIRECT_SHAPE,
    ProvisaSession,
    RedirectSettingInvalid,
    _redirect_row,
    startup_redirect_settings,
)


@pytest.fixture(autouse=True)
def _redirect_config(monkeypatch):
    monkeypatch.setattr(
        "provisa.executor.redirect.request_redirect_config",
        lambda threshold: SimpleNamespace(default_format=None, threshold=threshold),
    )


def test_a_startup_parameter_or_option_turns_it_on():
    assert startup_redirect_settings(
        {"provisa.redirect": "on", "provisa.redirect_format": "orc"}
    ) == {"provisa.redirect": True, "provisa.redirect_format": "orc"}
    assert startup_redirect_settings(
        {"options": "-c provisa.redirect=true -c provisa.redirect_format=parquet"}
    ) == {"provisa.redirect": True, "provisa.redirect_format": "parquet"}
    assert startup_redirect_settings({"user": "analyst"}) == {}


@pytest.mark.parametrize(
    "params",
    [
        {"provisa.redirect": "maybe"},
        {"provisa.redirect_format": "xlsx"},
        {"provisa.redirect": "on", "options": "-c provisa.redirect=off"},
    ],
)
def test_a_startup_value_that_cannot_be_read_is_refused(params):
    with pytest.raises(RedirectSettingInvalid):
        startup_redirect_settings(params)


def _session() -> ProvisaSession:
    session = ProvisaSession()
    session.role_id = "analyst"
    return session


def test_set_and_reset_change_the_sessions_delivery():
    session = _session()
    assert session.forced_delivery() is None
    assert session._set_redirect("SET provisa.redirect = on")
    assert session._set_redirect("SET provisa.redirect_format TO 'orc';")
    delivery = session.forced_delivery()
    assert delivery is not None and (delivery.output_format, delivery.role) == ("orc", "analyst")
    assert session._set_redirect("RESET provisa.redirect")
    assert session.forced_delivery() is None
    assert not session._set_redirect("SET search_path = public")


def test_a_set_value_that_cannot_be_read_is_refused():
    session = _session()
    with pytest.raises(RedirectSettingInvalid, match="'xlsx'"):
        session._set_redirect("SET provisa.redirect_format = 'xlsx'")
    assert session.redirect_format is None


def test_the_answer_is_one_row_naming_the_delivery():
    handle = {"redirect_url": "http://store/o.parquet", "row_count": 2, "expires_in": 3600}
    before = dt.datetime.now(dt.timezone.utc)
    result = _redirect_row(handle, SimpleNamespace(output_format="parquet"))
    assert result.column_names == [name for name, _ in _REDIRECT_SHAPE]
    ((url, fmt, rows, expires_at),) = result.rows
    assert (url, fmt, rows) == ("http://store/o.parquet", "parquet", 2)
    assert dt.timedelta(seconds=3599) <= expires_at - before <= dt.timedelta(seconds=3601)


@pytest.mark.asyncio
async def test_a_write_asked_to_be_delivered_is_refused_by_name(monkeypatch):
    """A delivery lands a result set; a write answers a count. The pipeline refuses the pair
    before routing rather than handing the write to the engine's CTAS terminal."""
    from unittest.mock import AsyncMock, patch

    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline
    from tests.unit.test_governed_sql_engine_internals import _state

    state = _state(["*"])
    # REQ-1942: prod's runtime -- a write goes to its source, not to a change log.
    state._active_runtime = lambda: SimpleNamespace(mutation_handling=None)
    state.tables = [
        {
            **t,
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "columns": [{**c, "writable_by": ["analyst"]} for c in t["columns"]],
        }
        for t in state.tables
    ]
    state.roles["analyst"]["capabilities"] = ["write"]
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))
    session = _session()
    session._set_redirect("SET provisa.redirect = on")
    routed = AsyncMock()
    with patch.object(_pipeline, "_optimize_and_route", new=routed):
        with pytest.raises(ValueError, match="a write has no result set to deliver"):
            await _pipeline._govern_and_route(
                "DELETE FROM sales.orders WHERE id = 2",
                "analyst",
                deliver=session.forced_delivery(),
            )
    routed.assert_not_awaited()
