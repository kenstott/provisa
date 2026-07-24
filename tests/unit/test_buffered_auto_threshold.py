# Copyright (c) 2026 Kenneth Stott
# Canary: fa8957d5-0716-41f5-bf4c-1d618392bae5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1224 (streaming-uniformity Defect 4): the AUTOMATIC stream↔CTAS threshold at the single
terminal (``_execute_plan``). A buffered transport (JSON:API, GraphQL, Bolt) carries an ``auto_deliver``
policy on its governed plan; the terminal DECIDES per-result — inline the body when it fits under the
config row threshold, land an engine-native CTAS (``run_materialize``) off Provisa's heap when it
exceeds. No caller side-channel, no transport-local branch. Opt-in: disabled → the policy is None and
the terminal returns rows inline exactly as before."""

from __future__ import annotations

import pytest

from provisa.executor.redirect import Delivery, RedirectConfig, auto_delivery_for_buffered


def _config(threshold: int) -> RedirectConfig:
    return RedirectConfig(
        enabled=True,
        threshold=threshold,
        bucket="b",
        endpoint_url="",
        access_key="",
        secret_key="",
        ttl=60,
    )


def test_auto_delivery_disabled_by_default(monkeypatch):
    monkeypatch.delenv("PROVISA_REDIRECT_ENABLED", raising=False)
    assert auto_delivery_for_buffered("role-x") is None


def test_auto_delivery_enabled_carries_config(monkeypatch):
    monkeypatch.setenv("PROVISA_REDIRECT_ENABLED", "true")
    monkeypatch.setenv("PROVISA_REDIRECT_FORMAT", "orc")
    d = auto_delivery_for_buffered("role-x")
    assert d is not None
    assert d.role == "role-x"
    assert d.output_format == "orc"
    assert d.config.enabled is True


class _FakeStream:
    """A minimal ResultStream whose row generator records early closure (the engine cursor's
    ``finally``) — so the test can assert the terminal does NOT drain past the threshold."""

    def __init__(self, rows: list[tuple], closed_flag: list[bool]) -> None:
        self.column_names = ["n"]
        self.column_types = ["bigint"]
        self._rows = rows
        self._closed = closed_flag

    def iter_rows(self):
        try:
            for r in self._rows:
                yield r
        finally:
            self._closed[0] = True


class _FakeEngine:
    def __init__(self, rows: list[tuple], closed_flag: list[bool]) -> None:
        self._rows = rows
        self._closed = closed_flag
        self.calls: list[str] = []

    def execute_engine_sync(self, sql, params=None, *, session_hints=None):
        self.calls.append(sql)
        return _FakeStream(self._rows, self._closed)


class _FakeState:
    def __init__(self, engine) -> None:
        self.federation_engine = engine


def _plan(auto_deliver):
    from provisa.pgwire import _pipeline as P
    from provisa.transpiler.router import Route

    return P._Plan(
        route=Route.ENGINE,
        sql="SELECT n FROM t",
        source_id="s",
        dialect="duckdb",
        physical_sql="SELECT n FROM t",
        auto_deliver=auto_deliver,
        stamp=P._mint_stamp(),  # governed-provenance: the terminal refuses an unstamped plan
    )


@pytest.mark.asyncio
async def test_terminal_inlines_below_threshold():
    from provisa.pgwire._pipeline import _execute_plan

    closed = [False]
    engine = _FakeEngine([(i,) for i in range(3)], closed)
    deliv = Delivery(output_format="parquet", config=_config(threshold=5), role="r")
    result = await _execute_plan(_plan(deliv), _FakeState(engine))

    assert result.redirect is None
    assert result.rows == [(0,), (1,), (2,)]
    assert result.column_names == ["n"]
    assert closed[0]  # stream drained to exhaustion (its finally ran)


@pytest.mark.asyncio
async def test_terminal_materializes_above_threshold(monkeypatch):
    from provisa.pgwire import _pipeline as P

    closed = [False]
    engine = _FakeEngine([(i,) for i in range(100)], closed)

    captured: dict = {}

    async def _fake_run_materialize(state, sql, deliv):
        captured["sql"] = sql
        captured["deliv"] = deliv
        return {"sink": "s3://b/x.parquet", "row_count": None, "redirect_url": "http://x"}

    import provisa.executor.redirect as R

    monkeypatch.setattr(R, "run_materialize", _fake_run_materialize)

    deliv = Delivery(output_format="parquet", config=_config(threshold=10), role="r")
    result = await P._execute_plan(_plan(deliv), _FakeState(engine))

    assert result.rows == []
    assert result.redirect == {"sink": "s3://b/x.parquet", "row_count": None, "redirect_url": "http://x"}
    assert captured["sql"] == "SELECT n FROM t"  # the engine-physical CTAS source
    assert closed[0]  # partial buffer abandoned — the stream was closed early, not fully drained


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
