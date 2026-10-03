# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2c9e73-1f4b-4a85-b7c0-9e3a5d1f8b42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``query_audit_log`` records how a statement was answered and how much it returned
(REQ-074/REQ-1386, amended 2026-10-01): ``route`` (cache | direct | engine) and ``row_count``,
on every transport, through the one audit seam.

A terminal that knows the row count when it finalizes reports it on the plan. A terminal that
finalizes a STREAMED result before it is drained defers the row to the end of the drain, where
the count — and whether the stream failed part-way — is known.
"""

# Requirements: REQ-074, REQ-1386

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.audit.pipeline import PendingAudit, write_denial
from provisa.audit.context import audit_identity_scope
from provisa.audit.writer import audit_writer_status, flush_audit
from provisa.pgwire._pipeline import _Plan, audit_on_drain, finalize_audit
from provisa.transpiler.router import Route


@pytest.fixture
def rows(monkeypatch):
    from provisa.encryption import NullEncryption

    written: list[dict] = []

    async def _log_queries(pool, batch):
        written.extend(batch)

    monkeypatch.setattr("provisa.audit.query_log.log_queries", _log_queries)
    monkeypatch.setattr("provisa.encryption.runtime.encryption_service", NullEncryption)

    def _rows() -> list[dict]:
        assert flush_audit(5.0), audit_writer_status()
        return written

    return _rows


_ENTRY = SimpleNamespace(age_seconds=3)  # the response-cache entry a hit was served from


def _state():
    return SimpleNamespace(
        tenant_db=object(),
        org_id="acme",
        admin_db=None,
        hot_counts=None,  # REQ-826: no Hot-count store; counting has its own tests
        federation_engine=SimpleNamespace(dialect="duckdb"),
        model_stamp=1,
    )


def _plan(route: Route = Route.DIRECT, **over) -> _Plan:
    return _Plan(
        route=route,
        sql="SELECT 1",
        source_id="s",
        dialect="postgres",
        audit=PendingAudit("alice", "pgwire", "analyst", "SELECT 1", [7], 0.0, 1, {}),
        **over,
    )


def _facts(row: dict) -> tuple:
    return row["route"], row["row_count"], row["status_code"]


def test_the_table_and_its_postgres_ddl_carry_route_and_row_count():
    from provisa.audit.query_log import AUDIT_SCHEMA_SQL
    from provisa.core.schema_org import query_audit_log

    assert {"route", "row_count"} <= set(query_audit_log.c.keys())
    assert query_audit_log.c.route.nullable and query_audit_log.c.row_count.nullable
    assert "route TEXT" in AUDIT_SCHEMA_SQL and "row_count INT" in AUDIT_SCHEMA_SQL


@pytest.mark.parametrize(
    ("route", "expected"), [(Route.DIRECT, "direct"), (Route.ENGINE, "engine")]
)
def test_a_finalized_plan_records_its_route_and_the_rows_its_terminal_reported(
    rows, route, expected
):
    plan = _plan(route)
    plan.row_count = 3
    asyncio.run(finalize_audit(plan, 200, _state()))
    assert [_facts(r) for r in rows()] == [(expected, 3, 200)]


def test_a_statement_served_from_the_response_cache_records_the_cache_route(rows):
    plan = _plan(Route.ENGINE)
    plan.row_count = 1
    asyncio.run(finalize_audit(plan, 200, _state(), cache_hit=True, cache_entry=_ENTRY))
    assert [_facts(r) for r in rows()] == [("cache", 1, 200)]


def test_a_refused_statement_has_neither(rows):
    with audit_identity_scope("mallory", "http"):
        asyncio.run(write_denial("SELECT 1", "ghost", None, None, _state()))
    assert [_facts(r) for r in rows()] == [(None, None, 403)]


def test_a_streamed_result_is_recorded_when_its_drain_ends_with_the_rows_it_delivered(rows):
    plan = _plan(Route.ENGINE)
    asyncio.run(finalize_audit(plan, 200, _state(), defer_to_drain=True))
    stream = audit_on_drain(plan, iter([[(1,), (2,)], [(3,)]]))
    assert next(stream) == [(1,), (2,)]
    assert rows() == []  # nothing is recorded while the result is still streaming
    assert list(stream) == [[(3,)]]
    assert [_facts(r) for r in rows()] == [("engine", 3, 200)]


def test_a_stream_that_fails_part_way_is_recorded_as_failed_with_the_rows_before_it(rows):
    def _batches():
        yield [(1,)]
        raise ConnectionError("source went away")

    plan = _plan(Route.DIRECT)
    asyncio.run(finalize_audit(plan, 200, _state(), defer_to_drain=True))
    stream = audit_on_drain(plan, _batches())
    with pytest.raises(ConnectionError):
        list(stream)
    assert [_facts(r) for r in rows()] == [("direct", 1, 500)]


def test_a_stream_the_client_stops_reading_is_recorded_with_what_was_delivered(rows):
    plan = _plan(Route.DIRECT)
    asyncio.run(finalize_audit(plan, 200, _state(), defer_to_drain=True))
    stream = audit_on_drain(plan, iter([[(1,), (2,)], [(3,)]]))
    next(stream)
    stream.close()
    assert [_facts(r) for r in rows()] == [("direct", 2, 200)]


def test_batches_are_counted_by_their_own_measure(rows):
    """Arrow batches report ``num_rows``; the caller says how a batch is counted."""
    plan = _plan(Route.ENGINE)
    asyncio.run(finalize_audit(plan, 200, _state(), defer_to_drain=True))
    batches = [SimpleNamespace(num_rows=40), SimpleNamespace(num_rows=2)]
    assert len(list(audit_on_drain(plan, iter(batches), rows_in=lambda b: b.num_rows))) == 2
    assert [_facts(r) for r in rows()] == [("engine", 42, 200)]


def test_a_plan_already_recorded_is_not_recorded_again_by_its_drain(rows):
    """A cache hit (or the buffered chokepoint) writes the row itself; wrapping the result it
    hands back must not write a second."""
    plan = _plan(Route.ENGINE)
    plan.row_count = 5
    asyncio.run(finalize_audit(plan, 200, _state(), cache_hit=True, cache_entry=_ENTRY))
    asyncio.run(finalize_audit(plan, 200, _state(), defer_to_drain=True))  # idempotent: no-op
    assert list(audit_on_drain(plan, iter([[(1,)]]))) == [[(1,)]]
    assert [_facts(r) for r in rows()] == [("cache", 5, 200)]
