# Copyright (c) 2026 Kenneth Stott
# Canary: 8ac438f1-37d2-4fd0-8638-b6c26eb9a7cb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table read where it lies, which declares a probing change signal, is watched for its views.

A live table had no event-loop node, so the change signal it declared never ran: a landing into it
told the views over it nothing, and they refreshed only on their own cadence. A live table that
declares ``probe`` or ``ttl_probe`` (its own, else its source's) is now a signal-only node: its
probe runs on its cadence, a change is posted to the views over it, and nothing lands. A live table
that declares none -- the default ttl -- still has no node."""

# Requirements: REQ-2055, REQ-929, REQ-941

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.events.boot import build_source_node_spec, watched_live


def _spec(monkeypatch, *, table_signal=None, source_signal=None, materialized=False):
    from provisa.federation.strategy import Strategy

    strategy = Strategy.MATERIALIZED if materialized else Strategy.VIRTUAL
    monkeypatch.setattr("provisa.federation.strategy.federate", lambda src, engine, **kw: strategy)
    monkeypatch.setattr(
        "provisa.federation.residency.resolve_landing_args",
        lambda src, tbl, platform=None: SimpleNamespace(
            columns=[("line_id", "integer"), ("landed_at", "timestamp")],
            change_signal=table_signal or source_signal or "ttl",
            watermark_column="landed_at",
            pk_columns=["line_id"],
            probe_type="watermark",
        ),
    )
    table = SimpleNamespace(
        source_id="sales",
        schema_name="sales",
        table_name="bronze",
        columns=[SimpleNamespace(name="line_id", native_filter_type=None)],
        row_materialize=False,
        cache_ttl=10,
        mv_preprocess=None,
        change_signal=table_signal,
        file_glob=None,
    )
    source = SimpleNamespace(
        id="sales", type=SimpleNamespace(value="postgresql"), change_signal=source_signal
    )
    runtime = SimpleNamespace(
        replica_address=lambda *, source_id, schema_name, table_name: SimpleNamespace(
            schema="org_replicas", table="sales__sales__bronze"
        )
    )
    return build_source_node_spec(
        table,
        source,
        engine=SimpleNamespace(dialect="duckdb"),
        engine_runtime=runtime,
        source_fetch=lambda src, tbl: pytest.fail("a live table is never fetched to land"),
        probe_scalar=lambda src, tbl: None,
    )


@pytest.mark.parametrize("signal", ["probe", "ttl_probe"])
def test_a_live_table_declaring_a_probe_is_a_signal_only_node(monkeypatch, signal):
    spec = _spec(monkeypatch, table_signal=signal)
    assert spec is not None and spec.kind == "source"
    assert spec.probe_factory is not None and spec.poll_seconds == 10
    assert not hasattr(spec.handle, "replica_key")  # nothing is replicated


def test_a_live_table_inherits_its_source_s_probe(monkeypatch):
    assert _spec(monkeypatch, source_signal="probe") is not None


@pytest.mark.parametrize("signal", [None, "ttl"])
def test_a_live_table_declaring_no_probe_has_no_node(monkeypatch, signal):
    assert _spec(monkeypatch, table_signal=signal) is None


def test_watched_live_reads_the_table_s_signal_before_its_source_s():
    table = SimpleNamespace(change_signal="ttl")
    assert not watched_live(table, SimpleNamespace(change_signal="probe"))
    assert watched_live(SimpleNamespace(change_signal=None), SimpleNamespace(change_signal="probe"))


@pytest.mark.asyncio
async def test_its_handle_lands_nothing_and_reports_the_change(monkeypatch):
    spec = _spec(monkeypatch, table_signal="probe")
    event_type, payload, content_hash = await spec.handle([{"id": 1}, {"id": 2}], prior_hash="h")
    assert event_type == "replace" and payload == {"signals": 2, "landed": None}
    assert content_hash is None  # never gated: every claimed signal ripples to the views


@pytest.mark.parametrize("stype", ["kafka", "websocket"])
def test_a_push_fed_table_is_a_signal_node_that_asks_for_no_build(monkeypatch, stype):
    """Its push listener lands each batch and posts it to this node; the node lands nothing, and
    never asks for a whole-table build its source cannot give (it was refused, and retried, for
    ever)."""
    from provisa.federation.strategy import Strategy

    monkeypatch.setattr(
        "provisa.federation.strategy.federate", lambda src, engine, **kw: Strategy.MATERIALIZED
    )
    monkeypatch.setattr(
        "provisa.federation.residency.resolve_landing_args",
        lambda src, tbl, platform=None: SimpleNamespace(
            columns=[("order_id", "varchar")],
            change_signal="ttl",
            watermark_column=None,
            pk_columns=["order_id"],
            probe_type="none",
        ),
    )
    table = SimpleNamespace(
        source_id="orders",
        schema_name="default",
        table_name="orders",
        columns=[SimpleNamespace(name="order_id", native_filter_type=None)],
        row_materialize=False,
        cache_ttl=None,
        mv_preprocess=None,
        change_signal=None,
        file_glob=None,
    )
    source = SimpleNamespace(
        id="orders", type=SimpleNamespace(value=stype), change_signal=None, cache_ttl=None
    )
    runtime = SimpleNamespace(
        replica_address=lambda *, source_id, schema_name, table_name: SimpleNamespace(
            schema="org_replicas", table="orders__default__orders"
        )
    )
    spec = build_source_node_spec(
        table,
        source,
        engine=SimpleNamespace(dialect="duckdb"),
        engine_runtime=runtime,
        source_fetch=lambda src, tbl: pytest.fail("a push-fed table is never fetched to land"),
        probe_scalar=lambda src, tbl: None,
        replica_build=lambda key: pytest.fail(f"a build was asked for {key}"),
    )
    assert spec is not None and spec.kind == "source"
    assert not hasattr(spec.handle, "replica_key")
