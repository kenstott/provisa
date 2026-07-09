# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-825/932: residency prep — resolve landing args + run_prep lands stale MATERIALIZED tables."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.change_signal import APPEND, CDC, REPLACE, select_landing_shape
from provisa.federation.plan import Plan, PrepStep
from provisa.federation.residency import resolve_landing_args, run_prep
from provisa.federation.strategy import Strategy


def _col(name, data_type: str | None = "text", pk=False):
    return SimpleNamespace(name=name, data_type=data_type, is_primary_key=pk)


def _table(source_id="s1", *, change_signal=None, watermark_column=None, live=None, columns=None):
    return SimpleNamespace(
        source_id=source_id,
        schema_name="public",
        table_name="events",
        change_signal=change_signal,
        watermark_column=watermark_column,
        live=live,
        columns=columns or [_col("id", "bigint", pk=True), _col("status", "text")],
    )


def _source(source_id="s1", *, change_signal="ttl"):
    return SimpleNamespace(id=source_id, type="openapi", change_signal=change_signal)


class TestResolveLandingArgs:
    def test_columns_and_pk(self):
        args = resolve_landing_args(_source(), _table())
        assert args.columns == [("id", "bigint"), ("status", "text")]
        assert args.pk_columns == ["id"]

    def test_columns_translated_to_ir_by_platform(self):
        # REQ-846: engine-normalized native spellings the generic aliases don't cover — Trino
        # varbinary/row/varchar(n)/timestamp-with-tz — resolve to canonical IR at the landing seam.
        t = _table(
            columns=[
                _col("blob", "varbinary"),
                _col("doc", "row(x integer)"),
                _col("name", "varchar(255)"),
                _col("ts", "timestamp with time zone"),
            ]
        )
        args = resolve_landing_args(_source(), t, platform="trino")
        assert args.columns == [
            ("blob", "bytea"),
            ("doc", "text"),
            ("name", "text"),
            ("ts", "timestamp"),
        ]

    def test_unmapped_native_type_raises(self):
        # REQ-846: an unmapped native type is a vocabulary gap — raise, never a silent varchar default.
        with pytest.raises(ValueError, match="not in the IR vocabulary"):
            resolve_landing_args(
                _source(), _table(columns=[_col("x", "geometry")]), platform="trino"
            )

    def test_table_signal_overrides_source(self):
        args = resolve_landing_args(_source(change_signal="ttl"), _table(change_signal="debezium"))
        assert args.change_signal == "debezium"

    def test_inherits_source_signal(self):
        args = resolve_landing_args(_source(change_signal="probe"), _table(change_signal=None))
        assert args.change_signal == "probe"

    def test_legacy_live_strategy_read_through(self):
        live = SimpleNamespace(strategy="debezium", watermark_column=None)
        args = resolve_landing_args(_source(), _table(change_signal=None, live=live))
        assert args.change_signal == "debezium"

    def test_watermark_from_table_then_live(self):
        assert (
            resolve_landing_args(_source(), _table(watermark_column="updated_at")).watermark_column
            == "updated_at"
        )
        live = SimpleNamespace(strategy="poll", watermark_column="seq")
        assert (
            resolve_landing_args(
                _source(), _table(watermark_column=None, live=live)
            ).watermark_column
            == "seq"
        )

    def test_missing_data_type_raises(self):
        t = _table(columns=[_col("id", None)])
        with pytest.raises(ValueError, match="no resolved data_type"):
            resolve_landing_args(_source(), t)

    def test_shape_matches_resolved_signal(self):
        # ttl + no watermark → REPLACE; poll + watermark → APPEND; push → CDC
        a = resolve_landing_args(_source(change_signal="ttl"), _table())
        assert select_landing_shape(a.change_signal, a.watermark_column) == REPLACE
        b = resolve_landing_args(_source(change_signal="ttl_probe"), _table(watermark_column="u"))
        assert select_landing_shape(b.change_signal, b.watermark_column) == APPEND
        c = resolve_landing_args(_source(change_signal="kafka"), _table())
        assert select_landing_shape(c.change_signal, c.watermark_column) == CDC


class _FakeRuntime:
    dialect = "trino"  # engine-normalized stored types are Trino spellings (REQ-846)

    def __init__(self):
        self.calls = []

    async def materialize_source(
        self, source, columns, rows, *, change_signal, watermark_column, pk_columns
    ):
        self.calls.append(
            SimpleNamespace(
                id=source.id,
                schema_name=source.schema_name,
                table_name=source.table_name,
                columns=columns,
                rows=rows,
                change_signal=change_signal,
                watermark_column=watermark_column,
                pk_columns=pk_columns,
            )
        )


class _FakeLoader:
    def __init__(self, rows_by_table):
        self.rows_by_table = rows_by_table
        self.loaded = []

    async def load(self, source, table):
        self.loaded.append((source.id, table.table_name))
        return self.rows_by_table.get(table.table_name, [])


@pytest.mark.asyncio
async def test_run_prep_lands_each_prep_table():
    src = _source("s1", change_signal="ttl")
    tbl = _table("s1", watermark_column="updated_at")  # poll + watermark → append signal-wise
    plan = Plan(prep=[PrepStep("s1", Strategy.MATERIALIZED)])
    runtime = _FakeRuntime()
    loader = _FakeLoader({"events": [{"id": 1, "status": "new"}]})

    landed = await run_prep(
        plan,
        sources_by_id={"s1": src},
        tables_by_source={"s1": [tbl]},
        runtime=runtime,
        loader=loader,
    )
    assert landed == [("s1", "events")]
    assert loader.loaded == [("s1", "events")]
    assert len(runtime.calls) == 1
    call = runtime.calls[0]
    assert call.rows == [{"id": 1, "status": "new"}]
    assert call.columns == [("id", "bigint"), ("status", "text")]
    assert call.pk_columns == ["id"]
    assert call.watermark_column == "updated_at"
    assert call.change_signal == "ttl"
    assert call.table_name == "events"


@pytest.mark.asyncio
async def test_run_prep_empty_plan_is_noop():
    runtime = _FakeRuntime()
    landed = await run_prep(
        Plan(prep=[]),
        sources_by_id={},
        tables_by_source={},
        runtime=runtime,
        loader=_FakeLoader({}),
    )
    assert landed == []
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_run_prep_multi_table_source_lands_all():
    src = _source("s1")
    t1 = _table("s1")
    t1.table_name = "a"
    t2 = _table("s1")
    t2.table_name = "b"
    plan = Plan(prep=[PrepStep("s1", Strategy.MATERIALIZED)])
    runtime = _FakeRuntime()
    loader = _FakeLoader({"a": [{"id": 1}], "b": [{"id": 2}]})

    landed = await run_prep(
        plan,
        sources_by_id={"s1": src},
        tables_by_source={"s1": [t1, t2]},
        runtime=runtime,
        loader=loader,
    )
    assert landed == [("s1", "a"), ("s1", "b")]
    assert {c.table_name for c in runtime.calls} == {"a", "b"}
