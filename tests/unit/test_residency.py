# Copyright (c) 2026 Kenneth Stott
# Canary: e1974808-1285-44e0-a638-711c07f88ab3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-825/932/1661: residency prep — resolve landing args; the base backend lands stale MATERIALIZED tables."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.change_signal import APPEND, CDC, REPLACE, select_landing_shape
from provisa.federation.residency import resolve_landing_args
from provisa.federation.strategy import Strategy


def _col(name, data_type: str | None = "text", pk=False):
    return SimpleNamespace(name=name, data_type=data_type, is_primary_key=pk)


def _table(
    source_id="s1",
    *,
    change_signal=None,
    watermark_column=None,
    live=None,
    columns=None,
    probe_type=None,
):
    return SimpleNamespace(
        source_id=source_id,
        schema_name="public",
        table_name="events",
        change_signal=change_signal,
        watermark_column=watermark_column,
        probe_type=probe_type,
        live=live,
        columns=columns or [_col("id", "bigint", pk=True), _col("status", "text")],
    )


def _source(source_id="s1", *, change_signal="ttl", type="openapi"):
    return SimpleNamespace(
        id=source_id,
        type=type,
        change_signal=change_signal,
        freshness_gate=False,
        cache_ttl=None,
        prefer_materialized=False,
        load_protected=False,
    )


class TestResolveProbeType:  # REQ-982
    def test_ttl_resolves_to_none(self):
        args = resolve_landing_args(_source(change_signal="ttl"), _table())
        assert args.probe_type == "none"

    def test_sql_probe_default_watermark(self):
        args = resolve_landing_args(
            _source(change_signal="probe", type="postgresql"),
            _table(watermark_column="updated_at"),
        )
        assert args.probe_type == "watermark"

    def test_sql_probe_default_count_without_watermark(self):
        args = resolve_landing_args(_source(change_signal="probe", type="postgresql"), _table())
        assert args.probe_type == "count"

    def test_api_probe_default_hash(self):
        args = resolve_landing_args(_source(change_signal="ttl_probe", type="openapi"), _table())
        assert args.probe_type == "hash"

    def test_explicit_type_outside_capability_raises(self):
        import pytest as _pytest

        with _pytest.raises(ValueError, match="not supported"):
            resolve_landing_args(
                _source(change_signal="probe", type="csv"), _table(probe_type="watermark")
            )


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

    def test_watermark_is_a_single_existing_column(self):
        # REQ-924: the watermark is one column NAME drawn from the table's own columns — a
        # single string field, never a derived/synthetic source column. Naming a real column
        # gates APPEND landing; the landed shape carries that column like any other.
        t = _table(
            watermark_column="updated_at",
            columns=[
                _col("id", "bigint", pk=True),
                _col("updated_at", "timestamp"),
            ],
        )
        args = resolve_landing_args(_source(change_signal="ttl_probe"), t)
        assert args.watermark_column == "updated_at"
        assert "updated_at" in {name for name, _ in args.columns}
        assert select_landing_shape(args.change_signal, args.watermark_column) == APPEND

    def test_view_derived_watermark_treated_like_source_column(self):
        # REQ-931: a derived/synthetic watermark is manufactured in the SQL/view layer, so it
        # reaches residency as an ordinary projected column (e.g. GREATEST(created_at,
        # updated_at) aliased `wm`). Landing treats it identically — no source-level synthetic
        # stamping is needed; the computed column gates APPEND like a native one.
        t = _table(
            watermark_column="wm",
            columns=[
                _col("id", "bigint", pk=True),
                _col("wm", "timestamp"),  # view-computed GREATEST(...) projected as `wm`
            ],
        )
        args = resolve_landing_args(_source(change_signal="probe"), t)
        assert args.watermark_column == "wm"
        assert select_landing_shape(args.change_signal, args.watermark_column) == APPEND

    def test_shape_matches_resolved_signal(self):
        # ttl + no watermark → REPLACE; poll + watermark → APPEND; push → CDC
        a = resolve_landing_args(_source(change_signal="ttl"), _table())
        assert select_landing_shape(a.change_signal, a.watermark_column) == REPLACE
        b = resolve_landing_args(_source(change_signal="ttl_probe"), _table(watermark_column="u"))
        assert select_landing_shape(b.change_signal, b.watermark_column) == APPEND
        c = resolve_landing_args(_source(change_signal="kafka"), _table())
        assert select_landing_shape(c.change_signal, c.watermark_column) == CDC


class _FakeLoader:
    def __init__(self, rows_by_table):
        self.rows_by_table = rows_by_table
        self.loaded = []

    async def load(self, source, table):
        self.loaded.append((source.id, table.table_name))
        return self.rows_by_table.get(table.table_name, [])


class _FakeBackend:
    """The base ``materialize_pending`` over a fake engine: records what it lands and where."""

    def __init__(self):
        from provisa.federation.backend import EngineBackend

        self.engine = SimpleNamespace(
            name="fake",
            materialize_store=lambda: "postgresql://store/db",
            native_store="postgresql",
            file_native=False,
        )
        self.dialect = "trino"
        self.calls = []
        self._impl = EngineBackend.materialize_pending

    def landing_target(self, *, store_schema, source_id, source_type, schema_name, table_name):
        return store_schema, f"{source_id}__{schema_name}__{table_name}"

    async def land_source_table(self, state, *, schema, table, columns, rows, **kw):
        self.calls.append(
            SimpleNamespace(schema=schema, table=table, columns=columns, rows=rows, **kw)
        )
        return f"{schema}.{table}"

    async def materialize_pending(self, state, **kw):
        return await self._impl(self, state, **kw)  # type: ignore[arg-type]


@pytest.mark.asyncio
def _registry_is_config(monkeypatch):
    """REQ-1674: the landing path reads the registry view; in these tests the config IS the registry."""

    async def _sources(state, conn=None):
        return list(state.config.sources)

    async def _tables(state, conn=None):
        return list(state.config.tables)

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)


async def test_materialize_pending_lands_each_stale_source_table(monkeypatch):
    """REQ-1661: the base backend lands every table of a stale MATERIALIZED source at the engine's
    landing address through its store write face -- the event loop's own path."""
    import provisa.federation.backend as backend_mod

    monkeypatch.setattr(backend_mod, "_env_store_schema", lambda dsn: "mat")
    monkeypatch.setattr(
        "provisa.federation.plan.federate", lambda s, e, **kw: Strategy.MATERIALIZED
    )
    src = _source("s1", change_signal="ttl")
    tbl = _table("s1", watermark_column="updated_at")
    state = SimpleNamespace(config=SimpleNamespace(sources=[src], tables=[tbl]))
    _registry_is_config(monkeypatch)
    backend = _FakeBackend()
    loader = _FakeLoader({"events": [{"id": 1, "status": "new"}]})

    landed = await backend.materialize_pending(state, loader=loader, is_stale=lambda sid: True)
    assert landed == [("s1", "events")]
    assert loader.loaded == [("s1", "events")]
    call = backend.calls[0]
    assert (call.schema, call.table) == ("mat", "s1__public__events")
    assert call.rows == [{"id": 1, "status": "new"}]
    assert call.columns == [("id", "bigint"), ("status", "text")]
    assert call.pk_columns == ["id"]
    assert call.watermark_column == "updated_at"
    assert call.change_signal == "ttl"


@pytest.mark.asyncio
async def test_materialize_pending_is_a_noop_when_nothing_is_stale_or_named(monkeypatch):
    monkeypatch.setattr(
        "provisa.federation.plan.federate", lambda s, e, **kw: Strategy.MATERIALIZED
    )
    src = _source("s1")
    state = SimpleNamespace(config=SimpleNamespace(sources=[src], tables=[_table("s1")]))
    _registry_is_config(monkeypatch)
    backend = _FakeBackend()
    loader = _FakeLoader({})
    assert await backend.materialize_pending(state, loader=loader, is_stale=lambda s: False) == []
    assert (
        await backend.materialize_pending(
            state, loader=loader, is_stale=lambda s: True, source_ids={"other"}
        )
        == []
    )
    assert backend.calls == [] and loader.loaded == []
