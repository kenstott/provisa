# Copyright (c) 2026 Kenneth Stott
# Canary: 0c7e4a92-5b1d-4f38-9e26-a3d8f1b64c05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1907 (amended 2026-09-30): an admin table or source save is refused when it would leave a
ttl / ttl_probe table with no table or source cache_ttl, and an updateTable save keeps the table's
stored cache_ttl, role_ttl and row_materialize (TableInput carries none of them).

A real SQLite control plane (sources + registered_tables) through the admin mutations."""

# Requirements: REQ-1907

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from provisa.api.admin._landing_ttl import SourceTtl, TableTtl
from provisa.api.admin.types import ColumnInput, SourceInput, TableInput
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import registered_tables, roles, sources, table_columns

pytestmark = pytest.mark.unit

_TABLES = [sources, registered_tables, table_columns, roles]


class _Pool:
    def __init__(self, db: Database) -> None:
        self._db = db

    def acquire(self):
        return self._db.acquire()


@asynccontextmanager
async def _db(
    tmp_path,
    *,
    source_signal="ttl",
    source_ttl=None,
    table_signal=None,
    table_ttl=None,
    table_replicate=0,
    source_replicate=None,
    source_type="postgresql",
):
    """One source ``s`` (of ``source_type``) with one table ``orders``. ``table_replicate`` 0 (always) says the table
    is replicated (REQ-1907 option B); None = it inherits its source's value, which by default
    is not set either."""
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as c:
        sources.metadata.create_all(c)
    db = Database(engine, name="cp")
    async with db.acquire() as conn:
        await conn.execute_core(
            sources.insert().values(
                id="s",
                type=source_type,
                host="h",
                port=5432,
                database="d",
                username="u",
                change_signal=source_signal,
                cache_ttl=source_ttl,
                replicate=source_replicate,
            )
        )
        await conn.execute_core(
            registered_tables.insert().values(
                source_id="s",
                domain_id="",
                schema_name="public",
                table_name="orders",
                change_signal=table_signal,
                cache_ttl=table_ttl,
                role_ttl={"analyst": 360},
                pagination={"max_rows": 50},  # REQ-318: saved through updateTablePaging
                row_materialize=False,
                replicate=table_replicate,
            )
        )
    try:
        yield db
    finally:
        await db.close()
        engine.dispose()


async def _table_row(db: Database):
    async with db.acquire() as conn:
        res = await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.cache_ttl,
                registered_tables.c.role_ttl,
                registered_tables.c.pagination,
                registered_tables.c.change_signal,
            ).where(registered_tables.c.table_name == "orders")
        )
        return res.fetchone()


async def _source_row(db: Database):
    async with db.acquire() as conn:
        res = await conn.execute_core(
            select(sources.c.cache_ttl, sources.c.change_signal).where(sources.c.id == "s")
        )
        return res.fetchone()


def _info(monkeypatch):
    """A caller holding the capabilities the gated cache mutations require."""
    from tests.unit.gate_identity import grant

    return grant(monkeypatch, "source_registration", "table_registration")[0]


def _mutation(db: Database):
    from provisa.api.admin.schema_mutation import Mutation

    return Mutation(), patch(
        "provisa.api.admin.schema_mutation._get_pool", new=AsyncMock(return_value=_Pool(db))
    )


# --- the shared check -----------------------------------------------------------------------


def _t(signal, ttl, **kw) -> TableTtl:
    flags = dict(materialize=False, row_materialize=False, replicate=None, load_protected=None)
    flags.update(kw)
    return TableTtl("public", "orders", signal, ttl, **flags)


def _s(signal, ttl, **kw) -> SourceTtl:
    flags = dict(replicate=None, load_protected=False)
    flags.update(kw)
    return SourceTtl(signal, ttl, **flags)


_LANDING = [
    ({"materialize": True}, {}),
    ({"row_materialize": True}, {}),
    ({"replicate": 0}, {}),
    ({}, {"replicate": 0}),
    ({"load_protected": True}, {}),
    ({}, {"load_protected": True}),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
@pytest.mark.parametrize(("tflag", "sflag"), _LANDING)
async def test_the_check_refuses_a_landed_ttl_table_with_no_cache_ttl(
    tmp_path, signal, tflag, sflag
):
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    async with _db(tmp_path, source_signal="kafka", table_ttl=60) as db:
        async with db.acquire() as conn:
            bad = await landing_ttl_refusal(
                conn, "s", source=_s("kafka", None, **sflag), table=_t(signal, None, **tflag)
            )
            assert bad is not None and bad.success is False
            assert bad.code == "schema.landing_ttl_required"
            assert "orders" in bad.message and "add a cache_ttl" in bad.message
            # inherited from the source
            inherited = await landing_ttl_refusal(
                conn, "s", source=_s(signal, None, **sflag), table=_t(None, None, **tflag)
            )
            assert inherited is not None and inherited.code == "schema.landing_ttl_required"
            # a table or source cache_ttl satisfies it
            for src, tbl in (
                (_s("kafka", None, **sflag), _t(signal, 60, **tflag)),
                (_s(signal, 300, **sflag), _t(None, None, **tflag)),
            ):
                assert await landing_ttl_refusal(conn, "s", source=src, table=tbl) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
async def test_the_check_accepts_a_ttl_table_config_does_not_force_to_land(tmp_path, signal):
    """Nothing in the operator's settings lands it, and the bound engine reads the source in
    place, so it has no replica and needs no landing clock."""
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    async with _db(tmp_path, source_signal="kafka", table_replicate=None) as db:
        async with db.acquire() as conn:
            assert await landing_ttl_refusal(conn, "s", table=_t(signal, None)) is None
            assert (
                await landing_ttl_refusal(conn, "s", source=_s(signal, None), table=_t(None, None))
                is None
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("signal", ["probe", "native", "debezium", "kafka", "signal"])
@pytest.mark.parametrize(("tflag", "sflag"), _LANDING)
async def test_the_no_ttl_freshness_signals_need_no_cache_ttl(tmp_path, signal, tflag, sflag):
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    async with _db(tmp_path, source_signal="kafka") as db:
        async with db.acquire() as conn:
            assert (
                await landing_ttl_refusal(
                    conn, "s", source=_s("kafka", None, **sflag), table=_t(signal, None, **tflag)
                )
                is None
            )
            assert (
                await landing_ttl_refusal(
                    conn, "s", source=_s(signal, None, **sflag), table=_t(None, None, **tflag)
                )
                is None
            )


# --- the admin mutations --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_table_cache_refuses_clearing_the_only_cache_ttl(tmp_path, monkeypatch):
    async with _db(tmp_path, source_signal="ttl", table_ttl=60) as db:
        table_id = (await _table_row(db)).id
        m, p = _mutation(db)
        with p:
            result = await m.update_table_cache(
                _info(monkeypatch), table_id=table_id, cache_ttl=None
            )
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert (await _table_row(db)).cache_ttl == 60  # nothing written


@pytest.mark.asyncio
async def test_update_source_cache_refuses_clearing_a_ttl_the_tables_inherit(tmp_path, monkeypatch):
    async with _db(tmp_path, source_signal="ttl", source_ttl=300) as db:
        m, p = _mutation(db)
        with p:
            result = await m.update_source_cache(
                _info(monkeypatch), source_id="s", cache_enabled=True, cache_ttl=None
            )
            assert result.success is False and result.code == "schema.landing_ttl_required"
            assert (await _source_row(db)).cache_ttl == 300  # nothing written
            ok = await m.update_source_cache(
                _info(monkeypatch), source_id="s", cache_enabled=True, cache_ttl=120
            )
        assert ok.success is True and (await _source_row(db)).cache_ttl == 120


def _source_input(change_signal: str, cache_ttl: int | None) -> SourceInput:
    return SourceInput(
        id="s",
        type="postgresql",
        host="h",
        port=5432,
        database="d",
        username="u",
        password="",
        change_signal=change_signal,
        cache_ttl=cache_ttl,
    )


@pytest.mark.asyncio
async def test_update_source_refuses_a_ttl_signal_its_tables_cannot_clock(tmp_path):
    async with _db(tmp_path, source_signal="kafka") as db:
        m, p = _mutation(db)
        with (
            p,
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
        ):
            result = await m.update_source(
                MagicMock(), _source_input(change_signal="ttl", cache_ttl=None)
            )
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert (await _source_row(db)).change_signal == "kafka"  # nothing written


@pytest.mark.asyncio
async def test_create_source_refuses_a_ttl_signal_its_registered_tables_cannot_clock(tmp_path):
    """A source id re-created over tables still registered under it is judged against them."""
    async with _db(tmp_path, source_signal="kafka") as db:
        async with db.acquire() as conn:
            await conn.execute_core(sources.delete().where(sources.c.id == "s"))
        m, p = _mutation(db)
        with (
            p,
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
            patch(
                "provisa.api.admin.schema_mutation._refuse_over_source_limit",
                new=AsyncMock(return_value=None),
            ),
        ):
            result = await m.create_source(
                MagicMock(), _source_input(change_signal="ttl_probe", cache_ttl=None)
            )
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert await _source_row(db) is None  # nothing written


def _table_input(**kw) -> TableInput:
    base = dict(
        source_id="s",
        domain_id="",
        schema_name="public",
        table_name="orders",
        columns=[ColumnInput(name="id", visible_to=["analyst"])],
    )
    base.update(kw)
    return TableInput(**base)


def _table_patches(db: Database):
    from provisa.core.models import Column

    return (
        patch("provisa.api.admin.schema_mutation._get_pool", new=AsyncMock(return_value=_Pool(db))),
        patch("provisa.api.admin.capabilities.require_capability", return_value=None),
        patch(
            "provisa.api.admin.schema_mutation._build_columns_for_input",
            new=AsyncMock(
                return_value=(
                    [Column(name="id", data_type="integer", visible_to=["analyst"])],
                    None,
                )
            ),
        ),
        patch(
            "provisa.api.admin.schema_mutation._domain_table_conflict",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "provisa.api.admin.schema_mutation._dataset_ownership_conflict",
            new=AsyncMock(return_value=None),
        ),
        patch("provisa.api.admin.schema_mutation._rebuild_schemas", new=AsyncMock()),
        patch("provisa.api.admin.schema_mutation._remove_view_mv"),
    )


@pytest.mark.asyncio
async def test_update_table_refuses_a_ttl_signal_with_no_cache_ttl(tmp_path):
    from contextlib import ExitStack

    from provisa.api.admin.schema_mutation import Mutation

    async with _db(tmp_path, source_signal="kafka") as db:
        with ExitStack() as stack:
            for p in _table_patches(db):
                stack.enter_context(p)
            result = await Mutation().update_table(MagicMock(), _table_input(change_signal="ttl"))
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert (await _table_row(db)).change_signal is None  # nothing written


@pytest.mark.asyncio
async def test_update_table_keeps_the_stored_cache_ttl_role_ttl_and_paging(tmp_path):
    """TableInput carries no cache_ttl / role_ttl / row_materialize / pagination; saving the
    table's other fields must not reset them (they are saved through updateTableCache /
    updateTableRoleTtl / updateTablePaging)."""
    from contextlib import ExitStack

    from provisa.api.admin.schema_mutation import Mutation

    async with _db(tmp_path, source_signal="ttl", table_ttl=60) as db:
        with ExitStack() as stack:
            for p in _table_patches(db):
                stack.enter_context(p)
            result = await Mutation().update_table(MagicMock(), _table_input(description="edited"))
        assert result.success is True, result.message
        row = await _table_row(db)
        assert row.cache_ttl == 60
        assert row.role_ttl == {"analyst": 360}
        assert row.pagination == {"max_rows": 50}


@pytest.mark.asyncio
async def test_register_table_refuses_inheriting_a_ttl_signal_with_no_cache_ttl(tmp_path):
    from provisa.api.admin import schema_mutation_ops as ops
    from provisa.core.models import Column

    async with _db(tmp_path, source_signal="ttl", table_ttl=60, source_replicate=0) as db:
        with (
            patch.object(ops, "_get_pool", new=AsyncMock(return_value=_Pool(db))),
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
            patch.object(
                ops,
                "_build_columns_for_input",
                new=AsyncMock(
                    return_value=(
                        [Column(name="id", data_type="integer", visible_to=["analyst"])],
                        None,
                    )
                ),
            ),
        ):
            result = await ops.register_table(MagicMock(), _table_input(table_name="payments"))
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert "payments" in result.message


@pytest.mark.asyncio
async def test_update_table_cache_accepts_clearing_it_on_a_table_config_does_not_land(
    tmp_path, monkeypatch
):
    async with _db(tmp_path, source_signal="ttl", table_ttl=60, table_replicate=None) as db:
        table_id = (await _table_row(db)).id
        m, p = _mutation(db)
        with p:
            result = await m.update_table_cache(
                _info(monkeypatch), table_id=table_id, cache_ttl=None
            )
        assert result.success is True
        assert (await _table_row(db)).cache_ttl is None


# --- the replicate setters (REQ-826) --------------------------------------------------------


async def _replicate_of(db: Database):
    async with db.acquire() as conn:
        table = await conn.execute_core(
            select(registered_tables.c.replicate).where(registered_tables.c.table_name == "orders")
        )
        source = await conn.execute_core(select(sources.c.replicate).where(sources.c.id == "s"))
        return table.fetchone().replicate, source.fetchone().replicate


def _replicate_mutation(db: Database):
    """The mutation with the control plane pointed at ``db``, a state that declares no source in
    config, and the schema rebuild recorded."""
    from provisa.api.admin import schema_mutation

    from provisa.federation.engine import build_engine

    rebuild = AsyncMock()
    # A bound engine that reads PostgreSQL in place: only the operator's settings land a table.
    state = SimpleNamespace(config=None, tables=[], federation_engine=build_engine("trino"))
    return (
        schema_mutation.Mutation(),
        rebuild,
        (
            patch.object(schema_mutation, "_get_pool", new=AsyncMock(return_value=_Pool(db))),
            patch.object(schema_mutation, "_rebuild_schemas", new=rebuild),
            patch("provisa.api.app.state", state),
        ),
        state,
    )


def _granted(monkeypatch, state):
    from tests.unit.gate_identity import grant

    return grant(monkeypatch, "source_registration", "table_registration", state=state)[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [0, 500])
async def test_update_table_replicate_refuses_a_ttl_table_with_no_cache_ttl(
    tmp_path, monkeypatch, value
):
    """Saving Always or a Hot threshold on a ttl table that has no refresh clock used to succeed
    and then fail every read of the table; it is refused at save."""
    async with _db(tmp_path, source_signal="ttl", table_replicate=None) as db:
        table_id = (await _table_row(db)).id
        m, rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            result = await m.update_table_replicate(
                _granted(monkeypatch, state), table_id=table_id, replicate=value
            )
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert "orders" in result.message
        assert await _replicate_of(db) == (None, None)
        rebuild.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_table_replicate_saves_and_rebuilds_so_the_value_routes(tmp_path, monkeypatch):
    async with _db(tmp_path, source_signal="ttl", table_ttl=60, table_replicate=None) as db:
        table_id = (await _table_row(db)).id
        m, rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            result = await m.update_table_replicate(
                _granted(monkeypatch, state), table_id=table_id, replicate=0
            )
        assert result.success is True and result.code == "schema.table_replicate_set"
        assert await _replicate_of(db) == (0, None)
        rebuild.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_source_replicate_refuses_a_ttl_table_it_would_replicate_with_no_cache_ttl(
    tmp_path, monkeypatch
):
    async with _db(tmp_path, source_signal="ttl", table_replicate=None) as db:
        m, rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            result = await m.update_source_replicate(
                _granted(monkeypatch, state), source_id="s", replicate=0
            )
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert await _replicate_of(db) == (None, None)
        rebuild.assert_not_awaited()


@pytest.mark.asyncio
async def test_never_is_refused_on_a_load_protected_source(tmp_path, monkeypatch):
    """load_protected with replicate -1 is contradictory (REQ-826): refused at save, naming it."""
    async with _db(tmp_path, source_signal="kafka", table_replicate=None) as db:
        async with db.acquire() as conn:
            await conn.execute_core(
                sources.update().where(sources.c.id == "s").values(load_protected=True)
            )
        table_id = (await _table_row(db)).id
        m, rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            info = _granted(monkeypatch, state)
            on_source = await m.update_source_replicate(info, source_id="s", replicate=-1)
            on_table = await m.update_table_replicate(info, table_id=table_id, replicate=-1)
        for result in (on_source, on_table):
            assert result.success is False
            assert result.code == "schema.replicate_contradicts_load_protected"
            assert "contradictory" in result.message
        assert await _replicate_of(db) == (None, None)
        rebuild.assert_not_awaited()


@pytest.mark.asyncio
async def test_load_protection_is_refused_on_a_source_set_to_never(tmp_path, monkeypatch):
    async with _db(
        tmp_path, source_signal="kafka", source_ttl=60, table_replicate=None, source_replicate=-1
    ) as db:
        m, rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            result = await m.update_source_load_protection(
                _granted(monkeypatch, state), source_id="s", load_protected=True
            )
        assert result.success is False
        assert result.code == "schema.replicate_contradicts_load_protected"
        rebuild.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [-2, -50])
async def test_a_value_the_setting_does_not_have_is_refused(tmp_path, monkeypatch, value):
    async with _db(tmp_path, source_signal="kafka", table_replicate=None) as db:
        m, _rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            result = await m.update_source_replicate(
                _granted(monkeypatch, state), source_id="s", replicate=value
            )
        assert result.success is False and result.code == "schema.replicate_invalid"
        assert await _replicate_of(db) == (None, None)


@pytest.mark.asyncio
async def test_a_non_standard_threshold_is_kept_as_saved(tmp_path, monkeypatch):
    """Any threshold above 0 is a legal value, not only the drop-down's standard ones."""
    async with _db(tmp_path, source_signal="kafka", table_replicate=None) as db:
        m, _rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            result = await m.update_source_replicate(
                _granted(monkeypatch, state), source_id="s", replicate=750
            )
        assert result.success is True
        assert await _replicate_of(db) == (None, 750)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["elasticsearch", "redis", "cassandra", "prometheus"])
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
async def test_a_table_the_engine_lands_needs_a_cache_ttl_at_save(
    tmp_path, monkeypatch, source_type, signal
):
    """A source the bound engine cannot read in place is served from a replica, whatever the
    operator's settings say, so its ttl / ttl_probe tables need a landing clock when they are
    saved — not first at the read (REQ-1907)."""
    import provisa.api.app as appmod
    from provisa.api.admin._landing_ttl import landing_ttl_refusal
    from provisa.federation.engine import build_engine

    monkeypatch.setattr(appmod.state, "federation_engine", build_engine("duckdb"), raising=False)
    async with _db(tmp_path, source_type=source_type, table_replicate=None) as db:
        async with db.acquire() as conn:
            bad = await landing_ttl_refusal(conn, "s", table=_t(signal, None))
            assert bad is not None and bad.code == "schema.landing_ttl_required"
            assert "orders" in bad.message and "add a cache_ttl" in bad.message
            # A table or source cache_ttl gives it the clock.
            assert await landing_ttl_refusal(conn, "s", table=_t(signal, 60)) is None
            assert (
                await landing_ttl_refusal(conn, "s", source=_s(signal, 300), table=_t(None, None))
                is None
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["ingest", "govdata", "data_profiler"])
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
async def test_a_type_that_owns_no_replica_needs_no_cache_ttl_at_save(
    tmp_path, monkeypatch, source_type, signal
):
    """A type that owns no replica (ingest REQ-1771, govdata REQ-1730, a profiler's results
    REQ-1934) is read where it is written and never landed, so its ttl tables are saved without a
    landing clock, whatever the bound engine reads in place."""
    import provisa.api.app as appmod
    from provisa.api.admin._landing_ttl import landing_ttl_refusal
    from provisa.federation.engine import build_engine

    monkeypatch.setattr(appmod.state, "federation_engine", build_engine("duckdb"), raising=False)
    async with _db(tmp_path, source_type=source_type, table_replicate=None) as db:
        async with db.acquire() as conn:
            assert await landing_ttl_refusal(conn, "s", table=_t(signal, None)) is None
