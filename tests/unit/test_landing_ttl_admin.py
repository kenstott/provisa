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

from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from provisa.api.admin._landing_ttl import SourceTtl, TableTtl
from provisa.api.admin.types import ColumnInput, MutationResult, SourceInput, TableInput
from provisa.core.database import Database
from provisa.core.schema_org import registered_tables, sources
from tests.unit.gate_identity import serve_every_column

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which ones a caller may read is REQ-2152's own tests."""
    serve_every_column(monkeypatch)


class _Pool:
    def __init__(self, db: Database) -> None:
        self._db = db

    def acquire(self):
        return self._db.acquire()


@asynccontextmanager
async def _db(
    control_plane,
    *,
    source_signal="ttl",
    source_ttl=None,
    table_signal=None,
    table_ttl=None,
    table_replicate=0,
    source_replicate=None,
    source_type="postgresql",
    federation_hints=None,
):
    """One source ``s`` (of ``source_type``) with one table ``orders``. ``table_replicate`` 0 (always) says the table
    is replicated (REQ-1907 option B); None = it inherits its source's value, which by default
    is not set either."""
    db = control_plane.org()
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
                federation_hints=federation_hints or {},
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
    yield db


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
    flags = dict(replicate=None, load_protected=False, freshness_gate=False)
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
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe", "probe"])
@pytest.mark.parametrize(("tflag", "sflag"), _LANDING)
async def test_the_check_refuses_a_landed_ttl_table_with_no_cache_ttl(
    control_plane, signal, tflag, sflag
):
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    async with _db(control_plane, source_signal="kafka", table_ttl=60) as db:
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
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe", "probe"])
async def test_the_check_accepts_a_ttl_table_config_does_not_force_to_land(control_plane, signal):
    """Nothing in the operator's settings lands it, and the bound engine reads the source in
    place, so it has no replica and needs no landing clock."""
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    async with _db(control_plane, source_signal="kafka", table_replicate=None) as db:
        async with db.acquire() as conn:
            assert await landing_ttl_refusal(conn, "s", table=_t(signal, None)) is None
            assert (
                await landing_ttl_refusal(conn, "s", source=_s(signal, None), table=_t(None, None))
                is None
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(("tflag", "sflag"), _LANDING)
async def test_a_probe_on_a_freshness_gated_source_needs_no_cache_ttl(control_plane, tflag, sflag):
    """REQ-860: a gated source's probe is judged at each read; without the gate a probe has no
    cadence but the cache_ttl, and is refused without one."""
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    async with _db(control_plane, source_signal="kafka") as db:
        async with db.acquire() as conn:
            gated = _s("kafka", None, freshness_gate=True, **sflag)
            table = _t("probe", None, **tflag)
            assert await landing_ttl_refusal(conn, "s", source=gated, table=table) is None
            ungated = _s("kafka", None, **sflag)
            refused = await landing_ttl_refusal(conn, "s", source=ungated, table=table)
            assert refused is not None and refused.code == "schema.landing_ttl_required"
            assert "change_signal 'probe'" in refused.message


@pytest.mark.asyncio
@pytest.mark.parametrize("signal", ["native", "debezium", "kafka", "signal"])
@pytest.mark.parametrize(("tflag", "sflag"), _LANDING)
async def test_the_no_ttl_freshness_signals_need_no_cache_ttl(control_plane, signal, tflag, sflag):
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    async with _db(control_plane, source_signal="kafka") as db:
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
async def test_update_table_cache_refuses_clearing_the_only_cache_ttl(control_plane, monkeypatch):
    async with _db(control_plane, source_signal="ttl", table_ttl=60) as db:
        table_id = (await _table_row(db)).id
        m, p = _mutation(db)
        with p:
            result = await m.update_table_cache(
                _info(monkeypatch), table_id=table_id, cache_ttl=None
            )
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert (await _table_row(db)).cache_ttl == 60  # nothing written


@pytest.mark.asyncio
async def test_update_source_cache_refuses_clearing_a_ttl_the_tables_inherit(
    control_plane, monkeypatch
):
    async with _db(control_plane, source_signal="ttl", source_ttl=300) as db:
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
async def test_update_source_refuses_a_ttl_signal_its_tables_cannot_clock(control_plane):
    async with _db(control_plane, source_signal="kafka") as db:
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
async def test_create_source_refuses_a_ttl_signal_its_registered_tables_cannot_clock(control_plane):
    """A source created under an id that has tables registered is judged against them.

    (On SQLite this deleted the source first and created it again over its orphaned tables;
    PostgreSQL cascades a source's tables with it, so that state cannot exist, and the create is
    made while the id still holds them.)"""
    async with _db(control_plane, source_signal="kafka") as db:
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
        assert (await _source_row(db)).change_signal == "kafka"  # nothing written


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


@contextmanager
def _model_store(db: Database):
    from provisa.api.app import state

    held = state.model_db
    state.model_db = db
    try:
        yield
    finally:
        state.model_db = held


def _table_patches(db: Database):
    from provisa.core.models import Column

    return (
        patch("provisa.api.admin.schema_mutation._get_pool", new=AsyncMock(return_value=_Pool(db))),
        patch("provisa.api.admin.capabilities.require_capability", return_value=None),
        # The caller holds every right: the paired right-in-domain check (REQ-1944) too.
        patch("provisa.api.admin.capabilities.right_domain_refusal", return_value=None),
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
        # The org's model store, as the mutation's other readers of it (the checker reschedule)
        # find it: the same store _get_pool hands out.
        _model_store(db),
    )


@pytest.mark.asyncio
async def test_update_table_refuses_a_ttl_signal_with_no_cache_ttl(control_plane):
    from contextlib import ExitStack

    from provisa.api.admin.schema_mutation import Mutation

    async with _db(control_plane, source_signal="kafka") as db:
        with ExitStack() as stack:
            for p in _table_patches(db):
                stack.enter_context(p)
            result = await Mutation().update_table(MagicMock(), _table_input(change_signal="ttl"))
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert (await _table_row(db)).change_signal is None  # nothing written


@pytest.mark.asyncio
async def test_update_table_keeps_the_stored_cache_ttl_role_ttl_and_paging(control_plane):
    """TableInput carries no cache_ttl / role_ttl / row_materialize / pagination; saving the
    table's other fields must not reset them (they are saved through updateTableCache /
    updateTableRoleTtl / updateTablePaging)."""
    from contextlib import ExitStack

    from provisa.api.admin.schema_mutation import Mutation

    async with _db(control_plane, source_signal="ttl", table_ttl=60) as db:
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
async def test_register_table_refuses_inheriting_a_ttl_signal_with_no_cache_ttl(control_plane):
    from provisa.api.admin import schema_mutation_ops as ops
    from provisa.core.models import Column

    async with _db(control_plane, source_signal="ttl", table_ttl=60, source_replicate=0) as db:
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
async def test_register_table_takes_the_cache_ttl_the_registration_states(control_plane):
    """The same table that is refused for want of a clock passes the landing check when the
    registration states its own (#208); a negative one is refused by name."""
    from provisa.api.admin import schema_mutation_ops as ops
    from provisa.core.models import Column

    async with _db(control_plane, source_signal="ttl", table_ttl=60, source_replicate=0) as db:
        judged: list[TableTtl] = []

        async def judge(conn, source_id, *, table=None, **kw):
            judged.append(table)
            return MutationResult(success=False, message="stop here", code="test.stop")

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
            negative = await ops.register_table(
                MagicMock(), _table_input(table_name="payments", cache_ttl=-2)
            )
            with patch("provisa.api.admin._landing_ttl.landing_ttl_refusal", new=judge):
                await ops.register_table(
                    MagicMock(), _table_input(table_name="payments", cache_ttl=120)
                )
        assert negative.success is False and negative.code == "schema.invalid_cache_ttl"
        # The landing check judged the table with the clock the registration stated ...
        assert [t.cache_ttl for t in judged] == [120]
        async with db.acquire() as conn:
            # ... and that clock satisfies it.
            assert await landing_ttl_refusal_of(conn, judged[0]) is None


async def landing_ttl_refusal_of(conn, table):
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    return await landing_ttl_refusal(conn, "s", table=table)


def test_the_registration_input_carries_a_cache_ttl():
    import strawberry

    # REQ-2109: left out, the input carries none (a registration has none; an update keeps its own).
    assert _table_input().cache_ttl is strawberry.UNSET
    assert _table_input(cache_ttl=None).cache_ttl is None
    assert _table_input(cache_ttl=120).cache_ttl == 120


@pytest.mark.asyncio
async def test_the_form_is_told_whether_a_kinds_tables_land(monkeypatch):
    import provisa.api.app as appmod
    from provisa.api.admin import source_kinds_router as kinds
    from provisa.federation.engine import build_engine

    monkeypatch.setattr(kinds, "require_capability_request", lambda request, capability: None)
    monkeypatch.setattr(appmod.state, "federation_engine", build_engine("duckdb"), raising=False)
    # DuckDB attaches postgresql and reads it in place; it has no connector for these.
    assert (await kinds.source_kind(None, "postgresql"))["tables_land"] is False
    for kind in ("elasticsearch", "redis", "microsoft_365"):
        assert (await kinds.source_kind(None, kind))["tables_land"] is True, kind
    # A kind that owns no replica is read where it is written.
    assert (await kinds.source_kind(None, "ingest"))["tables_land"] is False


@pytest.mark.asyncio
async def test_update_table_cache_accepts_clearing_it_on_a_table_config_does_not_land(
    control_plane, monkeypatch
):
    async with _db(control_plane, source_signal="ttl", table_ttl=60, table_replicate=None) as db:
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
    control_plane, monkeypatch, value
):
    """Saving Always or a Hot threshold on a ttl table that has no refresh clock used to succeed
    and then fail every read of the table; it is refused at save."""
    async with _db(control_plane, source_signal="ttl", table_replicate=None) as db:
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
async def test_update_table_replicate_saves_and_rebuilds_so_the_value_routes(
    control_plane, monkeypatch
):
    async with _db(control_plane, source_signal="ttl", table_ttl=60, table_replicate=None) as db:
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
    control_plane, monkeypatch
):
    async with _db(control_plane, source_signal="ttl", table_replicate=None) as db:
        m, rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            result = await m.update_source_replicate(
                _granted(monkeypatch, state), source_id="s", replicate=0
            )
        assert result.success is False and result.code == "schema.landing_ttl_required"
        assert await _replicate_of(db) == (None, None)
        rebuild.assert_not_awaited()


@pytest.mark.asyncio
async def test_never_is_refused_on_a_load_protected_source(control_plane, monkeypatch):
    """load_protected with replicate -1 is contradictory (REQ-826): refused at save, naming it."""
    async with _db(control_plane, source_signal="kafka", table_replicate=None) as db:
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
async def test_load_protection_is_refused_on_a_source_set_to_never(control_plane, monkeypatch):
    async with _db(
        control_plane,
        source_signal="kafka",
        source_ttl=60,
        table_replicate=None,
        source_replicate=-1,
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
async def test_a_value_the_setting_does_not_have_is_refused(control_plane, monkeypatch, value):
    async with _db(control_plane, source_signal="kafka", table_replicate=None) as db:
        m, _rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            result = await m.update_source_replicate(
                _granted(monkeypatch, state), source_id="s", replicate=value
            )
        assert result.success is False and result.code == "schema.replicate_invalid"
        assert await _replicate_of(db) == (None, None)


@pytest.mark.asyncio
async def test_a_non_standard_threshold_is_kept_as_saved(control_plane, monkeypatch):
    """Any threshold above 0 is a legal value, not only the drop-down's standard ones."""
    async with _db(control_plane, source_signal="kafka", table_replicate=None) as db:
        m, _rebuild, patches, state = _replicate_mutation(db)
        with patches[0], patches[1], patches[2]:
            result = await m.update_source_replicate(
                _granted(monkeypatch, state), source_id="s", replicate=750
            )
        assert result.success is True
        assert await _replicate_of(db) == (None, 750)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["elasticsearch", "redis", "cassandra", "prometheus"])
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe", "probe"])
async def test_a_table_the_engine_lands_needs_a_cache_ttl_at_save(
    control_plane, monkeypatch, source_type, signal
):
    """A source the bound engine cannot read in place is served from a replica, whatever the
    operator's settings say, so its ttl / ttl_probe tables need a landing clock when they are
    saved — not first at the read (REQ-1907)."""
    import provisa.api.app as appmod
    from provisa.api.admin._landing_ttl import landing_ttl_refusal
    from provisa.federation.engine import build_engine

    monkeypatch.setattr(appmod.state, "federation_engine", build_engine("duckdb"), raising=False)
    async with _db(control_plane, source_type=source_type, table_replicate=None) as db:
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
@pytest.mark.parametrize("source_type", ["ingest", "data_profiler", "soda", "great_expectations"])
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe", "probe"])
async def test_a_type_that_owns_no_replica_needs_no_cache_ttl_at_save(
    control_plane, monkeypatch, source_type, signal
):
    """A type that owns no replica (ingest REQ-1771, a profiler's results
    REQ-1934, a checker's scans REQ-1443) is read where it is written and never landed, so its ttl tables are saved without a
    landing clock, whatever the bound engine reads in place."""
    import provisa.api.app as appmod
    from provisa.api.admin._landing_ttl import landing_ttl_refusal
    from provisa.federation.engine import build_engine

    monkeypatch.setattr(appmod.state, "federation_engine", build_engine("duckdb"), raising=False)
    async with _db(control_plane, source_type=source_type, table_replicate=None) as db:
        async with db.acquire() as conn:
            assert await landing_ttl_refusal(conn, "s", table=_t(signal, None)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
async def test_a_databricks_source_read_in_place_needs_no_landing_clock(
    control_plane, monkeypatch, signal
):
    """REQ-1867: on the DuckDB engine a Databricks source lands unless the steward asks to read it
    in place (federation hint reach: scan); only the landed one needs a landing clock. The hint is
    read from the stored source, or from the hints being saved."""
    import provisa.api.app as appmod
    from provisa.api.admin._landing_ttl import landing_ttl_refusal
    from provisa.federation.engine import build_engine

    monkeypatch.setattr(appmod.state, "federation_engine", build_engine("duckdb"), raising=False)
    async with _db(control_plane, source_type="databricks", table_replicate=None) as db:
        async with db.acquire() as conn:
            bad = await landing_ttl_refusal(conn, "s", table=_t(signal, None))
            assert bad is not None and bad.code == "schema.landing_ttl_required"
            # the same source, saved with the hint
            assert (
                await landing_ttl_refusal(
                    conn, "s", table=_t(signal, None), federation_hints={"reach": "scan"}
                )
                is None
            )
    async with control_plane.org().acquire() as conn:
        await conn.execute_core(
            registered_tables.delete().where(registered_tables.c.source_id == "s")
        )
        await conn.execute_core(sources.delete().where(sources.c.id == "s"))
    async with _db(
        control_plane,
        source_type="databricks",
        table_replicate=None,
        federation_hints={"reach": "scan"},
    ) as db:
        async with db.acquire() as conn:
            assert await landing_ttl_refusal(conn, "s", table=_t(signal, None)) is None


# --- a kind whose every table lands states the clock on the source (#208) ---

_MAIL_KINDS = ["microsoft_365", "google_workspace"]


def test_only_the_mail_kinds_must_state_the_clock_on_the_source():
    from provisa.api.admin._landing_ttl import source_states_the_clock

    assert [source_states_the_clock(kind) for kind in _MAIL_KINDS] == [True, True]
    for kind in ("postgresql", "openapi", "graphql_remote", "ingest", None):
        assert source_states_the_clock(kind) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _MAIL_KINDS)
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
async def test_a_new_mail_source_is_refused_without_a_cache_ttl(control_plane, kind, signal):
    """Nothing is registered under it yet: the source is still asked for the clock, since every
    table it will have lands and refreshes by it."""
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    async with _db(control_plane) as db:
        async with db.acquire() as conn:
            bad = await landing_ttl_refusal(conn, "mail", source=_s(signal, None), source_type=kind)
            assert bad is not None and bad.success is False
            assert bad.code == "schema.source_cache_ttl_required"
            assert bad.params == {"source": "mail", "signal": signal}
            # The operator's own value satisfies it; a signal with its own clock needs none.
            for said in (_s(signal, 300), _s("kafka", None)):
                assert (
                    await landing_ttl_refusal(conn, "mail", source=said, source_type=kind) is None
                )
            # Another kind is not asked at create: its tables may state their own.
            assert (
                await landing_ttl_refusal(
                    conn, "pg", source=_s(signal, None), source_type="postgresql"
                )
                is None
            )


@pytest.mark.asyncio
async def test_create_source_refuses_a_mail_source_with_no_cache_ttl(control_plane):
    async with _db(control_plane) as db:
        m, p = _mutation(db)
        asked = _source_input(change_signal="ttl", cache_ttl=None)
        asked.id, asked.type = "mail", "microsoft_365"
        with (
            p,
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
            patch(
                "provisa.api.admin.schema_mutation._refuse_over_source_limit",
                new=AsyncMock(return_value=None),
            ),
        ):
            result = await m.create_source(MagicMock(), asked)
        assert result.success is False and result.code == "schema.source_cache_ttl_required"
        async with db.acquire() as conn:
            stored = await conn.execute_core(select(sources.c.id).where(sources.c.id == "mail"))
            assert stored.fetchone() is None  # nothing written


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _MAIL_KINDS)
async def test_a_table_of_a_stored_mail_source_with_no_clock_is_refused_naming_the_source(
    control_plane, kind
):
    """A source saved before the rule has no cache_ttl: registering a table of it is refused by
    the source's own refusal, which says where the value is set."""
    from provisa.api.admin._landing_ttl import landing_ttl_refusal

    async with _db(control_plane, source_type=kind, table_replicate=None) as db:
        async with db.acquire() as conn:
            bad = await landing_ttl_refusal(conn, "s", table=_t(None, None))
            assert bad is not None and bad.code == "schema.source_cache_ttl_required"
            assert bad.params == {"source": "s", "signal": "ttl"}
            assert "Load Management and Timeliness" in bad.message


@pytest.mark.asyncio
async def test_the_form_is_told_which_kinds_state_the_clock(monkeypatch):
    from provisa.api.admin import source_kinds_router as kinds

    asked: list[str] = []
    monkeypatch.setattr(
        kinds, "require_capability_request", lambda request, capability: asked.append(capability)
    )
    assert (await kinds.source_kind(None, "microsoft_365"))["cache_ttl_required"] is True
    assert (await kinds.source_kind(None, "postgresql"))["cache_ttl_required"] is False
    assert asked == ["source_registration"] * 2


def test_every_locale_words_both_landing_clock_refusals():
    import json
    import re
    from pathlib import Path

    locales = Path(__file__).parents[2] / "provisa-ui/src/i18n/locales"
    wanted = {
        "source_cache_ttl_required": {"source", "signal"},
        "landing_ttl_required": {"table", "source"},
        "sentinel_path_not_applicable": {"source", "type"},
    }
    catalogs = sorted(locales.glob("*/serverErrors.json"))
    assert len(catalogs) == 13
    for catalog in catalogs:
        said = json.loads(catalog.read_text())["serverErrors"]["schema"]
        for key, particulars in wanted.items():
            assert set(re.findall(r"{{(\w+)}}", said[key])) == particulars, (catalog, key)


@pytest.mark.parametrize(
    ("source_type", "columns", "named", "watermark"),
    [
        ("microsoft_365", ["id", "landed_at"], None, "landed_at"),
        ("google_workspace", ["id", "landed_at"], None, "landed_at"),
        ("microsoft_365", ["id", "landed_at"], "received_at", "received_at"),  # the operator's
        ("microsoft_365", ["id"], None, None),  # registered without the stamp
        ("postgresql", ["id", "landed_at"], None, None),  # another kind's column of that name
    ],
)
def test_a_mail_table_registered_with_the_landing_stamp_takes_it_as_its_watermark(
    source_type, columns, named, watermark
):
    """Rows land mailbox by mailbox, in no order of the provider's own times, so live delivery
    goes by when a row landed."""
    from types import SimpleNamespace

    from provisa.api.admin.schema_mutation_ops import default_mail_watermark

    model = SimpleNamespace(
        watermark_column=named, columns=[SimpleNamespace(name=name) for name in columns]
    )
    default_mail_watermark(model, source_type)
    assert model.watermark_column == watermark
