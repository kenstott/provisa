# Copyright (c) 2026 Kenneth Stott
# Canary: 14754c06-58fe-40a1-a095-d6e8eecb4bad

"""A table's delta (incremental reload) declaration is validated at the model and the load (REQ-874)."""

from __future__ import annotations

import pytest

from provisa.core.models import Column, DeltaConfig, Source, SourceType, Table

pytestmark = pytest.mark.unit


def _table(**kw):
    cols = kw.pop(
        "columns", [Column(name="id", data_type="integer", visible_to=["*"], is_primary_key=True)]
    )
    return Table(source_id="pg", domain_id="d", schema="public", table="orders", columns=cols, **kw)


def test_a_valid_upsert_delta_is_accepted():
    t = _table(watermark_column="updated_at", delta=DeltaConfig(apply="upsert"))
    assert t.delta is not None and t.delta.apply == "upsert"


def test_upsert_needs_a_primary_key():
    with pytest.raises(ValueError, match="is_primary_key"):
        _table(
            watermark_column="updated_at",
            columns=[Column(name="id", data_type="integer", visible_to=["*"])],
            delta=DeltaConfig(apply="upsert"),
        )


def test_delta_needs_a_watermark_column():
    with pytest.raises(ValueError, match="watermark_column"):
        _table(delta=DeltaConfig(apply="append"))


def test_tombstone_needs_a_column():
    with pytest.raises(ValueError, match="tombstone_column"):
        _table(watermark_column="u", delta=DeltaConfig(deletes="tombstone"))


def test_an_authored_query_must_carry_both_placeholders():
    with pytest.raises(ValueError, match="placeholders"):
        _table(watermark_column="u", delta=DeltaConfig(query="SELECT * WHERE u > $wm"))
    ok = _table(watermark_column="u", delta=DeltaConfig(query="SELECT {{fields}} WHERE u > $wm"))
    assert ok.delta is not None


def test_rebuild_every_must_be_positive():
    with pytest.raises(ValueError, match="rebuild_every"):
        _table(watermark_column="u", delta=DeltaConfig(apply="append", rebuild_every=0))


def _config(table, source_type="postgresql"):
    kw = {"path": "/tmp/x"} if source_type == "files" else {"host": "h"}
    source = Source(id="pg", type=SourceType(source_type), **kw)
    return type("C", (), {"sources": [source], "tables": [table]})()


def test_the_load_refuses_a_delta_on_a_non_sql_source():
    # REQ-874: a generated SQL delta is defined only for SQL sources; a delta on any other source
    # type is refused by name at config load. A SQL source, replicated, is accepted.
    from provisa.core.config_loader import _validate_delta

    t = _table(watermark_column="u", replicate=0, delta=DeltaConfig(apply="append"))
    with pytest.raises(ValueError, match="only available for SQL sources"):
        _validate_delta(_config(t, source_type="files"))
    _validate_delta(_config(t))  # postgresql source, replicated: no raise


def test_the_save_refuses_a_delta_on_a_non_sql_source():
    import asyncio
    from types import SimpleNamespace

    from provisa.api.admin._delta_guard import table_delta_refusal

    async def _refuse(source_type):
        import provisa.core.repositories.source as source_repo

        orig = source_repo.get

        async def _fake_get(_conn, _id):
            return {"id": "pg", "type": source_type}

        source_repo.get = _fake_get
        try:
            model = SimpleNamespace(
                table_name="orders", source_id="pg", delta=DeltaConfig(apply="append")
            )
            return await table_delta_refusal(object(), model)
        finally:
            source_repo.get = orig

    # No delta → no refusal (no source lookup needed).
    assert asyncio.run(table_delta_refusal(object(), SimpleNamespace(delta=None))) is None
    # Non-SQL source → refused by name.
    r = asyncio.run(_refuse("files"))
    assert r is not None and r.success is False
    assert r.code == "schema.delta_not_sql_source"
    assert "only available for SQL sources" in r.message
    # SQL source → allowed.
    assert asyncio.run(_refuse("postgresql")) is None


def test_delta_round_trips_through_the_control_plane(tmp_path):
    import asyncio
    from types import SimpleNamespace

    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.core.repositories import table as table_repo
    from provisa.federation.registry_view import registered_tables

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 't.db'}")
    db = Database(engine, name="org")
    asyncio.run(init_schema(db, "", org_id="default"))

    async def _go():
        async with db.acquire() as conn:
            await table_repo.upsert(
                conn,
                _table(watermark_column="u", delta=DeltaConfig(apply="upsert", rebuild_every=3600)),
            )
        state = SimpleNamespace(model_db=db, tenant_db=db, config=SimpleNamespace(tables=[]))
        async with db.acquire() as conn:
            return await registered_tables(state, conn)

    rows = asyncio.run(_go())
    (t,) = [r for r in rows if r.table_name == "orders"]
    assert t.delta is not None
    assert t.delta.apply == "upsert" and t.delta.rebuild_every == 3600
    engine.dispose()


def test_delta_of_treats_json_null_and_non_dict_as_no_delta():
    # A JSON column stores Python None as the string "null"; the raw-SQL fetch returns it as text.
    from provisa.federation.registry_view import _delta_of

    assert _delta_of(None) is None
    assert _delta_of("null") is None
    assert _delta_of("") is None
    assert _delta_of('{"apply": "append"}').apply == "append"
    assert _delta_of({"apply": "upsert"}).apply == "upsert"


def test_delta_maps_from_admin_input_and_back_to_the_view():
    # REQ-874: the admin surface round-trips -- a DeltaConfigInput becomes a DeltaConfig model, and
    # the persisted dict becomes the DeltaConfigType the table form reads back.
    from types import SimpleNamespace

    from provisa.api.admin._live_mappers import delta_model_from_input
    from provisa.api.admin._row_mappers import _delta_type_from_row

    assert delta_model_from_input(None) is None
    inp = SimpleNamespace(
        query=None, apply="upsert", deletes="tombstone", tombstone_column="_d", rebuild_every=3600
    )
    model = delta_model_from_input(inp)
    assert (model.apply, model.deletes, model.tombstone_column, model.rebuild_every) == (
        "upsert",
        "tombstone",
        "_d",
        3600,
    )
    view = _delta_type_from_row(model.model_dump())
    assert (view.apply, view.deletes, view.tombstone_column, view.rebuild_every) == (
        "upsert",
        "tombstone",
        "_d",
        3600,
    )
    assert _delta_type_from_row(None) is None
    assert (
        _delta_type_from_row("null") is None
    )  # JSONB null stored as text -> no delta, not a crash
