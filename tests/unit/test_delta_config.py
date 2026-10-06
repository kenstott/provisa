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


def _config(table):
    source = Source(id="pg", type=SourceType("postgresql"), host="h")
    return type("C", (), {"sources": [source], "tables": [table]})()


def test_the_load_refuses_any_delta_until_the_apply_path_lands():
    # REQ-874 guard: the apply path is not wired yet, so a declared delta is refused by name at
    # config load (this test is updated when the guard is reverted).
    from provisa.core.config_loader import _validate_delta

    t = _table(watermark_column="u", delta=DeltaConfig(apply="append"))
    with pytest.raises(ValueError, match="delta replication is not available yet"):
        _validate_delta(_config(t))


def test_the_save_refuses_a_delta_table_by_name():
    from types import SimpleNamespace

    from provisa.api.admin._delta_guard import table_delta_refusal

    assert table_delta_refusal(SimpleNamespace(table_name="t", delta=None)) is None
    r = table_delta_refusal(SimpleNamespace(table_name="orders", delta=DeltaConfig(apply="append")))
    assert r is not None and r.success is False
    assert r.code == "schema.delta_not_available"
    assert "not available yet" in r.message


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
