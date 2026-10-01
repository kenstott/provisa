# Copyright (c) 2026 Kenneth Stott
# Canary: a4d17c59-0e63-4b2f-9a81-c5e7f2b3d640
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The config stamp and the reload it drives, on a SQLite control plane (REQ-1914).

A change to the model or its governance must reach every worker of every instance. The control
plane holds one stamp per kind of configuration and advances it, by trigger, in the transaction
of any write to a table of that kind; every process reloads when the stored stamp differs from
the one it loaded. These tests hold the stamp's half of that to its rules:

* any insert, update or delete on a config table advances the stamp, in the SAME transaction;
* runtime bookkeeping on the same plane does not;
* a change of one kind does not move another kind's stamp;
* a process reloads exactly when the stored stamp differs from the one it loaded, and a failed
  reload is tried again.

The same rules on PostgreSQL: ``tests/integration/test_config_stamp_pg.py``."""

# Requirements: REQ-1914

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import sqlalchemy as sa

from provisa.core import config_stamp, config_watch, schema_org
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo


@pytest.fixture
def tenant_db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    db = Database(engine, name="org")
    asyncio.run(init_schema(db, "", org_id="default"))
    yield db
    engine.dispose()


def _stamps(db: Database) -> dict[str, int]:
    return asyncio.run(config_stamp.read(db))


def _model(db: Database) -> int:
    return _stamps(db)[config_stamp.MODEL]


def test_every_plane_table_named_for_a_stamp_exists():
    """A table named here that the schema does not define would get no trigger."""
    from provisa.core import schema_admin

    assert set(config_stamp.TENANT_TABLES) <= set(schema_org.metadata.tables)
    # On PostgreSQL the org schema's tables are the ones schema.sql creates: a table that exists
    # only in the portable metadata is not in the org schema when the triggers are created.
    schema_sql = (Path(schema_org.__file__).parent / "schema.sql").read_text()
    undeclared = [
        t
        for t in config_stamp.TENANT_TABLES
        if f"CREATE TABLE IF NOT EXISTS {t} (" not in schema_sql
    ]
    assert undeclared == []
    assert set(config_stamp.PLATFORM_TABLES) <= {t.name for t in schema_admin.REGISTRY_TABLES}
    for table, columns in config_stamp.UPDATE_COLUMNS.items():
        assert set(columns) <= set(schema_org.metadata.tables[table].columns.keys())


def test_both_kinds_have_a_row_after_the_schema_is_created(tenant_db):
    assert set(_stamps(tenant_db)) == {config_stamp.MODEL, config_stamp.SETTINGS}


def test_insert_update_and_delete_each_advance_the_stamp(tenant_db):
    with tenant_db.engine.begin() as conn:
        conn.execute(sa.insert(schema_org.roles).values(id="risk_reviewer"))
    after_insert = _model(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.execute(
            sa.update(schema_org.roles)
            .where(schema_org.roles.c.id == "risk_reviewer")
            .values(domain_access=["sales"])
        )
    after_update = _model(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.execute(sa.delete(schema_org.roles).where(schema_org.roles.c.id == "risk_reviewer"))
    after_delete = _model(tenant_db)
    assert after_insert < after_update < after_delete


def test_the_stamp_is_written_in_the_transaction_of_the_change(tenant_db):
    before = _model(tenant_db)
    conn = tenant_db.engine.connect()
    try:
        conn.execute(sa.insert(schema_org.roles).values(id="risk_reviewer"))
        # Inside the writer's own transaction the stamp has already advanced...
        inside = conn.execute(sa.text("SELECT stamp FROM config_stamp WHERE kind = 'model'"))
        assert inside.scalar_one() > before
        # ...and no other process sees it, or the row, until that transaction commits.
        assert _model(tenant_db) == before
        conn.rollback()
    finally:
        conn.close()
    assert _model(tenant_db) == before
    with tenant_db.engine.connect() as check:
        ids = [row[0] for row in check.execute(sa.select(schema_org.roles.c.id))]
    assert "risk_reviewer" not in ids


def test_a_write_that_bypasses_the_repositories_still_advances_the_stamp(tenant_db):
    before = _model(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.exec_driver_sql("INSERT INTO naming_rules (pattern, replacement) VALUES ('a', 'b')")
    assert _model(tenant_db) > before


def test_runtime_bookkeeping_does_not_advance_the_stamp(tenant_db):
    with tenant_db.engine.begin() as conn:
        conn.execute(
            sa.insert(schema_org.materialized_views).values(
                id="mv1",
                source_tables=["t"],
                target_catalog="c",
                target_schema="s",
                target_table="t",
            )
        )
    before = _stamps(tenant_db)
    with tenant_db.engine.begin() as conn:
        # What a refresh writes on a materialized view's row: its state, not its definition.
        conn.execute(
            sa.update(schema_org.materialized_views)
            .where(schema_org.materialized_views.c.id == "mv1")
            .values(status="fresh", row_count=10, writer="w1", materialized_input_version="v2")
        )
        conn.execute(
            sa.insert(schema_org.mv_refresh_log).values(mv_id="mv1", status="success", row_count=1)
        )
        conn.execute(sa.insert(schema_org.node_ids).values(**_first_row(schema_org.node_ids)))
    assert _stamps(tenant_db) == before
    with tenant_db.engine.begin() as conn:
        conn.execute(
            sa.update(schema_org.materialized_views)
            .where(schema_org.materialized_views.c.id == "mv1")
            .values(enabled=False)
        )
    assert _model(tenant_db) > before[config_stamp.MODEL]


def _first_row(table: sa.Table) -> dict:
    """A row for ``table`` with every required column set to a value of its type."""
    values: dict = {}
    for column in table.columns:
        if column.nullable or column.server_default is not None or column.autoincrement is True:
            continue
        values[column.name] = 1 if isinstance(column.type, sa.Integer) else "x"
    return values


def test_a_change_of_one_kind_does_not_move_the_other_kinds_stamp(tenant_db):
    before = _stamps(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.execute(sa.insert(schema_org.org_settings).values(key="cache", value={"ttl": 1}))
    after = _stamps(tenant_db)
    assert after[config_stamp.SETTINGS] > before[config_stamp.SETTINGS]
    assert after[config_stamp.MODEL] == before[config_stamp.MODEL]

    with tenant_db.engine.begin() as conn:
        conn.execute(sa.insert(schema_org.roles).values(id="risk_reviewer"))
    assert _stamps(tenant_db)[config_stamp.SETTINGS] == after[config_stamp.SETTINGS]


def test_installing_twice_changes_nothing(tenant_db):
    before = _stamps(tenant_db)
    with tenant_db.engine.begin() as conn:
        config_stamp.install(conn, config_stamp.TENANT_TABLES)
    assert _stamps(tenant_db) == before
    with tenant_db.engine.begin() as conn:
        conn.execute(sa.insert(schema_org.roles).values(id="risk_reviewer"))
    assert _model(tenant_db) == before[config_stamp.MODEL] + 1  # one trigger per event, not two


def test_an_embedded_duckdb_plane_gets_the_rows_and_no_triggers(tmp_path):
    """REQ-828: DuckDB has no triggers, and its file admits one process — the one that makes a
    change and rebuilds itself — so there is no other process for the stamp to tell."""
    engine = create_engine_from_url(f"duckdb:///{tmp_path / 'tenant.duckdb'}")
    db = Database(engine, name="org")
    try:
        asyncio.run(init_schema(db, "", org_id="default"))
        assert set(_stamps(db)) == {config_stamp.MODEL, config_stamp.SETTINGS}
    finally:
        engine.dispose()


def test_another_dialect_is_refused_by_name():
    class _Dialect:
        name = "mysql"

    class _Conn:
        dialect = _Dialect()

    with pytest.raises(NotImplementedError, match="mysql"):
        config_stamp.install(_Conn(), config_stamp.TENANT_TABLES)


def test_a_table_upsert_is_one_transaction(tenant_db):
    """The table row and its replaced columns commit together: a failure part-way leaves the
    registered table exactly as it was, and the stamp where it was."""

    async def _register(columns: list[Column]) -> None:
        async with tenant_db.acquire() as conn:
            await table_repo.upsert(
                conn,
                Table(
                    source_id="src",
                    domain_id="sales",
                    schema_name="public",
                    table_name="orders",
                    columns=columns,
                ),
            )

    with tenant_db.engine.begin() as conn:
        conn.execute(sa.insert(schema_org.sources).values(id="src", type="postgresql"))
        conn.execute(sa.insert(schema_org.domains).values(id="sales"))
    asyncio.run(
        _register(
            [
                Column(name="id", visible_to=["analyst"], data_type="integer"),
                Column(name="region", visible_to=["analyst"], data_type="varchar"),
            ]
        )
    )
    before = _model(tenant_db)

    # `region` loses its type and has none stored once the columns are deleted for the replace,
    # so the upsert fails AFTER the table row was updated and the old columns were removed.
    untyped = Column(name="amount", visible_to=["analyst"])
    with pytest.raises(ValueError, match="no data_type"):
        asyncio.run(
            _register([Column(name="id", visible_to=["analyst"], data_type="integer"), untyped])
        )

    with tenant_db.engine.connect() as conn:
        names = conn.execute(
            sa.select(schema_org.table_columns.c.column_name).order_by(
                schema_org.table_columns.c.column_name
            )
        ).fetchall()
    assert [n[0] for n in names] == ["id", "region"]
    assert _model(tenant_db) == before


# --- the watcher ----------------------------------------------------------------------------------


class _Copy:
    """What a process holds of one kind: loaded at a stamp, reloaded by reading the plane."""

    def __init__(self, db: Database, kind: str, fail: int = 0) -> None:
        self.db, self.kind, self.fail = db, kind, fail
        self.stamp: int | None = None
        self.reloads = 0

    async def load(self) -> None:
        if self.fail:
            self.fail -= 1
            raise RuntimeError("the control plane refused the reload")
        self.stamp = (await config_stamp.read(self.db))[self.kind]
        self.reloads += 1

    def target(self, name: str) -> config_watch.Target:
        return config_watch.Target(
            name=name, db=self.db, kind=self.kind, loaded=lambda: self.stamp, reload=self.load
        )


def _check(*copies: tuple[str, _Copy]) -> list[str]:
    return asyncio.run(config_watch.check([copy.target(name) for name, copy in copies]))


def _other_worker_changes_the_model(db: Database, role: str) -> None:
    with db.engine.begin() as conn:
        conn.execute(sa.insert(schema_org.roles).values(id=role))


def test_a_process_reloads_exactly_when_the_stored_stamp_differs(tenant_db):
    model = _Copy(tenant_db, config_stamp.MODEL)
    settings = _Copy(tenant_db, config_stamp.SETTINGS)
    asyncio.run(model.load())
    asyncio.run(settings.load())
    model.reloads = settings.reloads = 0

    assert _check(("model", model), ("settings", settings)) == []

    _other_worker_changes_the_model(tenant_db, "risk_reviewer")
    assert _check(("model", model), ("settings", settings)) == ["model"]
    assert (model.reloads, settings.reloads) == (1, 0)  # one kind's change reloads only that kind
    assert model.stamp == _model(tenant_db)

    assert _check(("model", model), ("settings", settings)) == []
    assert (model.reloads, settings.reloads) == (1, 0)


def test_two_processes_on_one_plane_each_reload_once(tenant_db):
    worker_a = _Copy(tenant_db, config_stamp.MODEL)
    worker_b = _Copy(tenant_db, config_stamp.MODEL)
    asyncio.run(worker_a.load())
    asyncio.run(worker_b.load())

    _other_worker_changes_the_model(tenant_db, "risk_reviewer")
    assert _check(("a", worker_a)) == ["a"]
    assert _check(("b", worker_b)) == ["b"]
    assert _check(("a", worker_a), ("b", worker_b)) == []


def test_a_copy_not_yet_loaded_is_left_to_whoever_is_loading_it(tenant_db):
    building = _Copy(tenant_db, config_stamp.MODEL)
    assert _check(("building", building)) == []
    assert building.reloads == 0


def test_a_failed_reload_is_tried_again_on_the_next_check(tenant_db, caplog):
    loaded = _Copy(tenant_db, config_stamp.MODEL)
    asyncio.run(loaded.load())
    was = loaded.stamp
    _other_worker_changes_the_model(tenant_db, "risk_reviewer")
    loaded.fail = 1

    assert _check(("model", loaded)) == []
    assert loaded.stamp == was  # still behind, and known to be
    assert "config reload of model failed" in caplog.text

    assert _check(("model", loaded)) == ["model"]
    assert loaded.stamp == _model(tenant_db)


def test_one_failed_reload_does_not_stop_the_others(tenant_db):
    broken = _Copy(tenant_db, config_stamp.MODEL)
    healthy = _Copy(tenant_db, config_stamp.MODEL)
    asyncio.run(broken.load())
    asyncio.run(healthy.load())
    _other_worker_changes_the_model(tenant_db, "risk_reviewer")
    broken.fail = 1
    assert _check(("broken", broken), ("healthy", healthy)) == ["healthy"]


def test_the_reload_interval_is_an_operator_setting_with_a_floor():
    from provisa.core import settings_registry

    declared = settings_registry.setting(config_watch.INTERVAL_SETTING)
    assert (declared.type, declared.effect, declared.default, declared.min) == (
        "float",
        "live",
        2.0,
        0.5,
    )
    assert declared.editable and declared.card in settings_registry.CARDS
    with pytest.raises(settings_registry.SettingInvalid):
        settings_registry.parse(declared, 0.1, "stored")
