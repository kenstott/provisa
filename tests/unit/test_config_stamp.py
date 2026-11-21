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
import datetime
from pathlib import Path

import pytest
import sqlalchemy as sa

from provisa.core import config_stamp, config_watch, schema_org
from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.federation.replica_state import RetryPolicy


@pytest.fixture
def tenant_db(control_plane):
    return control_plane.org()


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


_TENANT_KINDS = {
    config_stamp.MODEL,
    config_stamp.BUILD,
    config_stamp.SETTINGS,
    config_stamp.REPLICA,
}


def test_every_kind_has_a_row_after_the_schema_is_created(tenant_db):
    assert set(_stamps(tenant_db)) == _TENANT_KINDS


def test_the_replica_stamp_has_no_trigger_and_moves_only_when_advanced(tenant_db):
    """REQ-826 / REQ-1920: ``replica_state`` is state — written on every build request, progress
    report and refresh. None of those moves a stamp; the state store advances the ``replica``
    stamp itself, on the two changes that move a table between live and its replica."""
    from datetime import UTC, datetime

    from provisa.federation import replica_state

    assert "replica_state" not in config_stamp.TENANT_TABLES
    key = ("pg", "public", "orders")

    async def _build_and_refresh() -> None:
        async with tenant_db.acquire() as conn:
            await replica_state.request_build(conn, key, replica_state.REASON_MODEL)
            now = datetime.now(UTC)
            await replica_state.claim(conn, key, holder="h:1", now=now, retry=RetryPolicy(60, 3600))
            await replica_state.record_progress(conn, key, rows_copied=10)
            for _ in range(2):
                await replica_state.record_completed(
                    conn,
                    key,
                    rows_copied=10,
                    method="stream_batches",
                    content_hash="h",
                    store="store-a",
                    next_refresh_at=None,
                    now=now,
                )

    before = _stamps(tenant_db)
    asyncio.run(_build_and_refresh())
    assert _stamps(tenant_db) == before

    async def _promote() -> None:
        async with tenant_db.acquire() as conn:
            await replica_state.set_promoted(conn, key, True)

    asyncio.run(_promote())
    after = _stamps(tenant_db)
    assert after[config_stamp.REPLICA] == before[config_stamp.REPLICA] + 1
    # ...and it is not a change to the model: no worker rebuilds its schemas for it
    assert after[config_stamp.MODEL] == before[config_stamp.MODEL]
    assert after[config_stamp.SETTINGS] == before[config_stamp.SETTINGS]


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
        # What a refresh writes: the region's build of the view (REQ-1922), not its definition.
        conn.execute(
            sa.insert(schema_org.mv_build_state).values(
                mv_id="mv1",
                region="default",
                status="fresh",
                row_count=10,
                writer="w1",
                materialized_input_version="v2",
            )
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
        config_stamp.install(
            conn, config_stamp.TENANT_TABLES, advanced=config_stamp.TENANT_ADVANCED
        )
    assert _stamps(tenant_db) == before
    with tenant_db.engine.begin() as conn:
        conn.execute(sa.insert(schema_org.roles).values(id="risk_reviewer"))
    assert _model(tenant_db) == before[config_stamp.MODEL] + 1  # one trigger per event, not two


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


def test_a_copy_that_is_gone_is_not_reported_as_reloaded(tenant_db, caplog):
    """A reload that found no copy left to load (its runtime was dropped or replaced under it)
    answers NOT_RELOADED. The check does not list it among those reloaded and does not log
    "config reloaded" for it: it says the copy is gone and its replacement loads its own."""
    import logging

    class _Gone(_Copy):
        async def load(self):
            self.reloads += 1
            return config_watch.NOT_RELOADED

    gone = _Gone(tenant_db, config_stamp.MODEL)
    gone.stamp = -1  # loaded at a stamp the plane no longer holds: the check will ask it to reload

    with caplog.at_level(logging.INFO, logger="provisa.core.config_watch"):
        assert _check(("org acme: model", gone)) == []

    assert gone.reloads == 1
    said = [r.getMessage() for r in caplog.records]
    assert not any(m.startswith("config reloaded") for m in said), said
    assert any("config not reloaded: org acme: model is gone" in m for m in said), said


# REQ-1914 (amended 2026-11-09): the build stamp advances for what the model build reads. A change
# the build does not read advances the model stamp alone.

_PRODUCT = "orders_product"


@pytest.fixture
def product(tenant_db):
    with tenant_db.engine.begin() as conn:
        conn.execute(sa.insert(schema_org.domains).values(id="sales"))
        conn.execute(sa.insert(schema_org.domains).values(id="finance"))
        for pid in (_PRODUCT, "taker"):
            conn.execute(
                sa.insert(schema_org.data_products).values(id=pid, domain_id="sales", name=pid)
            )
    return _PRODUCT


def _update_product(db: Database, **values) -> None:
    with db.engine.begin() as conn:
        conn.execute(
            sa.update(schema_org.data_products)
            .where(schema_org.data_products.c.id == _PRODUCT)
            .values(**values)
        )


def _moved(db: Database, before: dict[str, int]) -> tuple[bool, bool]:
    """Whether the (model, build) stamps moved since ``before``."""
    after = _stamps(db)
    return (
        after[config_stamp.MODEL] != before[config_stamp.MODEL],
        after[config_stamp.BUILD] != before[config_stamp.BUILD],
    )


@pytest.mark.parametrize(
    "values",
    [
        {"status": "live"},
        {"status_reason": "because"},
        {"retire_on": datetime.date(2030, 1, 1)},
        {"status_by": "someone"},
        {"status_at": datetime.datetime(2030, 1, 1, tzinfo=datetime.UTC)},
        {"successor_id": "taker"},
        {"status": "deprecated", "status_reason": "old", "status_by": "someone"},
    ],
    ids=lambda v: "+".join(v),
)
@pytest.mark.usefixtures("product")
def test_a_status_change_moves_the_model_stamp_and_not_the_build_stamp(tenant_db, values):
    before = _stamps(tenant_db)
    _update_product(tenant_db, **values)
    assert _moved(tenant_db, before) == (True, False)


@pytest.mark.parametrize(
    "values",
    [
        {"owner_role": "org_admin"},  # what the build reads (REQ-2188)
        {"domain_id": "finance"},  # what the build reads (REQ-2219)
        {"name": "renamed"},  # a column not listed as one the build ignores
        {"status": "live", "owner_role": "org_admin"},  # a status beside what the build reads
    ],
    ids=lambda v: "+".join(v),
)
@pytest.mark.usefixtures("product")
def test_a_change_the_build_reads_moves_both_stamps(tenant_db, values):
    before = _stamps(tenant_db)
    _update_product(tenant_db, **values)
    assert _moved(tenant_db, before) == (True, True)


@pytest.mark.usefixtures("product")
def test_a_new_or_deleted_product_moves_both_stamps(tenant_db):
    before = _stamps(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.execute(
            sa.insert(schema_org.data_products).values(id="another", domain_id="sales", name="a")
        )
    assert _moved(tenant_db, before) == (True, True)

    before = _stamps(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.execute(
            sa.delete(schema_org.data_products).where(schema_org.data_products.c.id == "another")
        )
    assert _moved(tenant_db, before) == (True, True)


@pytest.mark.usefixtures("product")
def test_a_truncate_of_the_products_moves_both_stamps(tenant_db):
    before = _stamps(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE data_products CASCADE"))
    assert _moved(tenant_db, before) == (True, True)


@pytest.mark.usefixtures("product")
def test_a_product_rewritten_with_the_values_it_holds_moves_neither_stamp(tenant_db):
    before = _stamps(tenant_db)
    _update_product(tenant_db, status="draft", name=_PRODUCT)
    assert _moved(tenant_db, before) == (False, False)


def test_a_change_to_any_other_model_table_moves_both_stamps(tenant_db):
    before = _stamps(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.execute(sa.insert(schema_org.roles).values(id="risk_reviewer"))
    assert _moved(tenant_db, before) == (True, True)

    before = _stamps(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.execute(
            sa.update(schema_org.roles)
            .where(schema_org.roles.c.id == "risk_reviewer")
            .values(domain_access=["sales"])
        )
    assert _moved(tenant_db, before) == (True, True)


def test_a_settings_change_moves_neither_the_model_nor_the_build_stamp(tenant_db):
    before = _stamps(tenant_db)
    with tenant_db.engine.begin() as conn:
        conn.execute(
            sa.insert(schema_org.org_settings).values(key="naming", value={"use_domains": False})
        )
    assert _moved(tenant_db, before) == (False, False)


@pytest.mark.usefixtures("product")
def test_the_status_actions_of_the_repository_do_not_move_the_build_stamp(tenant_db):
    from provisa.core.repositories import data_product as data_product_repo

    async def _publish_then_deprecate() -> None:
        async with tenant_db.acquire() as conn:
            await data_product_repo.publish(conn, _PRODUCT, by="someone")
            await data_product_repo.deprecate(
                conn,
                _PRODUCT,
                reason="superseded",
                retire_on=None,
                by="someone",
                today=datetime.date(2026, 11, 9),
            )

    before = _stamps(tenant_db)
    asyncio.run(_publish_then_deprecate())
    assert _moved(tenant_db, before) == (True, False)


@pytest.mark.usefixtures("product")
def test_an_install_over_an_older_trigger_function_replaces_it(tenant_db):
    """No migrations: a schema laid out by an earlier release keeps its triggers, and the install
    replaces the function they call and seeds the row, so the rule reaches every org schema."""
    with tenant_db.engine.begin() as conn:
        conn.execute(
            sa.text(
                "CREATE OR REPLACE FUNCTION advance_config_stamp() RETURNS trigger "
                "LANGUAGE plpgsql AS $fn$ BEGIN "
                "EXECUTE format('UPDATE %I.config_stamp SET stamp = stamp + 1 WHERE kind = $1', "
                "TG_TABLE_SCHEMA) USING TG_ARGV[0]; RETURN NULL; END $fn$"
            )
        )
        conn.execute(sa.text("DELETE FROM config_stamp WHERE kind = 'build'"))
        config_stamp.install(
            conn, config_stamp.TENANT_TABLES, advanced=config_stamp.TENANT_ADVANCED
        )
    assert config_stamp.BUILD in _stamps(tenant_db)
    before = _stamps(tenant_db)
    _update_product(tenant_db, status="live")
    assert _moved(tenant_db, before) == (True, False)


def test_installs_of_two_schemas_do_not_wait_on_each_other(control_plane):
    """REQ-2006: the install's lock is the schema's. A sandbox's open lays its environments out side
    by side, and one lock for every schema queued each behind the other; two installs of one schema
    still take turns."""
    from provisa.core.db import init_schema

    pool = control_plane.org()
    schema_sql = (Path(config_stamp.__file__).parent / "schema.sql").read_text()
    asyncio.run(init_schema(pool, schema_sql, org_id="default", env="other"))
    engine = pool.engine

    def _install(conn, schema: str) -> None:
        config_stamp.install(
            conn, config_stamp.TENANT_TABLES, schema, advanced=config_stamp.TENANT_ADVANCED
        )

    with engine.connect() as holder:
        with holder.begin():
            _install(holder, "org_default")  # holds org_default's install lock until it ends
            with engine.connect() as other:
                with other.begin():
                    other.execute(sa.text("SET LOCAL lock_timeout = '2s'"))
                    _install(other, "org_default_env_other")  # another schema: no wait
                with pytest.raises(sa.exc.OperationalError, match="lock timeout"):
                    with other.begin():
                        other.execute(sa.text("SET LOCAL lock_timeout = '200ms'"))
                        _install(other, "org_default")  # the same schema: its turn
