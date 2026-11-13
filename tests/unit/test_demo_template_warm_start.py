# Copyright (c) 2026 Kenneth Stott
# Canary: e7fa6156-2ebd-45ab-92bc-3370c41e859b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox tier's start serves its demos' templates as they were built (REQ-2006).

Every start (each wake from idle, each deploy) used to prepare every template again: its runtime
built, then every replica polled until landed. A template that finished building is recorded --
its demo version and Provisa release, and the rows each replica and view holds -- and a start that
finds the same version and the same rows on disk serves it at once. A changed version, or a store
whose rows differ from the record, is built again through the build queue."""

# Requirements: REQ-2006

from __future__ import annotations

import re
from pathlib import Path

import pytest
from sqlalchemy import insert, select

from provisa.api import demo_templates
from provisa.api.demo_templates import PublishedDemo, TemplateBuilds
from provisa.core.schema_admin import demo_template_readiness, orgs

pytestmark = pytest.mark.unit

_BUILT = {"replicas": {"src__public__orders": 40}, "views": {"view_answer": 12}}


def _demo(name: str, version: str = "20261019-abc") -> PublishedDemo:
    return PublishedDemo(name=name, title=name.title(), directory=Path("/bundles") / name / version)


@pytest.fixture
async def platform(control_plane):
    return control_plane.platform


async def _built(pool, demo: PublishedDemo, *, version: str | None = None) -> None:
    """``demo``'s template as a finished build leaves it: its org ready, and its record."""
    async with pool.acquire() as conn:
        await conn.execute_core(
            insert(orgs).values(id=demo.template_org, name=demo.title, provisioning_state="ready")
        )
        await conn.execute_core(
            insert(demo_template_readiness).values(
                template_org=demo.template_org,
                version=version or demo_templates.readiness_version(demo),
                replicas=_BUILT["replicas"],
                views=_BUILT["views"],
            )
        )


def _on_disk(monkeypatch, counts: dict[str, dict]) -> None:
    monkeypatch.setattr(
        "provisa.api.demo_replicas.store_counts",
        lambda org: counts.get(org, {"replicas": {}, "views": {}}),
    )


async def test_an_unchanged_template_with_its_store_on_disk_is_ready_as_built(
    platform, monkeypatch
):
    demo = _demo("alpha")
    await _built(platform, demo)
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    assert await demo_templates.warm_ready(platform, demo) is True


async def test_a_changed_demo_version_is_built_again(platform, monkeypatch):
    demo = _demo("alpha")
    await _built(platform, demo, version="20261001-old@0.1.0a480")
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    assert await demo_templates.warm_ready(platform, demo) is False


async def test_a_template_built_under_other_control_plane_tables_is_built_again(
    platform, monkeypatch
):
    """A cloud patch keeps the Provisa release, so the release alone never told a template built
    by older code: its tables are that code's (no migrations), and a column added since failed
    every sandbox of it. The record names the DDL of the tables the code creates."""
    demo = _demo("alpha")
    await _built(platform, demo)
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    monkeypatch.setattr("provisa.core.schema_org.ddl_fingerprint", lambda: "0123456789abcdef")
    assert demo_templates.readiness_version(demo).endswith("@0123456789abcdef")
    assert await demo_templates.warm_ready(platform, demo) is False


def _metadata_copy():
    from sqlalchemy import MetaData

    from provisa.core import schema_org

    copy = MetaData()
    for table in schema_org.metadata.tables.values():
        table.to_metadata(copy)
    return copy


def test_the_ddl_fingerprint_changes_with_a_column_and_with_schema_sql():
    from sqlalchemy import Column, Text

    from provisa.core import schema_org

    copy, sources = _metadata_copy(), schema_org.ddl_sources()
    base = schema_org.fingerprint_of(copy, sources)
    assert len(base) == 16 and int(base, 16) >= 0
    assert schema_org.fingerprint_of(copy, sources) == base
    changed = {**sources, "core/schema.sql": sources["core/schema.sql"] + b"\n-- a column"}
    assert schema_org.fingerprint_of(copy, changed) != base
    copy.tables["calendars"].append_column(Column("a_column_added_since", Text))
    assert schema_org.fingerprint_of(copy, sources) != base
    assert schema_org.ddl_fingerprint() == schema_org.ddl_fingerprint()


@pytest.mark.parametrize(
    "path, before, after",
    [
        # config_stamp's trigger function and its triggers (cloud: relation "config_stamp" does not
        # exist in a sandbox opened from a template built before it).
        ("core/config_stamp.py", b"CREATE OR REPLACE FUNCTION", b"CREATE OR REPLACE FUNCTION  "),
        ("core/config_stamp.py", b"CREATE TRIGGER", b"CREATE  TRIGGER"),
        ("core/env_changes.py", b"CreateTable(", b"CreateTable( "),
        ("api/_meta_views.py", b"CREATE OR REPLACE VIEW", b"CREATE OR REPLACE VIEW "),
    ],
    ids=["function", "trigger", "env-changes-table", "view"],
)
def test_the_ddl_fingerprint_changes_with_a_trigger_function_or_view(path, before, after):
    from provisa.core import schema_org

    copy, sources = _metadata_copy(), schema_org.ddl_sources()
    assert before in sources[path]
    changed = {**sources, path: sources[path].replace(before, after, 1)}
    assert schema_org.fingerprint_of(copy, changed) != schema_org.fingerprint_of(copy, sources)


# Every module that runs DDL, by the statements it names, and where that DDL lands when it is not an
# org's control-plane schema. A module here or in schema_org.ORG_SCHEMA_DDL_SOURCES, never neither:
# a new source of an org's tables, views, functions or triggers fails this until it is classed.
_DDL = re.compile(
    rb"CREATE (OR REPLACE )?(TABLE|VIEW|FUNCTION|TRIGGER|INDEX|UNIQUE INDEX|MATERIALIZED VIEW"
    rb"|SEQUENCE|TYPE)|CreateTable\(|CreateIndex\(|\.create_all\(|CreateSchema"
)
_NOT_ORG_SCHEMA_DDL = {
    "core/schema_org.py": "this metadata, compiled by the fingerprint itself",
    "core/schema_create.py": "creates the schema its caller names, under a lock; no table",
    "core/schema_admin.py": "the platform plane, never an org's schema",
    "core/boot_lock.py": "the platform plane's boot lock",
    "core/org_registry_view.py": "a view in the root org's schema, never a template's",
    "core/duckdb_store.py": "the embedded DuckDB control plane's dialect",
    "core/ir_types.py": "names CREATE TABLE in a comment",
    "core/trino_system_catalogs.py": "Iceberg's JDBC catalog tables, for the Trino catalogs",
    "observability/ops_schema.py": "the ops schema",
    "observability/ops_trino.py": "the otel catalog",
    "scheduler/jobs.py": "the otel catalog",
    "api_source/engine_cache.py": "an API source's cache catalog",
    "api/demo_region_seed.py": "a region store's copy of a demo seed (data)",
    "api/admin/schema_mutation_ops.py": "a view deployed to a source",
    "api/admin/introspect.py": "names CREATE VIEW in a comment",
    "api/admin/orgs_router.py": "names CREATE VIEW in a comment",
    "api/admin/schema_query.py": "names CREATE TABLE in a comment",
    "api/airport/server.py": "an Airport action's name",
    "api/flight/server.py": "names statements it refuses",
    "api/metadata_export/unity_catalog.py": "exported text for Unity Catalog",
    "api/org_runtime.py": "names CREATE VIEW in a comment",
    "api/rest/graph_tools_router.py": "a Cypher index statement",
    "codehost/pgcatalog.py": "an in-process DuckDB catalog",
    "compiler/definitions.py": "names statements it classes",
    "ephemeral/indexes.py": "a session's scratch store",
    "ephemeral/store.py": "a session's scratch store",
    "events/triggers.py": "triggers in a source's own database",
    "subscriptions/pg_triggers.py": "triggers in a source's own database",
    "executor/drivers/postgresql.py": "names CREATE INDEX in a comment",
    "executor/trino_write.py": "the results catalog",
    "hive/fetch.py": "names CREATE TABLE in a comment",
    "mv/refresh.py": "a materialized view's target (data)",
    "mv/bitemporal.py": "a materialized view's target (data)",
    "pgwire/catalog_constraints.py": "pgwire's in-process catalog",
    "pgwire/catalog_pg17.py": "pgwire's in-process catalog",
    "pgwire/catalog_pg17_procs.py": "pgwire's in-process catalog",
    "pgwire/catalog_pg17_tables.py": "pgwire's in-process catalog",
    "pgwire/catalog_pg17_viewdefs.py": "pgwire's in-process catalog",
    "pgwire/catalog_populate.py": "pgwire's in-process catalog",
    "pgwire/catalog_privileges.py": "pgwire's in-process catalog",
    "pgwire/catalog_server_answers.py": "pgwire's in-process catalog",
    "pgwire/ext_surfaces.py": "names CREATE INDEX in a comment",
    "pgwire/server.py": "command tags",
    "pgwire/system_tables.py": "pgwire's in-process catalog",
    "transpiler/metadata_functions.py": "text a catalog answer returns",
    "federation/backend.py": "an engine's own schemas",
    "federation/bigquery_store.py": "a BigQuery store",
    "federation/clickhouse_connectors.py": "a ClickHouse engine",
    "federation/clickhouse_runtime.py": "a ClickHouse engine",
    "federation/clickhouse_store.py": "a ClickHouse store",
    "federation/connector_duckdb.py": "a DuckDB engine's views of a source",
    "federation/custom_connectors.py": "an engine's views of a source",
    "federation/databricks_connectors.py": "a Databricks engine",
    "federation/databricks_runtime.py": "a Databricks engine",
    "federation/databricks_store.py": "a Databricks store",
    "federation/duckdb_runtime.py": "a DuckDB engine",
    "federation/mail_write.py": "an in-process scratch table",
    "federation/materialize_broker.py": "replicas in the materialization store (data)",
    "federation/mssql_warehouse_runtime.py": "a warehouse engine",
    "federation/pg_runtime.py": "a pg engine's views of a source",
    "federation/replica_target.py": "replicas in their store (data)",
    "federation/replica_target_warehouse.py": "replicas in a warehouse (data)",
    "federation/session_rows.py": "indexes of a replica (data)",
    "federation/snowflake_store.py": "a Snowflake store",
    "federation/sqlalchemy_runtime.py": "an engine's own schemas",
    "federation/store_connection.py": "the materialization store",
    "federation/store_writer.py": "the materialization store",
    "reference_library/store.py": "a held passage dimension's vector index (data)",
}


def test_every_module_running_ddl_is_classed_for_the_fingerprint():
    from provisa.core import schema_org

    package = Path(schema_org.__file__).resolve().parents[1]
    found = {
        str(f.relative_to(package))
        for f in package.rglob("*")
        if f.suffix in (".py", ".sql") and _DDL.search(f.read_bytes())
    }
    listed = set(schema_org.ORG_SCHEMA_DDL_SOURCES)
    assert not listed & set(_NOT_ORG_SCHEMA_DDL)
    assert found - listed - set(_NOT_ORG_SCHEMA_DDL) == set(), "class each new source of DDL"
    assert listed <= found, "a listed source that runs no DDL"


@pytest.mark.parametrize(
    "held",
    [
        {"replicas": {}, "views": {}},  # the store file is gone
        {"replicas": {"src__public__orders": 40}, "views": {}},  # a view's table is missing
        {"replicas": {"src__public__orders": 39}, "views": {"view_answer": 12}},  # rows differ
    ],
    ids=["no-store", "missing-view", "rows-differ"],
)
async def test_a_store_that_differs_from_its_record_is_built_again(platform, monkeypatch, held):
    demo = _demo("alpha")
    await _built(platform, demo)
    _on_disk(monkeypatch, {demo.template_org: held})
    assert await demo_templates.warm_ready(platform, demo) is False


async def test_a_template_never_recorded_is_built(platform, monkeypatch):
    _on_disk(monkeypatch, {})
    assert await demo_templates.warm_ready(platform, _demo("alpha")) is False


async def test_a_finished_build_records_the_rows_its_store_holds(platform, monkeypatch):
    demo = _demo("alpha")
    async with platform.acquire() as conn:
        await conn.execute_core(insert(orgs).values(id=demo.template_org, name=demo.title))
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    await demo_templates.record_ready(platform, demo)
    assert await demo_templates.warm_ready(platform, demo) is True
    async with platform.acquire() as conn:
        row = (await conn.execute_core(select(demo_template_readiness))).fetchone()
    assert (row.version, row.replicas, row.views) == (
        demo_templates.readiness_version(demo),
        _BUILT["replicas"],
        _BUILT["views"],
    )


async def test_the_start_serves_an_unchanged_template_and_queues_only_the_changed(
    platform, monkeypatch
):
    warm, changed = _demo("alpha"), _demo("beta")
    await _built(platform, warm)
    await _built(platform, changed, version="20261001-old@0.1.0a480")
    _on_disk(monkeypatch, {warm.template_org: _BUILT, changed.template_org: _BUILT})
    queue = TemplateBuilds()
    rows: list[str] = []
    spawned: list[str] = []

    async def _template_row(_pool, demo):
        rows.append(demo.name)
        return True

    def _spawn(coro, *, name):
        spawned.append(name)
        coro.close()
        return _Task()

    monkeypatch.setattr(demo_templates, "template_builds", queue)
    monkeypatch.setattr(demo_templates, "published_demos", lambda: {"alpha": warm, "beta": changed})
    monkeypatch.setattr(demo_templates, "bundled_demos", lambda: {})
    monkeypatch.setattr(demo_templates, "seeding_release", lambda: False)
    monkeypatch.setattr("provisa.api.demo_region_seed.regional", lambda: False)
    monkeypatch.setattr(demo_templates, "_template_row", _template_row)
    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", _spawn)
    monkeypatch.setattr("provisa.core.settings_registry.value", lambda _key: 2)
    retired: list[str] = []

    async def _retire(name: str) -> None:
        retired.append(name)

    monkeypatch.setattr(demo_templates, "_retire_template", _retire)
    outcome = await demo_templates.ensure_demo_templates(platform)
    assert outcome == {"alpha": "ready (as built)", "beta": "building"}
    assert retired == ["beta"]  # built for another version: replaced, not loaded
    assert rows == ["beta"]  # the unchanged template is neither marked for building nor queued
    assert queue.state_of("alpha") == "ready"
    assert queue.state_of("beta") == "queued"
    assert spawned == ["demo-templates"]


async def test_a_template_another_release_built_is_replaced_not_loaded(platform, monkeypatch):
    """A template org left ready by the release before (same demo version) has that release's
    schema; loading it failed every read of a column added since (sources.download_from on the
    sandbox tier, 500 on opening starthere). It is removed and built again."""
    from sqlalchemy import delete

    demo = _demo("alpha")
    await _built(platform, demo, version=f"{demo.directory.name}@0.1.0a480")
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    queue = TemplateBuilds()
    retired: list[str] = []

    async def _retire(name: str) -> None:
        retired.append(name)
        async with platform.acquire() as conn:
            await conn.execute_core(delete(orgs).where(orgs.c.id == demo.template_org))

    monkeypatch.setattr(demo_templates, "template_builds", queue)
    monkeypatch.setattr(demo_templates, "bundled_demos", lambda: ("alpha",))
    monkeypatch.setattr(demo_templates, "seeding_release", lambda: False)
    monkeypatch.setattr("provisa.api.demo_region_seed.regional", lambda: False)
    monkeypatch.setattr(demo_templates, "_retire_template", _retire)
    assert await demo_templates._queue_template(platform, demo) == "building"
    assert retired == ["alpha"]
    assert queue.state_of("alpha") == "queued"
    async with platform.acquire() as conn:
        state = (
            await conn.execute_core(
                select(orgs.c.provisioning_state).where(orgs.c.id == demo.template_org)
            )
        ).scalar_one()
    assert state == "provisioning"


class _Task:
    def add_done_callback(self, _callback) -> None:
        pass


async def test_a_bundle_ready_after_the_start_has_its_template_queued(platform, monkeypatch):
    """REQ-2049: the node marks a bundle ready after the tier started; the tier's scheduled pass
    queues its template as the start queued the others, and starts the builds once."""
    import threading

    first, late = _demo("alpha"), _demo("beta")
    _on_disk(monkeypatch, {})
    queue = TemplateBuilds()
    published = {"alpha": first}
    rows: list[str] = []
    spawned: list[str] = []

    async def _template_row(_pool, demo):
        rows.append(demo.name)
        return True

    def _spawn(coro, *, name):
        spawned.append(name)
        coro.close()
        return _Task()

    monkeypatch.setattr(demo_templates, "template_builds", queue)
    monkeypatch.setattr(demo_templates, "_startup_queued", threading.Event())
    monkeypatch.setattr(demo_templates, "published_demos", lambda: dict(published))
    monkeypatch.setattr(demo_templates, "bundled_demos", lambda: ())
    monkeypatch.setattr(demo_templates, "seeding_release", lambda: False)
    monkeypatch.setattr("provisa.api.demo_region_seed.regional", lambda: False)
    monkeypatch.setattr(demo_templates, "_template_row", _template_row)
    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", _spawn)
    monkeypatch.setattr("provisa.core.settings_registry.value", lambda _key: 2)

    async def _retire(_name: str) -> None:
        pass  # never recorded ready: nothing of it to remove

    monkeypatch.setattr(demo_templates, "_retire_template", _retire)
    # Before the start has queued its own, the pass queues nothing.
    assert await demo_templates.queue_late_templates(platform) == []
    await demo_templates.ensure_demo_templates(platform)
    published["beta"] = late
    assert queue.state_of("beta") == "unknown"
    assert await demo_templates.queue_late_templates(platform) == ["beta"]
    assert queue.state_of("beta") == "queued"
    assert rows == ["alpha", "beta"]
    # The start's workers had not begun (the spawn above ran nothing): no second start.
    assert spawned == ["demo-templates"]
    assert await demo_templates.queue_late_templates(platform) == []


async def test_a_fresh_disk_in_bundled_mode_keeps_the_shared_template(
    platform, monkeypatch, tmp_path
):
    """A node with a fresh disk under a shared control plane, serving bundles: the plane records
    the template ready from this version, and nothing on this node says what it was built from.
    The template the other nodes serve is kept -- prepared again here, never retired."""
    demo = _demo("alpha")
    await _built(platform, demo)
    _on_disk(monkeypatch, {})  # this node holds none of its store
    queue = TemplateBuilds()
    retired: list[str] = []

    async def _retire(name: str) -> None:
        retired.append(name)

    monkeypatch.setattr(demo_templates, "template_builds", queue)
    monkeypatch.setattr(demo_templates, "bundled_demos", lambda: ("alpha",))
    monkeypatch.setattr(demo_templates, "bundles_root", lambda: tmp_path)  # a fresh disk
    monkeypatch.setattr(demo_templates, "seeding_release", lambda: False)
    monkeypatch.setattr("provisa.api.demo_region_seed.regional", lambda: False)
    monkeypatch.setattr(demo_templates, "_retire_template", _retire)
    assert await demo_templates._queue_template(platform, demo) == "ready"
    assert retired == []
    assert queue.state_of("alpha") == "queued"


async def test_a_regions_tiers_finished_template_is_recorded_and_not_replaced_at_the_next_open(
    platform, monkeypatch
):
    """A regions tier's template is recorded ready when its build finishes, as any tier's is: the
    record says which demo version it is (_templated, _built_for). Unrecorded, every visitor's open
    after the build took the finished template for another version's, retired it and built it
    again, and the demo was never ready to open."""
    from types import SimpleNamespace

    demo = _demo("alpha")
    async with platform.acquire() as conn:
        await conn.execute_core(
            insert(orgs).values(id=demo.template_org, name=demo.title, provisioning_state="ready")
        )

    async def _runtime(org_id):
        return SimpleNamespace(org_id=org_id)

    async def _nothing(*_args, **_kwargs):
        return None

    async def _all_landed(_rt):
        return 1, [], 1, ""

    async def _no_failed_runs(_demo, _org_id):
        return []

    monkeypatch.setattr("provisa.api.demo_region_seed.regional", lambda: True)
    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _runtime)
    monkeypatch.setattr("provisa.api.admin.orgs_router._admin_pool", lambda: platform)
    monkeypatch.setattr(demo_templates, "create_environments", _nothing)
    monkeypatch.setattr(demo_templates, "demo_environments", lambda _directory: [])
    monkeypatch.setattr(demo_templates, "seeding_release", lambda: False)
    monkeypatch.setattr(demo_templates, "_replica_progress", _all_landed)
    monkeypatch.setattr(demo_templates, "profile_template", _no_failed_runs)
    monkeypatch.setattr(demo_templates, "_ready_hooks", [])
    monkeypatch.setattr("provisa.api.demo_synthetic.prepare", _nothing)
    monkeypatch.setattr("provisa.api.demo_synthetic.synthetic_envs", lambda _envs: [])
    _on_disk(monkeypatch, {demo.template_org: _BUILT})

    assert await demo_templates._prepare(demo, False)
    assert await demo_templates._templated(platform, "alpha") == demo.directory.name

    retired: list[str] = []

    async def _retire(name):
        retired.append(name)

    queue = TemplateBuilds()
    monkeypatch.setattr(demo_templates, "template_builds", queue)
    monkeypatch.setattr(demo_templates, "_retire_template", _retire)
    assert await demo_templates._queue_template(platform, demo) == "ready"
    assert retired == []


def _profiled(tmp_path: Path, rows: int) -> PublishedDemo:
    """A demo whose setup profiles, its bundle carrying one relation of ``rows`` runs."""
    import json

    directory = tmp_path / "bundles" / "profiling" / "20261102-abc"
    (directory / "seeded").mkdir(parents=True)
    (directory / "setup.yaml").write_text("steps:\n- profile: harbor-profiler\n")
    carried = [{"run_id": f"r{i}", "status": "ok"} for i in range(rows)]
    (directory / "seeded" / "profile_results.json").write_text(
        json.dumps(
            {
                "results": [
                    {"table": ["crm", "public", "customers"], "kind": "runs", "rows": carried}
                ]
            }
        )
    )
    return PublishedDemo(name="profiling", title="Profiling", directory=directory)


async def _holding(platform, demo: PublishedDemo, rows: int) -> None:
    """The template's schema: its member registered as id 309, and its runs relation, ``rows``."""
    from sqlalchemy import text
    from sqlalchemy.schema import CreateTable

    from provisa.core.environments import org_schema
    from provisa.profiler.schema import result_sa_table

    schema = org_schema(demo.template_org)
    relation = result_sa_table("customers", 309, "runs")
    relation.schema = schema
    async with platform.acquire() as conn:
        await conn.execute_core(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await conn.execute_core(text(f'CREATE SCHEMA "{schema}"'))
        await conn.execute_core(
            text(
                f'CREATE TABLE "{schema}".registered_tables '
                "(id int, source_id text, schema_name text, table_name text)"
            )
        )
        await conn.execute_core(
            text(
                f'INSERT INTO "{schema}".registered_tables '
                "VALUES (309, 'crm', 'public', 'customers')"
            )
        )
        await conn.execute_core(CreateTable(relation))
        for i in range(rows):
            await conn.execute_core(insert(relation).values(run_id=f"r{i}", status="ok"))


@pytest.mark.parametrize("held", [0, 1], ids=["emptied", "short"])
async def test_a_template_not_holding_its_bundles_profile_runs_is_built_again(
    platform, monkeypatch, tmp_path, held
):
    """REQ-2052: a template is ready only when it holds what its bundle carries. One whose profile
    result relations were emptied was served as built on cloud-dev, and every sandbox of the
    profiling demo had an empty run history."""
    demo = _profiled(tmp_path, rows=2)
    await _built(platform, demo)
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    monkeypatch.setattr("provisa.api.admin.orgs_router._pool", lambda: platform)
    await _holding(platform, demo, rows=2)
    assert await demo_templates.warm_ready(platform, demo) is True
    await _holding(platform, demo, rows=held)
    assert await demo_templates.warm_ready(platform, demo) is False
    assert await demo_templates.unheld_profile_runs(demo) == [
        f"customers_309_profile_runs holds {held} rows of the 2 carried"
    ]


async def test_a_template_missing_a_carried_relation_is_built_again(
    platform, monkeypatch, tmp_path
):
    from sqlalchemy import text

    from provisa.core.environments import org_schema

    demo = _profiled(tmp_path, rows=2)
    await _built(platform, demo)
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    monkeypatch.setattr("provisa.api.admin.orgs_router._pool", lambda: platform)
    await _holding(platform, demo, rows=2)
    async with platform.acquire() as conn:
        await conn.execute_core(
            text(f'DROP TABLE "{org_schema(demo.template_org)}".customers_309_profile_runs')
        )
    assert await demo_templates.unheld_profile_runs(demo) == [
        "customers_309_profile_runs is not there"
    ]
    assert await demo_templates.warm_ready(platform, demo) is False
