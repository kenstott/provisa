# Copyright (c) 2026 Kenneth Stott
# Canary: 5f0f3a52-2f9e-4c44-9a3e-1b0b6c3f8e21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox's first runtime build does not seed again what its clone carried (REQ-2006).

A sandbox's schema is provisioned and its model cloned from its template a moment before its
runtime's first build: every row the built-in seed writes -- the meta, ontology and ops tables'
registrations, about 270 tables and 2,700 columns -- arrives with the clone, rebased onto its own
schema. That build makes only what a schema holds of its own: the built-in views, and the catalog
client's grants on them. Each statement went to a control plane on another machine."""

# Requirements: REQ-2006

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from provisa.api import startup_seed

_ROW_STEPS = (
    "_seed_tag_param_values",
    "_seed_meta_domain",
    "_seed_ontology_domain",
    "_seed_ops_pg",
    "_seed_ops_domain",
    "_ensure_ops_steward_grant",
    "_seed_meta_relationships",
    "_compute_and_store_clusters",
)


class _Conn:
    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    async def upsert(self, *_a, **_kw):
        del _a, _kw
        self.sent.append("upsert")

    async def execute(self, sql: str):
        self.sent.append(sql)
        return ""

    async def execute_core(self, _stmt):
        del _stmt
        self.sent.append("execute_core")
        return SimpleNamespace(scalar=lambda: 0)

    @asynccontextmanager
    async def advisory_lock(self, _key: int):
        del _key
        self.sent.append("lock")
        yield self


class _Db:
    dialect = "postgresql"

    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    @asynccontextmanager
    async def acquire(self):
        yield _Conn(self.sent)


def _seed(monkeypatch, *, cloned: bool) -> tuple[list[str], list[str]]:
    """The seed's statements and steps for org ``acme``: what reached the connection directly, and
    the steps run (a view by its DDL's first line)."""
    sent: list[str] = []
    steps: list[str] = []

    def _step(name: str):
        async def _ran(*args, **kwargs):
            del args, kwargs
            steps.append(name)

        return _ran

    for name in (*_ROW_STEPS, "_seed_catalog_client"):
        monkeypatch.setattr(startup_seed, name, _step(name))

    async def _view(conn, ddl: str) -> None:
        del conn
        steps.append(f"view {ddl.strip().splitlines()[0]}")

    monkeypatch.setattr(startup_seed, "_seed_view", _view)
    monkeypatch.setattr(startup_seed._source_repo, "remove_where", _step("remove_where"))
    monkeypatch.setattr(
        startup_seed,
        "state",
        SimpleNamespace(
            model_db=_Db(sent), federation_engine=SimpleNamespace(name="duckdb"), org_id="default"
        ),
    )
    asyncio.run(
        startup_seed._seed_built_in_sources("h", 5432, "d", "u", org_id="acme", cloned=cloned)
    )
    return sent, steps


def _every_view() -> list[str]:
    return [
        *startup_seed._META_TABLE_VIEWS.values(),
        *startup_seed.ONTOLOGY_TABLE_VIEWS.values(),
        *startup_seed._OPS_LOG_TABLE_VIEWS.values(),
        startup_seed._ops_table_usage_ddl(),
        *startup_seed._OPS_REPORT_VIEWS.values(),
    ]


def test_a_cloned_orgs_seed_makes_every_built_in_view_in_one_script_then_the_grants(monkeypatch):
    """Every built-in view, in the order the seed makes them, sent as one script: one round trip
    where one transaction a view was about 800 (9 s of an open on the hosted tier)."""
    sent, steps = _seed(monkeypatch, cloned=True)
    assert steps == ["_seed_catalog_client"]
    scripts = [s for s in sent if s != "lock"]
    assert len(scripts) == 1
    script = scripts[0]
    at = -1
    for ddl in _every_view():
        found = script.find(ddl.strip().removesuffix(";"), at + 1)
        assert found > at, ddl.strip().splitlines()[0]
        at = found


def test_a_cloned_orgs_seed_writes_none_of_the_rows_its_clone_carried(monkeypatch):
    sent, steps = _seed(monkeypatch, cloned=True)
    assert not set(steps) & {*_ROW_STEPS, "remove_where"}
    # What it sends itself: the lock its schema's seed is held under (REQ-1900), and its views.
    assert sent[0] == "lock" and len(sent) == 2


def test_an_org_not_cloned_is_seeded_whole(monkeypatch):
    sent, steps = _seed(monkeypatch, cloned=False)
    for name in ("_seed_meta_domain", "_seed_ontology_domain", "_seed_ops_domain"):
        assert name in steps
    assert sent.count("upsert") == 3  # its provisa-admin, provisa-otel and __derived__ rows


def test_a_sandbox_runtime_built_cloned_asks_for_the_cloned_build_and_none_other_does(monkeypatch):
    """_build hands the registry _sandbox_runtime with cloned=True, its first build; any other
    build of the sandbox's runtime (a request after a restart, a model change) is an ordinary one."""
    from provisa.api import app, demo_sandbox

    asked: list[bool] = []

    async def _lane(org_id, env):
        del org_id, env
        return None

    async def _build(org_id, *, storage_url=None, model_cloned=False):
        del org_id, storage_url
        asked.append(model_cloned)

    monkeypatch.setattr(app, "_region_lane_of", _lane)
    monkeypatch.setattr(app, "build_org_runtime", _build)
    asyncio.run(demo_sandbox._sandbox_runtime("sbx0aef6d8dbe7d", cloned=True))
    asyncio.run(demo_sandbox._sandbox_runtime("sbx0aef6d8dbe7d"))
    assert asked == [True, False]


def test_a_sandbox_environments_rows_are_landed_on_a_runtime_built_cloned(monkeypatch):
    """Its Test (synthetic) environment was created from its prod a moment before its rows are
    landed, every built-in row carried with that copy: the runtime they are landed through is
    built cloned. On cloud the profiling open spent 70.7 s in "its environments' rows", the
    environment's runtime seeding its meta domain alone for 27.4 s."""
    from provisa.api import demo_synthetic
    from provisa.federation import materialize_broker

    class _Landed(Exception):
        pass

    asked: list[tuple[str, str, bool]] = []

    async def _model_db(org_id, env, *, cloned=False):
        asked.append((org_id, env, cloned))
        raise _Landed

    def _copy_schema(*args):
        del args

    monkeypatch.setattr(materialize_broker, "copy_schema", _copy_schema)
    monkeypatch.setattr("provisa.api.demo_replicas._store_path", lambda org: f"/stores/{org}")
    monkeypatch.setattr(demo_synthetic, "_model_db", _model_db)
    for cloned in (True, False):
        try:
            asyncio.run(
                demo_synthetic._land(
                    "sbx0aef6d8dbe7d", "synthetic", "/s", "from", {}, cloned=cloned
                )
            )
        except _Landed:
            pass
    assert asked == [
        ("sbx0aef6d8dbe7d", "synthetic", True),
        ("sbx0aef6d8dbe7d", "synthetic", False),
    ]


def test_an_environment_asked_for_cloned_is_built_cloned(monkeypatch):
    """ensure_org_runtime(model_cloned=True) builds the environment's runtime on its lane as a
    cloned build (build_org_runtime's model_cloned); an ordinary ask builds an ordinary one."""
    from provisa.api import app, redaction_load

    built: list[tuple[str, str, bool]] = []

    async def _none(*args, **kwargs):
        del args, kwargs
        return None

    async def _flags(org_id):
        del org_id
        return app.OrgLane(False, False, None, None, None, "", None)

    async def _env(admin_db, org_id, env):
        del admin_db, org_id, env
        return {"expires_at": None, "data_mode": "test_synthetic", "mutation_handling": "refused"}

    async def _build(org_id, *, env, model_cloned=False, **kwargs):
        del kwargs
        built.append((org_id, env, model_cloned))
        return SimpleNamespace()

    class _Registry:
        async def get_or_build(self, key, build):
            return await build(key)

    monkeypatch.setattr(app, "ensure_org_encryption", _none)
    monkeypatch.setattr(app, "_read_org_flags", _flags)
    monkeypatch.setattr(app, "_region_lane_of", _none)
    monkeypatch.setattr(app, "build_org_runtime", _build)
    monkeypatch.setattr("provisa.core.env_store.get_env", _env)
    monkeypatch.setattr(redaction_load, "ensure_loaded", _none)
    monkeypatch.setattr(app.state, "org_registry", _Registry())
    monkeypatch.setattr(app.state, "admin_db", object())
    asyncio.run(app.ensure_org_runtime("sbx0aef6d8dbe7d", "synthetic", model_cloned=True))
    asyncio.run(app.ensure_org_runtime("sbx0aef6d8dbe7d", "synthetic"))
    assert built == [
        ("sbx0aef6d8dbe7d", "synthetic", True),
        ("sbx0aef6d8dbe7d", "synthetic", False),
    ]


def test_cloned_env_schema_copies_source_catalogs_from_prod(monkeypatch):
    """Bug R508e/J1157b: _install_cloned_env_schema left source_catalogs empty on the
    cloned env's runtime — the pgwire pipeline guard's _known_cats_pgwire was then empty
    too, so a physical catalog reference in any query hit "unknown catalog" (REQ-2006)."""
    import asyncio
    from contextlib import asynccontextmanager
    from provisa.api import app
    from provisa.api.org_runtime import OrgRuntime

    prod_rt = OrgRuntime(org_id="sbxtestorg")
    prod_rt.source_catalogs = {"zoo-db": "org_sbxtestorg__zoo_db"}
    prod_rt.source_types = {"zoo-db": "sqlite"}
    prod_rt.source_dialects = {"zoo-db": "duckdb"}
    prod_rt.surfaces = {}
    # minimal fields _install_cloned_env_schema assigns from prod_rt
    for _field in (
        "role_build_inputs",
        "tables",
        "relationships",
        "roles",
        "role_chains",
        "draft_table_ids",
        "view_context",
        "view_path_map",
        "names_table",
        "view_sql_map",
        "mv_registry",
        "bitemporal_view_reads",
        "stored_view_reads",
        "class_tables",
        "refused_classes",
        "class_bodies",
        "table_cache",
        "pg_notify_tables",
        "table_watermarks",
        "user_attributes",
        "masking_rules",
        "masked_columns",
        "tracked_functions",
        "tracked_webhooks",
        "column_transforms",
        "undefined_commands",
        "replica_routes",
        "metrics",
        "schema_build_cache",
        "api_sources",
        "api_endpoints",
        "table_approval_hooks",
        "source_approval_hooks",
        "pg_enum_types",
        "pg_source_enums",
        "graphql_remote_sources",
        "openapi_specs",
        "grpc_remote_sources",
        "ingest_engines",
        "ingest_tables",
        "websocket_sources",
        "rss_sources",
        "cdc_sources",
        "source_cache",
        "source_allowed_domains",
        "source_federation_hints",
        "kafka_windows",
        "kafka_bootstrap",
        "kafka_declared",
        "kafka_table_configs",
        "kafka_table_physical",
        "model_stamp",
    ):
        if not hasattr(prod_rt, _field):
            setattr(prod_rt, _field, None)

    cloned_rt = OrgRuntime(org_id="sbxtestorg", env="dev")

    class _MockConn:
        async def execute_core(self, stmt):
            del stmt
            return SimpleNamespace(fetchall=lambda: [])

    @asynccontextmanager
    async def _acquire():
        yield _MockConn()

    class _MockState:
        schema_version = 0
        model_db = SimpleNamespace(acquire=_acquire)

        def _active_runtime(self):
            return cloned_rt

    monkeypatch.setattr(app, "state", _MockState())

    async def _unbound(_conn):
        del _conn
        return frozenset()

    monkeypatch.setattr(app, "_unbound_sources", _unbound)

    import provisa.grpc.field_numbering as _fn
    import provisa.api.admin.lineage_router as _lr

    async def _load_fn(_conn):
        del _conn
        return None

    async def _persist_fn(_conn, _fn):
        del _conn, _fn

    async def _lineage(_conn, _state):
        del _conn, _state

    monkeypatch.setattr(_fn, "load_field_number_allocator", _load_fn)
    monkeypatch.setattr(_fn, "persist_field_numbers", _persist_fn)
    monkeypatch.setattr(_lr, "write_lineage_edges", _lineage)

    asyncio.run(app._install_cloned_env_schema(prod_rt))

    assert cloned_rt.source_catalogs == {"zoo-db": "org_sbxtestorg__zoo_db"}, (
        "_install_cloned_env_schema must copy source_catalogs from prod_rt; "
        f"got {cloned_rt.source_catalogs!r}"
    )


def _prod_rt_for_clone_test():
    """Return a minimal prod_rt for _install_cloned_env_schema unit tests.

    Sets prod_rt.masking_rules to a sentinel that has no fake generators, mirroring
    a real PROD build (data_mode=None → faking=False in _load_masking_rules).
    """
    from provisa.api.org_runtime import OrgRuntime

    prod_rt = OrgRuntime(org_id="sbxtestorg")
    prod_rt.source_catalogs = {}
    prod_rt.source_types = {}
    prod_rt.source_dialects = {}
    prod_rt.surfaces = {}
    prod_rt.masking_rules = {"prod_rule": "no_fakes"}  # PROD built with faking=False
    prod_rt.masked_columns = []
    prod_rt.roles = {}
    prod_rt.role_chains = {}
    prod_rt.schema_build_cache = {"column_types": {}}
    for _field in (
        "role_build_inputs",
        "tables",
        "relationships",
        "draft_table_ids",
        "view_context",
        "view_path_map",
        "names_table",
        "view_sql_map",
        "mv_registry",
        "bitemporal_view_reads",
        "stored_view_reads",
        "class_tables",
        "refused_classes",
        "class_bodies",
        "table_cache",
        "pg_notify_tables",
        "table_watermarks",
        "user_attributes",
        "tracked_functions",
        "tracked_webhooks",
        "column_transforms",
        "undefined_commands",
        "replica_routes",
        "metrics",
        "api_sources",
        "api_endpoints",
        "table_approval_hooks",
        "source_approval_hooks",
        "pg_enum_types",
        "pg_source_enums",
        "graphql_remote_sources",
        "openapi_specs",
        "grpc_remote_sources",
        "ingest_engines",
        "ingest_tables",
        "websocket_sources",
        "rss_sources",
        "cdc_sources",
        "source_cache",
        "source_allowed_domains",
        "source_federation_hints",
        "kafka_windows",
        "kafka_bootstrap",
        "kafka_declared",
        "kafka_table_configs",
        "kafka_table_physical",
        "model_stamp",
    ):
        if not hasattr(prod_rt, _field):
            setattr(prod_rt, _field, None)
    return prod_rt


def test_cloned_env_test_fake_reloads_masking_rules(monkeypatch):
    """Bug R508e3: _install_cloned_env_schema copied prod_rt.masking_rules unchanged to a
    test_fake cloned env.  PROD's masking_rules have no fake generators (built with faking=False,
    data_mode=None).  A test_fake env must call _load_masking_rules with faking=True so queries
    return synthetic fakes, not production rows (REQ-2053 / REQ-1494)."""
    import asyncio
    from contextlib import asynccontextmanager
    from provisa.api import app
    from provisa.api.org_runtime import OrgRuntime

    prod_rt = _prod_rt_for_clone_test()
    cloned_rt = OrgRuntime(org_id="sbxtestorg", env="dev", data_mode="test_fake")

    class _MockConn:
        async def execute_core(self, stmt):
            del stmt
            return SimpleNamespace(fetchall=lambda: [])

    @asynccontextmanager
    async def _acquire():
        yield _MockConn()

    class _MockState:
        schema_version = 0
        model_db = SimpleNamespace(acquire=_acquire)

        def _active_runtime(self):
            return cloned_rt

    monkeypatch.setattr(app, "state", _MockState())

    masking_rules_calls: list[bool] = []
    measure_model_calls: list[bool] = []

    async def _mock_load_masking_rules(conn, col_types, roles, role_chains=None):
        del conn, col_types, roles, role_chains
        masking_rules_calls.append(True)
        # simulate _load_masking_rules writing fake-aware rules (non-PROD value)
        cloned_rt.masking_rules = {"fake_rule": "has_fakes"}
        cloned_rt.masked_columns = []

    monkeypatch.setattr(app, "_load_masking_rules", _mock_load_masking_rules)

    import provisa.fakes.measured as _measured

    async def _mock_measure_model(state_arg):
        del state_arg
        measure_model_calls.append(True)

    monkeypatch.setattr(_measured, "measure_model", _mock_measure_model)

    async def _unbound(_conn):
        del _conn
        return frozenset()

    monkeypatch.setattr(app, "_unbound_sources", _unbound)

    import provisa.grpc.field_numbering as _fn
    import provisa.api.admin.lineage_router as _lr

    async def _load_fn(_conn):
        del _conn
        return None

    async def _persist_fn(_conn, _fn):
        del _conn, _fn

    async def _lineage(_conn, _state):
        del _conn, _state

    monkeypatch.setattr(_fn, "load_field_number_allocator", _load_fn)
    monkeypatch.setattr(_fn, "persist_field_numbers", _persist_fn)
    monkeypatch.setattr(_lr, "write_lineage_edges", _lineage)

    asyncio.run(app._install_cloned_env_schema(prod_rt))

    assert masking_rules_calls, (
        "test_fake cloned env must call _load_masking_rules to build env-specific fake rules; "
        "PROD masking_rules have no fakes (data_mode=None during PROD build) — "
        "copying them leaves the env serving production rows (REQ-2053)"
    )
    assert measure_model_calls, (
        "test_fake cloned env must call measure_model after _load_masking_rules "
        "to populate measured values fake generators need (REQ-1494)"
    )
    assert cloned_rt.masking_rules != {"prod_rule": "no_fakes"}, (
        "test_fake env must NOT use PROD masking_rules; they have no fake generators"
    )


def test_cloned_env_non_test_fake_does_not_reload_masking_rules(monkeypatch):
    """Non-test_fake envs (None / inherit / unbound) inherit PROD masking_rules as-is; only
    test_fake needs a fresh _load_masking_rules call.  Calling it unnecessarily wastes a DB
    round-trip and would override column masks for non-fake modes."""
    import asyncio
    from contextlib import asynccontextmanager
    from provisa.api import app
    from provisa.api.org_runtime import OrgRuntime

    prod_rt = _prod_rt_for_clone_test()
    cloned_rt = OrgRuntime(org_id="sbxtestorg", env="dev", data_mode=None)

    class _MockConn:
        async def execute_core(self, stmt):
            del stmt
            return SimpleNamespace(fetchall=lambda: [])

    @asynccontextmanager
    async def _acquire():
        yield _MockConn()

    class _MockState:
        schema_version = 0
        model_db = SimpleNamespace(acquire=_acquire)

        def _active_runtime(self):
            return cloned_rt

    monkeypatch.setattr(app, "state", _MockState())

    masking_rules_calls: list[bool] = []

    async def _mock_load_masking_rules(conn, col_types, roles, role_chains=None):
        del conn, col_types, roles, role_chains
        masking_rules_calls.append(True)

    monkeypatch.setattr(app, "_load_masking_rules", _mock_load_masking_rules)

    async def _unbound(_conn):
        del _conn
        return frozenset()

    monkeypatch.setattr(app, "_unbound_sources", _unbound)

    import provisa.grpc.field_numbering as _fn
    import provisa.api.admin.lineage_router as _lr

    async def _load_fn(_conn):
        del _conn
        return None

    async def _persist_fn(_conn, _fn):
        del _conn, _fn

    async def _lineage(_conn, _state):
        del _conn, _state

    monkeypatch.setattr(_fn, "load_field_number_allocator", _load_fn)
    monkeypatch.setattr(_fn, "persist_field_numbers", _persist_fn)
    monkeypatch.setattr(_lr, "write_lineage_edges", _lineage)

    asyncio.run(app._install_cloned_env_schema(prod_rt))

    assert not masking_rules_calls, (
        "non-test_fake cloned env must NOT call _load_masking_rules; "
        "PROD masking_rules are correct for inherit/unbound/None modes"
    )
