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

# Requirements: REQ-2006, REQ-1942

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


# ---------------------------------------------------------------------------
# R508g: clone-path synthetic-env bug regression tests
# Bugs A, B, C + R508e fix verified without containers.
# ---------------------------------------------------------------------------


class _Row:
    """Thin row double: ``._mapping`` dict."""

    def __init__(self, **values: object) -> None:
        self._mapping = values


class _ConnClone:
    """DB double for _install_cloned_env_schema: answers SELECT sources and
    SELECT … WHERE binding=SYNTHETIC."""

    def __init__(self, source_rows: list[_Row]) -> None:
        self._rows = source_rows

    async def execute_core(self, stmt):
        # Both queries in _install_cloned_env_schema read from the sources table.
        # Distinguish by presence of a WHERE clause on the binding column.
        try:
            whereclause = stmt.whereclause
        except AttributeError:
            whereclause = None
        if whereclause is not None:
            # synthetic_sources() does ``for sid, where in bound.fetchall()``:
            # return (id, synthetic) tuples, not _Row objects.
            from provisa.core.env_classes import SYNTHETIC

            synthetic = [
                (r._mapping["id"], r._mapping.get("synthetic"))
                for r in self._rows
                if r._mapping.get("binding") == SYNTHETIC
            ]
            return SimpleNamespace(fetchall=lambda: synthetic)
        return SimpleNamespace(fetchall=lambda: list(self._rows))


class _DbClone:
    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows

    @asynccontextmanager
    async def acquire(self):
        yield _ConnClone(self._rows)


#: Every field of an env runtime derived from its sources' bindings: what a full build sets from
#: the env's own ``sources`` rows (types, catalogs, connections) and what it compiles from them.
_BINDING_DEPENDENT = (
    "surfaces",
    "role_build_inputs",
    "source_catalogs",
    "source_types",
    "source_dialects",
    "source_cache",
    "source_federation_hints",
    "source_allowed_domains",
    "tables",
    "view_sql_map",
    "pg_notify_tables",
    "api_sources",
    "api_endpoints",
    "graphql_remote_sources",
    "openapi_specs",
    "replica_routes",
    "unbound_sources",
    "source_binding_env",
    "runtime_sources",
)


def _run_install_cloned(
    monkeypatch, source_rows: list[_Row], install_surfaces=lambda rt, surfs: None
) -> SimpleNamespace:
    """Run _install_cloned_env_schema with minimal mocks; return _rt after the call.

    ``_rt.full_builds`` records each full build run (its ``converge``); the stubbed full build
    sets every binding-dependent field to ``("full build", field)``, what it derives itself."""
    from provisa.api import app, app_loaders

    # _rt accumulates every attribute assigned inside _install_cloned_env_schema.
    # model_db must be set here: state.model_db is a property (AppState) that delegates to
    # _active_runtime().model_db; after _active_runtime is patched to return this SimpleNamespace,
    # the assert / async with calls need model_db on it directly (the property getter follows
    # the patched _active_runtime, not the deployment runtime the monkeypatch setter wrote to).
    _rt = SimpleNamespace(
        env="synthetic", model_db=_DbClone(source_rows), full_builds=[], data_mode=None
    )
    # prod_rt supplies the PROD-compiled state; keep it simple.
    prod_rt = SimpleNamespace(
        surfaces={},
        role_build_inputs=None,
        source_catalogs={"__derived__": "mem", "sales_pg": "sales_pg_cat"},
        source_types={"sales_pg": "postgresql"},
        source_dialects={"sales_pg": "postgresql"},
        tables=[
            {
                "source_id": "petstore",
                "schema_name": "api",
                "table_name": "get_pet_by_id",
                "columns": [
                    {"column_name": "id", "native_filter_type": None, "data_type": "integer"},
                    {
                        "column_name": "petId",
                        "native_filter_type": "path_param",
                        "data_type": None,
                    },
                ],
            }
        ],
        relationships=None,
        roles=None,
        role_chains=None,
        draft_table_ids=None,
        view_context=None,
        view_path_map=None,
        names_table=None,
        view_sql_map=None,
        mv_registry=None,
        bitemporal_view_reads=None,
        stored_view_reads=None,
        class_tables=None,
        refused_classes=None,
        class_bodies=None,
        table_cache=None,
        pg_notify_tables=None,
        table_watermarks=None,
        user_attributes=None,
        masking_rules=None,
        masked_columns=None,
        tracked_functions=None,
        tracked_webhooks=None,
        column_transforms=None,
        undefined_commands=None,
        replica_routes=None,
        metrics=None,
        schema_build_cache=None,
        api_sources=None,
        api_endpoints=None,
        table_approval_hooks=None,
        source_approval_hooks=None,
        pg_enum_types=None,
        pg_source_enums=None,
        graphql_remote_sources=None,
        openapi_specs=None,
        grpc_remote_sources=None,
        ingest_engines=None,
        ingest_tables=None,
        websocket_sources=None,
        rss_sources=None,
        cdc_sources=None,
        source_cache=None,
        source_allowed_domains=None,
        source_federation_hints=None,
        kafka_windows=None,
        kafka_bootstrap=None,
        kafka_declared=None,
        kafka_table_configs=None,
        kafka_table_physical=None,
        model_stamp="ms1",
    )

    class _SchemaVersionGuard:
        def __enter__(self):
            return self

        def __exit__(self, *_a: object) -> None:
            pass

    from provisa.federation.replica_address import ReplicaRoutes

    _default_routes = ReplicaRoutes()

    async def _stub_replica_routes(state: object) -> ReplicaRoutes:
        return _default_routes

    monkeypatch.setattr(app, "_schema_version_guard", _SchemaVersionGuard())
    monkeypatch.setattr(app_loaders, "_install_cloned_surfaces", install_surfaces)
    monkeypatch.setattr(app.state, "_active_runtime", lambda: _rt)
    monkeypatch.setattr(app.state, "schema_version", 0)
    # Bug D (R508g2): _install_cloned_env_schema now rebuilds replica_routes via
    # replica_routing.replica_routes(state); stub it out so the existing unit tests that
    # only cover other properties of the clone path are not affected by the new call.
    monkeypatch.setattr("provisa.federation.replica_routing.replica_routes", _stub_replica_routes)

    # Field-number and lineage writes are side-effects not under test; stub them out.
    async def _noop(*_a: object, **_kw: object) -> SimpleNamespace:
        return SimpleNamespace()

    monkeypatch.setattr("provisa.grpc.field_numbering.load_field_number_allocator", _noop)
    monkeypatch.setattr("provisa.grpc.field_numbering.persist_field_numbers", _noop)
    monkeypatch.setattr("provisa.api.admin.lineage_router.write_lineage_edges", _noop)

    async def _full_build(*_a: object, converge: bool = True, **_kw: object) -> None:
        _rt.full_builds.append(converge)
        for field in _BINDING_DEPENDENT:
            setattr(_rt, field, ("full build", field))

    monkeypatch.setattr(app, "_rebuild_schemas", _full_build)
    _rt.prod_rt = prod_rt
    asyncio.run(app._install_cloned_env_schema(prod_rt))
    return _rt


def test_cloned_env_unbound_sources_excludes_synthetic_bindings(monkeypatch):
    """Bug A (R508g): a source bound to the synthetic store after generation is SYNTHETIC, not
    UNBOUND; it must not be among unbound_sources or queries raise 409 (REQ-1942). An env with
    one is built in full (R508g3), whose source reconcile counts UNBOUND alone; an env without one
    is cloned, and the clone counts UNBOUND alone too."""
    from provisa.core.env_classes import COPIED, SYNTHETIC, UNBOUND

    rows = [
        _Row(id="sales-pg", binding=UNBOUND),
        _Row(id="petstore", binding=SYNTHETIC),  # generated → served from store
        _Row(id="admin-db", binding="own"),
    ]
    _rt = _run_install_cloned(monkeypatch, rows)
    assert _rt.full_builds == [False], "an env with a SYNTHETIC-bound source was not built in full"
    assert _rt.unbound_sources == ("full build", "unbound_sources")

    cloned = _run_install_cloned(
        monkeypatch,
        [
            _Row(id="sales-pg", binding=UNBOUND),
            _Row(id="petstore", binding=COPIED),
            _Row(id="admin-db", binding="own"),
        ],
    )
    assert cloned.full_builds == []
    assert cloned.unbound_sources == frozenset({"sales-pg"})


def test_cloned_env_copies_source_catalogs_from_prod(monkeypatch):
    """R508e (4fbab161ec, R508g): the clone path must copy source_catalogs/types/dialects from
    PROD or the federation layer cannot resolve source catalog names (400 unknown catalog)."""
    rows = [_Row(id="sales-pg", binding="own")]
    _rt = _run_install_cloned(monkeypatch, rows)
    assert _rt.source_catalogs == {"__derived__": "mem", "sales_pg": "sales_pg_cat"}, (
        "source_catalogs not copied from PROD; unknown catalog errors would follow"
    )
    assert _rt.source_types == {"sales_pg": "postgresql"}
    assert _rt.source_dialects == {"sales_pg": "postgresql"}
    # R508g3: the env's maps are its own -- a source the env registers or reconciles later
    # writes into them, and PROD's must not move with it.
    for field in ("source_catalogs", "source_types", "source_dialects"):
        assert getattr(_rt, field) is not getattr(_rt.prod_rt, field), (
            f"{field} is PROD's own dict; a write in the env would change PROD's {field}"
        )


def test_cloned_env_applies_as_generated_on_synthetic_bound_tables(monkeypatch):
    """Bug C (R508g): PROD's tables still carry native_filter_type on API columns; an env bound to
    its synthetic store reads them as_generated (REQ-1942). R508g3: that is the full build's
    _rebuild_schemas_impl, which applies as_generated to the env's own tables -- the env is built
    in full, and PROD's tables are not what it serves."""
    from provisa.core.env_classes import SYNTHETIC

    rows = [
        _Row(
            id="petstore",
            binding=SYNTHETIC,
            synthetic={
                "schema": "s",
                "tables": [["api", "get_pet_by_id"]],
                "parameters": {"api.get_pet_by_id": {"petId": "bigint"}},
                "model_type": "openapi",
            },
        )
    ]
    _rt = _run_install_cloned(monkeypatch, rows)
    assert _rt.full_builds == [False]
    assert _rt.tables == ("full build", "tables"), (
        "PROD's tables were installed; the API table would refuse queries with 400 "
        "'requires parameter'"
    )


def test_a_synthetic_bound_env_is_what_a_full_build_of_it_produces(monkeypatch):
    """R508g3 (REQ-1942, REQ-2006): binding a source to the synthetic store changes its type to
    the store's and its tables to their generated forms, and from those a build derives the env's
    catalogs, types and dialects, its compiled surfaces and view SQL, its API sources, its routes,
    its unbound set and its connections. PROD compiled none of them from that binding: an env
    holding PROD's answered list_pets from the live API (5 rows, not the 10 generated) and named
    PROD's catalog 'sales_pg', unknown to it. Every binding-dependent field of a SYNTHETIC-bound
    env is the full build's, none PROD's; the full build runs once, leaving replica convergence
    to the runtime build (converge=False)."""
    from provisa.core.env_classes import COPIED, SYNTHETIC

    installed: list[object] = []
    rows = [
        _Row(id="sales-pg", binding=COPIED),
        _Row(
            id="petstore",
            binding=SYNTHETIC,
            synthetic={"schema": "s", "tables": [], "parameters": {}, "model_type": "openapi"},
        ),
    ]
    _rt = _run_install_cloned(
        monkeypatch, rows, install_surfaces=lambda rt, surfs: installed.append(surfs)
    )
    assert _rt.full_builds == [False]
    assert installed == [], "PROD's compiled surfaces were installed in a SYNTHETIC-bound env"
    for field in _BINDING_DEPENDENT:
        assert getattr(_rt, field) == ("full build", field), (
            f"{field} is not what a full build of the env derives"
        )


def test_walk_registry_derived_source_not_in_catalogs_does_not_crash():
    """Bug B (R508g): __derived__ has no catalog entry in a fresh clone; _attach_tbl must guard
    _NO_REMOTE_SOURCE_IDS before accessing source_catalogs, or it raises KeyError (REQ-1942)."""
    from provisa.federation.native_backend import NativeEngineBackend

    attached: list = []
    backend = NativeEngineBackend(SimpleNamespace(name="duckdb"))
    backend._runtime = SimpleNamespace(attach_source=attached.append)
    derived = SimpleNamespace(id="__derived__", type=SimpleNamespace(value="duckdb"))
    config = SimpleNamespace(
        sources=[derived],
        tables=[
            SimpleNamespace(
                source_id="__derived__",
                schema_name="views",
                table_name="orders_view",
                region=None,
            )
        ],
    )
    state = SimpleNamespace(
        # __derived__ is intentionally absent — this is the pre-R508e clone-path state
        source_catalogs={},
        replica_routes=SimpleNamespace(kept_for_good=set()),
        foreign_regions={},
        tables=[],
        runtime_sources={},
        model_db=None,
    )
    # Before the fix this raised KeyError('__derived__'); after the fix it returns True.
    result = backend._walk_registry(state, config)
    assert result is True
    assert attached == [], "__derived__ must never be attached as a live source"


def test_cloned_env_rebuilds_replica_routes_not_copies_prods(monkeypatch):
    """Bug D (R508g2): _install_cloned_env_schema must rebuild replica_routes for the cloned env
    rather than copying PROD's; otherwise a demo source with replicate=0 (always replicate /
    floored) routes queries to mat_store which is not attached, causing:
        BinderException: Binder Error: Catalog "mat_store" does not exist! (REQ-1942, REQ-2006)

    PROD's replica_routes floors a replicate=0 source to the materialisation store catalog; the
    cloned env's store is the synthetic dataset, not the materialisation store catalog, so its
    replica_routes must be rebuilt from the env's current state, not copied from PROD."""
    import asyncio

    from provisa.api import app, app_loaders
    from provisa.federation.replica_address import ReplicaRoutes

    prod_routes = ReplicaRoutes(synthetic={99: "prod_dataset"})
    env_routes = ReplicaRoutes(synthetic={99: "env_dataset"})

    async def _build_routes(state: object) -> ReplicaRoutes:
        return env_routes

    monkeypatch.setattr("provisa.federation.replica_routing.replica_routes", _build_routes)

    # Build prod_rt with replica_routes set to prod_routes — the value that must NOT be copied.
    prod_rt = SimpleNamespace(
        surfaces={},
        role_build_inputs=None,
        source_catalogs={},
        source_types={},
        source_dialects={},
        tables=[],
        relationships=None,
        roles=None,
        role_chains=None,
        draft_table_ids=None,
        view_context=None,
        view_path_map=None,
        names_table=None,
        view_sql_map=None,
        mv_registry=None,
        bitemporal_view_reads=None,
        stored_view_reads=None,
        class_tables=None,
        refused_classes=None,
        class_bodies=None,
        table_cache=None,
        pg_notify_tables=None,
        table_watermarks=None,
        user_attributes=None,
        masking_rules=None,
        masked_columns=None,
        tracked_functions=None,
        tracked_webhooks=None,
        column_transforms=None,
        undefined_commands=None,
        replica_routes=prod_routes,
        metrics=None,
        schema_build_cache=None,
        api_sources=None,
        api_endpoints=None,
        table_approval_hooks=None,
        source_approval_hooks=None,
        pg_enum_types=None,
        pg_source_enums=None,
        graphql_remote_sources=None,
        openapi_specs=None,
        grpc_remote_sources=None,
        ingest_engines=None,
        ingest_tables=None,
        websocket_sources=None,
        rss_sources=None,
        cdc_sources=None,
        source_cache=None,
        source_allowed_domains=None,
        source_federation_hints=None,
        kafka_windows=None,
        kafka_bootstrap=None,
        kafka_declared=None,
        kafka_table_configs=None,
        kafka_table_physical=None,
        model_stamp="ms2",
    )

    db = _DbClone([_Row(id="zoo-db", binding="own")])
    _rt = SimpleNamespace(env="dev", model_db=db, schema_version=0, data_mode=None)

    class _Guard:
        def __enter__(self) -> "_Guard":
            return self

        def __exit__(self, *_e: object) -> None:
            pass

    async def _noop(*_a: object, **_kw: object) -> SimpleNamespace:
        return SimpleNamespace()

    monkeypatch.setattr(app, "_schema_version_guard", _Guard())
    monkeypatch.setattr(app_loaders, "_install_cloned_surfaces", lambda rt, surfs: None)
    monkeypatch.setattr(app.state, "_active_runtime", lambda: _rt)
    monkeypatch.setattr("provisa.grpc.field_numbering.load_field_number_allocator", _noop)
    monkeypatch.setattr("provisa.grpc.field_numbering.persist_field_numbers", _noop)
    monkeypatch.setattr("provisa.api.admin.lineage_router.write_lineage_edges", _noop)

    asyncio.run(app._install_cloned_env_schema(prod_rt))

    assert _rt.replica_routes is not prod_routes, (
        "replica_routes was copied from PROD; a replicate=0 source would route to "
        "mat_store which is not attached in the cloned env, raising Catalog 'mat_store' "
        "does not exist"
    )
    assert _rt.replica_routes is env_routes, (
        "replica_routes was not rebuilt for the cloned env; it must call replica_routes(state)"
    )


def test_cloned_env_schema_copies_source_catalogs_from_prod(monkeypatch):
    """Bug R508e/J1157b: _install_cloned_env_schema left source_catalogs empty on the
    cloned env's runtime — the pgwire pipeline guard's _known_cats_pgwire was then empty
    too, so a physical catalog reference in any query hit "unknown catalog" (REQ-2006)."""

    async def _no_routes(state):  # R508g: the clone rebuilds its routes; not what this test checks
        return None

    monkeypatch.setattr("provisa.federation.replica_routing.replica_routes", _no_routes)
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

    async def _no_routes(state):  # R508g: the clone rebuilds its routes; not what this test checks
        return None

    monkeypatch.setattr("provisa.federation.replica_routing.replica_routes", _no_routes)
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

    async def _no_routes(state):  # R508g: the clone rebuilds its routes; not what this test checks
        return None

    monkeypatch.setattr("provisa.federation.replica_routing.replica_routes", _no_routes)
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
