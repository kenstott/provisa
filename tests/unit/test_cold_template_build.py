# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e1f7a-58d2-4b0e-9a61-0f2d7c4e8b15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo template's first build after a reset (REQ-2006).

Its schema is created a moment before its runtime's first build, and holds none of the built-in
views: they go as one script, as a sandbox's do, where one transaction a view was the bulk of the
20-89 s its meta domain took on cloud. And its Test (synthetic) environment, created from the prod
just built, is seeded once and cloned: on cloud the profiling template's was seeded whole twice,
39.0 s and then 16.8 s."""

# Requirements: REQ-2006

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from provisa.api import startup_seed

_DOMAINS = ("_seed_meta_domain", "_seed_ontology_domain", "_seed_ops_domain")
_OTHER_STEPS = (
    "_seed_tag_param_values",
    "_seed_ops_pg",
    "_ensure_ops_steward_grant",
    "_seed_meta_relationships",
    "_compute_and_store_clusters",
    "_seed_catalog_client",
)


class _Conn:
    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    async def upsert(self, *args, **kwargs):
        self.sent.append("upsert")

    async def execute(self, sql: str):
        self.sent.append(sql)
        return ""

    async def execute_core(self, stmt):
        return SimpleNamespace(scalar=lambda: 0)

    @asynccontextmanager
    async def advisory_lock(self, key: int):
        self.sent.append("lock")
        yield self


class _Db:
    dialect = "postgresql"

    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    @asynccontextmanager
    async def acquire(self):
        yield _Conn(self.sent)


def _seed(monkeypatch, *, provisioned: bool) -> tuple[list[str], list[str], dict[str, bool]]:
    """What reached the connection, each view made one by one, and what each domain seed was told
    of its views."""
    sent: list[str] = []
    one_by_one: list[str] = []
    told: dict[str, bool] = {}

    async def _nothing(*args, **kwargs):
        return None

    def _domain(name: str):
        async def _ran(conn, *, org_id, env, views_made=False):
            told[name] = views_made

        return _ran

    for name in _OTHER_STEPS:
        monkeypatch.setattr(startup_seed, name, _nothing)
    for name in _DOMAINS:
        monkeypatch.setattr(startup_seed, name, _domain(name))

    async def _view(conn, ddl: str) -> None:
        one_by_one.append(ddl)

    monkeypatch.setattr(startup_seed, "_seed_view", _view)
    monkeypatch.setattr(startup_seed._source_repo, "remove_where", _nothing)
    monkeypatch.setattr(
        startup_seed,
        "state",
        SimpleNamespace(
            model_db=_Db(sent), federation_engine=SimpleNamespace(name="duckdb"), org_id="default"
        ),
    )
    asyncio.run(
        startup_seed._seed_built_in_sources(
            "h", 5432, "d", "u", org_id="sbxtplprofiling", provisioned=provisioned
        )
    )
    return sent, one_by_one, told


def test_a_provisioned_schemas_seed_makes_every_built_in_view_in_one_script(monkeypatch):
    sent, one_by_one, told = _seed(monkeypatch, provisioned=True)
    scripts = [s for s in sent if s not in ("lock", "upsert")]
    assert scripts == [startup_seed.views_script(startup_seed._built_in_view_ddl())]
    assert not one_by_one
    # Each domain is still registered, its views already there.
    assert told == dict.fromkeys(_DOMAINS, True)


def test_a_schema_not_provisioned_here_has_its_domains_make_their_views(monkeypatch):
    sent, _one_by_one, told = _seed(monkeypatch, provisioned=False)
    assert [s for s in sent if s not in ("lock", "upsert")] == []
    assert told == dict.fromkeys(_DOMAINS, False)


@pytest.mark.parametrize("views_made", [True, False])
def test_the_meta_domain_makes_its_views_unless_they_are_made(monkeypatch, views_made):
    made: list[str] = []

    async def _view(conn, ddl):
        made.append(ddl)

    class _Stop(Exception):
        pass

    async def _stop(*args, **kwargs):
        raise _Stop

    monkeypatch.setattr(startup_seed, "_seed_view", _view)
    monkeypatch.setattr(startup_seed, "_drop_sibling_environment_registrations", _stop)
    with pytest.raises(_Stop):
        asyncio.run(startup_seed._seed_meta_domain(object(), org_id="acme", views_made=views_made))
    assert made == ([] if views_made else list(startup_seed._META_TABLE_VIEWS.values()))


@pytest.mark.parametrize("exists", [False, True])
def test_provisioning_says_whether_it_made_the_schema(monkeypatch, exists):
    """An org provisioned again over the schema it has (a template a stopped start left
    provisioning, an org whose provisioning failed) may hold views of an earlier release."""
    import provisa.api.admin.orgs_router as mod
    from provisa.api import app

    asked: list[str] = []
    built: list[dict] = []

    class _Built(Exception):
        pass

    async def _exists(pool, schema):
        asked.append(schema)
        return exists

    async def _nothing(*args, **kwargs):
        return None

    async def _flags(org_id):
        return SimpleNamespace(shard="")

    async def _build(org_id, **kwargs):
        built.append(kwargs)
        raise _Built

    class _Registry:
        async def rebuild(self, key, build):
            return await build(key)

    class _Pool:
        dialect = "postgresql"

        @asynccontextmanager
        async def acquire(self):
            yield SimpleNamespace(execute_core=_nothing)

    monkeypatch.setattr(mod, "_pool", lambda: _Pool())
    monkeypatch.setattr(mod, "_admin_pool", lambda: _Pool())
    monkeypatch.setattr("provisa.core.org_provisioning.schema_exists", _exists)
    monkeypatch.setattr("provisa.core.org_provisioning.provision_org", _nothing)
    monkeypatch.setattr("provisa.core.env_store.ensure_prod", _nothing)
    monkeypatch.setattr(app, "_read_org_flags", _flags)
    monkeypatch.setattr(app, "build_org_runtime", _build)
    monkeypatch.setattr(app.state, "org_registry", _Registry())
    asyncio.run(mod._provision_org_task("sbxtplprofiling", True, None, False))
    assert asked == ["org_sbxtplprofiling"]
    assert [b["provisioned"] for b in built] == [not exists]


def test_a_template_environment_created_from_its_prod_is_landed_cloned(monkeypatch, tmp_path):
    from provisa.api import demo_synthetic

    store, record = tmp_path / "synthetic.duckdb", tmp_path / "synthetic.json"
    record.write_text(json.dumps({"environments": {"synthetic": {}, "kept": {}}}))
    landed: list[tuple[str, bool]] = []

    async def _adopt(org_id, env, store, held, *, cloned):
        landed.append((env, cloned))

    monkeypatch.setattr(demo_synthetic, "seed_paths", lambda _directory: (store, record))
    monkeypatch.setattr(demo_synthetic, "_adopt", _adopt)
    asyncio.run(
        demo_synthetic.prepare(
            "sbxtplprofiling",
            tmp_path,
            ["synthetic", "kept"],
            exporting=False,
            created=["synthetic"],
        )
    )
    assert landed == [("synthetic", True), ("kept", False)]


@pytest.mark.parametrize("cloned", [True, False])
def test_a_landed_environments_runtime_is_built_again_cloned_once_bound(monkeypatch, cloned):
    """Its sources bound, its runtime is built again here, cloned: the build before seeded (or
    cloned) its model a moment ago. Left to the next ask, that build seeded it whole again."""
    from provisa.api import app, demo_synthetic
    from provisa.federation import materialize_broker
    from provisa.synthetic import env_model

    done: list[tuple] = []

    class _Conn:
        @asynccontextmanager
        async def transaction(self):
            yield

        async def execute_core(self, stmt):
            return SimpleNamespace(fetchall=lambda: [])

    class _Db:
        @asynccontextmanager
        async def acquire(self):
            yield _Conn()

    async def _model_db(org_id, env, *, cloned=False):
        done.append(("build", env, cloned))
        return _Db()

    async def _bind(state, org_id, env):
        done.append(("bind", env))

    async def _set_data(admin, org_id, env, **kwargs):
        done.append(("ready", env))

    monkeypatch.setattr(materialize_broker, "copy_schema", lambda *args: None)
    monkeypatch.setattr("provisa.api.demo_replicas._store_path", lambda org: f"/stores/{org}")
    monkeypatch.setattr(demo_synthetic, "_model_db", _model_db)
    monkeypatch.setattr(demo_synthetic, "_admin", object)
    monkeypatch.setattr(env_model, "bind_generated", _bind)
    monkeypatch.setattr("provisa.core.env_store.set_data", _set_data)
    monkeypatch.setattr(
        app.state,
        "org_registry",
        SimpleNamespace(invalidate=lambda key: done.append(("invalidate", key))),
    )
    records = {table.name: [] for table in demo_synthetic._record_tables()}
    asyncio.run(
        demo_synthetic._land("sbxtplprofiling", "synthetic", "/s", "from", records, cloned=cloned)
    )
    assert done == [
        ("build", "synthetic", cloned),
        ("bind", "synthetic"),
        ("invalidate", "sbxtplprofiling_env_synthetic"),
        ("build", "synthetic", True),
        ("ready", "synthetic"),
    ]
