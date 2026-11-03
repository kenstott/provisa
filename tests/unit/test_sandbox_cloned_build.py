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

    async def upsert(self, *args, **kwargs):
        self.sent.append("upsert")

    async def execute_core(self, stmt):
        self.sent.append("execute_core")
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


def _seed(monkeypatch, *, cloned: bool) -> tuple[list[str], list[str]]:
    """The seed's statements and steps for org ``acme``: what reached the connection directly, and
    the steps run (a view by its DDL's first line)."""
    sent: list[str] = []
    steps: list[str] = []

    def _step(name: str):
        async def _ran(*args, **kwargs):
            steps.append(name)

        return _ran

    for name in (*_ROW_STEPS, "_seed_catalog_client"):
        monkeypatch.setattr(startup_seed, name, _step(name))

    async def _view(conn, ddl: str) -> None:
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
        f"view {ddl.strip().splitlines()[0]}"
        for ddl in (
            *startup_seed._META_TABLE_VIEWS.values(),
            *startup_seed.ONTOLOGY_TABLE_VIEWS.values(),
            *startup_seed._OPS_LOG_TABLE_VIEWS.values(),
            startup_seed._ops_table_usage_ddl(),
            *startup_seed._OPS_REPORT_VIEWS.values(),
        )
    ]


def test_a_cloned_orgs_seed_makes_every_built_in_view_and_the_catalog_clients_grants(monkeypatch):
    _sent, steps = _seed(monkeypatch, cloned=True)
    assert steps == [*_every_view(), "_seed_catalog_client"]


def test_a_cloned_orgs_seed_writes_none_of_the_rows_its_clone_carried(monkeypatch):
    sent, steps = _seed(monkeypatch, cloned=True)
    assert not set(steps) & {*_ROW_STEPS, "remove_where"}
    # The one statement it sends itself is the lock its schema's seed is held under (REQ-1900).
    assert sent == ["lock"]


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
        return None

    async def _build(org_id, *, storage_url=None, model_cloned=False):
        asked.append(model_cloned)

    monkeypatch.setattr(app, "_region_lane_of", _lane)
    monkeypatch.setattr(app, "build_org_runtime", _build)
    asyncio.run(demo_sandbox._sandbox_runtime("sbx0aef6d8dbe7d", cloned=True))
    asyncio.run(demo_sandbox._sandbox_runtime("sbx0aef6d8dbe7d"))
    assert asked == [True, False]
