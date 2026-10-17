# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two orgs on one native engine, each with a source of the same id and a table of the same
name in a store of its own (REQ-1266, REQ-1529): each org's statement reads its own store,
whichever org's walk ran last, and each org's walk stands for that org until its registry
changes."""

# Requirements: REQ-1266, REQ-1529, REQ-825

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from provisa.core.request_context import reset_current_org, set_current_org
from provisa.federation.engine import build_engine
from provisa.federation.replica_address import ReplicaRoutes

pytestmark = pytest.mark.unbound

_DEPLOYMENT = "default"
_OWN = "sbxlake"  # an org on the engine as its own (a demo sandbox)


def _store(tmp_path, name: str, rows: int) -> str:
    path = tmp_path / f"{name}.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO orders VALUES (?)", [(i,) for i in range(rows)])
    conn.commit()
    conn.close()
    return str(path)


# The deployment's configuration file: one object, whichever org is bound; each org's model is
# its own registry (runtime_sources and tables, read from its store).
_CONFIG = SimpleNamespace(sources=[], tables=[])


def _org_state(org: str, store: str) -> SimpleNamespace:
    """What the state shows a statement bound to ``org``: its own registry, objects of its own."""
    return SimpleNamespace(
        org_id=_DEPLOYMENT,
        active_isolated_org=_OWN if org == _OWN else None,
        config=_CONFIG,
        runtime_sources={
            "lake": {
                "type": "sqlite",
                "path": store,
                "region": None,
                "password_ref": None,
                "replicate": None,
                "load_protected": None,
                "max_live_concurrency": None,
            }
        },
        tables=[
            {
                "source_id": "lake",
                "schema_name": "main",
                "table_name": "orders",
                "region": None,
                "cache_ttl": None,
            }
        ],
        model_db=None,
        replica_routes=ReplicaRoutes(),
        source_catalogs={"lake": f"org_{org}__lake"},
    )


@pytest.fixture
def engine(tmp_path, monkeypatch):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    monkeypatch.setattr("provisa.core.operator_floor.floor_setting", lambda _src: None)
    engine = build_engine("duckdb")
    store = f"duckdb:///{tmp_path / 'mat.duckdb'}"
    engine.backend._runtime = DuckDBFederationRuntime(materialize_dsn=store)
    yield engine
    engine.backend.close(None)


def _count(engine, org: str, state) -> int:
    token = set_current_org(org)
    try:
        runtime = engine.backend._runtime_for(state)
        return runtime._con.execute(
            f'SELECT COUNT(*) FROM "org_{org}__lake"."main"."orders"'
        ).fetchone()[0]
    finally:
        reset_current_org(token)


def test_each_orgs_statement_reads_its_own_store_whichever_walked_last(engine, tmp_path):
    deployment = _org_state(_DEPLOYMENT, _store(tmp_path, "deployment", 2))
    own = _org_state(_OWN, _store(tmp_path, "own", 3))
    for _ in range(3):
        assert _count(engine, _DEPLOYMENT, deployment) == 2
        assert _count(engine, _OWN, own) == 3


def test_each_orgs_walk_stands_for_it_while_the_other_org_is_served(engine, tmp_path, monkeypatch):
    deployment = _org_state(_DEPLOYMENT, _store(tmp_path, "deployment", 2))
    own = _org_state(_OWN, _store(tmp_path, "own", 3))
    walks: list[str] = []
    walk = type(engine.backend)._walk_registry

    def _counted(self, state, config):
        walks.append(state.source_catalogs["lake"])
        return walk(self, state, config)

    monkeypatch.setattr(type(engine.backend), "_walk_registry", _counted)
    for _ in range(3):
        _count(engine, _DEPLOYMENT, deployment)
        _count(engine, _OWN, own)
    assert walks == ["org_default__lake", "org_sbxlake__lake"]
