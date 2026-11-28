# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0c2b7e-6a41-4d58-9b1e-2d7a5c8e4f10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dev-mode code source is attached once the schema build has named it (REQ-2267): the
reconcile at boot walks the registry before that build, and the walk leaves the source for the
walk after the build rather than raising."""

# Requirements: REQ-2267

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.request_context import reset_current_org, set_current_org
from provisa.federation.engine import build_engine
from provisa.federation.replica_address import ReplicaRoutes

pytestmark = pytest.mark.unbound

_ROW = {
    "type": "python",
    "mapping": {"mode": "dev"},
    "region": None,
    "password_ref": None,
    "replicate": None,
    "load_protected": None,
    "max_live_concurrency": None,
    "download_from": None,
    "cache_ttl": None,
}


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        org_id="default",
        active_isolated_org=None,
        config=SimpleNamespace(sources=[], tables=[]),
        runtime_sources={"scores-src": _ROW},
        tables=[
            {
                "source_id": "scores-src",
                "schema_name": "default",
                "table_name": "scores",
                "region": None,
                "cache_ttl": None,
            }
        ],
        model_db=None,
        replica_routes=ReplicaRoutes(),
        source_catalogs={"scores-src": "scores_src"},
    )


@pytest.fixture
def engine(tmp_path, monkeypatch):
    import provisa.api.app  # noqa: F401
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    monkeypatch.setattr("provisa.core.operator_floor.floor_setting", lambda _src: None)
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    engine = build_engine("duckdb")
    engine.backend._runtime = DuckDBFederationRuntime(
        materialize_dsn=f"duckdb:///{tmp_path / 'mat.duckdb'}"
    )
    yield engine
    engine.backend.close(None)


def test_the_walk_before_the_schema_build_names_a_dev_source_raises_nothing(engine):
    token = set_current_org("default")
    try:
        engine.backend._runtime_for(_state())
    finally:
        reset_current_org(token)
