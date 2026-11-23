# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9b1f0a-6c2d-4f7e-9a51-b8d4c0e2f713
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A SQLAlchemy-runtime engine whose dialect is Postgres (cockroachdb, greenplum, yugabytedb,
opengauss) reaches pgwire's ENGINE passthrough. The passthrough first re-points in-place reads
(REQ-1867), then asks for a pooled Postgres connection (REQ-1863). The SQLAlchemy runtime has no
in-place read and no such pool, so the passthrough does not apply (PassthroughError), and pgwire
decodes the statement instead."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.federation.native_backend import NativeEngineBackend
from provisa.federation.runtime import EngineRuntime
from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime
from provisa.pgwire.pg_passthrough import PassthroughError


class _SqlAlchemyBackend:
    """NativeEngineBackend's passthrough entry points, over one SQLAlchemy runtime."""

    refresh_live_relations = NativeEngineBackend.refresh_live_relations
    borrow_raw_pg_connection = NativeEngineBackend.borrow_raw_pg_connection

    def __init__(self, runtime: SqlAlchemyFederationRuntime) -> None:
        self.engine = SimpleNamespace(name="cockroachdb")
        self._runtime = runtime

    def _runtime_for(self, state: Any) -> SqlAlchemyFederationRuntime:
        del state
        return self._runtime


@pytest.fixture
def sqlalchemy_runtime():
    runtime = SqlAlchemyFederationRuntime(url="sqlite://")
    yield runtime
    runtime.close()


def test_refresh_live_relations_has_nothing_to_re_point(sqlalchemy_runtime) -> None:
    assert sqlalchemy_runtime.refresh_live_relations('SELECT * FROM "s"."t"') is None


def test_engine_passthrough_does_not_apply_to_a_sqlalchemy_runtime(sqlalchemy_runtime) -> None:
    backend = _SqlAlchemyBackend(sqlalchemy_runtime)
    rt = EngineRuntime(SimpleNamespace(backend=backend), state=object())
    with pytest.raises(PassthroughError, match="cockroachdb"):
        rt.execute_pg_engine_passthrough("SELECT 1", None, [0], described_oids=None)
