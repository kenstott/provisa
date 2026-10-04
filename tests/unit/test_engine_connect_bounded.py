# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Boot's engine-connect phase is logged step by step and bounded (REQ-1913, REQ-1332).

In the cluster lane the API pod logged its seed phase and then nothing until the liveness probe
killed it: engine-connect waited, unbounded and silent, on something it could not reach. Each step
now logs as it starts and ends, every wait in it is bounded by engine.ready_timeout, and a step that
fails or runs out names itself and what it was waiting on.
"""

# Requirements: REQ-1913, REQ-1332

from __future__ import annotations

import logging
import time
from types import SimpleNamespace

import pytest
from sqlalchemy.engine import URL

from provisa.federation import trino_lifecycle
from provisa.federation.trino_lifecycle import BootStepFailed

# TEST-NET-1 (RFC 5737): reserved for documentation, never routed, so a connect to it never answers.
_UNREACHABLE = "192.0.2.1"


def test_an_unreachable_control_plane_fails_the_step_within_its_limit():
    from provisa.core.trino_system_catalogs import ensure_iceberg_catalog_tables

    url = URL.create(
        "postgresql+psycopg", username="u", password="p", host=_UNREACHABLE, port=5432, database="d"
    )
    began = time.monotonic()
    with pytest.raises(BootStepFailed) as raised:
        trino_lifecycle._boot_step(
            "register system catalogs",
            f"the control plane at {_UNREACHABLE}:5432",
            1,
            lambda: ensure_iceberg_catalog_tables(url, 1),
        )
    assert time.monotonic() - began < 15  # bounded by the step's limit, not the OS connect timeout
    message = str(raised.value)
    assert "'register system catalogs'" in message
    assert f"{_UNREACHABLE}:5432" in message
    assert "engine.ready_timeout" in message


def test_each_step_is_logged_as_it_begins_and_ends(caplog):
    with caplog.at_level(logging.WARNING, logger=trino_lifecycle.__name__):
        trino_lifecycle._boot_step(
            "seed ops tables", "the otel store at http://s3:9000", 5, lambda: None
        )
    lines = [r.getMessage() for r in caplog.records]
    assert any("seed ops tables" in m and "begin" in m and "http://s3:9000" in m for m in lines)
    assert any("seed ops tables" in m and "done" in m for m in lines)


def test_the_boot_connection_asks_the_engine_to_cut_off_statements_at_the_limit(monkeypatch):
    connections: list[dict] = []
    monkeypatch.setattr(
        trino_lifecycle.trino.dbapi, "connect", lambda **kw: connections.append(kw) or object()
    )
    monkeypatch.setattr(
        trino_lifecycle, "terminal_conn_kwargs", lambda state: {"host": "trino", "port": 8080}
    )
    from provisa.core import settings_registry

    monkeypatch.setattr(settings_registry, "value", lambda key: 7.0)
    seen: dict = {}
    import provisa.compiler.schema_service as schema_service
    import provisa.core.trino_system_catalogs as catalogs
    import provisa.observability.ops_trino as ops

    monkeypatch.setattr(catalogs, "otel_object_store", lambda: {"endpoint": "http://minio:9000"})
    monkeypatch.setattr(
        catalogs, "register_system_catalogs", lambda conn, url, org, limit: seen.update(limit=limit)
    )
    monkeypatch.setattr(schema_service, "init", lambda engine: None)

    def _store_never_answers(conn, views):
        raise RuntimeError("Query exceeded the maximum run time limit of 7.00s")

    monkeypatch.setattr(ops, "seed_ops_trino", _store_never_answers)
    state = SimpleNamespace(
        tenant_engine=SimpleNamespace(url=URL.create("postgresql", host="pg", port=5432)),
        org_id="default",
        federation_engine=None,
    )
    with pytest.raises(BootStepFailed) as raised:
        trino_lifecycle.provision(state, [])

    boot = [c for c in connections if "session_properties" in c]
    assert boot and boot[0]["session_properties"] == {"query_max_run_time": "7s"}
    assert seen["limit"] == 7.0
    message = str(raised.value)
    assert "'seed ops tables'" in message
    assert "the otel store at http://minio:9000" in message


def test_a_seed_statement_the_engine_cut_off_stops_boot():
    from provisa.observability.ops_trino import seed_ops_trino

    class _TimedOut(Exception):
        error_name = "EXCEEDED_TIME_LIMIT"

    class _Cursor:
        def execute(self, sql):
            raise _TimedOut("Query exceeded the maximum run time limit")

        def fetchall(self):
            return []

    conn = SimpleNamespace(cursor=_Cursor)
    with pytest.raises(_TimedOut):
        seed_ops_trino(conn, [])  # pyright: ignore[reportArgumentType]
