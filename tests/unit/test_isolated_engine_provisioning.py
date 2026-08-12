# Copyright (c) 2026 Kenneth Stott
# Canary: 4c1e97b2-08da-4f65-9a37-11e2c5d6b8a0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1043/REQ-1244/REQ-1427: source provisioning onto a sleeping isolated coordinator.

An isolated org's terminal is bound with connection kwargs and no connection on purpose — its
dedicated coordinator sleeps until real traffic arrives. Registering the org's sources IS that
traffic. Gating the catalog DDL on a live ``engine_conn`` turned registration into a silent
no-op, so the coordinator never received the org's catalogs and the org's first query failed with
CATALOG_NOT_FOUND — the state a live Trino would report only after the org was already broken.
"""

# Requirements: REQ-1043, REQ-1244, REQ-1427

from __future__ import annotations

import types

import pytest

from typing import cast

from provisa.federation.backend import TrinoBackend
from provisa.federation.engine import FederationEngine


class _Conn:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def backend() -> TrinoBackend:
    return TrinoBackend(cast(FederationEngine, types.SimpleNamespace(name="trino")))


def _source() -> types.SimpleNamespace:
    return types.SimpleNamespace(id="pet_store_sqlite")


def test_bound_but_unconnected_terminal_still_issues_catalog_ddl(backend, monkeypatch):
    state = types.SimpleNamespace(
        engine_conn=None, engine_conn_kwargs={"host": "trino-kstott", "port": 8080}
    )
    opened = _Conn()
    monkeypatch.setattr("provisa.federation.trino_lifecycle.connect", lambda kwargs: opened)
    seen: dict = {}
    monkeypatch.setattr(
        "provisa.core.catalog.create_catalog",
        lambda conn, source, pw, catalog_name=None: seen.update(
            conn=conn, source=source.id, catalog_name=catalog_name
        ),
    )

    backend.register_source(state, _source(), "pw", catalog_name="org_kstott__pet_store_sqlite")

    assert seen["conn"] is opened
    assert seen["catalog_name"] == "org_kstott__pet_store_sqlite"
    # The woken connection belongs to provisioning alone — it never becomes the org's terminal.
    assert opened.closed
    assert state.engine_conn is None


def test_live_terminal_reuses_its_connection_and_keeps_it_open(backend, monkeypatch):
    live = _Conn()
    state = types.SimpleNamespace(engine_conn=live, engine_conn_kwargs={"host": "trino"})
    monkeypatch.setattr(
        "provisa.federation.trino_lifecycle.connect",
        lambda kwargs: pytest.fail("a live terminal must not open a second connection"),
    )
    seen: dict = {}
    monkeypatch.setattr(
        "provisa.core.catalog.create_catalog",
        lambda conn, source, pw, catalog_name=None: seen.update(conn=conn),
    )

    backend.register_source(state, _source(), "pw")

    assert seen["conn"] is live
    assert not live.closed


def test_terminal_with_neither_connection_nor_kwargs_provisions_nothing(backend, monkeypatch):
    state = types.SimpleNamespace(engine_conn=None, engine_conn_kwargs=None)
    monkeypatch.setattr(
        "provisa.core.catalog.create_catalog",
        lambda *a, **k: pytest.fail("no terminal to provision against"),
    )

    backend.register_source(state, _source(), "pw")


def test_drop_and_analyze_follow_the_same_wake_contract(backend, monkeypatch):
    state = types.SimpleNamespace(engine_conn=None, engine_conn_kwargs={"host": "trino-kstott"})
    conns = [_Conn(), _Conn()]
    monkeypatch.setattr("provisa.federation.trino_lifecycle.connect", lambda kwargs: conns.pop(0))
    calls: list[str] = []
    monkeypatch.setattr(
        "provisa.core.catalog.drop_catalog",
        lambda conn, source_id, catalog_name=None: calls.append(f"drop:{catalog_name}"),
    )
    monkeypatch.setattr(
        "provisa.core.catalog.analyze_source_tables",
        lambda conn, source, tables, catalog_name=None: calls.append(f"analyze:{catalog_name}"),
    )

    backend.drop_source(state, "pet_store_sqlite", catalog_name="org_kstott__pet_store_sqlite")
    backend.analyze(state, _source(), [], catalog_name="org_kstott__pet_store_sqlite")

    assert calls == [
        "drop:org_kstott__pet_store_sqlite",
        "analyze:org_kstott__pet_store_sqlite",
    ]


def test_the_ops_catalogs_reach_the_org_s_own_coordinator(backend, monkeypatch):
    """REQ-1332/REQ-1428: provisa_admin, otel and results belong to every terminal, not to one.

    register_system_catalogs ran only in provision(), which serves the deployment's shared engine.
    An org moved to its own coordinator therefore had the Provisa-owned catalogs on the coordinator
    it had just left: admin / reports / traces returned
    "FederationError(type=USER_ERROR, name=CATALOG_NOT_FOUND, Catalog 'otel' not found)", and no
    amount of telemetry compaction could put a row in front of that org.
    """
    state = types.SimpleNamespace(
        engine_conn=None,
        engine_conn_kwargs={"host": "trino-kstott", "port": 8080},
        tenant_engine=types.SimpleNamespace(url="postgresql://cp/provisa"),
        org_id="kstott",
    )
    opened = _Conn()
    monkeypatch.setattr("provisa.federation.trino_lifecycle.connect", lambda kwargs: opened)
    calls: list[tuple] = []
    monkeypatch.setattr(
        "provisa.core.trino_system_catalogs.register_system_catalogs",
        lambda conn, url, org_id: calls.append(("catalogs", conn, url, org_id)),
    )
    monkeypatch.setattr(
        "provisa.observability.ops_trino.seed_ops_trino",
        lambda conn, views, hours: calls.append(("views", conn, tuple(views), hours)),
    )

    backend.reseed_ops(state, ["traces"], 24)

    # Catalogs first: the ops views are created inside `otel`.
    assert [c[0] for c in calls] == ["catalogs", "views"]
    assert calls[0] == ("catalogs", opened, "postgresql://cp/provisa", "kstott")
    assert calls[1] == ("views", opened, ("traces",), 24)
    assert opened.closed
    assert state.engine_conn is None
