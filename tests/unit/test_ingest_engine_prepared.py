# Copyright (c) 2026 Kenneth Stott
# Canary: c09ccc1b-eb8d-4db1-bbd2-e8f994ec663c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Ingest write engines run PostgreSQL on the control plane's prepared-statement policy
(REQ-331, REQ-828): ``prepare_threshold=0`` with a bounded cache, off behind PgBouncer — set by
the one shared ``core.database.pg_engine``, never a second copy."""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import event

import provisa.core.database as D
import provisa.ingest.engine as E


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    real = D.sa.create_engine

    def spy(url: Any, **kwargs: Any) -> Any:
        calls.append(kwargs)
        return real(url, **kwargs)

    monkeypatch.setattr(D.sa, "create_engine", spy)
    E._engines.clear()
    yield calls
    E.dispose_all()


def _ingest(source_id: str, *, use_pgbouncer: bool, search_path: str | None = None) -> Any:
    return E.get_engine(
        source_id,
        "postgresql",
        "h",
        5432,
        "db",
        "u",
        "p",
        search_path=search_path,
        use_pgbouncer=use_pgbouncer,
    )


def test_direct_ingest_engine_prepares_on_first_execution(captured: list[dict[str, Any]]) -> None:
    engine = _ingest("direct", use_pgbouncer=False, search_path="org_1")
    assert engine.url.drivername == "postgresql+psycopg"
    assert captured[-1]["connect_args"] == {
        "options": "-csearch_path=org_1",
        "prepare_threshold": 0,
    }
    assert event.contains(engine, "connect", D._on_pg_connect)  # bounds prepared_max
    assert D.pg_uses_pgbouncer(engine) is False


def test_pgbouncer_ingest_engine_never_prepares(captured: list[dict[str, Any]]) -> None:
    engine = _ingest("bouncer", use_pgbouncer=True)
    assert captured[-1]["connect_args"] == {"prepare_threshold": None}
    assert D.pg_uses_pgbouncer(engine) is True


def test_tenant_mirror_inherits_the_control_plane_pgbouncer_choice(
    captured: list[dict[str, Any]],
) -> None:
    tenant = D.create_engine_from_url(
        "postgresql+psycopg://u:p@h:6432/db?use_pgbouncer=true&direct=h:5432"
    )
    try:
        engine = _ingest("mirror", use_pgbouncer=D.pg_uses_pgbouncer(tenant))
        assert (
            captured[0]["connect_args"]
            == captured[-1]["connect_args"]
            == {"prepare_threshold": None}
        )
        assert D.pg_uses_pgbouncer(engine) is True
    finally:
        tenant.dispose()


def test_control_plane_and_ingest_share_one_policy(captured: list[dict[str, Any]]) -> None:
    tenant = D.create_engine_from_url("postgresql+psycopg://u:p@h:5432/db")
    try:
        _ingest("same", use_pgbouncer=False)
        assert (
            captured[0]["connect_args"] == captured[-1]["connect_args"] == {"prepare_threshold": 0}
        )
    finally:
        tenant.dispose()
