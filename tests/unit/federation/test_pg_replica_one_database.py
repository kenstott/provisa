# Copyright (c) 2026 Kenneth Stott
# Canary: 95dc5fbd-daca-4e9b-8657-2c0d7c1dc844
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pg engine reads its replicas in the database it runs in, so that database is its store.

Whole-table replicas already land there (``PgBackend.replica_target``); row-level ones took the
materialization store, whose default was the platform database. With ``PROVISA_ENGINE_URL`` on
another database, they were written to one and read from the other. A store the engine cannot
read is refused at the door every address comes through, which the boot passes."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.engine import (
    StoreOutsideEngineDatabase,
    build_pg_engine,
    build_trino_engine,
)
from provisa.federation.runtime import EngineRuntime

ENGINE = "postgresql://u:secret@engine-host:5433/provisa_bench"
PLATFORM = "postgresql+psycopg://u:secret@cp-host:5432/cp_62765_0"


@pytest.fixture
def separate_engine_database(monkeypatch):
    monkeypatch.delenv("PROVISA_MATERIALIZE_URL", raising=False)
    monkeypatch.setenv("PROVISA_ENGINE_URL", ENGINE)
    monkeypatch.setenv("TENANT_DATABASE_URL", PLATFORM)


@pytest.mark.usefixtures("separate_engine_database")
def test_the_engine_database_is_the_store_when_no_store_is_named():
    assert build_pg_engine().materialize_store() == ENGINE


def test_the_platform_database_is_the_store_of_an_engine_running_in_it(monkeypatch):
    monkeypatch.delenv("PROVISA_MATERIALIZE_URL", raising=False)
    monkeypatch.delenv("PROVISA_ENGINE_URL", raising=False)
    monkeypatch.setenv("TENANT_DATABASE_URL", PLATFORM)
    assert build_pg_engine().materialize_store() == "postgresql://u:secret@cp-host:5432/cp_62765_0"


@pytest.mark.usefixtures("separate_engine_database")
def test_a_store_naming_the_engine_database_is_accepted_whatever_its_driver_or_role(monkeypatch):
    named = "postgresql+psycopg://other:pw@ENGINE-HOST:5433/provisa_bench"
    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", named)
    assert build_pg_engine().materialize_store() == named


def test_an_omitted_port_is_the_default_port(monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE_URL", "postgresql://u:p@h/db")
    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", "postgresql://u:p@h:5432/db")
    assert build_pg_engine().materialize_store() == "postgresql://u:p@h:5432/db"


@pytest.mark.usefixtures("separate_engine_database")
@pytest.mark.parametrize(
    "store",
    [
        pytest.param("postgresql://u:secret@engine-host:5433/other_db", id="another database"),
        pytest.param("postgresql://u:secret@cp-host:5433/provisa_bench", id="another host"),
        pytest.param("postgresql://u:secret@engine-host:5999/provisa_bench", id="another port"),
        pytest.param("duckdb:////tmp/store.duckdb", id="another kind of store"),
    ],
)
def test_a_store_the_engine_cannot_read_is_refused_by_name(monkeypatch, store):
    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", store)
    with pytest.raises(StoreOutsideEngineDatabase) as refused:
        build_pg_engine().materialize_store()
    message = str(refused.value)
    assert "provisa_bench" in message and "PROVISA_MATERIALIZE_URL" in message
    assert "secret" not in message


@pytest.mark.usefixtures("separate_engine_database")
def test_the_boot_check_stops_on_that_store(monkeypatch):
    """``app.py`` calls this at start, suppressing only an unconfigured store."""
    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", PLATFORM)
    runtime = EngineRuntime(build_pg_engine(), SimpleNamespace())
    with pytest.raises(StoreOutsideEngineDatabase):
        runtime.materialize_store_dsn()


@pytest.mark.usefixtures("separate_engine_database")
def test_a_pinned_store_elsewhere_is_refused_too():
    """An org lane's store, and an org's own store, come through the same door."""
    engine = build_pg_engine()
    engine.pin_materialize_store(PLATFORM)
    with pytest.raises(StoreOutsideEngineDatabase):
        engine.materialize_store()


@pytest.mark.usefixtures("separate_engine_database")
def test_the_database_a_running_engine_holds_decides(monkeypatch):
    """A running engine stays on the database it connected to, whatever a setting says now."""
    engine = build_pg_engine()
    engine.backend._runtime = SimpleNamespace(_engine_dsn="postgresql://u:p@running/db")
    engine.pin_materialize_store("postgresql://u:p@running/db")
    assert engine.materialize_store() == "postgresql://u:p@running/db"


def test_an_engine_reading_a_separate_store_keeps_it(monkeypatch):
    """Trino reads its store through a connector: a separate store is what it is built for."""
    monkeypatch.setenv("PROVISA_ENGINE_URL", ENGINE)
    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", "postgresql://u:p@store/replicas")
    assert build_trino_engine().materialize_store() == "postgresql://u:p@store/replicas"
