# Copyright (c) 2026 Kenneth Stott
# Canary: 7c6d5b73-35b1-4a87-a4b3-bff7bc50894b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What each coordinator's system catalogs were last created from, kept in the PLATFORM STATE
STORE (REQ-1429): a table of the platform schema like any other, on a SQLite platform database
here (tests/integration/test_catalog_registrar_pgbouncer.py has it on PostgreSQL)."""

from __future__ import annotations

import pytest

from provisa.core.platform_state import TABLES, catalogs


@pytest.fixture
def record(tmp_path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import engine_system_catalogs, metadata

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'platform.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[engine_system_catalogs])
    return catalogs.CatalogRecord(Database(engine, "platform-state", holds="platform_state"))


def test_the_table_is_one_of_the_platform_state_stores_and_of_the_schema_init_creates():
    from provisa.core.schema_admin import REGISTRY_TABLES

    assert "engine_system_catalogs" in TABLES
    assert "engine_system_catalogs" in {t.name for t in REGISTRY_TABLES}


def test_nothing_is_recorded_for_a_catalog_never_created(record):
    assert record.created_from("trino:8080", "provisa_admin") is None


def test_a_recorded_hash_is_read_back_and_replaced_when_the_spec_changes(record):
    record.record("trino:8080", "provisa_admin", "abc")
    assert record.created_from("trino:8080", "provisa_admin") == "abc"
    record.record("trino:8080", "provisa_admin", "def")
    assert record.created_from("trino:8080", "provisa_admin") == "def"


def test_each_coordinator_and_each_catalog_has_its_own_entry(record):
    record.record("trino:8080", "provisa_admin", "abc")
    record.record("trino:8080", "otel", "o1")
    record.record("trino-acme:8080", "provisa_admin", "xyz")
    assert record.created_from("trino:8080", "provisa_admin") == "abc"
    assert record.created_from("trino:8080", "otel") == "o1"
    assert record.created_from("trino-acme:8080", "provisa_admin") == "xyz"
    assert record.created_from("trino-acme:8080", "otel") is None
