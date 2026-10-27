# Copyright (c) 2026 Kenneth Stott
# Canary: 84c7e1f4-ee86-435a-9fb0-3ee0a174f619
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every catalog a client sees is the served database (REQ-588): under multi-tenancy the org the
client named, single-tenant ``provisa``, as PostgreSQL's table_catalog is current_database()."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from provisa.api.flight import flightsql_catalog
from provisa.pgwire.catalog import answer
from provisa.proto import FlightSql_pb2 as pb


def _state(multitenancy: bool):
    pets = SimpleNamespace(
        table_id=1,
        field_name="pets",
        catalog_name="pg",
        schema_name="public",
        table_name="pets",
        domain_id="",
        source_id="",
        type_name="",
        source_type="",
        original_table_name="",
        display_name="",
        column_presets={},
    )
    ctx = MagicMock()
    ctx.tables = {"pets": pets}
    ctx.joins = {}
    state = MagicMock()
    state.multitenancy = multitenancy
    state.contexts = {"analyst": ctx}
    state.schema_build_cache = {
        "column_types": {
            1: [SimpleNamespace(column_name="id", data_type="integer", is_nullable=False)]
        }
    }
    return state


@pytest.fixture
def acme():
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org("acme")
    yield _state(multitenancy=True)
    reset_current_org(token)


def _catalogs(sql: str, state) -> set:
    return {row[0] for row in answer(sql, "analyst", state).rows}


def test_a_tenant_session_sees_its_database_as_every_catalog(acme):
    assert _catalogs("SELECT current_database()", acme) == {"acme"}
    assert _catalogs("SELECT DISTINCT table_catalog FROM information_schema.tables", acme) == {
        "acme"
    }
    assert _catalogs("SELECT DISTINCT table_catalog FROM information_schema.columns", acme) == {
        "acme"
    }
    assert _catalogs("SELECT DISTINCT catalog_name FROM information_schema.schemata", acme) == {
        "acme"
    }


def test_a_single_tenant_session_sees_provisa():
    state = _state(multitenancy=False)
    assert _catalogs("SELECT DISTINCT table_catalog FROM information_schema.tables", state) == {
        "provisa"
    }
    assert _catalogs("SELECT DISTINCT catalog_name FROM information_schema.schemata", state) == {
        "provisa"
    }


def test_flight_sql_lists_the_tenants_database(acme):
    catalog = flightsql_catalog.role_catalog(acme, "analyst")
    assert flightsql_catalog.catalogs(catalog).column("catalog_name").to_pylist() == ["acme"]
    schemas = flightsql_catalog.db_schemas(catalog, pb.CommandGetDbSchemas())
    assert set(schemas.column("catalog_name").to_pylist()) == {"acme"}
    tables = flightsql_catalog.tables(catalog, pb.CommandGetTables())
    assert set(tables.column("catalog_name").to_pylist()) == {"acme"}
