# Copyright (c) 2026 Kenneth Stott
# Canary: 0e982c18-3b06-4be5-8561-3d9e93ee2a8b
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the executable file in the root directory of this source tree.

"""Catalog-name agreement: what a source is recorded under must be what it is created as.

``state.source_catalogs[id]`` is what ``catalog_for()`` returns and what every compiled engine
query names; ``create_catalog`` is what physically makes the catalog. When those two disagree the
source registers cleanly and every query for it then dies on CATALOG_NOT_FOUND — which is exactly
what a SharePoint source did, because its Azure tenant GUID lives in ``database`` and the recorded
name was derived from that field.

These tests drive the real population path rather than restating its arithmetic; a formula copied
into the test cannot detect the formula changing.
"""

from __future__ import annotations

import pytest

from provisa.core.catalog import _to_catalog_name
from provisa.core.models import ProvisaConfig, Source, SourceType


def _config(*sources: Source) -> ProvisaConfig:
    return ProvisaConfig(sources=list(sources), domains=[], tables=[], roles=[])


class _NativeEngine:
    """Stand-in for the EngineRuntime that is bound before the catalog-name loader runs.

    _populate_source_catalog_names reads the otel-catalog capability off it, so the stub carries
    the native answer (no `otel` catalog) rather than leaving the slot empty.
    """

    has_otel_catalog = False


@pytest.fixture()
def state(monkeypatch):
    from provisa.api.app import state as app_state

    monkeypatch.setattr(app_state, "source_catalogs", {}, raising=False)
    monkeypatch.setattr(app_state, "source_types", {}, raising=False)
    monkeypatch.setattr(app_state, "source_dialects", {}, raising=False)
    monkeypatch.setattr(app_state, "source_cache", {}, raising=False)
    monkeypatch.setattr(app_state, "source_federation_hints", {}, raising=False)
    monkeypatch.setattr(app_state, "federation_engine", _NativeEngine(), raising=False)
    monkeypatch.setattr(app_state, "org_id", "default", raising=False)
    return app_state


@pytest.mark.parametrize(
    "source",
    [
        # The SharePoint case: `database` carries the Azure tenant GUID.
        Source(
            id="e2e-sharepoint",
            type=SourceType.sharepoint,
            database="5d2609cc-7eff-4b82-8f83-f0b28c71fafc",
        ),
        # A remote database name is equally not a catalog name.
        Source(id="pet-store-pg", type=SourceType.postgresql, database="provisa"),
        # No `database` at all — the case that always worked.
        Source(id="inquiries-sqlite", type=SourceType.sqlite, path="./x.sqlite"),
    ],
    ids=["sharepoint-tenant-guid", "postgres-db-name", "no-database"],
)
def test_recorded_catalog_is_the_one_create_catalog_makes(state, source):
    from provisa.api.app_loaders import _populate_source_catalog_names

    _populate_source_catalog_names(_config(source))

    assert state.source_catalogs[source.id] == _to_catalog_name(source.id)


def test_non_default_org_prefixes_the_source_id_derived_name(state):
    from provisa.api.app_loaders import _populate_source_catalog_names
    from provisa.core.request_context import current_org

    token = current_org.set("tenant-a")
    try:
        _populate_source_catalog_names(
            _config(Source(id="e2e-sharepoint", type=SourceType.sharepoint, database="tenant-guid"))
        )
    finally:
        current_org.reset(token)

    assert state.source_catalogs["e2e-sharepoint"] == "org_tenant-a__e2e_sharepoint"


def test_fixed_catalog_warehouse_pins_every_source_to_the_warehouse_database(state, monkeypatch):
    """A Synapse-bound source must be recorded under SYNAPSE_DATABASE, not source_to_catalog(id).

    createSource hyphenates source ids (``synapse-ext`` -> ``synapse_ext``), which is not the
    Synapse database name the fixed-catalog engine actually attaches/queries at.
    """
    from provisa.api.app_loaders import _populate_source_catalog_names, fixed_catalog_for_engine

    class _Engine:
        name = "synapse"

    class _FederationEngine:
        engine = _Engine()
        has_otel_catalog = False

    monkeypatch.setattr(state, "federation_engine", _FederationEngine(), raising=False)
    monkeypatch.setenv("SYNAPSE_DATABASE", "provisa_syn")

    assert fixed_catalog_for_engine(state) == "provisa_syn"

    _populate_source_catalog_names(_config(Source(id="synapse-ext", type=SourceType.parquet)))

    assert state.source_catalogs["synapse-ext"] == "provisa_syn"


class _RecordingEngine:
    """Records what the source-provisioning seam is asked to act on."""

    has_otel_catalog = False

    def __init__(self) -> None:
        self.registered: list[tuple[str, str | None]] = []
        self.analyzed: list[tuple[str, str | None]] = []

    def register_source(self, source, resolved_password, catalog_name=None) -> None:
        self.registered.append((source.id, catalog_name))

    def analyze(self, source, tables, catalog_name=None) -> None:
        self.analyzed.append((source.id, catalog_name))


def test_config_load_analyzes_at_the_catalog_it_registered(state):
    """ANALYZE must name the same catalog registration created (REQ-1266).

    Analyzing the bare name asked an isolated coordinator about a catalog it does not have, and
    every ANALYZE on a non-default org died TABLE_NOT_FOUND.
    """
    import asyncio

    from provisa.core.config_loader import _analyze_sources

    engine = _RecordingEngine()
    config = _config(Source(id="pet-store-pg", type=SourceType.postgresql, database="provisa"))
    asyncio.run(_analyze_sources(engine, config, {"pet-store-pg": "org_ks__pet_store_pg"}))

    assert engine.analyzed == [("pet-store-pg", "org_ks__pet_store_pg")]


def test_dynamic_source_registers_and_analyzes_at_the_recorded_catalog(state, monkeypatch):
    """The createSource path must use ``state.source_catalogs[id]``, not the bare derived name."""
    import asyncio

    from provisa.api.admin.schema_common import (
        _analyze_source_on_engine,
        _register_source_on_engine,
    )
    from provisa.api.admin.types import SourceInput

    engine = _RecordingEngine()
    monkeypatch.setattr(state, "federation_engine", engine, raising=False)
    state.source_catalogs["pet-store-pg"] = "org_ks__pet_store_pg"

    model = Source(id="pet-store-pg", type=SourceType.postgresql, database="provisa")
    source_input = SourceInput(id="pet-store-pg", type="postgresql", database="provisa")

    class _Result:
        def fetchall(self):
            class _Row:
                schema_name = "public"
                table_name = "pets"

            return [_Row()]

    class _Conn:
        async def execute_core(self, _stmt):
            return _Result()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

    class _Pool:
        def acquire(self):
            return _Conn()

    _register_source_on_engine(state, model, source_input)
    asyncio.run(_analyze_source_on_engine(state, _Pool(), model, source_input))

    assert engine.registered == [("pet-store-pg", "org_ks__pet_store_pg")]
    assert engine.analyzed == [("pet-store-pg", "org_ks__pet_store_pg")]


class _TrinoFederationEngine:
    """Stand-in for a Trino-bound EngineRuntime: names the live engine, and records whether
    materialize_store_target was asked for (it must be, for a FETCH-mechanism source; must NOT be,
    for one Trino reaches live — asking it for a type with no materialize landing would be a bug
    in its own right, caught here by simply not providing a working implementation)."""

    class _Engine:
        name = "trino"

    engine = _Engine()
    has_otel_catalog = True

    def materialize_store_target(self, org_id: str) -> tuple[str, str]:
        return ("provisa_admin", f"org_{org_id}_mv_cache")


def test_trino_routes_fetch_mechanism_sources_to_the_materialize_store(state, monkeypatch):
    """openapi/graphql_remote/sqlite (Trino Mechanism.FETCH — TrinoPgBackedConnector and
    subclasses) have no live Trino connector reach; their replica is landed into the materialize
    store, and that IS the catalog the compiler must name (TrinoBackend.landing_target)."""
    from provisa.api.app_loaders import catalog_name_for_source

    monkeypatch.setattr(state, "federation_engine", _TrinoFederationEngine(), raising=False)

    assert catalog_name_for_source(state, "openapi", "petstore-api") == "provisa_admin"
    assert catalog_name_for_source(state, "graphql_remote", "e2e-gql") == "provisa_admin"
    assert catalog_name_for_source(state, "sqlite", "inquiries-sqlite") == "provisa_admin"


def test_trino_routes_live_connector_sources_to_their_own_catalog(state, monkeypatch):
    """REQ-1730 regression: prometheus (and redis/mongodb/cassandra/elasticsearch/google_sheets)
    have their OWN live Trino connector (ATTACH_RW/ATTACH_R) despite prometheus also being
    "adapter-fetched" for DuckDB's own introspection — a different question entirely. Routing it
    through the materialize store landed every query on a catalog nothing ever populates
    ('provisa_admin.default.up' TABLE_NOT_FOUND on every run)."""
    from provisa.api.app_loaders import catalog_name_for_source

    monkeypatch.setattr(state, "federation_engine", _TrinoFederationEngine(), raising=False)

    assert catalog_name_for_source(state, "prometheus", "e2e-prom") == "e2e_prom"
    assert catalog_name_for_source(state, "redis", "e2e-redis") == "e2e_redis"


def test_trino_routes_neo4j_and_sparql_to_the_materialize_store(state, monkeypatch):
    """neo4j/sparql have no Trino connector at all (TRINO_CONNECTORS has no entry) — same
    materialize-store routing as a FETCH-mechanism source, for the same underlying reason (REQ-842:
    no Trino catalog is ever provisioned for a type with no Trino connector)."""
    from provisa.api.app_loaders import catalog_name_for_source

    monkeypatch.setattr(state, "federation_engine", _TrinoFederationEngine(), raising=False)

    assert catalog_name_for_source(state, "neo4j", "e2e-neo4j") == "provisa_admin"
    assert catalog_name_for_source(state, "sparql", "e2e-sparql") == "provisa_admin"
