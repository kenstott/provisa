# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Admin GraphQL Query resolvers (schema_query.py)."""

import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def _gql(client, query):
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200
    return resp.json()


class TestSchemaVersion:
    async def test_schema_version_hash(self, client):
        data = await _gql(client, "{ schemaVersion }")
        assert "errors" not in data
        assert len(data["data"]["schemaVersion"]) == 64


class TestCreationRequests:
    async def test_creation_requests_list(self, client):
        data = await _gql(client, "{ creationRequests { id requestType capability status } }")
        assert isinstance(data["data"]["creationRequests"], list)


class TestSources:
    async def test_list_sources(self, client):
        data = await _gql(client, "{ sources { id type host port } }")
        assert len(data["data"]["sources"]) > 0

    async def test_get_source_not_found(self, client):
        data = await _gql(client, '{ source(id: "no-such-source-xyz") { id } }')
        assert data["data"]["source"] is None


class TestDomains:
    async def test_list_domains(self, client):
        data = await _gql(client, "{ domains { id description } }")
        domains = data["data"]["domains"]
        assert any(d["id"] == "pet-store" for d in domains)


class TestTables:
    async def test_list_tables_with_columns(self, client):
        data = await _gql(
            client, "{ tables { id sourceId tableName columns { columnName visibleTo } } }"
        )
        tables = data["data"]["tables"]
        assert len(tables) > 0
        assert any(t["tableName"] == "pets" for t in tables)


class TestRelationships:
    async def test_list_relationships(self, client):
        data = await _gql(
            client,
            "{ relationships { id sourceColumn targetColumn cardinality autoSuggested } }",
        )
        assert len(data["data"]["relationships"]) > 0

    async def test_list_all_relationships(self, client):
        data = await _gql(client, "{ allRelationships { id sourceColumn cardinality } }")
        assert len(data["data"]["allRelationships"]) > 0


class TestRoles:
    async def test_list_roles(self, client):
        data = await _gql(client, "{ roles { id capabilities domainAccess } }")
        roles = data["data"]["roles"]
        admin = next(r for r in roles if r["id"] == "admin")
        assert "admin" in admin["capabilities"]


class TestRlsRules:
    async def test_rls_rules_list(self, client):
        data = await _gql(client, "{ rlsRules { id roleId filterExpr } }")
        assert isinstance(data["data"]["rlsRules"], list)


class TestAvailableSchemas:
    async def test_available_schemas_for_pet_store_pg(self, client):
        # When native_schemas() returns None (no direct-driver pool for this source),
        # the resolver falls through to the engine's connector registry via
        # state.federation_engine.engine.reachable(source_type) (schema_query.py:324).
        # This must resolve cleanly to a list — never an AttributeError.
        data = await _gql(client, '{ availableSchemas(sourceId: "pet-store-pg") }')
        assert "errors" not in data
        assert isinstance(data["data"]["availableSchemas"], list)

    async def test_available_schemas_unknown_source_degrades(self, client):
        # An unknown source_id has no direct-driver pool and is not engine-reachable,
        # so the resolver returns an empty list rather than erroring.
        data = await _gql(client, '{ availableSchemas(sourceId: "no-such-source-xyz") }')
        assert "errors" not in data
        assert data["data"]["availableSchemas"] == []


class TestAvailableTables:
    async def test_available_tables_for_pet_store_pg(self, client):
        data = await _gql(
            client,
            '{ availableTables(sourceId: "pet-store-pg", schemaName: "pet_store") { name comment } }',
        )
        assert isinstance(data["data"]["availableTables"], list)


class TestAvailableFunctions:
    async def test_available_functions_non_openapi_returns_empty(self, client):
        data = await _gql(
            client,
            '{ availableFunctions(sourceId: "pet-store-pg", schemaName: "public") { name } }',
        )
        assert data["data"]["availableFunctions"] == []

    async def test_available_functions_unknown_source_returns_empty(self, client):
        data = await _gql(client, '{ availableFunctions(sourceId: "no-such-source-xyz") { name } }')
        assert data["data"]["availableFunctions"] == []


class TestAvailableColumns:
    async def test_available_columns_unknown_table_degrades(self, client):
        data = await _gql(
            client,
            '{ availableColumns(sourceId: "pet-store-pg", schemaName: "pet_store", tableName: "no_such_table_xyz") }',
        )
        assert data["data"]["availableColumns"] == []


class TestAvailableColumnsMetadata:
    async def test_available_columns_metadata_unknown_table_degrades(self, client):
        data = await _gql(
            client,
            """
            { availableColumnsMetadata(
                sourceId: "pet-store-pg", schemaName: "pet_store", tableName: "no_such_table_xyz"
              ) { name dataType isPrimaryKey } }
            """,
        )
        assert data["data"]["availableColumnsMetadata"] == []


class TestSuggestTableAlias:
    async def test_suggest_alias_no_conflict(self, client):
        data = await _gql(
            client,
            '{ suggestTableAlias(tableName: "pets", domainId: "shelter", sourceId: "pet-store-pg") }',
        )
        assert data["data"]["suggestTableAlias"]


class TestMvList:
    async def test_mv_list(self, client):
        data = await _gql(client, "{ mvList { id status enabled } }")
        assert isinstance(data["data"]["mvList"], list)


class TestCacheStats:
    async def test_cache_stats(self, client):
        data = await _gql(client, "{ cacheStats { totalKeys storeType hitCount missCount } }")
        assert data["data"]["cacheStats"]["storeType"]


class TestCacheTableStats:
    async def test_cache_table_stats(self, client):
        data = await _gql(client, "{ cacheTableStats { tableId cachedEntries } }")
        assert isinstance(data["data"]["cacheTableStats"], list)


class TestHotTables:
    async def test_hot_tables(self, client):
        data = await _gql(client, "{ hotTables { tableName rowCount loaded isApi } }")
        assert isinstance(data["data"]["hotTables"], list)


class TestMaterializeStoreInfo:
    async def test_materialize_store_info(self, client):
        data = await _gql(client, "{ materializeStoreInfo { engineName mvCount storeRef } }")
        assert data["data"]["materializeStoreInfo"]["engineName"]


class TestSystemHealth:
    async def test_system_health(self, client):
        data = await _gql(
            client,
            """
            { systemHealth {
                engineConnected engineWorkerCount metadataPoolSize metadataDialect
                cacheMode cacheConnected mvRefreshLoopRunning
                protocols { name status port }
              } }
            """,
        )
        health = data["data"]["systemHealth"]
        assert health["metadataDialect"]


class TestScheduledTasks:
    async def test_scheduled_tasks_list(self, client):
        data = await _gql(client, "{ scheduledTasks { id name kind enabled cronExpression } }")
        assert isinstance(data["data"]["scheduledTasks"], list)


class TestGenerateDescriptions:
    async def test_generate_table_description_invalid_id_returns_empty(self, client):
        data = await _gql(client, '{ generateTableDescription(tableId: "not-a-number") }')
        assert data["data"]["generateTableDescription"] == ""

    async def test_generate_table_description_nonexistent_table(self, client):
        data = await _gql(client, '{ generateTableDescription(tableId: "999999999") }')
        assert (
            data["data"]["generateTableDescription"]
            == "Save the view first before generating descriptions"
        )

    async def test_generate_column_description_invalid_id_returns_empty(self, client):
        data = await _gql(
            client,
            '{ generateColumnDescription(tableId: "not-a-number", columnName: "id") }',
        )
        assert data["data"]["generateColumnDescription"] == ""

    async def test_generate_column_description_nonexistent_table(self, client):
        data = await _gql(
            client,
            '{ generateColumnDescription(tableId: "999999999", columnName: "id") }',
        )
        assert (
            data["data"]["generateColumnDescription"]
            == "Save the view first before generating descriptions"
        )
