# Copyright (c) 2026 Kenneth Stott
# Canary: e2585ac5-eefd-42d0-b243-1316b3717560
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Admin GraphQL Mutation resolvers (schema_mutation.py)."""

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


class TestRenameSource:
    async def test_rename_source_empty_new_id(self, client):
        data = await _gql(
            client,
            'mutation { renameSource(oldId: "pet-store-pg", newId: "") { success message } }',
        )
        result = data["data"]["renameSource"]
        assert result["success"] is False
        assert result["message"] == "New ID must not be empty"

    async def test_rename_source_not_found(self, client):
        data = await _gql(
            client,
            'mutation { renameSource(oldId: "no-such-source-xyz", newId: "new-id") { success message } }',
        )
        result = data["data"]["renameSource"]
        assert result["success"] is False
        assert result["message"] == "Source 'no-such-source-xyz' not found"


class TestDeleteSource:
    async def test_delete_source_not_found(self, client):
        data = await _gql(
            client, 'mutation { deleteSource(id: "no-such-source-xyz") { success message } }'
        )
        result = data["data"]["deleteSource"]
        assert result["success"] is False
        assert result["message"] == "Source 'no-such-source-xyz' not found"


class TestUpdateSource:
    async def test_update_source_not_found(self, client):
        data = await _gql(
            client,
            """
            mutation {
                updateSource(input: { id: "no-such-source-xyz", type: "postgresql" }) {
                    success message
                }
            }
            """,
        )
        result = data["data"]["updateSource"]
        assert result["success"] is False
        assert result["message"] == "Source 'no-such-source-xyz' not found"


class TestDomainLifecycle:
    async def test_create_and_delete_domain(self, client):
        create = await _gql(
            client,
            """
            mutation {
                createDomain(input: { id: "smtest-domain-1", description: "smtest" }) {
                    success message
                }
            }
            """,
        )
        assert create["data"]["createDomain"]["success"] is True

        delete = await _gql(
            client, 'mutation { deleteDomain(id: "smtest-domain-1") { success message } }'
        )
        assert delete["data"]["deleteDomain"]["success"] is True
        assert delete["data"]["deleteDomain"]["message"] == "Domain 'smtest-domain-1' deleted"

    async def test_delete_domain_not_found(self, client):
        data = await _gql(
            client, 'mutation { deleteDomain(id: "no-such-domain-xyz") { success message } }'
        )
        result = data["data"]["deleteDomain"]
        assert result["success"] is False
        assert result["message"] == "Domain 'no-such-domain-xyz' not found"


class TestRoleLifecycle:
    async def test_create_and_delete_role(self, client):
        create = await _gql(
            client,
            """
            mutation {
                createRole(input: {
                    id: "smtest-role-1",
                    capabilities: ["query_development"],
                    domainAccess: ["pet-store"]
                }) { success message }
            }
            """,
        )
        assert create["data"]["createRole"]["success"] is True

        delete = await _gql(
            client, 'mutation { deleteRole(id: "smtest-role-1") { success message } }'
        )
        assert delete["data"]["deleteRole"]["success"] is True
        assert delete["data"]["deleteRole"]["message"] == "Role 'smtest-role-1' deleted"

    async def test_delete_role_not_found(self, client):
        data = await _gql(
            client, 'mutation { deleteRole(id: "no-such-role-xyz") { success message } }'
        )
        result = data["data"]["deleteRole"]
        assert result["success"] is False
        assert result["message"] == "Role 'no-such-role-xyz' not found"


class TestDeleteTable:
    async def test_delete_table_not_found(self, client):
        data = await _gql(client, "mutation { deleteTable(id: 999999999) { success message } }")
        result = data["data"]["deleteTable"]
        assert result["success"] is False
        assert result["message"] == "Table 999999999 not found"


class TestRlsRule:
    async def test_upsert_and_delete_rls_rule(self, client):
        upsert = await _gql(
            client,
            """
            mutation {
                upsertRlsRule(input: {
                    domainId: "pet-store",
                    roleId: "analyst",
                    filterExpr: "1 = 1"
                }) { success message }
            }
            """,
        )
        assert upsert["data"]["upsertRlsRule"]["success"] is True

        delete = await _gql(
            client,
            'mutation { deleteRlsRule(roleId: "analyst", domainId: "pet-store") { success message } }',
        )
        assert delete["data"]["deleteRlsRule"]["success"] is True
        assert delete["data"]["deleteRlsRule"]["message"] == "RLS rule deleted"

    async def test_delete_rls_rule_not_found(self, client):
        data = await _gql(
            client,
            'mutation { deleteRlsRule(roleId: "no-such-role-xyz", domainId: "pet-store") { success message } }',
        )
        result = data["data"]["deleteRlsRule"]
        assert result["success"] is False
        assert result["message"] == "RLS rule not found"


class TestCreationRequests:
    async def test_execute_creation_request_not_found(self, client):
        data = await _gql(
            client, "mutation { executeCreationRequest(requestId: 999999999) { success message } }"
        )
        result = data["data"]["executeCreationRequest"]
        assert result["success"] is False
        assert result["message"] == "Request not found or already resolved"

    async def test_reject_creation_request_empty_reason(self, client):
        data = await _gql(
            client,
            'mutation { rejectCreationRequest(requestId: 1, reason: "") { success message } }',
        )
        result = data["data"]["rejectCreationRequest"]
        assert result["success"] is False
        assert result["message"] == "A rejection reason is required"

    async def test_reject_creation_request_not_found(self, client):
        data = await _gql(
            client,
            'mutation { rejectCreationRequest(requestId: 999999999, reason: "no thanks") { success message } }',
        )
        result = data["data"]["rejectCreationRequest"]
        assert result["success"] is False
        assert result["message"] == "Request not found or already resolved"


class TestRelationship:
    async def test_upsert_relationship_invalid_cardinality(self, client):
        data = await _gql(
            client,
            """
            mutation {
                upsertRelationship(input: {
                    id: "smtest-rel-1",
                    sourceTableId: "pets",
                    sourceColumn: "id",
                    cardinality: "not-a-real-cardinality"
                }) { success message }
            }
            """,
        )
        result = data["data"]["upsertRelationship"]
        assert result["success"] is False
        assert result["message"] == "Invalid cardinality: 'not-a-real-cardinality'"

    async def test_delete_relationship_not_found(self, client):
        data = await _gql(
            client,
            'mutation { deleteRelationship(id: "no-such-relationship-xyz") { success message } }',
        )
        result = data["data"]["deleteRelationship"]
        assert result["success"] is False
        assert result["message"] == "Relationship 'no-such-relationship-xyz' not found"


class TestCacheAndMaterializedSettings:
    async def test_update_source_cache_not_found(self, client):
        data = await _gql(
            client,
            'mutation { updateSourceCache(sourceId: "no-such-source-xyz", cacheEnabled: true) { success message } }',
        )
        result = data["data"]["updateSourceCache"]
        assert result["success"] is False
        assert result["message"] == "Source 'no-such-source-xyz' not found"

    async def test_update_table_cache_not_found(self, client):
        data = await _gql(
            client, "mutation { updateTableCache(tableId: 999999999) { success message } }"
        )
        result = data["data"]["updateTableCache"]
        assert result["success"] is False
        assert result["message"] == "Table 999999999 not found"

    async def test_update_source_prefer_materialized_not_found(self, client):
        data = await _gql(
            client,
            'mutation { updateSourcePreferMaterialized(sourceId: "no-such-source-xyz", preferMaterialized: true) { success message } }',
        )
        result = data["data"]["updateSourcePreferMaterialized"]
        assert result["success"] is False
        assert result["message"] == "Source 'no-such-source-xyz' not found"

    async def test_update_table_prefer_materialized_not_found(self, client):
        data = await _gql(
            client,
            "mutation { updateTablePreferMaterialized(tableId: 999999999) { success message } }",
        )
        result = data["data"]["updateTablePreferMaterialized"]
        assert result["success"] is False
        assert result["message"] == "Table 999999999 not found"


class TestNamingConvention:
    async def test_update_gql_naming_convention_invalid(self, client):
        data = await _gql(
            client,
            'mutation { updateGqlNamingConvention(convention: "not-a-real-convention") { success message } }',
        )
        result = data["data"]["updateGqlNamingConvention"]
        assert result["success"] is False

    async def test_update_gql_naming_convention_valid(self, client):
        data = await _gql(
            client,
            'mutation { updateGqlNamingConvention(convention: "apollo_graphql") { success message } }',
        )
        result = data["data"]["updateGqlNamingConvention"]
        assert result["success"] is True
        assert result["message"] == "Naming convention set to 'apollo_graphql'"

    async def test_update_source_naming_not_found(self, client):
        data = await _gql(
            client,
            'mutation { updateSourceNaming(sourceId: "no-such-source-xyz") { success message } }',
        )
        result = data["data"]["updateSourceNaming"]
        assert result["success"] is False
        assert result["message"] == "Source 'no-such-source-xyz' not found"

    async def test_update_source_allowed_domains_not_found(self, client):
        data = await _gql(
            client,
            'mutation { updateSourceAllowedDomains(sourceId: "no-such-source-xyz", allowedDomains: []) { success message } }',
        )
        result = data["data"]["updateSourceAllowedDomains"]
        assert result["success"] is False
        assert result["message"] == "Source 'no-such-source-xyz' not found"

    async def test_update_table_naming_not_found(self, client):
        data = await _gql(
            client, "mutation { updateTableNaming(tableId: 999999999) { success message } }"
        )
        result = data["data"]["updateTableNaming"]
        assert result["success"] is False
        assert result["message"] == "Table 999999999 not found"


class TestMvManagement:
    async def test_refresh_mv_not_found(self, client):
        data = await _gql(
            client, 'mutation { refreshMv(mvId: "no-such-mv-xyz") { success message } }'
        )
        result = data["data"]["refreshMv"]
        assert result["success"] is False
        assert result["message"] == "MV 'no-such-mv-xyz' not found"

    async def test_toggle_mv_not_found(self, client):
        data = await _gql(
            client,
            'mutation { toggleMv(mvId: "no-such-mv-xyz", enabled: true) { success message } }',
        )
        result = data["data"]["toggleMv"]
        assert result["success"] is False
        assert result["message"] == "MV 'no-such-mv-xyz' not found"


class TestCacheManagement:
    async def test_purge_cache(self, client):
        data = await _gql(client, "mutation { purgeCache { success message } }")
        result = data["data"]["purgeCache"]
        assert result["success"] is True

    async def test_purge_cache_by_table(self, client):
        data = await _gql(client, "mutation { purgeCacheByTable(tableId: 1) { success message } }")
        result = data["data"]["purgeCacheByTable"]
        assert result["success"] is True


class TestInvalidateFileSource:
    async def test_invalidate_file_source_not_found(self, client):
        data = await _gql(
            client, "mutation { invalidateFileSource(tableId: 999999999) { success message } }"
        )
        result = data["data"]["invalidateFileSource"]
        assert result["success"] is False
        assert result["message"] == "Table 999999999 not found"

    async def test_invalidate_file_source_non_sqlite(self, client):
        # Look up a table on the postgresql source (pets, on pet-store-pg) — not sqlite.
        tables = await _gql(client, "{ tables { id sourceId tableName } }")
        pg_table = next(t for t in tables["data"]["tables"] if t["sourceId"] == "pet-store-pg")
        data = await _gql(
            client,
            f"mutation {{ invalidateFileSource(tableId: {pg_table['id']}) {{ success message }} }}",
        )
        result = data["data"]["invalidateFileSource"]
        assert result["success"] is False
        assert result["message"] == "Source type 'postgresql' is not sqlite"


class TestScheduledTasks:
    async def test_toggle_scheduled_task_not_found(self, client):
        data = await _gql(
            client,
            'mutation { toggleScheduledTask(taskId: "no-such-task-xyz", enabled: true) { success message } }',
        )
        result = data["data"]["toggleScheduledTask"]
        assert result["success"] is False
        assert result["message"] == "Task 'no-such-task-xyz' not found"

    async def test_delete_scheduled_task_not_found(self, client):
        data = await _gql(
            client,
            'mutation { deleteScheduledTask(taskId: "no-such-task-xyz") { success message } }',
        )
        result = data["data"]["deleteScheduledTask"]
        assert result["success"] is False
        assert result["message"] == "Task 'no-such-task-xyz' not found"

    async def test_create_scheduled_task_unknown_kind(self, client):
        data = await _gql(
            client,
            """
            mutation {
                createScheduledTask(
                    id: "smtest-task-1", name: "n", cron: "* * * * *", kind: "carrier-pigeon"
                ) { success message }
            }
            """,
        )
        result = data["data"]["createScheduledTask"]
        assert result["success"] is False
        assert result["message"] == "Unknown trigger kind 'carrier-pigeon'"

    async def test_create_scheduled_task_missing_required_fields(self, client):
        data = await _gql(
            client,
            """
            mutation {
                createScheduledTask(id: "", name: "", cron: "", kind: "sql") { success message }
            }
            """,
        )
        result = data["data"]["createScheduledTask"]
        assert result["success"] is False
        assert result["message"] == "id, name, and cron are required"

    async def test_create_scheduled_task_webhook_missing_name(self, client):
        data = await _gql(
            client,
            """
            mutation {
                createScheduledTask(
                    id: "smtest-task-2", name: "n", cron: "* * * * *", kind: "webhook"
                ) { success message }
            }
            """,
        )
        result = data["data"]["createScheduledTask"]
        assert result["success"] is False
        assert result["message"] == "webhook_name is required for a webhook trigger"

    async def test_create_scheduled_task_webhook_not_found(self, client):
        data = await _gql(
            client,
            """
            mutation {
                createScheduledTask(
                    id: "smtest-task-3", name: "n", cron: "* * * * *", kind: "webhook",
                    webhookName: "no-such-webhook-xyz"
                ) { success message }
            }
            """,
        )
        result = data["data"]["createScheduledTask"]
        assert result["success"] is False
        assert result["message"] == "Webhook 'no-such-webhook-xyz' not found"

    async def test_create_scheduled_task_sql_missing_sql(self, client):
        data = await _gql(
            client,
            """
            mutation {
                createScheduledTask(
                    id: "smtest-task-4", name: "n", cron: "* * * * *", kind: "sql"
                ) { success message }
            }
            """,
        )
        result = data["data"]["createScheduledTask"]
        assert result["success"] is False
        assert result["message"] == "sql is required for a SQL trigger"

    async def test_create_and_delete_sql_task(self, client):
        create = await _gql(
            client,
            """
            mutation {
                createScheduledTask(
                    id: "smtest-task-5", name: "n", cron: "* * * * *", kind: "sql",
                    sql: "SELECT 1"
                ) { success message }
            }
            """,
        )
        result = create["data"]["createScheduledTask"]
        assert result["success"] is True
        assert result["message"] == "Scheduled task 'smtest-task-5' created"

        dup = await _gql(
            client,
            """
            mutation {
                createScheduledTask(
                    id: "smtest-task-5", name: "n", cron: "* * * * *", kind: "sql",
                    sql: "SELECT 1"
                ) { success message }
            }
            """,
        )
        dup_result = dup["data"]["createScheduledTask"]
        assert dup_result["success"] is False
        assert dup_result["message"] == "Trigger 'smtest-task-5' already exists"

        delete = await _gql(
            client, 'mutation { deleteScheduledTask(taskId: "smtest-task-5") { success message } }'
        )
        assert delete["data"]["deleteScheduledTask"]["success"] is True
        assert delete["data"]["deleteScheduledTask"]["message"] == "Task 'smtest-task-5' deleted"


class TestDeployViewToDb:
    async def test_deploy_view_to_db_not_found(self, client):
        data = await _gql(
            client, "mutation { deployViewToDb(tableId: 999999999) { success message } }"
        )
        result = data["data"]["deployViewToDb"]
        assert result["success"] is False
        assert result["message"] == "Table 999999999 not found"


class TestRebuildSchemas:
    async def test_rebuild_schemas_success(self, client):
        data = await _gql(client, "mutation { rebuildSchemas { success message } }")
        result = data["data"]["rebuildSchemas"]
        assert result["success"] is True
        assert result["message"] == "Schemas rebuilt"
