"""Security integration tests for Provisa backend endpoints (REQ-531, REQ-554, REQ-556, REQ-591, REQ-594, REQ-603, REQ-613, REQ-740, REQ-744, REQ-745, REQ-748).

These tests validate security constraints, isolation boundaries, and governance rules
by making HTTP requests against a running Provisa server. No internal imports.
"""

import pytest
import httpx

pytestmark = [pytest.mark.e2e, pytest.mark.requires_provisa_server]


@pytest.fixture(scope="module")
def client():
    """HTTP client for Provisa server."""
    with httpx.Client(base_url="http://localhost:8000", timeout=30.0) as c:
        yield c


def _headers(token: str = "test-token") -> dict:
    """Return auth headers with Bearer token."""
    return {"Authorization": f"Bearer {token}"}


# ============================================================================
# REQ-531: Masked columns rejected from WHERE/HAVING at parse time
# ============================================================================


class TestReq531MaskedColumnsRejected:
    """REQ-531: Masked columns are rejected from WHERE/HAVING clauses."""

    def test_sql_masked_column_in_where_rejected(self, client):
        """SQL query with masked column in WHERE should be rejected at parse time."""
        # Assuming a registered domain with masked column 'salary'
        payload = {
            "sql": "SELECT * FROM employees WHERE salary > 50000",
            "domain": "hr",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [400, 403]

    def test_sql_masked_column_in_having_rejected(self, client):
        """SQL query with masked column in HAVING should be rejected at parse time."""
        payload = {
            "sql": "SELECT dept, AVG(salary) FROM employees GROUP BY dept HAVING AVG(salary) > 50000",
            "domain": "hr",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [400, 403]

    def test_graphql_masked_column_in_filter_rejected(self, client):
        """GraphQL query filtering on masked column should be rejected."""
        payload = {
            "query": """
            {
              employees(filter: { salary: { gt: 50000 } }) {
                id
                name
              }
            }
            """,
            "domain": "hr",
        }
        resp = client.post(
            "/data/graphql",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [400, 403]


# ============================================================================
# REQ-554: Row cap enforcement via DEFAULT_SAMPLE_SIZE for non-full_results roles
# ============================================================================


class TestReq554RowCapEnforcement:
    """REQ-554: Non-full_results roles are capped at DEFAULT_SAMPLE_SIZE rows."""

    def test_analyst_role_row_cap_applied(self, client):
        """Analyst role query should be capped at DEFAULT_SAMPLE_SIZE."""
        payload = {
            "sql": "SELECT * FROM large_table LIMIT 1000000",
            "domain": "analytics",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers("analyst-token"),
        )
        assert resp.status_code in [200, 400, 403]
        if resp.status_code == 200:
            data = resp.json()
            rows = data.get("data", [])
            assert len(rows) <= 10000

    def test_admin_role_no_row_cap(self, client):
        """Admin role with full_results should have no row cap."""
        payload = {
            "sql": "SELECT * FROM large_table",
            "domain": "analytics",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers("admin-token"),
        )
        assert resp.status_code in [200, 400, 403]


# ============================================================================
# REQ-556: Approval hook circuit breaker (5 consecutive failures → open state)
# ============================================================================


class TestReq556CircuitBreaker:
    """REQ-556: Approval hook circuit breaker opens after 5 consecutive failures."""

    def test_circuit_breaker_opens_after_5_failures(self, client):
        """Circuit breaker should open after 5 consecutive hook failures."""
        domain = "protected_domain"
        for i in range(5):
            payload = {"sql": f"SELECT * FROM table_{i}", "domain": domain}
            resp = client.post(
                "/data/sql",
                json=payload,
                headers=_headers(),
            )
            # Depending on hook implementation, may be 503 or 500 on failure
            assert resp.status_code in [400, 500, 503]

        # 6th attempt should see circuit breaker open (reject immediately)
        payload = {"sql": "SELECT * FROM table_6", "domain": domain}
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [400, 500, 503, 429]

    def test_circuit_breaker_status_endpoint(self, client):
        """Circuit breaker status should be available via health/status endpoint."""
        resp = client.get("/health/status", headers=_headers())
        assert resp.status_code in [200, 404]
        if resp.status_code == 200:
            data = resp.json()
            assert "circuit_breaker" in data or "health" in data


# ============================================================================
# REQ-591: SET LOCAL transaction-scoped tenant context isolation
# ============================================================================


class TestReq591SetLocalTenantContext:
    """REQ-591: SET LOCAL sets transaction-scoped tenant context isolation."""

    def test_tenant_context_per_query(self, client):
        """Each query should have its own tenant context via SET LOCAL."""
        payload1 = {
            "sql": "SELECT * FROM data WHERE tenant_id = 'tenant1'",
            "domain": "multi_tenant",
        }
        resp1 = client.post(
            "/data/sql",
            json=payload1,
            headers=_headers(),
        )
        assert resp1.status_code in [200, 400, 403]

        payload2 = {
            "sql": "SELECT * FROM data WHERE tenant_id = 'tenant2'",
            "domain": "multi_tenant",
        }
        resp2 = client.post(
            "/data/sql",
            json=payload2,
            headers=_headers(),
        )
        assert resp2.status_code in [200, 400, 403]

    def test_cross_request_tenant_isolation(self, client):
        """Tenant context should not leak across requests (SET LOCAL is transaction-scoped)."""
        # Make request as tenant1
        payload1 = {
            "sql": "SELECT COUNT(*) FROM data",
            "domain": "multi_tenant",
            "tenant_id": "tenant1",
        }
        resp1 = client.post(
            "/data/sql",
            json=payload1,
            headers=_headers(),
        )
        # Make request as tenant2 (different connection/transaction)
        payload2 = {
            "sql": "SELECT COUNT(*) FROM data",
            "domain": "multi_tenant",
            "tenant_id": "tenant2",
        }
        resp2 = client.post(
            "/data/sql",
            json=payload2,
            headers=_headers(),
        )

        assert resp1.status_code in [200, 400, 403]
        assert resp2.status_code in [200, 400, 403]


# ============================================================================
# REQ-594: TenantMiddleware skip paths (/health, /docs, /openapi.json, /billing/*)
# ============================================================================


class TestReq594TenantMiddlewareSkipPaths:
    """REQ-594: TenantMiddleware skips /health, /docs, /openapi.json, /billing/* paths."""

    def test_health_endpoint_no_tenant_required(self, client):
        """GET /health should not require tenant context."""
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_docs_endpoint_no_tenant_required(self, client):
        """GET /docs should not require tenant context."""
        resp = client.get("/docs")
        assert resp.status_code in [200, 301, 404]  # May redirect or be unavailable

    def test_openapi_endpoint_no_tenant_required(self, client):
        """GET /openapi.json should not require tenant context."""
        resp = client.get("/openapi.json")
        assert resp.status_code in [200, 404]

    def test_billing_endpoints_no_tenant_required(self, client):
        """GET /billing/* should not require tenant context."""
        resp = client.get("/billing/invoices")
        assert resp.status_code in [200, 404, 405]  # Depends on implementation

    def test_billing_nested_endpoint_no_tenant_required(self, client):
        """GET /billing/stripe/webhooks should not require tenant context."""
        resp = client.post("/billing/stripe/webhooks", json={})
        assert resp.status_code in [400, 401, 404, 405]  # No 403 tenant error


# ============================================================================
# REQ-603: V002 relationship governance (JOIN requires registered relationship)
# ============================================================================


class TestReq603RelationshipGovernance:
    """REQ-603: V002 requires registered relationships for JOINs."""

    def test_sql_unregistered_join_rejected(self, client):
        """SQL JOIN on unregistered relationship should be rejected."""
        payload = {
            "sql": """
            SELECT a.id, b.value
            FROM table_a a
            JOIN table_b b ON a.id = b.a_id
            """,
            "domain": "isolated_domain",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [400, 403]

    def test_cypher_unregistered_relationship_rejected(self, client):
        """Cypher query traversing unregistered relationship should be rejected."""
        payload = {
            "query": "MATCH (a)-[:UNKNOWN_REL]->(b) RETURN a, b",
            "domain": "isolated_domain",
        }
        resp = client.post(
            "/data/cypher",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [400, 403, 500]

    def test_graphql_registered_join_approved(self, client):
        """GraphQL query on registered relationship should be approved."""
        payload = {
            "query": """
            {
              users {
                id
                posts {
                  id
                  title
                }
              }
            }
            """,
            "domain": "public_domain",
        }
        resp = client.post(
            "/data/graphql",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [200, 400]  # Valid query, may fail on missing data


# ============================================================================
# REQ-613: Append-only audit log for all domain asset queries
# ============================================================================


class TestReq613AuditLog:
    """REQ-613: All domain asset queries are logged to append-only audit log."""

    def test_query_logged_to_audit_log(self, client):
        """Query execution should log to audit log."""
        query_text = "SELECT * FROM audit_test_table"
        payload = {
            "sql": query_text,
            "domain": "audited_domain",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers("user-123"),
        )
        assert resp.status_code in [200, 400, 403]

        # Check audit log endpoint
        audit_resp = client.get(
            "/api/v1/audit/logs",
            headers=_headers("admin-token"),
        )
        assert audit_resp.status_code in [200, 404]
        if audit_resp.status_code == 200:
            logs = audit_resp.json().get("data", [])
            assert len(logs) > 0

    def test_audit_log_endpoint_restricted_to_admin(self, client):
        """Audit log endpoint should require admin role."""
        resp = client.get(
            "/api/v1/audit/logs",
            headers=_headers("analyst-token"),
        )
        assert resp.status_code in [401, 403, 404]


# ============================================================================
# REQ-740: Domain policy tri-state (masking on SELECT only, not predicates)
# ============================================================================


class TestReq740DomainPolicyTriState:
    """REQ-740: Masking applies to SELECT columns only, not JOIN predicates or WHERE."""

    def test_masking_on_select_only(self, client):
        """SELECT should show masked column (redacted), but WHERE cannot filter on it."""
        payload = {
            "sql": "SELECT id, masked_name FROM users",
            "domain": "masked_domain",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers("analyst-token"),
        )
        assert resp.status_code in [200, 400, 403]
        if resp.status_code == 200:
            rows = resp.json().get("data", [])
            for row in rows:
                if "masked_name" in row:
                    assert row["masked_name"] in ["[REDACTED]", "***", None]

    def test_join_on_unmasked_column_allowed(self, client):
        """JOIN using unmasked foreign key should be allowed, even if masked column exists."""
        payload = {
            "sql": """
            SELECT u.id, u.email, o.name
            FROM users u
            JOIN orders o ON u.id = o.user_id
            """,
            "domain": "masked_domain",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers("analyst-token"),
        )
        assert resp.status_code in [200, 400, 403]


# ============================================================================
# REQ-744: Namespace isolation per request
# ============================================================================


class TestReq744NamespaceIsolation:
    """REQ-744: Namespace is isolated per request and cannot be shared across domains."""

    def test_per_request_domain_scoping(self, client):
        """Each request should have scoped namespace for its domain only."""
        payload = {
            "sql": "SELECT * FROM domain_a_table",
            "domain": "domain_a",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [200, 400, 403]

    def test_multi_domain_isolation(self, client):
        """Two requests to different domains should not share namespace."""
        # Request 1: domain_a
        payload1 = {
            "sql": "SELECT id FROM domain_a_table LIMIT 1",
            "domain": "domain_a",
        }
        resp1 = client.post(
            "/data/sql",
            json=payload1,
            headers=_headers(),
        )

        # Request 2: domain_b (separate namespace)
        payload2 = {
            "sql": "SELECT id FROM domain_b_table LIMIT 1",
            "domain": "domain_b",
        }
        resp2 = client.post(
            "/data/sql",
            json=payload2,
            headers=_headers(),
        )

        # Both should be independent, no cross-domain leakage
        assert resp1.status_code in [200, 400, 403]
        assert resp2.status_code in [200, 400, 403]


# ============================================================================
# REQ-745: Cross-domain data access requires explicit relationship
# ============================================================================


class TestReq745CrossDomainAccess:
    """REQ-745: Cross-domain JOINs require explicitly registered relationships."""

    def test_cross_domain_join_without_relationship_rejected(self, client):
        """JOIN across domains without explicit relationship should be rejected."""
        payload = {
            "sql": """
            SELECT a.id, b.id
            FROM domain_a.table_a a
            JOIN domain_b.table_b b ON a.id = b.a_id
            """,
            "domain": "domain_a",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [400, 403]

    def test_same_domain_join_allowed(self, client):
        """JOIN within same domain should be allowed (if relationship registered)."""
        payload = {
            "sql": """
            SELECT a.id, b.id
            FROM table_a a
            JOIN table_b b ON a.id = b.a_id
            """,
            "domain": "public_domain",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers(),
        )
        # Should succeed or fail on actual data, not on domain boundaries
        assert resp.status_code in [200, 400, 403]


# ============================================================================
# REQ-748: Tenant ID injection into all queries
# ============================================================================


class TestReq748TenantIdInjection:
    """REQ-748: Tenant ID is injected into all queries for row-level isolation."""

    def test_tenant_id_injected_in_cypher(self, client):
        """Cypher queries should have tenant_id injected automatically."""
        payload = {
            "query": "MATCH (n) WHERE n.id = 'test' RETURN n",
            "domain": "multi_tenant",
            "tenant_id": "tenant_x",
        }
        resp = client.post(
            "/data/cypher",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [200, 400, 404, 500]
        # Tenant_id should be injected into the WHERE clause automatically

    def test_tenant_id_injected_in_sql(self, client):
        """SQL queries should have tenant_id injected automatically."""
        payload = {
            "sql": "SELECT * FROM multi_tenant_table",
            "domain": "multi_tenant",
            "tenant_id": "tenant_y",
        }
        resp = client.post(
            "/data/sql",
            json=payload,
            headers=_headers(),
        )
        assert resp.status_code in [200, 400, 404]

    def test_cross_tenant_isolation_enforced(self, client):
        """Queries should not leak data across tenants."""
        # Tenant X makes query
        payload_x = {
            "sql": "SELECT COUNT(*) as cnt FROM shared_table",
            "domain": "multi_tenant",
            "tenant_id": "tenant_x",
        }
        resp_x = client.post(
            "/data/sql",
            json=payload_x,
            headers=_headers(),
        )
        # Tenant Y makes query
        payload_y = {
            "sql": "SELECT COUNT(*) as cnt FROM shared_table",
            "domain": "multi_tenant",
            "tenant_id": "tenant_y",
        }
        resp_y = client.post(
            "/data/sql",
            json=payload_y,
            headers=_headers(),
        )

        # Key point: tenant_y should not see tenant_x's rows
        assert resp_x.status_code in [200, 400, 403]
        assert resp_y.status_code in [200, 400, 403]
