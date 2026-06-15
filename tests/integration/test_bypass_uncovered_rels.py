# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for bypass_uncovered_relationships across SQL, GQL, and CQL.

Requires the full Provisa stack with the demo install (pets + shelter/graphql sources).
Run with: docker compose -f docker-compose.core.yml -f docker-compose.dev-install.yml up

Validates:
  - SQL: uncovered remote relationship joins pass V002 validation when bypass is enabled.
  - GQL: compiling an inquiry→pet→assignment query succeeds without V002.
  - CQL: submitting the displayed Cypher for a pets→assignments query produces no V002.
  - SQL: joining registered tables on wrong columns always fails with V002.
"""

from __future__ import annotations

import os
import httpx
import pytest

pytestmark = pytest.mark.requires_provisa_server

BASE_URL = os.environ.get("PROVISA_URL", "http://localhost:8000")

def _headers() -> dict:
    token = os.environ.get("PROVISA_TOKEN", "")
    return {"Authorization": f"Bearer {token}"} if token else {}


def _admin_gql(query: str) -> dict:
    resp = httpx.post(
        f"{BASE_URL}/admin/graphql",
        json={"query": query},
        headers=_headers(),
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=BASE_URL, headers=_headers(), timeout=120) as c:
        yield c


@pytest.fixture(scope="module", autouse=True)
def _register_demo_data():
    """Register demo sources/tables/relationships required by GQL and CQL tests."""
    import urllib.request

    try:
        urllib.request.urlopen(f"{BASE_URL}/health", timeout=3)
    except Exception:
        yield
        return

    _admin_gql(
        'mutation { createSource(input: {id: "inquiries-sqlite", type: "sqlite"}) { success } }'
    )
    _admin_gql(
        'mutation { createSource(input: {id: "shelter", type: "graphql_remote"}) { success } }'
    )
    _admin_gql(
        'mutation { createDomain(input: {id: "shelter", description: "Animal shelter"}) { success } }'
    )
    _admin_gql("""
        mutation {
            registerTable(input: {
                sourceId: "inquiries-sqlite", domainId: "pet-store",
                schemaName: "default", tableName: "inquiries",
                governance: "pre-approved",
                columns: [
                    {name: "id", visibleTo: ["admin", "analyst"]},
                    {name: "pet_id", visibleTo: ["admin", "analyst"]},
                    {name: "inquiry_type", visibleTo: ["admin", "analyst"]},
                    {name: "message", visibleTo: ["admin", "analyst"]},
                    {name: "status", visibleTo: ["admin", "analyst"]},
                    {name: "submitted_at", visibleTo: ["admin", "analyst"]}
                ]
            }) { success message }
        }
    """)
    _admin_gql("""
        mutation {
            registerTable(input: {
                sourceId: "shelter", domainId: "shelter",
                schemaName: "default", tableName: "assignments",
                alias: "shelter__assignments",
                governance: "pre-approved",
                columns: [
                    {name: "id", visibleTo: ["admin", "analyst"]},
                    {name: "breedName", visibleTo: ["admin", "analyst"]}
                ]
            }) { success message }
        }
    """)
    _admin_gql("""
        mutation {
            upsertRelationship(input: {
                id: "inquiries-to-pets",
                sourceTableId: "inquiries",
                targetTableId: "pets",
                sourceColumn: "pet_id",
                targetColumn: "id",
                cardinality: "many-to-one",
                alias: "HAS_PETS"
            }) { success message }
        }
    """)
    _admin_gql("""
        mutation {
            upsertRelationship(input: {
                id: "pets-to-shelter-assignments",
                sourceTableId: "pets",
                targetTableId: "shelter__assignments",
                sourceColumn: "breed_name",
                targetColumn: "breedName",
                cardinality: "many-to-one"
            }) { success message }
        }
    """)

    yield

    for rel_id in ("pets-to-shelter-assignments", "inquiries-to-pets"):
        httpx.post(
            f"{BASE_URL}/admin/graphql",
            json={"query": f'mutation {{ deleteRelationship(id: "{rel_id}") {{ success }} }}'},
            headers=_headers(),
            timeout=120,
        )
    for src_id in ("inquiries-sqlite", "shelter"):
        httpx.post(
            f"{BASE_URL}/admin/graphql",
            json={"query": f'mutation {{ deleteSource(id: "{src_id}") {{ success }} }}'},
            headers=_headers(),
            timeout=120,
        )
    httpx.post(
        f"{BASE_URL}/admin/graphql",
        json={"query": 'mutation { deleteDomain(id: "shelter") { success } }'},
        headers=_headers(),
        timeout=120,
    )


# ---------------------------------------------------------------------------
# SQL — /data/sql endpoint
# ---------------------------------------------------------------------------


class TestSqlBypassUncovered:
    def test_uncovered_remote_join_passes_validation(self, client):
        """SQL JOIN from pets (postgresql) to inquiries (sqlite/remote) on an unregistered
        column pair must not produce V002 — bypass_uncovered_relationships applies because
        at least one side is a non-postgresql remote source."""
        resp = client.post(
            "/data/sql",
            json={
                "sql": (
                    "SELECT p.id, p.name, i.id AS inquiry_id "
                    'FROM "pet_store"."pets" AS p '
                    'LEFT JOIN "pet_store"."inquiries" AS i ON i.pet_id = p.id'
                ),
                "role": "admin",
            },
        )
        # 200 with data or 400 if Trino can't execute — V002 must never appear
        if resp.status_code == 200:
            data = resp.json()
            violations = data.get("violations") or []
            v002 = [v for v in violations if v.get("code") == "V002"]
            assert v002 == [], f"Unexpected V002 on remote join with bypass: {v002}"
        else:
            # 403 with V002 is the bug — any other non-200 is an execution issue, not governance
            if resp.status_code == 403:
                data = resp.json()
                detail = data.get("detail")
                violations = detail.get("violations") or [] if isinstance(detail, dict) else []
                v002 = [v for v in violations if v.get("code") == "V002"]
                assert v002 == [], (
                    f"V002 raised for uncovered remote join — bypass not applied: {v002}"
                )

    def test_wrong_column_join_fails_v002(self, client):
        """SQL joining two local postgresql tables on a non-registered column pair must fail
        with V002 — bypass does NOT apply when both sides are local (postgresql) sources."""
        resp = client.post(
            "/data/sql",
            json={
                "sql": (
                    "SELECT p.id, i.id "
                    'FROM "pet_store"."pets" AS p '
                    'LEFT JOIN "pet_store"."inquiries" AS i ON i.id = p.breed_name'
                ),
                "role": "admin",
            },
        )
        # 200 with violations, or 403 with violations — either is acceptable; V002 must be present
        detail = resp.json().get("detail") if resp.status_code == 403 else resp.json()
        violations = (detail or {}).get("violations") or [] if isinstance(detail, dict) else []
        v002 = [v for v in violations if v.get("code") == "V002"]
        if resp.status_code in (200, 403):
            assert v002, f"Expected V002 for wrong-column join on local tables, got: {violations}"
        else:
            assert resp.status_code in (400, 422), f"Unexpected status: {resp.text}"


# ---------------------------------------------------------------------------
# GQL — /data/graphql endpoint
# ---------------------------------------------------------------------------


class TestGqlBypassUncovered:
    def test_gql_inquiries_pet_assignment_no_v002(self, client):
        """GQL query traversing inquiries→pet→assignment must compile and execute
        without V002 violations (pets→shelter join uses bypass for the remote source)."""
        schema_resp = client.post(
            "/data/graphql",
            json={"query": "{ __schema { queryType { fields { name } } } }", "role": "admin"},
        )
        if schema_resp.status_code == 200:
            fields = [
                f["name"]
                for f in (schema_resp.json().get("data", {}) or {})
                .get("__schema", {})
                .get("queryType", {})
                .get("fields", [])
            ]
            if "ps__inquiries" not in fields or "ps__pets" not in fields:
                pytest.skip("ps__inquiries or ps__pets not in schema — demo pet-store domain not loaded")
        resp = client.post(
            "/data/graphql",
            json={
                "query": """
                    query {
                        ps__inquiries {
                            id
                            inquiryType
                            pet {
                                name
                                assignment {
                                    breedName
                                }
                            }
                        }
                    }
                """,
                "role": "admin",
            },
        )
        assert resp.status_code == 200, f"Unexpected status: {resp.text}"
        body = resp.json()
        errors = body.get("errors") or []
        v002_errors = [e for e in errors if "V002" in str(e)]
        assert v002_errors == [], f"Unexpected V002 in GQL response: {v002_errors}"


# ---------------------------------------------------------------------------
# CQL (Cypher) — /data/cypher endpoint
# ---------------------------------------------------------------------------


class TestCqlBypassUncovered:
    def test_cql_pets_assignment_correct_rel_type(self, client):
        """Cypher query using IS_ASSIGNMENT (pets→assignments forward rel) must not
        produce a V002 — the translated ON condition must use breed_name/breedName."""
        resp = client.post(
            "/data/cypher",
            json={
                "query": "MATCH (p:Pets)-[:IS_ASSIGNMENT]->(a:Assignments) RETURN p, a LIMIT 10",
                "role": "admin",
            },
        )
        # Endpoint returns 200 with data or violations; 400 only for parse errors
        assert resp.status_code in (200, 400), f"Unexpected status: {resp.text}"
        if resp.status_code == 200:
            data = resp.json()
            violations = data.get("violations") or []
            v002 = [v for v in violations if v.get("code") == "V002"]
            assert v002 == [], f"IS_ASSIGNMENT Cypher should not produce V002: {v002}"

    def test_cql_inquiries_full_traversal_no_v002(self, client):
        """Multi-hop Cypher traversal across Inquiries→Pets→Assignments→Employees must
        produce no V002.  HAS_PETS is the registered alias for inquiries-to-pets
        (pet_id/id); IS_ASSIGNMENT is the alias for pets-to-shelter-assignments
        (breed_name/breedName); Assignments and Employees share the same remote source
        so IS_EMPLOYEE is covered by bypass_uncovered_relationships."""
        query = (
            "MATCH (a:Inquiries) "
            "OPTIONAL MATCH (a:Inquiries)-[:HAS_PETS]->(b:Pets) "
            "OPTIONAL MATCH (b:Pets)-[:IS_ASSIGNMENT]->(c:Assignments) "
            "OPTIONAL MATCH (c:Assignments)-[:IS_EMPLOYEE]->(d:Employees) "
            "RETURN a, b, c, d LIMIT 25"
        )
        resp = client.post(
            "/data/cypher",
            json={"query": query, "role": "admin"},
        )
        assert resp.status_code in (200, 400), f"Unexpected status: {resp.text}"
        if resp.status_code == 200:
            data = resp.json()
            violations = data.get("violations") or []
            v002 = [v for v in violations if v.get("code") == "V002"]
            assert v002 == [], (
                f"Full Inquiries→Pets→Assignments→Employees traversal should not produce V002: {v002}"
            )
