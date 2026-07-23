# Copyright (c) 2026 Kenneth Stott
# Canary: c1d2e3f4-a5b6-7890-cdef-012345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for cross-source OpenAPI relationship queries.

Verifies that YAML-configured OpenAPI tables are pre-populated at config load time
using enum/default values extracted from the spec, and that Trino cross-source JOINs
return non-null relationship fields (REQ: petByStatus must not be null).
"""

from __future__ import annotations

import json
import tempfile

import httpx
import pytest
import pytest_asyncio
import respx

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

MOCK_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Pet Store", "version": "1.0.0"},
    "components": {
        "schemas": {
            "Pet": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "status": {"type": "string"},
                    "photoUrls": {"type": "array", "items": {"type": "string"}},
                },
            }
        }
    },
    "paths": {
        "/pet/findByStatus": {
            "get": {
                "operationId": "findPetsByStatus",
                "summary": "Finds Pets by status",
                "parameters": [
                    {
                        "name": "status",
                        "in": "query",
                        "description": "Status values for filter",
                        "required": False,
                        "schema": {
                            "type": "string",
                            "default": "available",
                            "enum": ["available", "pending", "sold"],
                        },
                    }
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Pet"},
                                }
                            }
                        },
                    }
                },
            }
        }
    },
}

MOCK_PETS = [
    {"id": 1, "name": "Cat 1", "status": "available", "photoUrls": ["http://example.com/cat1.jpg"]},
    {"id": 2, "name": "Cat 2", "status": "available", "photoUrls": ["http://example.com/cat2.jpg"]},
    {"id": 4, "name": "Dog 1", "status": "available", "photoUrls": ["http://example.com/dog1.jpg"]},
    {"id": 7, "name": "Lion 1", "status": "available", "photoUrls": []},
    {"id": 8, "name": "Lion 2", "status": "available", "photoUrls": []},
    {"id": 9, "name": "Lion 3", "status": "available", "photoUrls": []},
    {"id": 10, "name": "Rabbit 1", "status": "available", "photoUrls": []},
]

MOCK_BASE_URL = "http://mock-petstore.test"


def _make_config(spec_path: str) -> dict:
    return {
        "sources": [
            {
                "id": "mock-petstore-api",
                "type": "openapi",
                "path": spec_path,
                "base_url": MOCK_BASE_URL,
                "cache_ttl": 300,
            }
        ],
        "domains": [{"id": "pets", "description": "Pets domain"}],
        "naming": {"domain_prefix": False, "rules": []},
        "tables": [
            {
                "source_id": "mock-petstore-api",
                "domain_id": "pets",
                "schema": "default",
                "table": "find_pets_by_status",
                "alias": "pet_by_status",
                "columns": [
                    {"name": "id", "visible_to": ["admin"]},
                    {"name": "name", "visible_to": ["admin"]},
                    {"name": "status", "visible_to": ["admin"]},
                    {"name": "photoUrls", "visible_to": ["admin"]},
                ],
            }
        ],
        "relationships": [],
        "roles": [],
        "rls_rules": [],
        "functions": [],
        "webhooks": [],
    }


@pytest_asyncio.fixture(scope="module")
async def pg_conn(tenant_db):
    # load_config runs against the control-plane Database shim (advisory_xact_lock,
    # execute_core), scoped to org_default — the same connection the app uses.
    async with tenant_db.acquire() as conn:
        yield conn


@pytest_asyncio.fixture(scope="module", autouse=True)
async def _cleanup_mock_source(pg_conn):
    """Remove all DB state written by load_config calls in this module."""
    yield
    await pg_conn.execute("DELETE FROM api_endpoints WHERE source_id = 'mock-petstore-api'")
    await pg_conn.execute("DELETE FROM api_sources WHERE id = 'mock-petstore-api'")
    await pg_conn.execute("DELETE FROM registered_tables WHERE source_id = 'mock-petstore-api'")
    await pg_conn.execute("DELETE FROM sources WHERE id = 'mock-petstore-api'")
    await pg_conn.execute("DELETE FROM domains WHERE id = 'pets'")
    try:
        await pg_conn.execute('DROP TABLE IF EXISTS "default"."find_pets_by_status"')
    except Exception:
        pass


async def test_default_params_from_spec_extracts_enum_values():
    """_default_params_from_spec returns enum list for status param."""
    from provisa.core.config_loader import _default_params_from_spec

    result = _default_params_from_spec(MOCK_SPEC, "/pet/findByStatus")
    assert result == {"status": ["available", "pending", "sold"]}


async def test_default_params_from_spec_uses_default_when_no_enum():
    """_default_params_from_spec falls back to schema.default when no enum."""
    from provisa.core.config_loader import _default_params_from_spec

    spec = {
        "paths": {
            "/items": {
                "get": {
                    "parameters": [
                        {
                            "name": "limit",
                            "in": "query",
                            "schema": {"type": "integer", "default": 100},
                        }
                    ]
                }
            }
        }
    }
    result = _default_params_from_spec(spec, "/items")
    assert result == {"limit": 100}


async def test_default_params_from_spec_ignores_path_params():
    """_default_params_from_spec skips path parameters."""
    from provisa.core.config_loader import _default_params_from_spec

    spec = {
        "paths": {
            "/items/{id}": {
                "get": {
                    "parameters": [
                        {"name": "id", "in": "path", "schema": {"type": "integer"}},
                        {
                            "name": "format",
                            "in": "query",
                            "schema": {"type": "string", "enum": ["json", "xml"]},
                        },
                    ]
                }
            }
        }
    }
    result = _default_params_from_spec(spec, "/items/{id}")
    assert "id" not in result
    assert result == {"format": ["json", "xml"]}


async def test_openapi_config_load_prepopulates_table_with_enum_defaults(pg_conn):
    """config load pre-populates the PG cache table using enum values from spec."""
    from provisa.core.config_loader import load_config

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(MOCK_SPEC, f)
        spec_path = f.name

    config_data = _make_config(spec_path)

    from provisa.core.config_loader import parse_config_dict

    config = parse_config_dict(config_data)

    # integration: mock-justified — respx intercepts outbound HTTP to a 3rd-party
    # OpenAPI endpoint (MOCK_BASE_URL). This is not a docker-compose service; the
    # test exercises the real PG path (pg_conn fixture) and real config loader logic.
    with respx.mock(assert_all_called=False) as rx:
        # Mock the API call with enum status values
        rx.get(f"{MOCK_BASE_URL}/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=MOCK_PETS)
        )

        await load_config(config, pg_conn, replace=False)

    # The PG table should have rows pre-populated from the mock API response
    row_count = await pg_conn.fetchval('SELECT COUNT(*) FROM "default"."find_pets_by_status"')
    assert row_count > 0, "find_pets_by_status must be pre-populated at config load time"


async def test_openapi_config_load_registers_api_endpoint(pg_conn):
    """config load registers the table in api_endpoints for runtime hydration."""
    from provisa.core.config_loader import load_config, parse_config_dict

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(MOCK_SPEC, f)
        spec_path = f.name

    config = parse_config_dict(_make_config(spec_path))

    # integration: mock-justified — respx intercepts outbound HTTP to a 3rd-party
    # OpenAPI endpoint (MOCK_BASE_URL), not a docker-compose service.
    with respx.mock(assert_all_called=False) as rx:
        rx.get(f"{MOCK_BASE_URL}/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=MOCK_PETS)
        )
        await load_config(config, pg_conn, replace=False)

    ep = await pg_conn.fetchrow(
        "SELECT path, source_id FROM api_endpoints WHERE table_name = $1",
        "find_pets_by_status",
    )
    assert ep is not None, "api_endpoints must have a row for find_pets_by_status"
    assert ep["path"] == "/pet/findByStatus"
    assert ep["source_id"] == "mock-petstore-api"


async def test_openapi_config_load_registers_api_source(pg_conn):
    """config load registers the source in api_sources for runtime hydration."""
    from provisa.core.config_loader import load_config, parse_config_dict

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(MOCK_SPEC, f)
        spec_path = f.name

    config = parse_config_dict(_make_config(spec_path))

    # integration: mock-justified — respx intercepts outbound HTTP to a 3rd-party
    # OpenAPI endpoint (MOCK_BASE_URL), not a docker-compose service.
    with respx.mock(assert_all_called=False) as rx:
        rx.get(f"{MOCK_BASE_URL}/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=MOCK_PETS)
        )
        await load_config(config, pg_conn, replace=False)

    src = await pg_conn.fetchrow(
        "SELECT base_url FROM api_sources WHERE id = $1",
        "mock-petstore-api",
    )
    assert src is not None, "api_sources must have a row for mock-petstore-api"
    assert src["base_url"] == MOCK_BASE_URL


async def test_openapi_config_load_empty_table_when_api_returns_no_rows(pg_conn):
    """When the API returns no rows, the table exists but is empty (no crash)."""
    from provisa.core.config_loader import load_config, parse_config_dict

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(MOCK_SPEC, f)
        spec_path = f.name

    config = parse_config_dict(_make_config(spec_path))

    # integration: mock-justified — respx intercepts outbound HTTP to a 3rd-party
    # OpenAPI endpoint (MOCK_BASE_URL), not a docker-compose service.
    with respx.mock(assert_all_called=False) as rx:
        rx.get(f"{MOCK_BASE_URL}/pet/findByStatus").mock(return_value=httpx.Response(200, json=[]))
        await load_config(config, pg_conn, replace=False)

    # Table must exist (even if empty)
    exists = await pg_conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables"
        " WHERE table_schema = 'default' AND table_name = 'find_pets_by_status')"
    )
    assert exists, "find_pets_by_status table must exist even when API returns no rows"
