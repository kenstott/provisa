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
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import respx

from provisa.core.db import init_schema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()

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
                # REQ-1426: a data type is design-time metadata the YAML carries — the loader
                # assigns none, and the table repository refuses an untyped column.
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": ["admin"]},
                    {"name": "name", "data_type": "varchar", "visible_to": ["admin"]},
                    {"name": "status", "data_type": "varchar", "visible_to": ["admin"]},
                    {"name": "photoUrls", "data_type": "json", "visible_to": ["admin"]},
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
async def pg_conn(tenant_db, platform_admin_db):
    # platform_admin_db: apply_config binds the org vault (REQ-1580/REQ-1730), read off
    # state.admin_db — this module brings its own rather than inheriting another module's.
    # apply_config runs against the control-plane Database shim (advisory_xact_lock,
    # execute_core), as the app does; init_schema creates the org schema it loads into.
    #
    # REQ-1919: the org schema is this module's own. A config load removes what its file no
    # longer declares, so loading this module's file into the org other modules load a different
    # file into would judge their models as dropped. A deployment has one file; so does this org.
    async with tenant_db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_ORG_SCHEMA} CASCADE")
    await init_schema(tenant_db, _SCHEMA_SQL, org_id=_ORG_ID)
    async with tenant_db.acquire() as conn:
        await conn.execute(f"SET search_path TO {_ORG_SCHEMA}")
        yield conn


_ORG_ID = "openapipets"
_ORG_SCHEMA = f"org_{_ORG_ID}"


@pytest_asyncio.fixture(scope="module", autouse=True)
async def _cleanup_mock_source(pg_conn):
    """Remove all DB state written by apply_config calls in this module: its org schema, and the
    landing table the API source filled."""
    yield
    await pg_conn.execute(f"DROP SCHEMA IF EXISTS {_ORG_SCHEMA} CASCADE")
    await pg_conn.execute('DROP TABLE IF EXISTS "default"."find_pets_by_status"')


async def test_default_params_from_spec_extracts_enum_values():
    """default_params_from_spec returns enum list for status param."""
    from provisa.api_source.openapi_endpoint import default_params_from_spec

    result = default_params_from_spec(MOCK_SPEC, "/pet/findByStatus")
    assert result == {"status": ["available", "pending", "sold"]}


async def test_default_params_from_spec_uses_default_when_no_enum():
    """default_params_from_spec falls back to schema.default when no enum."""
    from provisa.api_source.openapi_endpoint import default_params_from_spec

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
    result = default_params_from_spec(spec, "/items")
    assert result == {"limit": 100}


async def test_default_params_from_spec_ignores_path_params():
    """default_params_from_spec skips path parameters."""
    from provisa.api_source.openapi_endpoint import default_params_from_spec

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
    result = default_params_from_spec(spec, "/items/{id}")
    assert "id" not in result
    assert result == {"format": ["json", "xml"]}


async def test_openapi_config_load_registers_the_enum_defaults_and_fetches_nothing(pg_conn):
    """REQ-1915: a config load registers the endpoint with the default parameters its spec
    names (the enum values of a query parameter) — what a build of its replica calls with —
    and calls nothing: rows fetched from a remote are never written into the control plane."""
    from provisa.core.config_loader import apply_config, parse_config_dict

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(MOCK_SPEC, f)
        spec_path = f.name

    config = parse_config_dict(_make_config(spec_path))

    # integration: mock-justified — respx intercepts outbound HTTP to a 3rd-party
    # OpenAPI endpoint (MOCK_BASE_URL). This is not a docker-compose service; the
    # test exercises the real PG path (pg_conn fixture) and real config loader logic.
    with respx.mock(assert_all_called=False) as rx:
        route = rx.get(f"{MOCK_BASE_URL}/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=MOCK_PETS)
        )

        await apply_config(config, pg_conn)

    assert route.call_count == 0, "a config load must not call the API"
    default_params = await pg_conn.fetchval(
        "SELECT default_params FROM api_endpoints WHERE table_name = $1", "find_pets_by_status"
    )
    if isinstance(default_params, str):
        default_params = json.loads(default_params)
    assert default_params == {"status": ["available", "pending", "sold"]}
    in_control_plane = await pg_conn.fetchval(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'find_pets_by_status'"
    )
    assert in_control_plane == 0, "no table of API rows may be made in the control plane"


async def test_openapi_config_load_registers_api_endpoint(pg_conn):
    """config load registers the table in api_endpoints for runtime hydration."""
    from provisa.core.config_loader import apply_config, parse_config_dict

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
        await apply_config(config, pg_conn)

    ep = await pg_conn.fetchrow(
        "SELECT path, source_id FROM api_endpoints WHERE table_name = $1",
        "find_pets_by_status",
    )
    assert ep is not None, "api_endpoints must have a row for find_pets_by_status"
    assert ep["path"] == "/pet/findByStatus"
    assert ep["source_id"] == "mock-petstore-api"


async def test_openapi_config_load_registers_api_source(pg_conn):
    """config load registers the source in api_sources for runtime hydration."""
    from provisa.core.config_loader import apply_config, parse_config_dict

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
        await apply_config(config, pg_conn)

    src = await pg_conn.fetchrow(
        "SELECT base_url FROM api_sources WHERE id = $1",
        "mock-petstore-api",
    )
    assert src is not None, "api_sources must have a row for mock-petstore-api"
    assert src["base_url"] == MOCK_BASE_URL
