# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2c9e41-0b6f-4a83-95e1-c4f8a3b72d06
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-2158, amended 2026-11-27): the logical-type promise check runs for every product member
on the org's scheduler, every member cache_ttl, reading the member's live source; its latest result
is kept per dataset and shown among the product's promises as Declared logical types.

The ``test`` instance: an isolated server on the duckdb engine, its control plane its own, reading
a table of the test stack's Postgres. The product's member declares ``id`` an integer and
``placed_on`` a date with a cache_ttl of 3 seconds; the live table holds ``placed_on`` as text.
The member's default offer derives its contract from the declared types, so the scheduled check
fails on ``placed_on`` alone and the product lineage endpoint lists it among the promises.
"""

# Requirements: REQ-2158

from __future__ import annotations

import os
import time

import httpx
import pytest
import sqlalchemy as sa
import yaml

pytestmark = [pytest.mark.integration]

_ORG = "contract_type_check_e2e"
_SCHEMA = "contract_type_check_e2e"
_ADMIN = "org_admin"
_TTL = 3


def _pg_url() -> str:
    user = os.environ.get("PG_USER", "provisa")
    password = os.environ.get("PG_PASSWORD", "provisa")
    host = os.environ.get("PG_HOST", "localhost")
    port = os.environ.get("PG_PORT", "5432")
    database = os.environ.get("PG_DATABASE", "provisa")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def _source_sql(*statements: str) -> None:
    engine = sa.create_engine(_pg_url())
    try:
        with engine.begin() as conn:
            for statement in statements:
                conn.execute(sa.text(statement))
    finally:
        engine.dispose()


def _config() -> dict:
    # "viewer" is a standard role a config may not define (REQ-2155); the reader is a custom role.
    readers = [_ADMIN, "ctc_reader"]
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ADMIN}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "roles": [
            {
                "id": r,
                "description": f"{r} role.",
                "capabilities": ["usage"],
                "domain_access": ["*"],
            }
            for r in readers
            if r != _ADMIN  # org_admin is a standard role, seeded by init_schema (REQ-2155)
        ],
        "domains": [{"id": "sales", "description": "the product's member"}],
        "sources": [
            {
                "id": "ctc-pg",
                "type": "postgresql",
                "host": "${env:PG_HOST}",
                "port": "${env:PG_PORT}",
                "database": "${env:PG_DATABASE}",
                "username": "${env:PG_USER}",
                "password": "${env:PG_PASSWORD}",
            }
        ],
        "tables": [
            {
                "source_id": "ctc-pg",
                "domain_id": "sales",
                "schema": _SCHEMA,
                "table": "orders",
                "cache_ttl": _TTL,
                "columns": [
                    {
                        "name": "id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": readers,
                    },
                    {"name": "placed_on", "data_type": "date", "visible_to": readers},
                ],
            }
        ],
        "data_products": [
            {"id": "order_book", "domain_id": "sales", "name": "Order book", "tables": ["orders"]}
        ],
    }


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    _source_sql(
        f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE",
        f"CREATE SCHEMA {_SCHEMA}",
        # placed_on is text at the source: it no longer fits the date the contract declares.
        f"CREATE TABLE {_SCHEMA}.orders (id bigint PRIMARY KEY, placed_on text)",
        f"INSERT INTO {_SCHEMA}.orders VALUES (1, '2026-11-27')",
    )
    work = tmp_path_factory.mktemp("contract_type_check")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()
        _source_sql(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")


def _builtin(srv) -> dict | None:
    response = httpx.get(
        f"{srv.base_url}/data-products/order_book/lineage",
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 60,
    )
    assert response.status_code == 200, f"{response.text}\n{srv.dump_stderr_debug()[-6000:]}"
    found = [p for p in response.json()["dq"]["promises"] if p["builtin"] is not None]
    assert len(found) <= 1, found
    return found[0] if found else None


def test_the_scheduled_check_reads_the_live_source_and_is_a_promise(server):
    # The job fires every cache_ttl seconds; the first run lands within a few intervals.
    deadline = time.monotonic() + 30 * _TTL
    promise = _builtin(server)
    while promise is None and time.monotonic() < deadline:
        time.sleep(1)
        promise = _builtin(server)
    assert promise is not None, server.dump_stderr_debug()[-6000:]

    assert promise["name"] == "Declared logical types"
    assert promise["checker_table_id"] is None and promise["table_name"] is None
    assert promise["checked_table"]["table_name"] == "orders"
    assert promise["owner_product_ids"] == ["order_book"]
    assert promise["latest_outcome"] == "fail"
    assert promise["latest_run_at"] is not None
    # id: integer declared, bigint live -- the same logical type. placed_on: date declared, text
    # live -- named with both types.
    assert [(f["column"], f["declared"]) for f in promise["failing_columns"]] == [
        ("placed_on", "date")
    ]
    assert promise["failing_columns"][0]["live"] is not None
    assert "date" not in promise["failing_columns"][0]["live"]
    assert promise["failing_checks"] == [
        {"name": "contract_logical_type", "column": "placed_on", "outcome": "fail"}
    ]

    # The next run replaces the result: its time moves, one result per dataset.
    first = promise["latest_run_at"]
    deadline = time.monotonic() + 10 * _TTL
    while promise["latest_run_at"] == first and time.monotonic() < deadline:
        time.sleep(1)
        promise = _builtin(server)
    assert promise["latest_run_at"] != first
