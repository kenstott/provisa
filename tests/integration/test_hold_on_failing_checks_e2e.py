# Copyright (c) 2026 Kenneth Stott
# Canary: 1d6f3b82-7e4a-4c19-b0d5-3a9e8f2c6b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-2133): a materialized view holds its refresh on its inputs' failing checks, and a code
source asks dq about a dataset as the role of the request it serves.

The ``test`` instance: an isolated server on the duckdb engine, its control plane its own, reading a
table of the test stack's Postgres. A view copies that table, with Hold on failing checks on. A Great
Expectations checker table (in a domain of its own) scans the table. Its scans are recorded as a
scan records them -- its rows appended to its result relation and its latest run written beside
them, in one transaction (provisa.dq.scan) -- so no checker library has to run here.

- With no scan yet, the view's refresh lands.
- With the checker's latest run failed, a refresh is aborted naming the checker and the input it
  scans, and the view keeps what it had: its row count and refresh time do not move, though the
  table gained a row.
- With the checker passing again, the refresh lands the new row.
- A Python code source's table calls ``ps.dq.dataset`` and returns what it answers: read as
  org_admin it names the checker and its latest outcome; read as a role that cannot reach the
  checker's domain, it names none.
"""

# Requirements: REQ-2133, REQ-1443, REQ-2126, REQ-1165

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import sqlalchemy as sa
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_code_sources_e2e import _archive

pytestmark = [pytest.mark.integration]

_ORG = "hold_checks_e2e"
_SCHEMA = "hold_checks_e2e"
_ADMIN = "org_admin"
_OUTSIDER = "outsider"
_CHECKER = "orders_quality"

_CODE = {
    "quality/quality.py": '''
from dataclasses import dataclass

import provisa_services as ps


@dataclass
class Check:
    checker: str
    outcome: str


def quality(dataset: str) -> list[Check]:
    """The checker tables on a dataset, as dq answers the role of the request.

    @table
    """
    found = ps.dq.dataset(dataset)
    return [
        Check(c["checker_table"], (c["latest_run"] or {}).get("outcome") or "never run")
        for c in found["checker_tables"]
    ]
''',
}


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
    both = [_ADMIN, _OUTSIDER]
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": r} for r in both],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [
            {"id": "hold", "description": "the data a view is built from"},
            {"id": "qa", "description": "the checks on it, which the outsider cannot reach"},
            {"id": "fx", "description": "the code source asking dq"},
        ],
        "roles": [
            {
                "id": _OUTSIDER,
                "description": "Reads the data and the catalog, not the checks.",
                "capabilities": ["query_development"],
                "domain_access": ["hold", "fx", "meta"],
            }
        ],
        "sources": [
            {
                "id": "hold-pg",
                "type": "postgresql",
                "host": "${env:PG_HOST}",
                "port": "${env:PG_PORT}",
                "database": "${env:PG_DATABASE}",
                "username": "${env:PG_USER}",
                "password": "${env:PG_PASSWORD}",
            },
            {
                "id": "hold-gx",
                "type": "great_expectations",
                "mapping": {
                    "database": "provisa",
                    "host": "127.0.0.1",
                    "password": "provisa",
                    "port": 5439,
                    "user": _ADMIN,
                },
            },
        ],
        "tables": [
            {
                "source_id": "hold-pg",
                "domain_id": "hold",
                "schema": _SCHEMA,
                "table": "orders",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": both,
                    },
                    {"name": "customer", "data_type": "varchar", "visible_to": both},
                ],
            },
            {
                "source_id": "hold-gx",
                "domain_id": "qa",
                "table": _CHECKER,
                "cache_ttl": 3600,
                "columns": [{"name": "scan_id", "data_type": "varchar", "visible_to": [_ADMIN]}],
                "dq_contract": json.dumps(
                    {
                        "meta": {"dataset": "provisa/hold/orders"},
                        "expectations": [
                            {
                                "type": "expect_column_values_to_not_be_null",
                                "kwargs": {"column": "customer"},
                            }
                        ],
                    }
                ),
            },
        ],
        "views": [
            {
                "id": "orders_copy",
                "domain_id": "hold",
                "sql": 'SELECT o.id, o.customer FROM "hold"."orders" o',
                "materialize": True,
                "refresh_interval": 86400,
                "hold_on_failing_checks": True,
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": [_ADMIN]},
                    {"name": "customer", "data_type": "varchar", "visible_to": [_ADMIN]},
                ],
            }
        ],
    }


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    _source_sql(
        f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE",
        f"CREATE SCHEMA {_SCHEMA}",
        f"CREATE TABLE {_SCHEMA}.orders (id int PRIMARY KEY, customer text)",
        f"INSERT INTO {_SCHEMA}.orders VALUES (1, 'ann'), (2, 'bo')",
    )
    work = tmp_path_factory.mktemp("hold_checks")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={"PROVISA_RUNTIME_DEPS_CACHE": str(work / "runtime-deps")},
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()
        _source_sql(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")


def _graphql(srv, query: str, role: str = _ADMIN) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "errors" not in body, response.text
    return body["data"]


def _sql(srv, sql: str, role: str = _ADMIN) -> list[dict]:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    return response.json()["data"]["sql"]


def _checker(srv) -> dict:
    (row,) = _sql(
        srv,
        "SELECT id, table_name, checked_table_id FROM meta.registered_tables "
        "WHERE checked_table_id IS NOT NULL",
    )
    return row


def _record_scan(srv, outcome: str) -> None:
    """One scan of the checker table, recorded as a scan records it: its rows appended to its result
    relation and its latest run written, in one transaction (provisa.dq.scan.scan_table)."""
    from provisa.core.schema_org import checker_latest_runs
    from provisa.dq.results import RESULT_FIELDS, results_sa_table
    from provisa.dq.scan import scan_outcome

    checker = _checker(srv)
    schema = f"org_{srv.org_id}"
    scan_id, scan_time = str(uuid.uuid4()), datetime.now(UTC)
    row: dict[str, Any] = {field: None for field in RESULT_FIELDS}
    row.update(
        scan_id=scan_id,
        scan_time=scan_time,
        checker="great_expectations",
        dataset="provisa/hold/orders",
        target_table="hold.orders",
        column_name="customer",
        check_name="expect_column_values_to_not_be_null",
        check_type="expect_column_values_to_not_be_null",
        outcome=outcome,
        rows_tested=3,
        failed_rows=0 if outcome == "pass" else 1,
    )
    relation = results_sa_table(checker["table_name"]).to_metadata(sa.MetaData(), schema=schema)
    latest = checker_latest_runs.to_metadata(sa.MetaData(), schema=schema)
    engine = srv.plane.engine()
    try:
        with engine.begin() as conn:
            conn.execute(relation.insert(), [row])
            conn.execute(sa.delete(latest).where(latest.c.table_id == checker["id"]))
            conn.execute(
                latest.insert(),
                [
                    {
                        "table_id": checker["id"],
                        "scan_id": scan_id,
                        "scan_time": scan_time,
                        "outcome": scan_outcome([row]),
                    }
                ],
            )
    finally:
        engine.dispose()


def _view(srv) -> dict:
    (view,) = [
        mv
        for mv in _graphql(srv, "{ mvList { id rowCount lastRefreshAt lastError } }")["mvList"]
        if mv["id"].endswith("orders_copy")
    ]
    return view


def _refresh(srv) -> dict:
    view_id = _view(srv)["id"]
    return _graphql(srv, f'mutation {{ refreshMv(mvId: "{view_id}") {{ success message }} }}')[
        "refreshMv"
    ]


def test_a_view_refreshes_holds_on_a_failing_check_and_refreshes_once_it_passes(server):
    # No scan yet: the checker has never run, which is not a failure.
    landed = _refresh(server)
    assert landed["success"], landed
    before = _view(server)
    assert before["rowCount"] == 2

    _source_sql(f"INSERT INTO {_SCHEMA}.orders VALUES (3, NULL)")
    _record_scan(server, "fail")
    held = _refresh(server)
    assert not held["success"], held
    assert "a check on its inputs failed" in held["message"], held
    assert "failed on hold.orders" in held["message"], held
    assert f".{_CHECKER} failed on hold.orders" in held["message"], held
    after = _view(server)
    assert after["rowCount"] == before["rowCount"]  # the view keeps what it had
    assert after["lastRefreshAt"] == before["lastRefreshAt"]

    time.sleep(1)  # the next scan is later than the failing one
    _record_scan(server, "pass")
    again = _refresh(server)
    assert again["success"], again
    now = _view(server)
    assert now["rowCount"] == 3 and now["lastError"] is None


@pytest.fixture(scope="module")
def quality(server):
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "quality.zip"},
        content=_archive(_CODE),
        headers={"X-Provisa-Role": _ADMIN, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    created = _graphql(
        server,
        'mutation { createSource(input: {id: "quality", type: "python", host: "", port: 0, '
        f'database: "", cacheTtl: 0, mappingJson: {json.dumps(json.dumps(mapping))}}}) '
        "{ success message } }",
    )["createSource"]
    assert created["success"], created

    def column(name: str) -> str:
        return f'{{name: "{name}", visibleTo: ["{_ADMIN}", "{_OUTSIDER}"], dataType: "text"}}'

    parameter = (
        '{name: "_nf_dataset", visibleTo: [], dataType: "text", '
        'nativeFilterType: "query_arg", nativeFilterRequired: true}'
    )
    registered = _graphql(
        server,
        'mutation { registerTable(input: {sourceId: "quality", domainId: "fx", '
        'schemaName: "default", tableName: "quality", cacheTtl: 0, '
        f"columns: [{column('checker')}, {column('outcome')}, {parameter}]}}) "
        "{ success message } }",
    )["registerTable"]
    assert registered["success"], registered
    released = _graphql(
        server,
        "mutation { "
        + release_field(registered_id(registered["message"]))
        + " { success message } }",
    )
    assert all(r["success"] for r in released.values()), released
    return "quality"


def test_a_code_source_asks_dq_as_the_role_of_the_request_it_serves(server, quality):
    _record_scan(server, "fail")
    query = "SELECT checker, outcome FROM quality WHERE _nf_dataset = 'hold.orders'"
    (seen,) = _sql(server, query, _ADMIN)
    assert seen["outcome"] == "fail" and seen["checker"].endswith(f".{_CHECKER}")
    assert _sql(server, query, _OUTSIDER) == []  # the checker's domain is beyond the outsider
