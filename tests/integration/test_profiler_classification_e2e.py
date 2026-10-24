# Copyright (c) 2026 Kenneth Stott
# Canary: d019427b-a25c-4b51-b6fc-3cc0d5eb7856
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a profile run classifies a table's columns from their sampled values, and a steward's
decisions stand on later runs (REQ-2147).

Everything here is the ``test`` instance on the local test platform: an isolated server (DuckDB
engine, its own Postgres control plane) over a SQLite source whose customers table holds valid US
social security numbers in tax_id and world cities in city, joined to a Data Profiler source.

A run suggests that tax_id looks like a US SSN and city like a world city, each with its evidence.
Accepting the SSN suggestion links tax_id to the SSN Property term, marks it pii and names the
ssn_last4 mask. Rejecting the city suggestion is recorded on the next run as rejected, not
suggested again. A kind the org disables is not tested, so a run records nothing of it."""

# Requirements: REQ-2147

from __future__ import annotations

import sqlite3

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ROLE = "org_admin"
_HEADERS = {"X-Provisa-Role": _ROLE}
_CITIES = (
    "Paris",
    "London",
    "Tokyo",
    "Berlin",
    "Madrid",
    "Toronto",
    "Chicago",
    "Mumbai",
    "Cairo",
    "Lima",
)
# Valid SSNs: area 123, group 45, serials 1001..1030 -- none 000, 666 or 9xx, none group 00.
_ROWS = [(i, f"123-45-{1000 + i:04d}", _CITIES[i % len(_CITIES)]) for i in range(1, 31)]
_PROFILER = {
    "cron": "0 2 * * *",
    "low_cardinality_max": 100,
    "drift_window": 7,
    "drift_season": "none",
    "drift_distance": 3.0,
    "drift_slope": 3.0,
    "drift_ks": 0.2,
    "drift_psi": 0.25,
    "correlation_max_columns": 20,
    "joint_max_distinct": 20,
    "category_max_columns": 10,
    "classification_sample": 500,
}


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("profiler_classification")
    db = sqlite3.connect(home / "crm.sqlite")
    db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, tax_id TEXT, city TEXT)")
    db.executemany("INSERT INTO customers VALUES (?, ?, ?)", _ROWS)
    db.commit()
    db.close()
    readers = [_ROLE]
    config = {
        "auth": {"provider": "none"},
        "naming": {"domain_prefix": False, "rules": []},
        "domains": [{"id": "crm", "description": "Customers"}],
        "roles": [],
        "sources": [
            {"id": "crm-db", "type": "sqlite", "path": str(home / "crm.sqlite")},
            {"id": "crm-profiler", "type": "data_profiler", "mapping": _PROFILER},
        ],
        "tables": [
            {
                "source_id": "crm-db",
                "domain_id": "crm",
                "schema": "main",
                "table": "customers",
                "profiler_source_id": "crm-profiler",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": readers,
                    },
                    {"name": "tax_id", "data_type": "varchar", "visible_to": readers},
                    {"name": "city", "data_type": "varchar", "visible_to": readers},
                ],
            }
        ],
        "relationships": [],
    }
    path = home / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(
        "profiler_classification", engine="duckdb", config=str(path), control_plane="own"
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _url(srv, path: str) -> str:
    return f"{srv.base_url}{path}"


def _ok(resp: httpx.Response) -> dict | list:
    assert resp.status_code == 200, resp.text
    return resp.json()


def _graphql(srv, query: str) -> dict:
    body = _ok(
        httpx.post(_url(srv, "/admin/graphql"), json={"query": query}, headers=_HEADERS, timeout=60)
    )
    assert isinstance(body, dict) and "errors" not in body, body
    return body["data"]


@pytest.fixture(scope="module")
def table_id(server) -> int:
    tables = _graphql(server, "{ tables { id tableName } }")["tables"]
    (found,) = [t["id"] for t in tables if t["tableName"] == "customers"]
    return int(found)


def _run(srv, table_id: int) -> list[dict]:
    """Profile the table now; the run's classifications rows, as the org admin reads them."""
    run = _ok(
        httpx.post(
            _url(srv, f"/admin/tables/{table_id}/profile-runs"), headers=_HEADERS, timeout=600
        )
    )
    assert isinstance(run, dict)
    results = _ok(
        httpx.get(
            _url(srv, f"/admin/tables/{table_id}/profile-runs/{run['runId']}"),
            headers=_HEADERS,
            timeout=60,
        )
    )
    assert isinstance(results, dict)
    return results["classifications"]


def _decide(srv, table_id: int, row: dict, status: str) -> dict:
    body = {
        "column": row["column_name"],
        "kind": row["kind"],
        "status": status,
        "evidence": row["evidence"],
        "runId": row["run_id"],
    }
    answer = _ok(
        httpx.post(
            _url(srv, f"/admin/tables/{table_id}/profile-classifications"),
            json=body,
            headers=_HEADERS,
            timeout=60,
        )
    )
    assert isinstance(answer, dict)
    return answer


def _by_column(rows: list[dict]) -> dict[tuple[str, str], dict]:
    return {(r["column_name"], r["kind"]): r for r in rows}


def test_a_run_suggests_each_kind_with_its_evidence_and_the_stewards_decisions_stand(
    server, table_id
):
    first = _by_column(_run(server, table_id))
    assert set(first) == {("tax_id", "us_ssn"), ("city", "city")}, first
    ssn, city = first[("tax_id", "us_ssn")], first[("city", "city")]
    assert ssn["evidence"] == (
        "customers.tax_id looks like a US social security number: 100% of sampled values pass "
        "the SSN rules"
    )
    assert city["evidence"] == (
        "customers.city looks like a world city: 100% of sampled values are known cities"
    )
    assert (ssn["values_tested"], ssn["values_passed"], ssn["status"]) == (30, 30, "suggested")
    assert ssn["sensitive"] is True and city["sensitive"] is False

    accepted = _decide(server, table_id, ssn, "accepted")
    assert accepted["term"] == "us social security number"
    assert accepted["pii"] is True
    assert accepted["maskPreset"] == "ssn_last4"
    term = _ok(
        httpx.get(
            _url(server, "/admin/glossary/ref"),
            params={"table_id": table_id, "column_name": "tax_id"},
            headers=_HEADERS,
            timeout=60,
        )
    )
    assert isinstance(term, dict) and term["name"] == "us social security number", term
    columns = _graphql(server, "{ tables { tableName columns { columnName isPii } } }")["tables"]
    (customers,) = [t for t in columns if t["tableName"] == "customers"]
    pii = {c["columnName"]: c["isPii"] for c in customers["columns"]}
    assert pii == {"id": False, "tax_id": True, "city": False}, pii

    assert _decide(server, table_id, city, "rejected") == {"status": "rejected"}
    second = _by_column(_run(server, table_id))
    assert second[("tax_id", "us_ssn")]["status"] == "accepted"
    # Rejected: recorded as the steward decided, not suggested again.
    assert second[("city", "city")]["status"] == "rejected"


def test_a_disabled_kind_is_not_tested(server, table_id):
    kinds = _ok(
        httpx.get(_url(server, "/admin/classification-kinds"), headers=_HEADERS, timeout=60)
    )
    assert isinstance(kinds, dict)
    enabled = [k["id"] for k in kinds["kinds"] if k["id"] != "city"]
    after = _ok(
        httpx.put(
            _url(server, "/admin/classification-kinds"),
            json={"enabled": enabled},
            headers=_HEADERS,
            timeout=60,
        )
    )
    assert isinstance(after, dict)
    assert {k["id"]: k["enabled"] for k in after["kinds"]}["city"] is False
    rows = _by_column(_run(server, table_id))
    assert ("city", "city") not in rows, rows
    assert ("tax_id", "us_ssn") in rows
