# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2c9e41-8f3b-4d07-b5e1-c4d7a0f2b968
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a many-to-many pair found by discovery accepted as a shared dimension (REQ-413).

The ``test`` instance: the smoke check's isolated instance of the default demo, with the files
demo's drop folder registered. The budget, the permits and the complaints share neighborhood
values, a key of none of them. A run proposes the neighborhood pairs (a stand-in model on the
loopback), measures them many-to-many, and a reviewer accepts the permits' pair: a view holding
each neighborhood once, named for the concept, and a many-to-one relationship from each side to
it. Accepting a complaints pair later reuses that dimension. The permits and the budget then join
through it in SQL and GraphQL.
"""

# Requirements: REQ-413

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from tests.integration import test_discovery_files_e2e as files_e2e
from tests.smoke.test_main_is_startable import Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.local_platform]

_HEADERS = {"X-Provisa-Role": "org_admin"}

#: What the stand-in model proposes: the neighborhood pairs, named for their concept.
_ANSWERS = [
    (
        "building_permits",
        "neighborhood",
        "budget_2026__budget",
        "neighborhood",
        0.8,
        "neighborhood",
    ),
    (
        "complaints_2026_08",
        "neighborhood",
        "budget_2026__budget",
        "neighborhood",
        0.8,
        "complaint-neighborhood",
    ),
]

_PERMITS = ("building_permits", "neighborhood", "budget_2026__budget", "neighborhood")
_COMPLAINTS = ("complaints_2026_08", "neighborhood", "budget_2026__budget", "neighborhood")


@pytest.fixture(scope="module")
def endpoint() -> Iterator[Any]:
    served = files_e2e._Endpoint()
    yield served
    served.stop()


@pytest.fixture(scope="module")
def instance(tmp_path_factory, endpoint) -> Iterator[Instance]:
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    mp.setattr(files_e2e, "_ANSWERS", _ANSWERS)
    try:
        yield from files_e2e.instance.__wrapped__(tmp_path_factory, endpoint)
    finally:
        mp.undo()


@pytest.fixture(scope="module")
def files(instance: Instance, tmp_path_factory) -> dict[str, int]:
    return files_e2e.files.__wrapped__(instance, tmp_path_factory)


def _ok(resp: httpx.Response) -> Any:
    assert resp.status_code == 200, resp.text
    return resp.json()


def _pair(inst: Instance, ids: dict[str, int], edge: tuple) -> tuple[int, dict]:
    run = files_e2e._run(inst, files_e2e._scope(source_domains=[files_e2e._DOMAIN]))
    listed = files_e2e._proposals(inst, run["id"])
    by_edge = {files_e2e._edge(ids, r): r for r in listed}
    return run["id"], by_edge[edge]


def _accept(inst: Instance, run_id: int, row: dict) -> list[dict]:
    return _ok(
        httpx.post(
            f"{inst.url}/admin/discover/runs/{run_id}/apply",
            json={"accept": [{"id": row["id"], "name": row["dimension"]["name"]}]},
            headers=_HEADERS,
            timeout=300,
        )
    )["results"]


def _sql(inst: Instance, sql: str) -> list[dict[str, Any]]:
    body = _ok(httpx.post(f"{inst.url}/data/sql", json={"sql": sql}, headers=_HEADERS, timeout=300))
    return body["data"]["sql"]


def _graphql(inst: Instance, query: str) -> dict[str, Any]:
    body = _ok(
        httpx.post(f"{inst.url}/data/graphql", json={"query": query}, headers=_HEADERS, timeout=300)
    )
    assert "errors" not in body, body
    return body["data"]


def test_a_many_to_many_pair_is_accepted_as_a_shared_dimension_both_sides_join(
    instance: Instance, files: dict[str, int]
):
    run_id, permits = _pair(instance, files, _PERMITS)
    assert permits["cardinality"] == "many-to-many" and permits["acceptable"] is True
    assert permits["dimension"] == {"name": "neighborhood", "reused": False}
    assert _accept(instance, run_id, permits) == [
        {"id": permits["id"], "action": "accept", "ok": True, "relationship_id": "neighborhood"}
    ]

    tables = files_e2e._gql(instance, "{ tables { id tableName viewSql } }")["tables"]
    (dimension,) = [t for t in tables if t["tableName"] == "neighborhood"]
    assert dimension["viewSql"] and " UNION " in dimension["viewSql"]
    dim_id = int(dimension["id"])
    edges = files_e2e._gql(
        instance, "{ relationships { id sourceTableId sourceColumn targetTableId cardinality } }"
    )["relationships"]
    joined = {
        (e["sourceTableId"], e["sourceColumn"], e["cardinality"])
        for e in edges
        if e["targetTableId"] == dim_id
    }
    assert joined == {
        (files["building_permits"], "neighborhood", "many-to-one"),
        (files["budget_2026__budget"], "neighborhood", "many-to-one"),
    }

    # DIAG-J1434b
    for _sql in (
        'SELECT t."neighborhood" AS "c0" FROM "city_files"."budget_2026__budget" t',
        "SELECT * FROM city_files.neighborhood",
    ):
        _r = httpx.post(
            f"{instance.url}/data/sql/explain", json={"sql": _sql}, headers=_HEADERS, timeout=300
        )
        print("DIAG explain", _sql, _r.status_code, _r.text[:4000])
        _r = httpx.post(
            f"{instance.url}/data/sql", json={"sql": _sql}, headers=_HEADERS, timeout=300
        )
        print("DIAG run", _sql, _r.status_code, _r.text[:2000])
    # A second pair on the same values reuses the dimension: the complaints join it too.
    run_id, complaints = _pair(instance, files, _COMPLAINTS)
    assert complaints["dimension"] == {"name": "neighborhood", "reused": True}
    assert _accept(instance, run_id, complaints)[0]["ok"] is True
    tables = files_e2e._gql(instance, "{ tables { tableName } }")["tables"]
    assert [t["tableName"] for t in tables].count("neighborhood") == 1
    edges = files_e2e._gql(instance, "{ relationships { sourceTableId targetTableId } }")[
        "relationships"
    ]
    assert {e["sourceTableId"] for e in edges if e["targetTableId"] == dim_id} == {
        files["building_permits"],
        files["budget_2026__budget"],
        files["complaints_2026_08"],
    }

    # SQL: the dimension holds each neighborhood once, and the permits and the budget join
    # through it.
    held = _sql(instance, "SELECT neighborhood FROM city_files.neighborhood")
    values = [r["neighborhood"] for r in held]
    assert len(values) == len(set(values)) == 6
    through = _sql(
        instance,
        "SELECT d.neighborhood, COUNT(DISTINCT p.permit_id) AS permits, "
        "COUNT(DISTINCT b.neighborhood) AS budgeted "
        "FROM city_files.neighborhood d "
        "JOIN city_files.building_permits p ON p.neighborhood = d.neighborhood "
        "JOIN city_files.budget_2026__budget b ON b.neighborhood = d.neighborhood "
        "GROUP BY d.neighborhood",
    )
    assert sorted(r["neighborhood"] for r in through) == sorted(values)
    assert all(r["permits"] > 0 and r["budgeted"] == 1 for r in through)

    # GraphQL: each side reaches the dimension through its many-to-one field.
    roots = _graphql(instance, "{ __schema { queryType { fields { name } } } }")
    names = [f["name"] for f in roots["__schema"]["queryType"]["fields"]]
    for table in ("building_permits", "budget_2026__budget"):
        (root,) = [n for n in names if n.endswith(f"__{table}")]
        rows = _graphql(
            instance,
            f"{{ {root}(limit: 5) {{ neighborhood neighborhood_dimension {{ neighborhood }} }} }}",
        )[root]
        assert rows, table
        for row in rows:
            assert row["neighborhood_dimension"] == {"neighborhood": row["neighborhood"]}, row
