# Copyright (c) 2026 Kenneth Stott
# Canary: 7c4e1b92-3d5a-4f08-b6e2-a19d0c8f5e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the Relationships page after a discovery run over the default demo (REQ-413).

The ``test`` instance: the smoke check's isolated instance of the default demo. A run over
pet-store and shelter, foreign keys and semantics (a stand-in model on the loopback proposing each
junction's keys both ways round), reads every foreign key constraint of their tables. A junction's
key already joined through its many-to-many relationship (pets-treated-by-vets through pet_visits,
the pets' companions, the playgroups' members) is left out as existing, never listed. A
reviewer applies what starts ticked, and afterwards no suggestion is left waiting; a second run
suggests none of the pairs decided. The Relationships page lists no suggestions: they are decided
only in a run's result. Every registered relationship names the columns it joins.
"""

# Requirements: REQ-413

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from tests.integration import test_discovery_files_e2e as files_e2e
from tests.smoke.test_main_is_startable import Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.local_platform]

_HEADERS = {"X-Provisa-Role": "org_admin"}


#: What the stand-in model proposes: each junction's keys, both ways round, and pairs of the
#: shelter.
_ANSWERS = [
    ("pet_visits", "pet_id", "pets", "id", 0.9),
    ("pets", "id", "pet_visits", "pet_id", 0.85),
    ("pet_visits", "vet_id", "vets", "id", 0.9),
    ("vets", "id", "pet_visits", "vet_id", 0.8),
    ("pet_companions", "pet_id", "pets", "id", 0.9),
    ("pet_companions", "companion_pet_id", "pets", "id", 0.9),
    ("pets", "id", "pet_companions", "companion_pet_id", 0.8),
    ("playgroup_members", "pet_id", "pets", "id", 0.9),
    ("schedules", "employee_id", "employees", "id", 0.9),
    ("assignments", "breed_name", "animal_breeds", "name", 0.9),
]


@pytest.fixture(scope="module")
def endpoint() -> Iterator[Any]:
    served = files_e2e._Endpoint()
    yield served
    served.stop()


@pytest.fixture(scope="module")
def instance(tmp_path_factory, endpoint) -> Iterator[Instance]:
    """The smoke check's instance of the default demo with the stand-in model's endpoint."""
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    mp.setattr(files_e2e, "_ANSWERS", _ANSWERS)
    try:
        yield from files_e2e.instance.__wrapped__(tmp_path_factory, endpoint)
    finally:
        mp.undo()


def _ok(resp: httpx.Response) -> Any:
    assert resp.status_code == 200, resp.text
    return resp.json()


def _gql(inst: Instance, query: str) -> dict[str, Any]:
    body = _ok(
        httpx.post(
            f"{inst.url}/admin/graphql", json={"query": query}, headers=_HEADERS, timeout=300
        )
    )
    assert "errors" not in body, body
    return body["data"]


def _run(inst: Instance, scope: dict) -> dict:
    run = _ok(
        httpx.post(f"{inst.url}/admin/discover/runs", json=scope, headers=_HEADERS, timeout=120)
    )["run"]
    deadline = time.monotonic() + 600
    while run["status"] == "running":
        assert time.monotonic() < deadline, run
        time.sleep(1)
        run = _ok(
            httpx.get(f"{inst.url}/admin/discover/runs/{run['id']}", headers=_HEADERS, timeout=60)
        )["run"]
    assert run["status"] == "done", f"{run}\n{(inst.log_dir / 'backend.log').read_text()[-6000:]}"
    return run


def _tables(inst: Instance) -> dict[int, str]:
    listed = _gql(inst, "{ tables { id tableName } }")["tables"]
    return {int(t["id"]): t["tableName"] for t in listed}


def _relationships(inst: Instance) -> list[dict]:
    return _gql(
        inst,
        "{ relationships { id sourceTableId targetTableId sourceColumn targetColumn "
        "targetFunctionName viaTableId } }",
    )["relationships"]


_SCOPE = {
    "kinds": ["foreign_keys", "semantics"],
    "vendor": files_e2e._VENDOR,
    "model": files_e2e._MODEL,
    "source_domains": ["pet-store", "shelter"],
    # The settings the dialog always sends, at their defaults (REQ-413, amended 2026-11-18).
    "threshold": 0.7,
    "samples_per_column": 5,
}

#: The legs of the demo's junction relationships: the junction's key to the end's key.
_JUNCTION_LEGS = {
    ("pet_visits", "pet_id", "pets", "id"),
    ("pet_visits", "vet_id", "vets", "id"),
    ("pet_companions", "pet_id", "pets", "id"),
    ("pet_companions", "companion_pet_id", "pets", "id"),
    ("playgroup_members", "pet_id", "pets", "id"),
    ("playgroup_members", "playgroup_id", "playgroups", "id"),
}


def _proposals(inst: Instance, run_id: int) -> dict:
    return _ok(
        httpx.get(
            f"{inst.url}/admin/discover/runs/{run_id}/proposals", headers=_HEADERS, timeout=60
        )
    )


def test_after_applying_a_run_no_suggestion_waits_for_a_joined_pair(instance: Instance):
    names = _tables(instance)

    def edge(p: dict) -> tuple:
        return (
            names.get(p["source_table_id"]),
            p["source_column"],
            names.get(p["target_table_id"]),
            p["target_column"],
        )

    def unordered(p: dict) -> tuple:
        e = edge(p)
        return e if e in _JUNCTION_LEGS else (e[2], e[3], e[0], e[1])

    run = _run(instance, _SCOPE)
    listed = _proposals(instance, run["id"])
    proposals, threshold = listed["proposals"], listed["threshold"]

    # Each junction's keys, proposed both ways round, are joined already: the run leaves them out
    # as existing and lists none of them (REQ-413, amended 2026-11-18), so none can be decided.
    assert run["report"]["removed"]["existing"] >= 4, run["report"]
    assert [edge(p) for p in proposals if unordered(p) in _JUNCTION_LEGS] == []

    ticked = [
        p
        for p in proposals
        if p["decidable"]
        and p["reason"] in (None, "pending")
        and p["confidence"] is not None
        and p["confidence"] >= threshold
    ]
    applied = _ok(
        httpx.post(
            f"{instance.url}/admin/discover/runs/{run['id']}/apply",
            json={
                "accept": [
                    {"id": p["id"], "name": "{}-{}-to-{}".format(*edge(p)[:3])} for p in ticked
                ],
                "not_again": [],
            },
            headers=_HEADERS,
            timeout=300,
        )
    )["results"]
    assert all(r["ok"] for r in applied), applied

    waiting = _ok(
        httpx.get(f"{instance.url}/admin/discover/candidates", headers=_HEADERS, timeout=60)
    )
    assert waiting == [], [edge(c) for c in waiting]

    # A second run asks again about none of them: a decided pair stays decided.
    again = _run(instance, _SCOPE)
    reopened = [
        (edge(p), p["reason"])
        for p in _proposals(instance, again["id"])["proposals"]
        if p["reason"] in (None, "pending")
    ]
    assert reopened == []
    assert again["suggestions"] == 0, again
    waiting = _ok(
        httpx.get(f"{instance.url}/admin/discover/candidates", headers=_HEADERS, timeout=60)
    )
    assert waiting == [], [edge(c) for c in waiting]


def test_every_registered_relationship_names_the_columns_it_joins(instance: Instance):
    """A remote GraphQL source's nested object with no key scalar beside it (shelter's
    Schedule.employee) offers no relationship; none is stored with empty columns."""
    unjoined = [
        r["id"]
        for r in _relationships(instance)
        if not r["sourceColumn"] or (not r["targetColumn"] and not r["targetFunctionName"])
    ]
    assert unjoined == []
