# Copyright (c) 2026 Kenneth Stott
# Canary: 2902f6cc-7d26-41e1-9547-9828abad470b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006, REQ-2013: the named demo ``starthere`` works as shipped.

A copy of the demo's directory is read by a real Provisa server (DuckDB engine, its own control
plane) loading the demo's config.yaml as is. What the org then has -- its sources, its registered
tables and its data products, read as the UI's tour reads them -- meets the prerequisites of the
core tour and of all nine Deep Dives (provisa-ui/src/tour/tourPrerequisites.json), so the Deep
Dives menu offers every one. The card's statement joins the two SQLite files and answers rows.
"""

# Requirements: REQ-2006, REQ-2013

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import httpx
import pytest

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "starthere"
_PREREQUISITES = json.loads(
    (_REPO / "provisa-ui" / "src" / "tour" / "tourPrerequisites.json").read_text()
)
_ROLE = "org_admin"
# The tours the Deep Dives menu offers, and the core tour it starts at.
_TOPICS = (
    "connect",
    "relationships",
    "model",
    "govern",
    "quality",
    "query",
    "testdata",
    "publish",
    "operate",
)
# The card: each pet with its customers' inquiries, the pet store's file joined to the inquiries'.
ANSWER_SQL = (
    "SELECT p.name, p.species, COUNT(i.id) AS inquiries "
    'FROM "pet_store"."pets" p JOIN "pet_store"."inquiries" i ON i.pet_id = p.id '
    "GROUP BY p.name, p.species ORDER BY inquiries DESC, p.name"
)


@pytest.fixture(scope="module")
def home(tmp_path_factory) -> Path:
    """A copy of the demo's directory, its data/ links followed: the server reads its own files."""
    copy = tmp_path_factory.mktemp("starthere_demo") / "starthere"
    shutil.copytree(_DEMO, copy)
    return copy


@pytest.fixture(scope="module")
def server(home, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    data = tmp_path_factory.mktemp("starthere_demo_data")
    srv = IsolatedServer(
        "starthere_demo",
        engine="duckdb",
        config=str(home / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{data / 'store.duckdb'}",
        env={"PROVISA_NAMED_DEMO_DIR": str(home), "PROVISA_DATA_DIR": str(data / "provisa")},
    )
    srv.start(timeout=400)
    try:
        yield srv
    finally:
        srv.stop_process()


def _post(server, path: str, body: dict) -> dict[str, Any]:
    resp = httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=300
    )
    assert resp.status_code == 200, f"{path}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()


def _org_has(server) -> dict[str, Any]:
    """What the org has, as provisa-ui/src/hooks/useOrgHas.ts reads it."""
    body = _post(
        server,
        "/admin/graphql",
        {"query": "{ sources { id } tables { sourceId tableName } dataProducts { id } }"},
    )
    assert not body.get("errors"), body["errors"]
    data = body["data"]
    return {
        "sources": {s["id"] for s in data["sources"]},
        "tables": {f"{t['sourceId']}.{t['tableName']}" for t in data["tables"]},
        "dataProducts": len(data["dataProducts"]),
    }


def test_the_prerequisites_name_only_the_core_tour_and_the_deep_dives():
    assert set(_PREREQUISITES) <= {"core", *_TOPICS}


@pytest.mark.parametrize("scope", ["core", *_TOPICS])
def test_the_org_has_what_each_tour_needs(server, scope):
    needs = _PREREQUISITES.get(scope, {})
    has = _org_has(server)
    assert set(needs.get("sources", [])) <= has["sources"]
    assert set(needs.get("tables", [])) <= has["tables"], sorted(has["tables"])
    assert not needs.get("dataProduct") or has["dataProducts"] > 0


def test_the_card_s_statement_joins_the_two_files(server):
    rows = _post(server, "/data/sql", {"sql": ANSWER_SQL})["data"]["sql"]
    assert rows
    assert sum(r["inquiries"] for r in rows) == 28  # every inquiry in demo/files/inquiries.sqlite
