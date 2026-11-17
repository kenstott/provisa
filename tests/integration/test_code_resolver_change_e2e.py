# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a9d12-4b7e-4c05-a8e1-92d0c5b7f4a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2249's @change on a real server: a code resolver set kept incrementally by its @change
function. Its first build calls the function with no cursor and lands every match; a refresh calls
it with the cursor stored after the last run and applies what it returns, upserting by key and
removing a key returned as removed; once rebuild_every has passed, a refresh rebuilds the whole
result with no cursor. A resolver with no @change for the set cannot have the setting on, and the
setting needs a rebuild interval.

The @change function answers by the cursor it is given, so what the bridge holds says how it was
called: every match with score 1.0 (no cursor), or store 2 rescored 0.5 and store 1's match
removed (the cursor of the first build)."""

# Requirements: REQ-2249

from __future__ import annotations

import json
import time

import httpx
import pytest
import yaml

from tests.integration.test_code_sources_e2e import _ROLE, _admin, _archive, _config
from tests.port_lease import lease_ports

pytestmark = [pytest.mark.integration]

_CODE = {
    "geo/geo.py": r'''
from dataclasses import dataclass

from provisa_services import TableRef


@dataclass
class Store:
    id: int
    region: int


@dataclass
class Region:
    id: int


@dataclass
class Match:
    store_id: int
    region_id: int
    score: float


@dataclass
class Changed:
    store_id: int
    region_id: int
    score: float
    removed: bool
    seq: int


def stores() -> list[Store]:
    """@table"""
    return [Store(1, 10), Store(2, 10)]


def regions() -> list[Region]:
    """@table"""
    return [Region(10)]


def match_stores(stores: TableRef[Store], regions: TableRef[Region]) -> list[Match]:
    """The resolver's ordinary function: every match, recomputed whole.

    @query
    """
    return [Match(1, 10, 9.0), Match(2, 10, 9.0)]


def store_regions_changes(
    stores: TableRef[Store], regions: TableRef[Region], since: int | None = None
) -> list[Changed]:
    """The matches changed since ``since``; every match when there is no cursor.

    @change table=store_regions
    @change key=store_id,region_id
    @change removed=removed
    @change cursor=seq
    """
    if since is None:
        return [Changed(1, 10, 1.0, False, 1), Changed(2, 10, 1.0, False, 2)]
    if since < 3:
        return [Changed(2, 10, 0.5, False, 3), Changed(1, 10, 0.0, True, 4)]
    return []
''',
}

_WHOLE = [[1, 10, 1.0], [2, 10, 1.0]]
_BY_CURSOR = [[2, 10, 0.5]]


def _column(name: str, data_type: str) -> str:
    return f'{{name: "{name}", visibleTo: ["{_ROLE}"], dataType: "{data_type}"}}'


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("code_resolver_change")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        "code_resolver_change",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={
            "PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps")),
            # The resolver's tables are given to its code over the minted listener (REQ-2226).
            "PROVISA_MINTED_PGWIRE_PORT": str(lease_ports(1)[0]),
        },
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


@pytest.fixture(scope="module")
def geo(server):
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "geo.zip"},
        content=_archive(_CODE),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    _admin(
        server,
        f'createSource(input: {{id: "geo", type: "python", host: "", port: 0, database: "", '
        f"cacheTtl: 300, mappingJson: {json.dumps(json.dumps(mapping))}}})",
    )
    for table, columns in (
        ("stores", [_column("id", "bigint"), _column("region", "bigint")]),
        ("regions", [_column("id", "bigint")]),
    ):
        _admin(
            server,
            'registerTable(input: {sourceId: "geo", domainId: "fx", schemaName: "default", '
            f'tableName: "{table}", cacheTtl: 300, columns: [{", ".join(columns)}]}})',
        )
    response = httpx.post(
        f"{server.base_url}/admin/actions/functions",
        json={
            "name": "match_stores",
            "sourceId": "geo",
            "functionName": "match_stores",
            "domainId": "fx",
            "visibleTo": [_ROLE],
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 120,
    )
    assert response.status_code == 200, response.text


def _set(set_id: str, more: str) -> str:
    return (
        f'id: "{set_id}", resolver: "match_stores", sourceTable: "stores", sourceColumn: "id", '
        'sourceMatch: "store_id", targetTable: "regions", targetColumn: "id", '
        f'targetMatch: "region_id", refreshInterval: 3600, {more}'
    )


def _try(srv, mutation: str) -> dict:
    """An admin mutation's result, refused or not."""
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message code } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    return result


def _query(srv, query: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return response.json()["data"]


def _matches(srv) -> list[list]:
    """What the bridge holds, read as a statement reads it."""
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={
            "sql": "SELECT store_id, region_id, score FROM fx.store_regions "
            "ORDER BY store_id, region_id"
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    return [[r["store_id"], r["region_id"], r["score"]] for r in response.json()["data"]["sql"]]


@pytest.fixture(scope="module")
def kept(server, geo):
    """The set saved with its matches kept incrementally; saving it built it once."""
    del geo
    saved = _try(
        server,
        f"createCodeResolver(input: {{{_set('store_regions', 'incremental: true, rebuildEvery: 3600')}}})",
    )
    assert saved["success"], saved
    return saved


def test_the_incremental_option_is_available_only_with_a_change_function(server, geo):
    del geo
    data = _query(
        server,
        '{ a: codeResolverIncremental(resolver: "match_stores", id: "store_regions") '
        "{ available code reason } "
        'b: codeResolverIncremental(resolver: "match_stores", id: "plain_regions") '
        "{ available code reason } }",
    )
    assert data["a"] == {"available": True, "code": None, "reason": None}
    assert data["b"]["available"] is False
    assert data["b"]["code"] == "schema.incremental_no_change"
    assert "@change table=plain_regions" in data["b"]["reason"]


def test_a_set_with_no_change_function_cannot_be_kept_incrementally(server, geo):
    del geo
    refused = _try(
        server,
        f"createCodeResolver(input: {{{_set('plain_regions', 'incremental: true, rebuildEvery: 60')}}})",
    )
    assert not refused["success"]
    assert refused["code"] == "schema.incremental_no_change", refused
    assert "REQ-" not in refused["message"]


def test_the_first_build_calls_change_with_no_cursor_and_lands_every_match(server, kept):
    del kept
    assert _matches(server) == _WHOLE
    listed = _query(
        server, "{ codeResolvers { id incremental rebuildEvery incrementalOption { available } } }"
    )
    assert listed["codeResolvers"] == [
        {
            "id": "store_regions",
            "incremental": True,
            "rebuildEvery": 3600,
            "incrementalOption": {"available": True},
        }
    ]


def test_a_set_kept_incrementally_needs_a_rebuild_interval(server, kept):
    """Refused before anything is saved: the set keeps what it holds."""
    del kept
    refused = _try(
        server, f"updateCodeResolver(input: {{{_set('store_regions', 'incremental: true')}}})"
    )
    assert not refused["success"]
    assert refused["code"] == "schema.incremental_needs_rebuild_every", refused
    assert "REQ-" not in refused["message"]


def test_a_refresh_applies_the_changes_since_the_stored_cursor(server, kept):
    """Called with the stored cursor: store 2's match is upserted by its key with its new score,
    and store 1's match, returned as removed, removes its key."""
    del kept
    refreshed = _admin(server, 'refreshMv(mvId: "view-store_regions")')
    assert refreshed["success"], refreshed
    assert _matches(server) == _BY_CURSOR


def test_once_rebuild_every_has_passed_a_refresh_rebuilds_whole(server, kept):
    """Last in the module. Saving the set with a one-second interval builds it again; the first
    whole build is older than that, so the function is called with no cursor and every match it
    returns replaces what the bridge held."""
    del kept
    time.sleep(1.5)
    saved = _try(
        server,
        f"updateCodeResolver(input: {{{_set('store_regions', 'incremental: true, rebuildEvery: 1')}}})",
    )
    assert saved["success"], saved
    assert _matches(server) == _WHOLE
