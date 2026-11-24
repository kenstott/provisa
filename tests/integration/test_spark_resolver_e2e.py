# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7d5e91-4c0b-4f36-9e18-b6f3a0c7d254
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Spark source's two-table @query as a code resolver, on a one-node Spark (REQ-2225, REQ-2249).

The governed tables ``fx.stores`` and ``fx.regions`` (a Python source) are matched by a Spark
script's @query that joins them on the cluster, each read over Arrow Flight (REQ-2270). The
command is offered as a resolver exactly as a Python one is (the Relationships form's test on the
registered command), and saving a set over it creates the bridge view and its relationship. The
landed matches equal what a live call of the command returns. A set whose resolver's script has no
@change for it cannot be kept incrementally, and says why; a set the script has a @change for is
built through it with no cursor and refreshed by the stored cursor.

Store 3's region is not a registered region, so a match for it would mean the join did not run.
The cluster, the connector's host and the server are the fixtures of
tests/integration/test_spark_connector_e2e.py; nothing here touches local-dev."""

# Requirements: REQ-2225, REQ-2249

from __future__ import annotations

import json
import textwrap

import httpx
import pytest

from tests.integration.test_code_sources_e2e import _admin, _archive
from tests.integration import test_spark_connector_e2e as _spark
from tests.integration.test_spark_connector_e2e import _ADMIN

pytestmark = [pytest.mark.integration]

# The one-node cluster, the connector's host and the server: test_spark_connector_e2e's fixtures.
client_python = _spark.client_python
cluster = _spark.cluster
flight_tls = _spark.flight_tls
spark_host = _spark.spark_host
provisa_server = _spark.provisa_server

_GEO = {
    "geo/geo.py": '''
from dataclasses import dataclass


@dataclass
class Store:
    id: int
    region: int


@dataclass
class Region:
    id: int


def stores() -> list[Store]:
    """@table"""
    return [Store(1, 10), Store(2, 10), Store(3, 20)]


def regions() -> list[Region]:
    """@table"""
    return [Region(10)]
''',
}

_SCRIPT = textwrap.dedent(
    '''
    from dataclasses import dataclass

    from provisa_services import TableRef
    from provisa_spark import dataframe


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


    def _joined(stores, regions) -> list:
        s, r = dataframe(stores), dataframe(regions)
        frame = s.join(r, s["region"] == r["id"]).select(
            s["id"].alias("store_id"), r["id"].alias("region_id")
        )
        return frame.orderBy("store_id").collect()


    def match_on_cluster(stores: TableRef[Store], regions: TableRef[Region]) -> list[Match]:
        """Each store matched to its region, joined on the cluster.

        @query
        """
        return [Match(row.store_id, row.region_id, 1.0) for row in _joined(stores, regions)]


    def kept_regions_changes(
        stores: TableRef[Store], regions: TableRef[Region], since: int | None = None
    ) -> list[Changed]:
        """The matches changed since ``since``; every match when there is no cursor.

        @change table=kept_regions
        @change key=store_id,region_id
        @change removed=removed
        @change cursor=seq
        """
        rows = _joined(stores, regions)
        if since is None:
            return [Changed(r.store_id, r.region_id, 1.0, False, r.store_id) for r in rows]
        first, last = rows[0], rows[-1]
        return [
            Changed(last.store_id, last.region_id, 0.5, False, since + 1),
            Changed(first.store_id, first.region_id, 0.0, True, since + 2),
        ]
    '''
)

#: Every match the cluster's join makes, as the command and a whole build return it.
_WHOLE = [[1, 10, 1.0], [2, 10, 1.0]]
#: After a refresh by cursor: store 2 rescored, store 1's match removed.
_BY_CURSOR = [[2, 10, 0.5]]


def _column(name: str, data_type: str) -> str:
    return f'{{name: "{name}", visibleTo: ["{_ADMIN}"], dataType: "{data_type}"}}'


@pytest.fixture(scope="module")
def resolver(provisa_server, spark_host, cluster):
    """The governed stores and regions, and the Spark command matching them, registered."""
    srv = provisa_server
    upload = httpx.post(
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-uploads/archive",
        params={"filename": "geo.zip"},
        content=_archive(_GEO),
        headers={"X-Provisa-Role": _ADMIN, "Content-Type": "application/octet-stream"},
        timeout=srv.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    geo = {"runtime": "python", "code": {"kind": "upload", "upload": upload.json()["content_hash"]}}
    _admin(
        srv,
        f'createSource(input: {{id: "geo", type: "python", host: "", port: 0, database: "", '
        f"cacheTtl: 300, mappingJson: {json.dumps(json.dumps(geo))}}})",
    )
    for table, columns in (
        ("stores", [_column("id", "bigint"), _column("region", "bigint")]),
        ("regions", [_column("id", "bigint")]),
    ):
        _admin(
            srv,
            'registerTable(input: {sourceId: "geo", domainId: "fx", schemaName: "default", '
            f'tableName: "{table}", cacheTtl: 300, columns: [{", ".join(columns)}]}})',
        )
    joining = {
        "connector_join": {
            "connector": "spark",
            "host": "127.0.0.1",
            "port": spark_host.port,
            "pairing_code": spark_host.pairing,
        },
        "settings": {"script": _SCRIPT, "cluster": cluster},
    }
    _admin(
        srv,
        f'createSource(input: {{id: "spark_match", type: "python", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(joining))}}})',
    )
    response = httpx.post(
        f"{srv.base_url}/admin/actions/functions",
        json={
            "name": "match_on_cluster",
            "sourceId": "spark_match",
            "functionName": "match_on_cluster",
            "domainId": "fx",
            "visibleTo": [_ADMIN],
        },
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    return srv


def _set(set_id: str, more: str) -> str:
    return (
        f'id: "{set_id}", resolver: "match_on_cluster", sourceTable: "stores", '
        'sourceColumn: "id", sourceMatch: "store_id", targetTable: "regions", '
        f'targetColumn: "id", targetMatch: "region_id", refreshInterval: 3600, {more}'
    )


def _graphql(srv, query: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ADMIN},
        # Saving a set builds its bridge: the resolver runs on the cluster.
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return response.json()["data"]


def _try(srv, mutation: str) -> dict:
    """An admin mutation's result, refused or not."""
    (result,) = _graphql(srv, "mutation { " + mutation + " { success message code } }").values()
    return result


def _rows(srv, sql: str) -> list[list]:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 300,
    )
    assert response.status_code == 200, response.text
    return [
        [int(r["store_id"]), int(r["region_id"]), float(r["score"])]
        for r in response.json()["data"]["sql"]
    ]


def _landed(srv, bridge: str) -> list[list]:
    return _rows(
        srv, f"SELECT store_id, region_id, score FROM fx.{bridge} ORDER BY store_id, region_id"
    )


@pytest.fixture(scope="module")
def plain(resolver):
    """A set over the Spark resolver, its matches landed and rebuilt whole."""
    saved = _try(
        resolver, f"createCodeResolver(input: {{{_set('store_regions', 'incremental: false')}}})"
    )
    assert saved["success"], saved
    return resolver


@pytest.fixture(scope="module")
def kept(resolver):
    """A set the script's @change keeps, its matches landed incrementally; saving built it."""
    saved = _try(
        resolver,
        f"createCodeResolver(input: {{{_set('kept_regions', 'incremental: true, rebuildEvery: 3600')}}})",
    )
    assert saved["success"], saved
    return resolver


def test_the_spark_query_is_offered_as_a_resolver_as_a_python_one_is(resolver):
    """What the Relationships form offers as a resolver (isCodeResolver in
    provisa-ui/src/components/relationships/relationship-types.ts): a code @query taking exactly
    two tables and returning columns."""
    response = httpx.get(
        f"{resolver.base_url}/admin/actions",
        headers={"X-Provisa-Role": _ADMIN},
        timeout=resolver.request_timeout,
    )
    assert response.status_code == 200, response.text
    (command,) = [f for f in response.json()["functions"] if f["name"] == "match_on_cluster"]
    assert command["implKind"] == "code_command"
    assert command["kind"] == "query"
    assert [a["argKind"] for a in command["arguments"]] == ["table_ref", "table_ref"]
    assert [c["name"] for c in command["outputColumns"]] == ["store_id", "region_id", "score"]


def test_saving_it_creates_the_bridge_view_and_its_relationship(plain):
    data = _graphql(
        plain,
        "{ codeResolvers { id resolver sourceMatch targetMatch incremental } "
        "relationships { id viaTableName viaSourceColumn viaTargetColumn } }",
    )
    assert {
        "id": "store_regions",
        "resolver": "match_on_cluster",
        "sourceMatch": "store_id",
        "targetMatch": "region_id",
        "incremental": False,
    } in data["codeResolvers"]
    (edge,) = [r for r in data["relationships"] if r["id"] == "store_regions"]
    assert edge == {
        "id": "store_regions",
        "viaTableName": "store_regions",
        "viaSourceColumn": "store_id",
        "viaTargetColumn": "region_id",
    }


def test_the_landed_matches_equal_a_live_call(plain):
    live = _rows(
        plain,
        "SELECT m.store_id, m.region_id, m.score "
        "FROM match_on_cluster('fx.stores', 'fx.regions') AS m ORDER BY m.store_id, m.region_id",
    )
    assert live == _WHOLE
    assert _landed(plain, "store_regions") == live


def test_a_resolver_with_no_change_for_the_set_has_the_incremental_option_disabled(resolver):
    data = _graphql(
        resolver,
        '{ none: codeResolverIncremental(resolver: "match_on_cluster", id: "store_regions") '
        "{ available code reason } "
        'kept: codeResolverIncremental(resolver: "match_on_cluster", id: "kept_regions") '
        "{ available code reason } }",
    )
    assert data["none"]["available"] is False
    assert data["none"]["code"] == "schema.incremental_no_change"
    assert "@change table=store_regions" in data["none"]["reason"]
    assert data["kept"] == {"available": True, "code": None, "reason": None}


def test_a_set_with_no_change_cannot_be_saved_incremental(resolver):
    refused = _try(
        resolver,
        f"createCodeResolver(input: {{{_set('other_regions', 'incremental: true, rebuildEvery: 60')}}})",
    )
    assert not refused["success"]
    assert refused["code"] == "schema.incremental_no_change", refused


def test_a_kept_set_is_built_by_change_with_no_cursor_and_refreshed_by_it(kept):
    """The first build calls the script's @change with no cursor and lands every match; a refresh
    calls it with the stored cursor, upserting store 2's rescored match and removing store 1's."""
    assert _landed(kept, "kept_regions") == _WHOLE
    refreshed = _admin(kept, 'refreshMv(mvId: "view-kept_regions")')
    assert refreshed["success"], refreshed
    assert _landed(kept, "kept_regions") == _BY_CURSOR
