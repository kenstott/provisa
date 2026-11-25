# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e9c14-5d63-4a8f-91e0-c3f6a84d27b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a dev-mode code source read by ClickHouse through its pgwire sidecar (REQ-2267
part 3).

The ``test`` instance: an isolated server on the ClickHouse engine, embedded (chdb, in the
server's process), so the engine reaches the sidecar on loopback. A Python code source is saved in
dev mode (its code in the server's worker pool) and its table declares a landing TTL of 0, so a
read joining it to a Postgres table is the engine's: the schema build names the source to
ClickHouse's code-host connector, whose PostgreSQL database engine mounts the sidecar's ``public``
schema. The join is answered with the function called on the read's parameter, and the plan reads
the code table from the source's own database, not from a landed copy. The source edited, its
sidecar stops, ClickHouse's database of it is dropped, and the next read mounts it again and
answers from the new code."""

# Requirements: REQ-2267

from __future__ import annotations

import json

import httpx
import pytest
import yaml

from tests.integration.test_code_source_attach_trino import _CODE, _JOIN, _ROLE, _config
from tests.integration.test_code_sources_e2e import (
    _admin,
    _archive,
    _column,
    _parameter,
    _rows,
    _sql,
)

pytestmark = [pytest.mark.integration]

_ORG = "codesidecar_clickhouse"
_SOURCE = "regions-src"


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    import asyncio

    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    work = tmp_path_factory.mktemp("codesidecar_clickhouse")
    path = work / "config.yaml"
    _config(path)
    config = yaml.safe_load(path.read_text())
    config["federation_engine"] = "clickhouse"
    path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(_ORG, engine="clickhouse", config=str(path))
    srv.start(timeout=300)
    try:
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def _upload(srv, files: dict[str, str]) -> str:
    upload = httpx.post(
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-uploads/archive",
        params={"filename": "regions.zip"},
        content=_archive(files),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=srv.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    return upload.json()["content_hash"]


def _mapping(upload: str) -> str:
    return json.dumps(
        json.dumps({"runtime": "python", "code": {"kind": "upload", "upload": upload}})
    )


@pytest.fixture(scope="module")
def dev_source(server):
    _admin(
        server,
        f'createSource(input: {{id: "{_SOURCE}", type: "python", host: "", port: 0, '
        f'database: "", mappingJson: {_mapping(_upload(server, _CODE))}}})',
    )
    columns = [
        _column("region", "text"),
        _column("label", "text"),
        _column("asked", "text"),
        _parameter("tag", "text", required=False),
    ]
    _admin(
        server,
        f'registerTable(input: {{sourceId: "{_SOURCE}", domainId: "fx", schemaName: "default", '
        f'tableName: "regions", cacheTtl: 0, columns: [{", ".join(columns)}]}})',
    )
    _admin(
        server,
        'createRelationship(input: {id: "orders-regions", sourceTableId: "orders", '
        'targetTableId: "regions", sourceColumn: "region", targetColumn: "region", '
        'cardinality: "many-to-one", graphqlAlias: "regionDetail"})',
    )
    return _SOURCE


def test_clickhouse_joins_a_dev_sources_function_read_through_its_sidecar(server, dev_source):
    rows = _rows(_sql(server, _JOIN))
    assert rows, "no eu-west order was joined to its region"
    assert {(r["label"], r["asked"]) for r in rows} == {("EU West", "joined")}


# REQ-2267: every front door must return the same rows.  The compiled path (_engine_form) was
# missing the push_nf_into_subqueries call that the raw-SQL/pgwire path had, so GraphQL and Cypher
# both returned zero rows from a ClickHouse code source.


_GQL_JOIN = """
{
  orders(where: {region: {eq: "eu-west"}}) {
    id
    regionDetail {
      label
      asked
    }
  }
}
"""

_CYPHER_JOIN = (
    "MATCH (o:orders)-[:regionDetail]->(r:regions) "
    "WHERE o.region = 'eu-west' "
    "RETURN o.id AS id, r.label AS label, r.asked AS asked"
)


def test_graphql_join_through_clickhouse_code_source_returns_same_rows(server, dev_source):
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": _GQL_JOIN},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 120,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    orders = body["data"]["orders"]
    assert orders, "GraphQL: no eu-west orders joined to their region"
    region_rows = [r for o in orders for r in (o.get("regionDetail") or [])]
    assert region_rows, "GraphQL: regionDetail is empty"
    assert {(r["label"], r["asked"]) for r in region_rows} == {("EU West", "joined")}


def test_cypher_join_through_clickhouse_code_source_returns_same_rows(server, dev_source):
    resp = httpx.post(
        f"{server.base_url}/data/cypher",
        json={"query": _CYPHER_JOIN},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 120,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    rows = body["rows"]
    assert rows, "Cypher: no eu-west orders joined to their region"
    assert {(r["label"], r["asked"]) for r in rows} == {("EU West", "joined")}


def test_the_join_reads_the_code_table_from_the_sources_database_not_a_landed_copy(
    server, dev_source
):
    plan = httpx.post(
        f"{server.base_url}/data/sql/explain",
        json={"sql": _JOIN},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 120,
    )
    assert plan.status_code == 200, plan.text
    text = json.dumps(plan.json())
    assert "regions_src" in text.replace("-", "_"), text[:2000]


def test_an_edited_source_is_read_from_its_new_code_through_clickhouse(server, dev_source):
    changed = {name: text.replace('"EU West"', '"Europe West"') for name, text in _CODE.items()}
    _admin(
        server,
        f'updateSource(input: {{id: "{_SOURCE}", type: "python", host: "", port: 0, '
        f'database: "", mappingJson: {_mapping(_upload(server, changed))}}})',
    )
    rows = _rows(_sql(server, _JOIN))
    assert rows and {r["label"] for r in rows} == {"Europe West"}
