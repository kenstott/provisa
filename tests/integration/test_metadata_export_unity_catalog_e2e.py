# Copyright (c) 2026 Kenneth Stott
# Canary: b60330b9-abb1-4a83-bc33-8ba9a064a1d7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2117 e2e: a Provisa server on Databricks publishes a data product to Unity Catalog.

The server's engine is the project's Databricks SQL warehouse, in a catalog of the run's own. The
data product's two members are tables of the test stack's Postgres, which the Databricks engine
cannot read in place: each is replicated into Delta on its first read. The target is saved with
its Delta Share on and published through the admin API; the product's schema, its views, its
comment and tags, and the share are then read back from Unity Catalog itself, and the catalog and
the share are dropped.

Without the warehouse's credentials the test fails, never skips: a target with no end-to-end
proof is not complete (REQ-2119).
"""

# Requirements: REQ-2117, REQ-2119

from __future__ import annotations

import os
import uuid

import pytest

from provisa.federation.replica_address import replica_schema, replica_table_name
from tests.integration.metadata_export_server import (
    DOMAIN,
    PRODUCT_FIELDS,
    SCHEMA,
    SOURCE,
    publish,
    published_server,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "mdx_unity_catalog_e2e"
_CREDENTIALS = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")


def _credentials() -> dict[str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the Unity Catalog e2e needs {', '.join(missing)}"
    return {name: os.environ[name] for name in _CREDENTIALS}


def _connect(creds: dict[str, str]):
    from databricks import sql as dbsql

    from provisa.federation.databricks_tls import databricks_tls_kwargs

    return dbsql.connect(
        server_hostname=creds["DATABRICKS_SERVER_HOSTNAME"],
        http_path=creds["DATABRICKS_HTTP_PATH"],
        access_token=creds["DATABRICKS_TOKEN"],
        **databricks_tls_kwargs(),
    )


def _rows(conn, sql: str) -> list[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        cur.close()


@pytest.fixture(scope="module")
async def published():
    creds = _credentials()
    run = uuid.uuid4().hex[:8]
    catalog = f"provisa_e2e_{run}"
    product = f"provisa_e2e_{run}"
    share = f"provisa_{product}"
    dsn = (
        f"databricks://token:{creds['DATABRICKS_TOKEN']}@{creds['DATABRICKS_SERVER_HOSTNAME']}"
        f"?http_path={creds['DATABRICKS_HTTP_PATH']}&catalog={catalog}"
    )
    try:
        async with published_server(
            ORG,
            engine="databricks",
            env={"PROVISA_ENGINE_URL": dsn},
            with_view=False,
            product=product,
        ) as client:
            # A member is replicated into Delta on its first read; the product's view reads it.
            for table in ("orders", "customers"):
                read = await client.post(
                    "/data/sql",
                    json={"sql": f'SELECT id FROM "{DOMAIN}"."{table}"', "role": "org_admin"},
                )
                assert read.status_code == 200, read.text
            result = await publish(client, {"provider": "unity_catalog", "delta_share": True})
        # What the publish refused is the finding: read back only what it says it published.
        assert result["ok"], result["errors"]
        conn = _connect(creds)
        try:
            views = {
                name: definition
                for name, definition in _rows(
                    conn,
                    f"SELECT table_name, view_definition FROM `{catalog}`.information_schema.views "
                    f"WHERE table_schema = '{product}'",
                )
            }
            (comment,) = _rows(
                conn,
                f"SELECT comment FROM `{catalog}`.information_schema.schemata "
                f"WHERE schema_name = '{product}'",
            )
            tags = dict(
                _rows(
                    conn,
                    f"SELECT tag_name, tag_value FROM `{catalog}`.information_schema.schema_tags "
                    f"WHERE schema_name = '{product}'",
                )
            )
            # SHOW ALL IN SHARE: name is share-relative (schema.view); shared_object is the full
            # name.
            shared = [row[2] for row in _rows(conn, f"SHOW ALL IN SHARE `{share}`")]
        finally:
            conn.close()
        yield result, views, comment[0], tags, shared, catalog, product
    finally:
        conn = _connect(creds)
        try:
            _rows(conn, f"DROP SHARE IF EXISTS `{share}`")
            _rows(conn, f"DROP CATALOG IF EXISTS `{catalog}` CASCADE")
        finally:
            conn.close()


async def test_the_publish_reports_the_product_its_views_and_its_share(published):
    result = published[0]
    assert result["ok"], result["errors"]
    assert result["published"] == {"data_products": 1, "views": 2, "shares": 1}


async def test_the_schema_holds_a_view_over_each_members_replica(published):
    views, catalog = published[1], published[5]
    assert set(views) == {"orders", "customers"}
    for name, definition in views.items():
        # Nothing copied: each view reads the member's replica.
        replica = replica_table_name(SOURCE, SCHEMA, name)
        assert f"{replica_schema(ORG)}" in definition and replica in definition, definition
        assert catalog in definition, definition


async def test_the_schema_carries_the_products_fields(published):
    comment, tags = published[2], published[3]
    assert PRODUCT_FIELDS["purpose"] in comment
    assert PRODUCT_FIELDS["limitations"] in comment
    assert tags["provisa_product:owner"] == PRODUCT_FIELDS["owner_role"]
    assert tags["provisa_product:version"] == PRODUCT_FIELDS["version"]


async def test_the_share_holds_both_views(published):
    shared, catalog, product = published[4], published[5], published[6]
    assert {f"{catalog}.{product}.orders", f"{catalog}.{product}.customers"} <= set(shared)
