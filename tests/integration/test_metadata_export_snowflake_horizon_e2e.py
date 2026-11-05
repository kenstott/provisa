# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2f0c58-9e13-4b6d-8c40-2d5e9f1a3b86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2119 e2e: a Provisa server on Snowflake publishes a data product to Horizon Catalog.

The server's engine is the project's Snowflake account, in a database this test creates for it.
The data product's two members are tables of the test stack's Postgres, which the Snowflake engine
cannot read in place: each is replicated into Snowflake on its first read, and the publish shares
the replica's export view. The target is saved and published through the admin API, then the share
and the organization listing are read back from Snowflake itself.

Without the account's credentials the test fails, never skips: a target with no end-to-end proof
is not complete (REQ-2119).
"""

# Requirements: REQ-2119, REQ-1635

from __future__ import annotations

import os
import uuid
from urllib.parse import quote

import pytest
import yaml

from provisa.federation.replica_address import replica_table_name
from tests.integration.metadata_export_server import (
    DOMAIN,
    PRODUCT_FIELDS,
    SCHEMA,
    SOURCE,
    publish,
    published_server,
    refused,
)

from tests.integration.live_teardown import run_name  # noqa: E402

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "mdx_horizon_e2e"
# The data product is named for the run (REQ-254), so its share and listing are the run's own:
# teardown drops exactly these, never another run's. The adapter names both as quoted
# identifiers, in the product id's own case: provisa_<id>_share.
PRODUCT = run_name("provisa_e2e")
SHARE = f"provisa_{PRODUCT}_share"
LISTING = f"provisa_{PRODUCT}_listing"
_CREDENTIALS = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE")


def _credentials() -> dict[str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the Snowflake Horizon e2e needs {', '.join(missing)}"
    return {name: os.environ[name] for name in _CREDENTIALS}


def _connect(creds: dict[str, str], database: str | None = None):
    import snowflake.connector

    return snowflake.connector.connect(
        account=creds["SNOWFLAKE_ACCOUNT"],
        user=creds["SNOWFLAKE_USER"],
        password=creds["SNOWFLAKE_PASSWORD"],
        warehouse=creds["SNOWFLAKE_WAREHOUSE"],
        database=database,
    )


def _rows(conn, sql: str) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        names = [d[0].lower() for d in cur.description]
        return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
    finally:
        cur.close()


@pytest.fixture(scope="module", autouse=True)
def _warehouse_left_as_found():
    """The module's statements resume the Snowflake warehouse if it is suspended; it is suspended
    again after the module however it ends, unless it was running before (REQ-254). Module-scoped
    autouse: its state is read before, and the suspend runs after, every other fixture here."""
    from tests.integration.snowflake_warehouse import warehouse_in_use

    with warehouse_in_use(_credentials()["SNOWFLAKE_WAREHOUSE"]):
        yield


@pytest.fixture(scope="module")
def engine_database():
    """A database of the run's own for the engine to land in, dropped with what was shared."""
    creds = _credentials()
    database = f"PROVISA_MDX_{uuid.uuid4().hex[:10].upper()}"
    conn = _connect(creds)
    try:
        _rows(conn, f"CREATE DATABASE {database}")
        yield creds, database
    finally:
        from tests.integration.live_teardown import run_every

        # Every drop is attempted and read back, then any that failed is raised (REQ-254).
        run_every(
            [
                (stmt, lambda stmt=stmt: _rows(conn, stmt))
                for stmt in (
                    f'DROP LISTING IF EXISTS "{LISTING}"',
                    f'DROP SHARE IF EXISTS "{SHARE}"',
                    f"DROP DATABASE IF EXISTS {database}",
                )
            ]
            + [
                (
                    f"read back {kind} {name} gone",
                    lambda kind=kind, name=name: _gone(conn, kind, name),
                )
                for kind, name in (
                    ("LISTINGS", LISTING),
                    ("SHARES", SHARE),
                    ("DATABASES", database),
                )
            ]
            + [("close the connection", conn.close)]
        )


def _gone(conn, kind: str, name: str) -> None:
    """Raise if ``SHOW <kind> LIKE '<name>'`` still lists ``name``. A share is listed under its
    account-qualified name (``<account>.<share>``); an unquoted database name in upper case."""
    from tests.integration.live_teardown import none_remain

    listed = [
        r["name"].split(".")[-1].strip('"') for r in _rows(conn, f"SHOW {kind} LIKE '{name}'")
    ]
    none_remain(f"Snowflake {kind.lower()}", [n for n in listed if n.upper() == name.upper()])


@pytest.fixture(scope="module")
async def published(engine_database):
    creds, database = engine_database
    dsn = (
        f"snowflake://{quote(creds['SNOWFLAKE_USER'], safe='')}:"
        f"{quote(creds['SNOWFLAKE_PASSWORD'], safe='')}@{creds['SNOWFLAKE_ACCOUNT']}/"
        f"{database}?warehouse={quote(creds['SNOWFLAKE_WAREHOUSE'], safe='')}"
    )
    # A draft (REQ-1634 STATUS): the organization listing stays unpublished in the shared account.
    async with published_server(
        ORG,
        engine="snowflake",
        env={"PROVISA_ENGINE_URL": dsn},
        with_view=False,
        status="draft",
        product=PRODUCT,
    ) as client:
        # A member is replicated into Snowflake on its first read; its export view is what the
        # listing shares.
        for table in ("orders", "customers"):
            read = await client.post(
                "/data/sql",
                json={"sql": f'SELECT id FROM "{DOMAIN}"."{table}"', "role": "org_admin"},
            )
            assert read.status_code == 200, read.text
        result = await publish(client, {"provider": "snowflake_horizon"})
        yield creds, database, result


async def test_the_publish_reports_the_data_product(published):
    result = published[2]
    assert result["ok"], refused(result)
    assert result["published"]["data_products"] == 1


async def test_the_share_holds_both_members(published):
    creds, database = published[0], published[1]
    conn = _connect(creds, database)
    try:
        assert _rows(conn, f"SHOW SHARES LIKE '{SHARE}'"), f"no share {SHARE}"
        objects = _rows(conn, f'DESCRIBE SHARE "{SHARE}"')
    finally:
        conn.close()
    # Each member is shared through its replica's export view, named as the replica is.
    shared = {
        o["name"].split(".")[-1].strip('"').lower() for o in objects if o["kind"] != "DATABASE"
    }
    members = {
        replica_table_name(SOURCE, SCHEMA, table).lower() for table in ("orders", "customers")
    }
    assert members <= shared, shared


async def test_the_organization_listing_carries_the_products_fields(published):
    creds, database = published[0], published[1]
    conn = _connect(creds, database)
    try:
        assert _rows(conn, f"SHOW LISTINGS LIKE '{LISTING}'"), f"no listing {LISTING}"
        (listing,) = _rows(conn, f'DESCRIBE LISTING "{LISTING}"')
    finally:
        conn.close()
    manifest = yaml.safe_load(listing["manifest_yaml"])
    assert manifest["title"] == PRODUCT_FIELDS["name"]
    # REQ-2118: the purpose, then the product's other ODPS fields.
    assert manifest["description"].startswith(PRODUCT_FIELDS["purpose"])
    assert f"Limitations: {PRODUCT_FIELDS['limitations']}" in manifest["description"]
    assert f"Version: {PRODUCT_FIELDS['version']}" in manifest["description"]
    assert manifest["support_contact"] == PRODUCT_FIELDS["support_contact"]
    assert manifest["organization_profile"] == "INTERNAL"
